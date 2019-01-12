/*
 * arrow_read.c - routines to parse apache arrow file
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"
#include "arrow_defs.h"

/* table/vtable of FlatBuffer */
typedef struct
{
	uint16		vlen;	/* vtable length */
	uint16		tlen;	/* table length */
	uint16		offset[FLEXIBLE_ARRAY_MEMBER];
} FBVtable;

typedef struct
{
	int32	   *table;
	FBVtable   *vtable;
} FBTable;

typedef struct
{
	int32		metaLength;
	int32		headOffset;
} FBMetaData;

/* static variables/functions */
static const char  *file_map_head;
static const char  *file_map_tail;
static void			__readArrowType(ArrowType *type,
									int type_tag, const char *type_pos);


static inline FBTable
fetchFBTable(void *p_table)
{
	FBTable		t;

	t.table  = (int32 *)p_table;
	t.vtable = (FBVtable *)((char *)p_table - *t.table);

	return t;
}

static inline void *
__fetchPointer(FBTable *t, int index)
{
	FBVtable   *vtable = t->vtable;

	if (offsetof(FBVtable, offset[index]) < vtable->vlen)
	{
		int		offset = vtable->offset[index];
		if (offset)
		{
			void   *addr = (char *)t->table + offset;

			assert((char *)addr < (char *)t->table + vtable->tlen);
			return addr;
		}
	}
	return NULL;
}

static inline bool
fetchBool(FBTable *t, int index)
{
	bool	   *ptr = __fetchPointer(t, index);
	return (ptr ? *ptr : false);
}

static inline int8
fetchChar(FBTable *t, int index)
{
	int8	   *ptr = __fetchPointer(t, index);
	return (ptr ? *ptr : 0);
}

static inline int16
fetchShort(FBTable *t, int index)
{
	int16	  *ptr = __fetchPointer(t, index);
	return (ptr ? *ptr : 0);
}

static inline int32
fetchInt(FBTable *t, int index)
{
	int32	  *ptr = __fetchPointer(t, index);
	return (ptr ? *ptr : 0);
}

static inline int64
fetchLong(FBTable *t, int index)
{
	int64	  *ptr = __fetchPointer(t, index);
	return (ptr ? *ptr : 0);
}

static inline void *
fetchOffset(FBTable *t, int index)
{
	int32  *ptr = __fetchPointer(t, index);
	return (ptr ? (char *)ptr + *ptr : NULL);
}

static inline const char *
fetchString(FBTable *t, int index, int *p_strlen)
{
	int32  *ptr = fetchOffset(t, index);

	if (!ptr)
		*p_strlen = 0;
	else
		*p_strlen = *ptr++;
	return (const char *)ptr;
}

static inline int32 *
fetchVector(FBTable *t, int index, int *p_nitems)
{
	int32  *vector = fetchOffset(t, index);

	if (!vector)
		*p_nitems = 0;
	else
		*p_nitems = *vector++;
	return vector;
}

static void
readArrowKeyValue(ArrowKeyValue *kv, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *)pos);

	memset(kv, 0, sizeof(ArrowKeyValue));
	kv->node.tag     = ArrowNodeTag__KeyValue;
	kv->key          = fetchString(&t, 0, &kv->_key_len);
	kv->value        = fetchString(&t, 1, &kv->_value_len);
}

static void
readArrowTypeInt(ArrowTypeInt *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->bitWidth  = fetchInt(&t, 0);
	node->is_signed = fetchBool(&t, 1);
}

static void
readArrowTypeFloatingPoint(ArrowTypeFloatingPoint *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->precision = fetchShort(&t, 0);
}

static void
readArrowTypeDecimal(ArrowTypeDecimal *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->precision = fetchInt(&t, 0);
	node->scale     = fetchInt(&t, 1);
}

static void
readArrowTypeDate(ArrowTypeDate *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->unit = fetchShort(&t, 0);
}

static void
readArrowTypeTime(ArrowTypeTime *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->unit = fetchShort(&t, 0);
	node->bitWidth = fetchInt(&t, 1);
}

static void
readArrowTypeTimestamp(ArrowTypeTimestamp *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->unit = fetchInt(&t, 0);
	node->timezone = fetchString(&t, 1, &node->_timezone_len);
}

static void
readArrowTypeInterval(ArrowTypeInterval *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->unit = fetchShort(&t, 0);
}

static void
readArrowTypeUnion(ArrowTypeUnion *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);
	int32	   *vector;
	int32		nitems;

	node->mode = fetchShort(&t, 0);
	vector = fetchVector(&t, 1, &nitems);
	if (nitems == 0)
		node->typeIds = NULL;
	else
	{
		node->typeIds = pg_zalloc(sizeof(int32) * nitems);
		memcpy(node->typeIds, vector, sizeof(int32) * nitems);
	}
	node->_num_typeIds = nitems;
}

static void
readArrowTypeFixedSizeBinary(ArrowTypeFixedSizeBinary *node, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *) pos);

	node->byteWidth = fetchInt(&t, 0);
}

static void
readArrowTypeFixedSizeList(ArrowTypeFixedSizeList *node, const char *pos)
{
	FBTable		t= fetchFBTable((int32 *) pos);

	node->listSize = fetchInt(&t, 0);
}

static void
readArrowTypeMap(ArrowTypeMap *node, const char *pos)
{
	FBTable		t= fetchFBTable((int32 *) pos);

	node->keysSorted = fetchBool(&t, 0);
}

static void
__readArrowType(ArrowType *type, int type_tag, const char *type_pos)
{
	memset(type, 0, sizeof(ArrowType));
	switch (type_tag)
	{
		case ArrowType__Null:
			type->node.tag = ArrowNodeTag__Null;
			break;
		case ArrowType__Int:
			type->node.tag = ArrowNodeTag__Int;
			if (type_pos)
				readArrowTypeInt(&type->Int, type_pos);
			break;
		case ArrowType__FloatingPoint:
			type->node.tag = ArrowNodeTag__FloatingPoint;
			if (type_pos)
				readArrowTypeFloatingPoint(&type->FloatingPoint, type_pos);
			break;
		case ArrowType__Binary:
			type->node.tag = ArrowNodeTag__Binary;
			break;
		case ArrowType__Utf8:
			type->node.tag = ArrowNodeTag__Utf8;
			break;
		case ArrowType__Bool:
			type->node.tag = ArrowNodeTag__Bool;
			break;
		case ArrowType__Decimal:
			type->node.tag = ArrowNodeTag__Decimal;
			if (type_pos)
				readArrowTypeDecimal(&type->Decimal, type_pos);
			break;
		case ArrowType__Date:
			type->node.tag = ArrowNodeTag__Date;
			if (type_pos)
				readArrowTypeDate(&type->Date, type_pos);
			break;
		case ArrowType__Time:
			type->node.tag = ArrowNodeTag__Time;
			if (type_pos)
				readArrowTypeTime(&type->Time, type_pos);
			break;
		case ArrowType__Timestamp:
			type->node.tag = ArrowNodeTag__Timestamp;
			if (type_pos)
				readArrowTypeTimestamp(&type->Timestamp, type_pos);
			break;
		case ArrowType__Interval:
			type->node.tag = ArrowNodeTag__Interval;
			if (type_pos)
				readArrowTypeInterval(&type->Interval, type_pos);
			break;
		case ArrowType__List:
			type->node.tag = ArrowNodeTag__List;
			break;
		case ArrowType__Struct:
			type->node.tag = ArrowNodeTag__Struct;
			break;
		case ArrowType__Union:
			type->node.tag = ArrowNodeTag__Union;
			if (type_pos)
				readArrowTypeUnion(&type->Union, type_pos);
			break;
		case ArrowType__FixedSizeBinary:
			type->node.tag = ArrowNodeTag__FixedSizeBinary;
			if (type_pos)
				readArrowTypeFixedSizeBinary(&type->FixedSizeBinary, type_pos);
			break;
		case ArrowType__FixedSizeList:
			type->node.tag = ArrowNodeTag__FixedSizeList;
			if (type_pos)
				readArrowTypeFixedSizeList(&type->FixedSizeList, type_pos);
			break;
		case ArrowType__Map:
			type->node.tag = ArrowNodeTag__Map;
			if (type_pos)
				readArrowTypeMap(&type->Map, type_pos);
			break;
		default:
			printf("type code = %d is not supported now\n", type_tag);
			break;
	}
}

static void
readArrowType(ArrowType *type, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *)pos);
	int			type_tag = fetchChar(&t, 0);
	const char *type_pos = fetchOffset(&t, 1);

	__readArrowType(type, type_tag, type_pos);
}

static void
readArrowDictionaryEncoding(ArrowDictionaryEncoding *dict, const char *pos)
{}

static void
readArrowField(ArrowField *field, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *)pos);
	int			type_tag;
	const char *type_pos;
	const char *dict_pos;
	int32	   *vector;
	int			i, nitems;

	memset(field, 0, sizeof(ArrowField));
	field->node.tag     = ArrowNodeTag__Field;
	field->name         = fetchString(&t, 0, &field->_name_len);
	field->nullable     = fetchBool(&t, 1);
	/* type */
	type_tag            = fetchChar(&t, 2);
	type_pos            = fetchOffset(&t, 3);
	__readArrowType(&field->type, type_tag, type_pos);

	/* dictionary */
	dict_pos = fetchOffset(&t, 4);
	if (dict_pos)
		readArrowDictionaryEncoding(&field->dictionary, dict_pos);

	/* children */
	vector = fetchVector(&t, 5, &nitems);
	if (nitems == 0)
		field->children = NULL;
	else
	{
		field->children = pg_zalloc(sizeof(ArrowType *) * nitems);
		for (i=0; i < nitems; i++)
		{
			int		offset = vector[i];
			ArrowType *t;

			if (offset == 0)
				continue;
			t = pg_zalloc(sizeof(ArrowType));
			readArrowType(t, (const char *)&vector[i] + offset);
			field->children[i] = t;
		}
	}
	field->_num_children = nitems;

	/* custom_metadata */
	vector = fetchVector(&t, 6, &nitems);
	if (nitems == 0)
		field->custom_metadata = NULL;
	else
	{
		field->custom_metadata = pg_zalloc(sizeof(ArrowKeyValue *) * nitems);
		for (i=0; i < nitems; i++)
		{
			int		offset = vector[i];
			ArrowKeyValue *kv;

			if (offset == 0)
				continue;
			kv = pg_zalloc(sizeof(ArrowKeyValue));
			readArrowKeyValue(kv, (const char *)&vector[i] + offset);
			field->custom_metadata[i] = kv;
		}
	}
	field->_num_custom_metadata = nitems;
}

static void
readArrowSchema(ArrowSchema *schema, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *)pos);
	int32	   *vector;
	int32		i, nitems;

	memset(schema, 0, sizeof(ArrowSchema));
	schema->node.tag     = ArrowNodeTag__Schema;
	schema->endianness   = fetchBool(&t, 0);
	/* [ fields ]*/
	vector = fetchVector(&t, 1, &nitems);
	if (nitems == 0)
		schema->fields = NULL;
	else
	{
		schema->fields = pg_zalloc(sizeof(ArrowField *) * nitems);
		for (i=0; i < nitems; i++)
		{
			int		offset = vector[i];
			ArrowField *f;

			if (offset == 0)
				continue;
			f = pg_zalloc(sizeof(ArrowField));
			readArrowField(f, (const char *)&vector[i] + offset);
			schema->fields[i] = f;
		}
	}
	schema->_num_fields = nitems;

	/* [ custom_metadata ] */
	vector = fetchVector(&t, 2, &nitems);
	if (nitems == 0)
		schema->custom_metadata = NULL;
	else
	{
		schema->custom_metadata = pg_zalloc(sizeof(ArrowKeyValue) * nitems);
		for (i=0; i < nitems; i++)
		{
			int		offset = vector[i];
			ArrowKeyValue *kv;

			if (offset == 0)
				continue;
			kv = pg_zalloc(sizeof(ArrowKeyValue));
			readArrowKeyValue(kv, (const char *)&vector[i] + offset);
			schema->custom_metadata[i] = kv;
		}
	}
	schema->_num_custom_metadata = nitems;
}

static void
readArrowRecordBatch(ArrowRecordBatch *rbatch, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *)pos);
	int64	   *vector;
	int			i, nitems;

	memset(rbatch, 0, sizeof(ArrowRecordBatch));
	rbatch->node.tag = ArrowNodeTag__RecordBatch;
	rbatch->length   = fetchLong(&t, 0);
	/* nodes: [FieldNode] */
	vector = (int64 *)fetchVector(&t, 1, &nitems);
	if (nitems > 0)
	{
		rbatch->nodes = pg_zalloc(sizeof(ArrowFieldNode *) * nitems);
		for (i=0; i < nitems; i++)
		{
			ArrowFieldNode *f = pg_zalloc(sizeof(ArrowFieldNode));
			f->node.tag   = ArrowNodeTag__FieldNode;
			f->length     = *vector++;
			f->null_count = *vector++;
			rbatch->nodes[i] = f;
		}
		rbatch->_num_nodes = nitems;
	}

	/* buffers: [Buffer] */
	vector = (int64 *)fetchVector(&t, 2, &nitems);
	if (nitems > 0)
	{
		rbatch->buffers = pg_zalloc(sizeof(ArrowBuffer *) * nitems);
		for (i=0; i < nitems; i++)
		{
			ArrowBuffer *b = pg_zalloc(sizeof(ArrowBuffer));
			b->node.tag = ArrowNodeTag__Buffer;
			b->offset   = *vector++;
			b->length   = *vector++;
			rbatch->buffers[i] = b;
		}
		rbatch->_num_buffers = nitems;
	}
}

static void
readArrowDictionaryBatch(ArrowDictionaryBatch *dbatch, const char *pos)
{
	FBTable		t = fetchFBTable((int32 *)pos);
	const char *next;

	memset(dbatch, 0, sizeof(ArrowDictionaryBatch));
	dbatch->node.tag = ArrowNodeTag__DictionaryBatch;
	dbatch->id      = fetchLong(&t, 0);
	next            = fetchOffset(&t, 1);
	readArrowRecordBatch(&dbatch->data, next);
	dbatch->isDelta = fetchBool(&t, 2);
}

static void
readArrowMessage(ArrowMessage *message, const char *pos)
{
	FBTable			t = fetchFBTable((int32 *)pos);
	int				mtype;
	const char	   *next;

	memset(message, 0, sizeof(ArrowMessage));
	message->node.tag     = ArrowNodeTag__Message;
	message->version      = fetchShort(&t, 0);
	mtype                 = fetchChar(&t, 1);
	next                  = fetchOffset(&t, 2);
	message->bodyLength   = fetchLong(&t, 3);

	if (message->version != ArrowMetadataVersion__V4)
		Elog("metadata version %d is not supported", message->version);

	switch (mtype)
	{
		case ArrowMessageHeader__Schema:
			readArrowSchema(&message->body.schema, next);
			break;
		case ArrowMessageHeader__DictionaryBatch:
			readArrowDictionaryBatch(&message->body.dictionaryBatch, next);
			break;
		case ArrowMessageHeader__RecordBatch:
			readArrowRecordBatch(&message->body.recordBatch, next);
			break;
		case ArrowMessageHeader__Tensor:
			Elog("message type: Tensor is not implemented");
			break;
		case ArrowMessageHeader__SparseTensor:
			Elog("message type: SparseTensor is not implemented");
			break;
		default:
			Elog("unknown message header type: %d", mtype);
			break;
	}
}

/*
 * readArrowFile - read the supplied apache arrow file
 */
void
readArrowFile(const char *pathname)
{
	int				fdesc;
	struct stat		st_buf;
	size_t			file_sz;
	FBMetaData	   *meta;
	int				i;

	fdesc = open(pathname, O_RDONLY);
	if (fdesc < 0)
		Elog("failed on open('%s'): %m", pathname);
	if (fstat(fdesc, &st_buf) != 0)
		Elog("failed on fstat('%s'): %m", pathname);
	file_sz = TYPEALIGN(sysconf(_SC_PAGESIZE), st_buf.st_size);

	file_map_head = mmap(NULL, file_sz, PROT_READ, MAP_SHARED, fdesc, 0);
	if (file_map_head == MAP_FAILED)
		Elog("failed on mmap(2): %m");
	file_map_tail = file_map_head + file_sz;

	/* check signature */
	if (memcmp(file_map_head, "ARROW1\0\0", 8) != 0)
		Elog("file does not look like Apache Arrow file");
	meta = (FBMetaData *)(file_map_head + 8);
	for (i=0; i < 2; i++)
	{
		const char	   *pos = (char *)&meta->headOffset + meta->headOffset;
		ArrowMessage	node;

		readArrowMessage(&node, pos);
		dumpArrowNode((ArrowNode *)&node, stdout);
		putchar('\n');

		meta = (FBMetaData *)((char *)&meta->headOffset + meta->metaLength);
	}
}
