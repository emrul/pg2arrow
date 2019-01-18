/*
 * arrow_write.c - routines to write out apache arrow format
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"

typedef struct
{
	uint16		vlen;	/* vtable length */
	uint16		tlen;	/* table length */
	uint16		offset[FLEXIBLE_ARRAY_MEMBER];
} FBVtable;

typedef struct
{
	void	  **extra_buf;		/* external buffer */
	int32	   *extra_sz;
	int32		nattrs;			/* number of variables */
	int32		length;			/* length of the flat image.
								 * If -1, buffer is not flatten yet. */
	FBVtable	vtable;
} FBTableBuf;

static FBTableBuf *
__allocFBTableBuf(int nattrs, const char *func_name)
{
	FBTableBuf *buf;
	size_t		required = (MAXALIGN(offsetof(FBTableBuf,
											  vtable.offset[nattrs])) +
							MAXALIGN(sizeof(int32) +
									 sizeof(Datum) * nattrs));
	buf = palloc0(required);
	buf->extra_buf		= palloc0(sizeof(void *) * nattrs);
	buf->extra_sz		= palloc0(sizeof(int) * nattrs);
	buf->nattrs			= nattrs;
	buf->length			= -1;	/* not flatten yet */
	buf->vtable.vlen	= sizeof(int32);
	buf->vtable.tlen	= sizeof(int32);

	return buf;
}
#define allocFBTableBuf(a)						\
	__allocFBTableBuf((a),__FUNCTION__)

static void
__addBufferScalar(FBTableBuf *buf, int index, void *ptr, int sz, int align)
{
	FBVtable   *vtable = &buf->vtable;

	assert(sz >= 0 && sz <= sizeof(int64));
	assert(index >= 0 && index < buf->nattrs);
	if (!ptr || sz == 0)
		vtable->offset[index] = 0;
	else
	{
		char   *table;
		int		offset;

		assert(buf->vtable.tlen >= sizeof(int32));
		table = (char *)&buf->vtable +
			INTALIGN(offsetof(FBVtable, offset[buf->nattrs]));
		offset = TYPEALIGN(align, vtable->tlen);
		memcpy(table + offset, ptr, sz);
		vtable->offset[index] = offset;
		vtable->tlen = offset + sz;
		vtable->vlen = Max(vtable->vlen,
						   offsetof(FBVtable, offset[index+1]));
	}
}

static void
__addBufferBinary(FBTableBuf *buf, int index, void *ptr, int sz, int32 shift)
{
	assert(index >= 0 && index < buf->nattrs);
	if (!ptr || sz == 0)
		buf->vtable.offset[index] = 0;
	else
	{
		buf->extra_buf[index]	= ptr;
		buf->extra_sz[index]	= sz;
		__addBufferScalar(buf, index, &shift, sizeof(shift), ALIGNOF_INT);
	}
}


static inline void
addBufferBool(FBTableBuf *buf, int index, bool value)
{
	if (value != 0)
		__addBufferScalar(buf, index, &value, sizeof(value), 1);
}

static inline void
addBufferChar(FBTableBuf *buf, int index, int8 value)
{
	if (value != 0)
		__addBufferScalar(buf, index, &value, sizeof(value), 1);
}

static inline void
addBufferShort(FBTableBuf *buf, int index, int16 value)
{
	if (value != 0)
		__addBufferScalar(buf, index, &value, sizeof(value), ALIGNOF_SHORT);
}

static inline void
addBufferInt(FBTableBuf *buf, int index, int32 value)
{
	if (value != 0)
		__addBufferScalar(buf, index, &value, sizeof(value), ALIGNOF_INT);
}

static inline void
addBufferLong(FBTableBuf *buf, int index, int64 value)
{
	if (value != 0)
		__addBufferScalar(buf, index, &value, sizeof(value), ALIGNOF_LONG);
}

static inline void
addBufferString(FBTableBuf *buf, int index, const char *cstring)
{
	int		slen, blen;
	char   *temp;

	if (cstring && (slen = strlen(cstring)) > 0)
	{
		blen = sizeof(int32) + INTALIGN(slen + 1);
		temp = palloc0(blen);
		*((int32 *)temp) = slen;
		strcpy(temp + sizeof(int32), cstring);
		__addBufferBinary(buf, index, temp, blen, 0);
	}
}

static inline void
addBufferOffset(FBTableBuf *buf, int index, FBTableBuf *sub)
{
	if (sub)
	{
		if (sub->length < 0)
			Elog("Bug? FBTableBuf is not flatten");
		__addBufferBinary(buf, index,
						  &sub->vtable,
						  sub->length,
						  sub->vtable.vlen);
	}
}

static void
addBufferVector(FBTableBuf *buf, int index, int nitems, FBTableBuf **elements)
{
	size_t	len = sizeof(int32) + sizeof(int32) * nitems;
	int32	i, *vector;
	char   *pos;

	/* calculation of flat buffer length */
	for (i=0; i < nitems; i++)
	{
		FBTableBuf *e = elements[i];

		if (e->length < 0)
			Elog("Bug? FBTableBuf is not flatten");

		len += MAXALIGN(e->length) + MAXIMUM_ALIGNOF; /* with margin */
	}
	vector = palloc0(len);
	vector[0] = nitems;
	pos = (char *)&vector[1 + nitems];
	for (i=0; i < nitems; i++)
	{
		FBTableBuf *e = elements[i];
		int			gap = (INTALIGN(pos + e->vtable.vlen) -
						   (uintptr_t)(pos + e->vtable.vlen));
		if (gap > 0)
		{
			memset(pos, 0, gap);
			pos += gap;
		}
		memcpy(pos, &e->vtable, e->length);
		vector[i+1] = (pos + e->vtable.vlen) - (char *)&vector[i+1];
		pos += e->length;
	}
	__addBufferBinary(buf, index, vector, pos - (char *)vector, 0);
}

static FBTableBuf *
__makeBufferFlatten(FBTableBuf *buf, const char *func_name)
{
	size_t		base_sz = buf->vtable.vlen + buf->vtable.tlen;
	size_t		extra_sz = 0;
	char	   *table;
	char	   *table_old;
	char	   *pos;
	int			i, diff;

	assert(buf->vtable.vlen == SHORTALIGN(buf->vtable.vlen));
	assert(buf->vtable.tlen >= sizeof(int32));
	/* close up the hole between vtable tail and table head if any */
	table = ((char *)&buf->vtable + buf->vtable.vlen);
	table_old = ((char *)&buf->vtable +
				 INTALIGN(offsetof(FBVtable, offset[buf->nattrs])));
	if (table != table_old)
	{
		assert(table < table_old);
		memmove(table, table_old, buf->vtable.tlen);
	}
	*((int32 *)table) = buf->vtable.vlen;
	diff = INTALIGN(buf->vtable.tlen) - buf->vtable.tlen;
	if (diff > 0)
		memset(table + buf->vtable.tlen, 0, diff);

	/* check extra buffer usage */
	for (i=0; i < buf->nattrs; i++)
	{
		if (buf->extra_buf[i] != NULL)
			extra_sz += MAXALIGN(buf->extra_sz[i]);
	}

	if (extra_sz == 0)
		buf->length = base_sz;
	else
	{
		buf = repalloc(buf, offsetof(FBTableBuf,
									 vtable) + MAXALIGN(base_sz) + extra_sz);
		table = (char *)&buf->vtable + buf->vtable.vlen;
		/*
		 * memo: we shall copy the flat image to the location where 'table'
		 * is aligned to INT. So, extra buffer should begin from INT aligned
		 * offset to the table.
		 */
		pos = table + INTALIGN(buf->vtable.tlen);
		for (i=0; i < buf->nattrs; i++)
		{
			int32  *offset;

			if (!buf->extra_buf[i])
				continue;
			assert(buf->vtable.offset[i] != 0);
			offset = (int32 *)(table + buf->vtable.offset[i]);
			assert(*offset >= 0 && *offset < buf->extra_sz[i]);
			*offset = (pos - (char *)offset) + *offset;

			memcpy(pos, buf->extra_buf[i], buf->extra_sz[i]);
			pos += INTALIGN(buf->extra_sz[i]);
		}
		buf->length = pos - (char *)&buf->vtable;
	}
	return buf;
}

#define makeBufferFlatten(a)	__makeBufferFlatten((a),__FUNCTION__)

static FBTableBuf *
createArrowTypeInt(ArrowTypeInt *node)
{
	FBTableBuf *buf = allocFBTableBuf(2);

	assert(node->tag == ArrowNodeTag__Int);
	addBufferInt(buf, 0, node->bitWidth);
	addBufferBool(buf, 1, node->is_signed);

	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowTypeFloatingPoint(ArrowTypeFloatingPoint *node)
{
	FBTableBuf *buf = allocFBTableBuf(1);

	assert(node->tag == ArrowNodeTag__FloatingPoint);
	addBufferInt(buf, 0, node->precision);

	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowType(ArrowType *node, ArrowTypeTag *p_type_tag)
{
	FBTableBuf	   *buf;
	ArrowTypeTag	tag;

	switch (node->tag)
	{
		case ArrowNodeTag__Int:
			tag = ArrowType__Int;
			buf = createArrowTypeInt((ArrowTypeInt *)node);
			break;
		case ArrowNodeTag__FloatingPoint:
			tag = ArrowType__FloatingPoint;
			buf = createArrowTypeFloatingPoint((ArrowTypeFloatingPoint *)node);
			break;
		default:
			Elog("unknown ArrowNodeTag: %d", node->tag);
			break;
	}
	*p_type_tag = tag;
	return buf;
}

struct ArrowBufferVector
{
	int32		nitems;
	struct {
		int64	offset;
		int64	length;
	} buffers[FLEXIBLE_ARRAY_MEMBER];
} __attribute__((packed));
typedef struct ArrowBufferVector	ArrowBufferVector;

static void
addBufferArrowBufferVector(FBTableBuf *buf, int index,
						   int nitems, ArrowBuffer *arrow_buffers)
{
	ArrowBufferVector *vector;
	size_t		length = offsetof(ArrowBufferVector, buffers[nitems]);
	int			i;

	vector = palloc0(length);
	vector->nitems = nitems;
	for (i=0; i < nitems; i++)
	{
		ArrowBuffer *b = &arrow_buffers[i];

		assert(b->tag == ArrowNodeTag__Buffer);
		vector->buffers[i].offset = b->offset;
		vector->buffers[i].length = b->length;
	}
	__addBufferBinary(buf, index, vector, length, 0);
}

struct ArrowFieldNodeVector
{
	int32		nitems;
	struct {
		int64	length;
		int64	null_count;
	} nodes[FLEXIBLE_ARRAY_MEMBER];
} __attribute__((packed));
typedef struct ArrowFieldNodeVector	ArrowFieldNodeVector;

static void
addBufferArrowFieldNodeVector(FBTableBuf *buf, int index,
							  int nitems, ArrowFieldNode *elements)
{
	ArrowFieldNodeVector *vector;
	size_t		length = offsetof(ArrowFieldNodeVector, nodes[nitems]);
	int			i;

	vector = palloc0(length);
	vector->nitems = nitems;
	for (i=0; i < nitems; i++)
	{
		ArrowFieldNode *f = &elements[i];

		assert(f->tag == ArrowNodeTag__FieldNode);
		vector->nodes[i].length		= f->length;
		vector->nodes[i].null_count	= f->null_count;
	}
	__addBufferBinary(buf, index, vector, length, 0);
}



static FBTableBuf *
createArrowKeyValue(ArrowKeyValue *node)
{
	FBTableBuf *buf = allocFBTableBuf(2);

	assert(node->tag == ArrowNodeTag__KeyValue);
	addBufferString(buf, 0, node->key);
	addBufferString(buf, 1, node->value);

	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowDictionaryEncoding(ArrowDictionaryEncoding *node)
{
	FBTableBuf *buf = allocFBTableBuf(3);
	FBTableBuf *typeInt;

	assert(node->tag == ArrowNodeTag__DictionaryEncoding);
	if (node->id == 0)
		return NULL;
	addBufferLong(buf, 0, node->id);
	typeInt = createArrowTypeInt(&node->indexType);
	assert(typeInt != NULL);
	addBufferOffset(buf, 1, typeInt);
	addBufferBool(buf, 2, node->isOrdered);

	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowField(ArrowField *node)
{
	FBTableBuf	   *buf = allocFBTableBuf(7);
	FBTableBuf	   *dictionary = NULL;
	FBTableBuf	   *type = NULL;
	FBTableBuf	  **vector;
	ArrowTypeTag	type_tag;
	int				i;

	assert(node->tag == ArrowNodeTag__Field);
	addBufferString(buf, 0, node->name);
	addBufferBool(buf, 1, node->nullable);
	type = createArrowType(&node->type, &type_tag);
	addBufferChar(buf, 2, type_tag);
	if (type)
		addBufferOffset(buf, 3, type);
	dictionary = createArrowDictionaryEncoding(&node->dictionary);
	addBufferOffset(buf, 4, dictionary);
	if (node->_num_children == 0)
		vector = NULL;
	else
	{
		vector = alloca(sizeof(FBTableBuf *) * node->_num_children);
		for (i=0; i < node->_num_children; i++)
			vector[i] =  createArrowField(&node->children[i]);
	}
	addBufferVector(buf, 5, node->_num_children, vector);

	if (node->_num_custom_metadata > 0)
	{
		vector = alloca(sizeof(FBTableBuf *) * node->_num_custom_metadata);
		for (i=0; i < node->_num_custom_metadata; i++)
			vector[i] = createArrowKeyValue(&node->custom_metadata[i]);
		addBufferVector(buf, 6, node->_num_custom_metadata, vector);
	}
	return makeBufferFlatten(buf);
}


static FBTableBuf *
createArrowSchema(ArrowSchema *node)
{
	FBTableBuf	   *buf = allocFBTableBuf(3);
	FBTableBuf	  **fields;
	FBTableBuf	  **cmetadata;
	int				i;

	assert(node->tag == ArrowNodeTag__Schema);
	addBufferBool(buf, 0, node->endianness);
	if (node->_num_fields > 0)
	{
		fields = alloca(sizeof(FBTableBuf *) * node->_num_fields);
		for (i=0; i < node->_num_fields; i++)
			fields[i] = createArrowField(&node->fields[i]);
		addBufferVector(buf, 1, node->_num_fields, fields);
	}
	if (node->_num_custom_metadata > 0)
	{
		cmetadata = alloca(sizeof(FBTableBuf *) *
						   node->_num_custom_metadata);
		for (i=0; i < node->_num_custom_metadata; i++)
			cmetadata[i] = createArrowKeyValue(&node->custom_metadata[i]);
		addBufferVector(buf, 2, node->_num_custom_metadata, cmetadata);
	}
	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowRecordBatch(ArrowRecordBatch *node)
{
	FBTableBuf *buf = allocFBTableBuf(3);

	assert(node->tag == ArrowNodeTag__RecordBatch);
	addBufferLong(buf, 0, node->length);
	addBufferArrowFieldNodeVector(buf, 1,
								  node->_num_nodes,
								  node->nodes);
	addBufferArrowBufferVector(buf, 2,
							   node->_num_buffers,
                               node->buffers);
	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowDictionaryBatch(ArrowDictionaryBatch *node)
{
	FBTableBuf *buf = allocFBTableBuf(3);
	FBTableBuf *dataBuf;

	assert(node->tag == ArrowNodeTag__DictionaryBatch);
	addBufferLong(buf, 0, node->id);
	dataBuf = createArrowRecordBatch(&node->data);
	addBufferOffset(buf, 1, dataBuf);
	addBufferBool(buf, 2, node->isDelta);

	return makeBufferFlatten(buf);
}

static FBTableBuf *
createArrowMessage(ArrowMessage *node)
{
	FBTableBuf *buf = allocFBTableBuf(4);
	FBTableBuf *data;
	ArrowMessageHeader tag;

	assert(node->tag == ArrowNodeTag__Message);
	addBufferShort(buf, 0, node->version);
	switch (node->body.tag)
	{
		case ArrowNodeTag__Schema:
			tag = ArrowMessageHeader__Schema;
			data = createArrowSchema(&node->body.schema);
			break;
		case ArrowNodeTag__DictionaryBatch:
			tag = ArrowMessageHeader__DictionaryBatch;
			data = createArrowDictionaryBatch(&node->body.dictionaryBatch);
			break;
		case ArrowNodeTag__RecordBatch:
			tag = ArrowMessageHeader__RecordBatch;
			data = createArrowRecordBatch(&node->body.recordBatch);
			break;
		default:
			Elog("unexpexted ArrowNodeTag: %d", node->body.tag);
			break;
	}
	addBufferChar(buf, 1, tag);
	addBufferOffset(buf, 2, data);
	addBufferLong(buf, 3, node->bodyLength);

	return makeBufferFlatten(buf);
}

struct ArrowBlockVector
{
	int32		nitems;
	struct {
		int64	offset;
		int32	metaDataLength;
		int32	__padding__;
		int64	bodyLength;
	} blocks[FLEXIBLE_ARRAY_MEMBER];
} __attribute__((packed));
typedef struct ArrowBlockVector		ArrowBlockVector;

static void
addBufferArrowBlockVector(FBTableBuf *buf, int index,
						  int nitems, ArrowBlock *arrow_blocks)
{
	ArrowBlockVector *vector;
	size_t		length = offsetof(ArrowBlockVector, blocks[nitems]);
	int			i;

	vector = palloc0(length);
    vector->nitems = nitems;
    for (i=0; i < nitems; i++)
	{
		ArrowBlock *b = &arrow_blocks[i];

		assert(b->tag == ArrowNodeTag__Block);
		vector->blocks[i].offset = b->offset;
		vector->blocks[i].metaDataLength = b->metaDataLength;
		vector->blocks[i].bodyLength = b->bodyLength;
	}
	__addBufferBinary(buf, index, vector, length, 0);
}

static FBTableBuf *
createArrowFooter(ArrowFooter *node)
{
	FBTableBuf	   *buf = allocFBTableBuf(4);
	FBTableBuf	   *schema;

	assert(node->tag == ArrowNodeTag__Footer);
	addBufferShort(buf, 0, node->version);
	schema = createArrowSchema(&node->schema);
	addBufferOffset(buf, 1, schema);
	addBufferArrowBlockVector(buf, 2,
							  node->_num_dictionaries,
							  node->dictionaries);
	addBufferArrowBlockVector(buf, 3,
							  node->_num_recordBatches,
							  node->recordBatches);
	return makeBufferFlatten(buf);
}

/* ----------------------------------------------------------------
 * Routines for File I/O
 * ---------------------------------------------------------------- */
typedef struct
{
	int32		metaLength;
	int32		rootOffset;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} FBMessageFileImage;

static ssize_t
writeFlatBufferMessage(int fdesc, ArrowMessage *message)
{
	FBTableBuf *payload = createArrowMessage(message);
	FBMessageFileImage *image;
	ssize_t		nbytes;
	ssize_t		offset;
	ssize_t		length;

	assert(payload->length > 0);
	offset = INTALIGN(payload->vtable.vlen) - payload->vtable.vlen;
    nbytes = INTALIGN(offset + payload->length);
	length = offsetof(FBMessageFileImage, data[nbytes]);
	image = alloca(length);
	image->metaLength = sizeof(int32) + nbytes;
	image->rootOffset = sizeof(int32) + INTALIGN(payload->vtable.vlen);
	if (offset > 0)
		memset(image->data, 0, offset);
	memcpy(image->data + offset, &payload->vtable, payload->length);
	offset += payload->length;
	if (offset < nbytes)
		memset(image->data + offset, 0, nbytes - offset);
	if (write(fdesc, image, length) != length)
		Elog("failed on write: %m");
	return length;
}

typedef struct
{
	int32		rootOffset;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} FBFooterFileImage;

typedef struct
{
	int32		metaOffset;
	char		signature[6];
} FBFooterTailImage;

static ssize_t
writeFlatBufferFooter(int fdesc, ArrowFooter *footer)
{
	FBTableBuf *payload = createArrowFooter(footer);
	FBFooterFileImage *image;
	FBFooterTailImage *tail;
	ssize_t		nbytes;
	ssize_t		offset;
	ssize_t		length;

	assert(payload->length > 0);
    offset = INTALIGN(payload->vtable.vlen) - payload->vtable.vlen;
	nbytes = INTALIGN(offset + payload->length);
	length = (offsetof(FBFooterFileImage, data[nbytes]) +
			  offsetof(FBFooterTailImage, signature[6]));
	image = alloca(length + 1);
	image->rootOffset = sizeof(int32) + INTALIGN(payload->vtable.vlen);
	if (offset > 0)
		memset(image->data, 0, offset);
    memcpy(image->data + offset, &payload->vtable, payload->length);
    offset += payload->length;
	if (offset < nbytes)
		memset(image->data + offset, 0, nbytes - offset);
	tail = (FBFooterTailImage *)(image->data + nbytes);
	tail->metaOffset = nbytes + sizeof(int32);
	strcpy(tail->signature, "ARROW1");
	if (write(fdesc, image, length) != length)
		Elog("failed on write: %m");
	return length;
}

static void
__setupArrowFieldNode(ArrowFieldNode *node, size_t nitems, SQLattribute *attr)
{
	memset(node, 0, sizeof(ArrowFieldNode));
	node->tag = ArrowNodeTag__FieldNode;
	node->length = nitems;
	node->null_count = attr->nullcount;
}

static int
__setupArrowBuffer(ArrowBuffer *node, size_t *p_offset, SQLattribute *attr)
{
	ArrowBuffer *base = node;
	size_t	offset = *p_offset;

	switch (attr->arrow_type.tag)
	{
		case ArrowNodeTag__Int:
		case ArrowNodeTag__FloatingPoint:
		case ArrowNodeTag__Bool:
		case ArrowNodeTag__Date:
		case ArrowNodeTag__Time:
		case ArrowNodeTag__Timestamp:
		case ArrowNodeTag__Interval:
			/* nullmap */
			node->tag = ArrowNodeTag__Buffer;
			node->offset = offset;
			if (attr->nullcount == 0)
				node->length = 0;
			else
			{
				node->length = MAXALIGN(attr->nullmap.usage);
				offset += node->length;
			}
			node++;
			/* fixed length values */
			node->tag = ArrowNodeTag__Buffer;
			node->offset = offset;
			node->length = MAXALIGN(attr->values.usage);
			offset += node->length;
			node++;
			break;

		case ArrowNodeTag__Null:
		case ArrowNodeTag__Struct:
		case ArrowNodeTag__Union:
			/* only nullmap? */
			Elog("unexpected node type %d", attr->arrow_type.tag);
			break;
		case ArrowNodeTag__Binary:
		case ArrowNodeTag__Utf8:
			/* nullmap */
			node->tag = ArrowNodeTag__Buffer;
			node->offset = offset;
			if (attr->nullcount == 0)
				node->length = 0;
			else
			{
				node->length = MAXALIGN(attr->nullmap.usage);
				offset += node->length;
			}
			node++;

			/* 32bit index */
			node->tag = ArrowNodeTag__Buffer;
			node->offset = offset;
			node->length = MAXALIGN(attr->values.usage);
            offset += node->length;
			node++;

			/* extra buffer */
			node->tag = ArrowNodeTag__Buffer;
			node->offset = offset;
			node->length = MAXALIGN(attr->extra.usage);
			offset += node->length;
			node++;
			break;
		default:
			Elog("unknown Arrow data type");
			break;
	}
	*p_offset = offset;
	return (node - base);
}

static void
writeArrowBodyOfAttribute(int fdesc, SQLattribute *attr)
{
	ssize_t		nbytes;

	switch (attr->arrow_type.tag)
	{
		case ArrowNodeTag__Int:
		case ArrowNodeTag__FloatingPoint:
		case ArrowNodeTag__Bool:
		case ArrowNodeTag__Date:
		case ArrowNodeTag__Time:
		case ArrowNodeTag__Timestamp:
		case ArrowNodeTag__Interval:
			/* nullmap */
			if (attr->nullcount > 0)
			{
				nbytes = write(fdesc,
							   attr->nullmap.ptr,
							   MAXALIGN(attr->nullmap.usage));
				if (nbytes != MAXALIGN(attr->nullmap.usage))
					Elog("failed on write: %m");
			}
			/* fixed length values */
			nbytes = write(fdesc,
						   attr->values.ptr,
						   MAXALIGN(attr->values.usage));
			if (nbytes != MAXALIGN(attr->values.usage))
				Elog("failed on write: %m");
			break;

		case ArrowNodeTag__Null:
		case ArrowNodeTag__Struct:
		case ArrowNodeTag__Union:
			/* only nullmap? */
			Elog("unexpected node type");
			break;
		case ArrowNodeTag__Binary:
		case ArrowNodeTag__Utf8:
			/* nullmap */
			if (attr->nullcount > 0)
			{
				nbytes = write(fdesc,
							   attr->nullmap.ptr,
							   MAXALIGN(attr->nullmap.usage));
				if (nbytes != MAXALIGN(attr->nullmap.usage))
					Elog("failed on write: %m");
			}
			/* 32bit index */
			nbytes = write(fdesc,
						   attr->values.ptr,
						   MAXALIGN(attr->values.usage));
			if (nbytes != MAXALIGN(attr->values.usage))
				Elog("failed on write: %m");
			/* extra buffer */
			nbytes = write(fdesc,
                           attr->extra.ptr,
                           MAXALIGN(attr->extra.usage));
			if (nbytes != MAXALIGN(attr->extra.usage))
				Elog("failed on write: %m");
			break;
		default:
			Elog("unknown Arrow data type");
			break;
	}
}

void
writeArrowRecordBatch(SQLtable *table,
					  size_t *p_metaLength,
					  size_t *p_bodyLength)
{
	ArrowMessage	message;
	ArrowRecordBatch *rbatch;
	ArrowFieldNode *nodes;
	ArrowBuffer	   *buffers;
	int32			i, j;
	size_t			metaLength;
	size_t			bodyLength = 0;

	/* fill up [nodes] vector */
	nodes = alloca(sizeof(ArrowFieldNode) * table->nfields);
	for (i=0; i < table->nfields; i++)
		__setupArrowFieldNode(&nodes[i], table->nitems, &table->attrs[i]);
	/* fill up [buffers] vector */
	buffers = alloca(sizeof(ArrowBuffer) * 3 * table->nfields);
	for (i=0, j=0; i < table->nfields; i++)
		j += __setupArrowBuffer(&buffers[j], &bodyLength, &table->attrs[i]);

	/* setup Message of Schema */
	memset(&message, 0, sizeof(ArrowMessage));
	message.tag = ArrowNodeTag__Message;
	message.version = ArrowMetadataVersion__V4;
	message.bodyLength = bodyLength;

	rbatch = &message.body.recordBatch;
	rbatch->tag = ArrowNodeTag__RecordBatch;
	rbatch->length = table->nitems;
	rbatch->nodes = nodes;
	rbatch->_num_nodes = table->nfields;
	rbatch->buffers = buffers;
	rbatch->_num_buffers = j;
	/* serialization */
	metaLength = writeFlatBufferMessage(table->fdesc, &message);
	for (i=0; i < table->nfields; i++)
		writeArrowBodyOfAttribute(table->fdesc, &table->attrs[i]);

	*p_metaLength = metaLength;
	*p_bodyLength = bodyLength;
}

static void
__setupArrowDictionaryEncoding(ArrowDictionaryEncoding *dict,
							   SQLattribute *attr)
{
	dict->tag = ArrowNodeTag__DictionaryEncoding;
}

static void
__setupArrowField(ArrowField *field, SQLattribute *attr)
{
	memset(field, 0, sizeof(ArrowField));
	field->tag = ArrowNodeTag__Field;
	field->name = attr->attname;
	field->_name_len = strlen(attr->attname);
	field->nullable = true;
	field->type = attr->arrow_type;
	__setupArrowDictionaryEncoding(&field->dictionary, attr);
	if (attr->subtypes)
	{
		SQLtable   *sub = attr->subtypes;
		int			i;

		field->children = alloca(sizeof(ArrowField) * sub->nfields);
		field->_num_children = sub->nfields;
		for (i=0; i < sub->nfields; i++)
			__setupArrowField(&field->children[i], &sub->attrs[i]);
	}
	//custom_metadata?
}

ssize_t
writeArrowSchema(SQLtable *table)
{
	ArrowMessage	message;
	ArrowSchema	   *schema;
	int32			i;

	/* setup Message of Schema */
	memset(&message, 0, sizeof(ArrowMessage));
	message.tag = ArrowNodeTag__Message;
	message.version = ArrowMetadataVersion__V4;
	schema = &message.body.schema;
	schema->tag = ArrowNodeTag__Schema;
	schema->endianness = ArrowEndianness__Little;
	schema->fields = alloca(sizeof(ArrowField) * table->nfields);
	schema->_num_fields = table->nfields;
	for (i=0; i < table->nfields; i++)
		__setupArrowField(&schema->fields[i], &table->attrs[i]);
	/* serialization */
	return writeFlatBufferMessage(table->fdesc, &message);
}

ssize_t
writeArrowFooter(SQLtable *table)
{
	ArrowFooter		footer;
	ArrowSchema	   *schema;
	int				i;

	/* setup Footer */
	memset(&footer, 0, sizeof(ArrowFooter));
	footer.tag = ArrowNodeTag__Footer;
	footer.version = ArrowMetadataVersion__V4;
	/* setup Schema of Footer */
	schema = &footer.schema;
	schema->tag = ArrowNodeTag__Schema;
	schema->endianness = ArrowEndianness__Little;
	schema->fields = alloca(sizeof(ArrowField) * table->nfields);
	schema->_num_fields = table->nfields;
	for (i=0; i < table->nfields; i++)
		__setupArrowField(&schema->fields[i], &table->attrs[i]);
	/* [dictionaries] */
	footer.dictionaries = table->dictionaries;
	footer._num_dictionaries = table->numDictionaries;

	/* [recordBatches] */
	footer.recordBatches = table->recordBatches;
	footer._num_recordBatches = table->numRecordBatches;

	/* serialization */
	return writeFlatBufferFooter(table->fdesc, &footer);
}
