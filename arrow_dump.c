/*
 * arrow_dump.c - routines to dump apache arrow structure
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"
#include "arrow_defs.h"

static void
dumpArrowTypeInt(ArrowTypeInt *node, FILE *out)
{
	fprintf(out, "{%s%d}",
			node->is_signed ? "Int" : "Uint", node->bitWidth);
}

static void
dumpArrowTypeFloatingPoint(ArrowTypeFloatingPoint *node, FILE *out)
{
	fprintf(out, "{Float%s}",
			node->precision == ArrowPrecision__Half ? "16" :
			node->precision == ArrowPrecision__Single ? "32" :
			node->precision == ArrowPrecision__Double ? "64" : "??");
}

static void
dumpArrowTypeDecimal(ArrowTypeDecimal *node, FILE *out)
{
	fprintf(out, "{Decimal: precision=%d, scale=%d}",
			node->precision,
			node->scale);
}

static void
dumpArrowTypeDate(ArrowTypeDate *node, FILE *out)
{
	fprintf(out, "{Date: unit=%s}",
			node->unit == ArrowDateUnit__Day ? "Day" :
			node->unit == ArrowDateUnit__MilliSecond ? "MilliSecond" : "???");
}

static void
dumpArrowTypeTime(ArrowTypeTime *node, FILE *out)
{
	fprintf(out, "{Time: unit=%s}",
			node->unit == ArrowTimeUnit__Second ? "sec" :
			node->unit == ArrowTimeUnit__MilliSecond ? "ms" :
			node->unit == ArrowTimeUnit__MicroSecond ? "us" :
			node->unit == ArrowTimeUnit__NanoSecond ? "ns" : "???");
}

static void
dumpArrowTypeTimestamp(ArrowTypeTimestamp *node, FILE *out)
{
	fprintf(out, "{Timestamp: unit=%s}",
			node->unit == ArrowTimeUnit__Second ? "sec" :
			node->unit == ArrowTimeUnit__MilliSecond ? "ms" :
			node->unit == ArrowTimeUnit__MicroSecond ? "us" :
			node->unit == ArrowTimeUnit__NanoSecond ? "ns" : "???");
	if (node->timezone)
		fprintf(out, ", timezone: %s", node->timezone);
	fprintf(out, "}");
}

static void
dumpArrowTypeInterval(ArrowTypeInterval *node, FILE *out)
{
	fprintf(out, "{Interval: unit=%s}",
			node->unit == ArrowIntervalUnit__Year_Month ? "Year_Month" :
			node->unit == ArrowIntervalUnit__Day_Time ? "Day_Time" : "???");
}

static void
dumpArrowTypeUnion(ArrowTypeUnion *node, FILE *out)
{
	int		i;

	fprintf(out, "{Union: mode=%s, typeIds=[",
			node->mode == ArrowUnionMode__Sparse ? "Sparse" :
			node->mode == ArrowUnionMode__Dense ? "Dense" : "unknown");
	for (i=0; i < node->_num_typeIds; i++)
		fprintf(out, "%s%d",
				i > 0 ? ", " : "",
				node->typeIds[i]);
	fprintf(out, "]}");
}

static void
dumpArrowTypeFixedSizeBinary(ArrowTypeFixedSizeBinary *node, FILE *out)
{
	fprintf(out, "{FixedSizeBinary: byteWidth=%d}",
			node->byteWidth);
}

static void
dumpArrowTypeFixedSizeList(ArrowTypeFixedSizeList *node, FILE *out)
{
	fprintf(out, "{FixedSizeList: listSize=%d}",
			node->listSize);
}

static void
dumpArrowTypeMap(ArrowTypeMap *node, FILE *out)
{
	fprintf(out, "{Map: keysSorted=%s}",
			node->keysSorted ? "true" : "false");
}

static void
dumpArrowBuffer(ArrowBuffer *node, FILE *out)
{
	fprintf(out, "{Buffer: offset=%ld, length=%ld}",
			node->offset,
			node->length);
}

static void
dumpArrowKeyValue(ArrowKeyValue *node, FILE *out)
{
	fprintf(out, "{KeyValue: key=%s, value=%s}",
			node->key,
			node->value);
}

static void
dumpArrowDictionaryEncoding(ArrowDictionaryEncoding *node, FILE *out)
{




}

static void
dumpArrowField(ArrowField *node, FILE *out)
{
	int		i;

	fprintf(out, "{Field: name=%s, nullable=%s, type=",
			node->name ? node->name : "NULL",
			node->nullable ? "true" : "false");
	dumpArrowNode((ArrowNode *)&node->type, out);
	fprintf(out, ", children=[");
	if (node->children)
	{
		for (i=0; i < node->_num_children; i++)
		{
			if (i > 0)
				fprintf(out, ", ");
			if (node->children[i])
				dumpArrowNode((ArrowNode *)node->children[i], out);
			else
				fprintf(out, "NULL");
		}
	}
	fprintf(out, "], custom_metadata=[");
	if (node->custom_metadata)
	{
		for (i=0; i < node->_num_custom_metadata; i++)
		{
			if (i > 0)
				fprintf(out, ", ");
			if (node->custom_metadata[i])
				dumpArrowNode((ArrowNode *)node->custom_metadata[i], out);
			else
				fprintf(out, "NULL");
		}
	}
	fprintf(out, "]}");
}

static void
dumpArrowFieldNode(ArrowFieldNode *node, FILE *out)
{
	fprintf(out, "{FieldNode: length=%ld, null_count=%ld}",
			node->length,
			node->null_count);
}

static void
dumpArrowSchema(ArrowSchema *node, FILE *out)
{
	int		i;

	fprintf(out, "{Schema: endianness=%s, fields=[",
			node->endianness == ArrowEndianness__Little ? "little" :
			node->endianness == ArrowEndianness__Big ? "big" : "???");
	for (i=0; i < node->_num_fields; i++)
	{
		if (i > 0)
			fprintf(out, ", ");
		if (node->fields[i])
			dumpArrowField(node->fields[i], out);
		else
			fprintf(out, "NULL");
	}
	fprintf(out, "], custom_metadata [");
	for (i=0; i < node->_num_custom_metadata; i++)
	{
		if (i > 0)
			fprintf(out, ", ");
		if (node->custom_metadata[i])
			dumpArrowKeyValue(node->custom_metadata[i], out);
		else
			fprintf(out, "NULL");
	}
	fprintf(out, "]}");
}

static void
dumpArrowRecordBatch(ArrowRecordBatch *node, FILE *out)
{
	int		i;

	fprintf(out, "{RecordBatch : length=%ld, nodes=[",
			node->length);
	for (i=0; i < node->_num_nodes; i++)
	{
		if (i > 0)
			fprintf(out, ", ");
		if (node->nodes[i])
			dumpArrowFieldNode(node->nodes[i], out);
		else
			fprintf(out, "NULL");
	}
	fprintf(out, "], buffers=[");
	for (i=0; i < node->_num_buffers; i++)
	{
		if (i > 0)
			fprintf(out, ", ");
		if (node->buffers[i])
			dumpArrowBuffer(node->buffers[i], out);
		else
			fprintf(out, "NULL");
	}
	fprintf(out, "]}");
}

static void
dumpArrowDictionaryBatch(ArrowDictionaryBatch *node, FILE *out)
{
	fprintf(out, "{DictionaryBatch: id=%ld, data=",
			node->id);
	dumpArrowRecordBatch(&node->data, out);
	fprintf(out, ", isDelta=%s}",
			node->isDelta ? "true" : "false");
}

static void
dumpArrowMessage(ArrowMessage *node, FILE *out)
{
	fprintf(out, "{Message: version=%d, body=",
			node->version);
	dumpArrowNode(&node->body.node, out);
	fprintf(out, ", bodyLength=%lu}", node->bodyLength);
}

void
dumpArrowNode(ArrowNode *node, FILE *out)
{
	if (!node)
		return;
	switch (node->tag)
	{
		case ArrowNodeTag__Null:
			fprintf(out, "{Null}");
			break;
		case ArrowNodeTag__Int:
			dumpArrowTypeInt((ArrowTypeInt *) node, out);
			break;
		case ArrowNodeTag__FloatingPoint:
			dumpArrowTypeFloatingPoint((ArrowTypeFloatingPoint *)node, out);
			break;
		case ArrowNodeTag__Utf8:
			fprintf(out, "{Utf8}");
			break;
		case ArrowNodeTag__Binary:
			fprintf(out, "{Binary}");
			break;
		case ArrowNodeTag__Bool:
			fprintf(out, "{Bool}");
			break;
		case ArrowNodeTag__Decimal:
			dumpArrowTypeDecimal((ArrowTypeDecimal *) node, out);
			break;
		case ArrowNodeTag__Date:
			dumpArrowTypeDate((ArrowTypeDate *) node, out);
			break;
		case ArrowNodeTag__Time:
			dumpArrowTypeTime((ArrowTypeTime *) node, out);
			break;
		case ArrowNodeTag__Timestamp:
			dumpArrowTypeTimestamp((ArrowTypeTimestamp *) node, out);
			break;
		case ArrowNodeTag__Interval:
			dumpArrowTypeInterval((ArrowTypeInterval *) node, out);
			break;
		case ArrowNodeTag__List:
			fprintf(out, "{List}");
			break;
		case ArrowNodeTag__Struct:
			fprintf(out, "{Struct}");
			break;
		case ArrowNodeTag__Union:
			dumpArrowTypeUnion((ArrowTypeUnion *) node, out);
			break;
		case ArrowNodeTag__FixedSizeBinary:
			dumpArrowTypeFixedSizeBinary((ArrowTypeFixedSizeBinary *)node,out);
			break;
		case ArrowNodeTag__FixedSizeList:
			dumpArrowTypeFixedSizeList((ArrowTypeFixedSizeList *)node, out);
			break;
		case ArrowNodeTag__Map:
			dumpArrowTypeMap((ArrowTypeMap *) node, out);
			break;
		case ArrowNodeTag__Buffer:
			dumpArrowBuffer((ArrowBuffer *) node, out);
			break;
		case ArrowNodeTag__KeyValue:
			dumpArrowKeyValue((ArrowKeyValue *) node, out);
			break;
		case ArrowNodeTag__DictionaryEncoding:
			dumpArrowDictionaryEncoding((ArrowDictionaryEncoding *)node, out);
			break;
		case ArrowNodeTag__Field:
			dumpArrowField((ArrowField *)node, out);
			break;
		case ArrowNodeTag__FieldNode:
			dumpArrowFieldNode((ArrowFieldNode *)node, out);
			break;
		case ArrowNodeTag__Schema:
			dumpArrowSchema((ArrowSchema *)node, out);
			break;
		case ArrowNodeTag__RecordBatch:
			dumpArrowRecordBatch((ArrowRecordBatch *) node, out);
			break;
		case ArrowNodeTag__DictionaryBatch:
			dumpArrowDictionaryBatch((ArrowDictionaryBatch *)node, out);
			break;
		case ArrowNodeTag__Message:
			dumpArrowMessage((ArrowMessage *)node, out);
			break;
		default:
			fprintf(out, "{!Unknown!}");
			break;
	}
}
