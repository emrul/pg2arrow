/*
 * arrow_types.c
 *
 * intermediation of PostgreSQL types <--> Apache Arrow types
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"


/* ----------------------------------------------------------------
 *
 * put_value handler for each data types (optional)
 *
 * ----------------------------------------------------------------
 */
static size_t
put_inline_8b_value(SQLattribute *attr, int row_index,
					const char *addr, int sz)
{
	size_t		usage;

	assert(attr->attlen == sizeof(uint8));
	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append_zero(&attr->values, sizeof(char));
	}
	else
	{
		assert(sz == sizeof(uint8));
		sql_buffer_setbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->values, addr, sz);
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}

static size_t
put_inline_16b_value(SQLattribute *attr, int row_index,
					 const char *addr, int sz)
{
	size_t		usage;
	uint16		value;

	assert(attr->attlen == sizeof(uint16));
	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append_zero(&attr->values, sizeof(uint16));
	}
	else
	{
		assert(sz == sizeof(uint16));
		value = ntohs(*((const uint16 *)addr));
		sql_buffer_setbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->values, &value, sz);
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}

static size_t
put_inline_32b_value(SQLattribute *attr, int row_index,
					 const char *addr, int sz)
{
	size_t		usage;
	uint32		value;

	assert(attr->attlen == sizeof(uint32));
	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append_zero(&attr->values, sizeof(uint32));
	}
	else
	{
		assert(sz == sizeof(uint32));
		value = ntohl(*((const uint32 *)addr));
		sql_buffer_setbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->values, &value, sz);
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}

static size_t
put_inline_64b_value(SQLattribute *attr, int row_index,
					 const char *addr, int sz)
{
	size_t		usage;
	uint64		value;
	uint32		h, l;

	assert(attr->attlen == sizeof(uint64));
	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append_zero(&attr->values, sizeof(uint64));
	}
	else
	{
		assert(sz == sizeof(uint64));
		h = ntohl(*((const uint32 *)(addr)));
		l = ntohl(*((const uint32 *)(addr + sizeof(uint32))));
		value = (uint64)h << 32 | (uint64)l;
		sql_buffer_setbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->values, &value, sz);
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}

#ifdef PG_INT128_TYPE
/* parameters of Numeric type */
#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS         0x0000
#define NUMERIC_NEG         0x4000
#define NUMERIC_NAN         0xC000

#define NBASE				10000
#define HALF_NBASE			5000
#define DEC_DIGITS			4	/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS    2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4
typedef int16				NumericDigit;

static size_t
put_decimal_value(SQLattribute *attr, int row_index,
				  const char *addr, int sz)
{
	size_t		usage = 0;

	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
        sql_buffer_append_zero(&attr->values, sizeof(int128));
	}
	else
	{
		struct {
			int16		ndigits;
			int16		weight;		/* weight of first digit */
			int16		sign;		/* NUMERIC_(POS|NEG|NAN) */
			int16		dscale;		/* display scale */
			NumericDigit digits[FLEXIBLE_ARRAY_MEMBER];
		}	   *rawdata = (void *)addr;
		int		ndigits	= ntohs(rawdata->ndigits);
		int		weight	= ntohs(rawdata->weight);
		int		sign	= ntohs(rawdata->sign);
		//int	dscale	= ntohs(rawdata->dscale);
		//int	precision = attr->arrow_type.Decimal.precision;
		int		ascale	= attr->arrow_type.Decimal.scale;
		int128	value = 0;
		int		d, dig;

		if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_NAN)
			Elog("Decimal128 cannot map NaN in PostgreSQL Numeric");

		/* makes integer portion first */
		for (d=0; d <= weight; d++)
		{
			dig = (d < ndigits) ? ntohs(rawdata->digits[d]) : 0;
			if (dig < 0 || dig >= NBASE)
				Elog("Numeric digit is out of range: %d", (int)dig);
			value = NBASE * value + (int128)dig;
		}

		/* makes floating point portion if any */
		while (ascale > 0)
		{
			dig = (d >= 0 && d < ndigits) ? ntohs(rawdata->digits[d]) : 0;
			if (dig < 0 || dig >= NBASE)
				Elog("Numeric digit is out of range: %d", (int)dig);

			if (ascale >= DEC_DIGITS)
				value = NBASE * value + dig;
			else if (ascale == 3)
				value = 1000L * value + dig / 10L;
			else if (ascale == 2)
				value =  100L * value + dig / 100L;
			else if (ascale == 1)
				value =   10L * value + dig / 1000L;
			else
				Elog("internal bug");
			ascale -= DEC_DIGITS;
			d++;
		}
		/* is it a negative value? */
		if ((sign & NUMERIC_NEG) != 0)
			value = -value;

		sql_buffer_setbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->values, &value, sizeof(value));
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}
#endif

static size_t
put_date_value(SQLattribute *attr, int row_index,
			   const char *addr, int sz)
{
	size_t		usage;

	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append_zero(&attr->values, attr->attlen);
	}
    else
    {
		DateADT	value;

		assert(sz == sizeof(DateADT));
		sql_buffer_setbit(&attr->nullmap, row_index);
		value = ntohl(*((const DateADT *)addr));
		value -= UNIX_EPOCH_JDATE;
		sql_buffer_append(&attr->values, &value, sz);
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}

static size_t
put_timestamp_value(SQLattribute *attr, int row_index,
					const char *addr, int sz)
{
	size_t		usage;

	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append_zero(&attr->values, attr->attlen);
	}
	else
	{
		Timestamp	value;
		uint32		h, l;

		assert(sz == sizeof(Timestamp));
		sql_buffer_setbit(&attr->nullmap, row_index);
		h = ((const uint32 *)addr)[0];
		l = ((const uint32 *)addr)[1];
		value = (Timestamp)ntohl(h) << 32 | (Timestamp)ntohl(l);
		/* convert PostgreSQL epoch to UNIX epoch */
		value += (POSTGRES_EPOCH_JDATE -
				  UNIX_EPOCH_JDATE) * USECS_PER_DAY;
		sql_buffer_append(&attr->values, &value, sizeof(Timestamp));
	}
	usage = ARROWALIGN(attr->values.usage);
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return 0;
}

static size_t
put_variable_value(SQLattribute *attr, int row_index,
				   const char *addr, int sz)
{
	size_t		usage;

	if (row_index == 0)
		sql_buffer_append_zero(&attr->values, sizeof(uint32));
	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->values, &attr->extra.usage, sizeof(uint32));
	}
	else
	{
		assert(attr->attlen == -1 || attr->attlen == sz);
		sql_buffer_setbit(&attr->nullmap, row_index);
		sql_buffer_append(&attr->extra, addr, sz);
		sql_buffer_append(&attr->values, &attr->extra.usage, sizeof(uint32));
	}
	usage = (ARROWALIGN(attr->values.usage) +
			 ARROWALIGN(attr->extra.usage));
	if (attr->nullcount > 0)
		usage += ARROWALIGN(attr->nullmap.usage);
	return usage;
}

static size_t
put_array_value(SQLattribute *attr, int row_index,
				const char *addr, int sz)
{
	Elog("not supported yet");
	return 0;
}

static size_t
put_composite_value(SQLattribute *attr, int row_index,
					const char *addr, int sz)
{
	/* see record_send() */
	SQLtable   *subtypes = attr->subtypes;
	size_t		usage = 0;
	int			j, nvalids;

	if (!addr)
	{
		attr->nullcount++;
		sql_buffer_clrbit(&attr->nullmap, row_index);
		usage += ARROWALIGN(attr->nullmap.usage);
		/* null for all the subtypes */
		for (j=0; j < subtypes->nfields; j++)
		{
			SQLattribute *subattr = &subtypes->attrs[j];
			usage += subattr->put_value(subattr, row_index, NULL, 0);
		}
	}
	else
	{
		const char *pos = addr;

		sql_buffer_setbit(&attr->nullmap, row_index);
		if (sz < sizeof(uint32))
			Elog("binary composite record corruption");
		if (attr->nullcount > 0)
			usage += ARROWALIGN(attr->nullmap.usage);
		nvalids = ntohl(*((const int *)pos));
		pos += sizeof(int);
		for (j=0; j < subtypes->nfields; j++)
		{
			SQLattribute *subattr = &subtypes->attrs[j];
			Oid		atttypid;
			int		attlen;

			if (j >= nvalids)
			{
				usage += subattr->put_value(subattr, row_index, NULL, 0);
				continue;
			}
			if ((pos - addr) + sizeof(Oid) + sizeof(int) > sz)
				Elog("binary composite record corruption");
			atttypid = ntohl(*((Oid *)pos));
			pos += sizeof(Oid);
			if (subattr->atttypid != atttypid)
				Elog("composite subtype mismatch");
			attlen = ntohl(*((int *)pos));
			pos += sizeof(int);
			if (attlen == -1)
			{
				usage += subattr->put_value(subattr, row_index, NULL, 0);
			}
			else
			{
				if ((pos - addr) + attlen > sz)
					Elog("binary composite record corruption");
				usage += subattr->put_value(subattr, row_index, pos, attlen);
				pos += attlen;
			}
		}
	}
	return usage;
}

/* ----------------------------------------------------------------
 *
 * stat_update handler for each data types (optional)
 *
 * ---------------------------------------------------------------- */
#define STAT_UPDATE_INLINE_TEMPLATE(TYPENAME,TO_DATUM)			\
	static void													\
	stat_update_##TYPENAME##_value(SQLattribute *attr,			\
								   const char *addr, int sz)	\
	{															\
		TYPENAME		value;									\
																\
		if (!addr)												\
			return;												\
		value = *((const TYPENAME *)addr);						\
		if (attr->min_isnull)									\
		{														\
			attr->min_isnull = false;							\
			attr->min_value  = TO_DATUM(value);					\
		}														\
		else if (value < attr->min_value)						\
			attr->min_value = value;							\
																\
		if (attr->max_isnull)									\
		{														\
			attr->max_isnull = false;							\
			attr->max_value  = TO_DATUM(value);					\
		}														\
		else if (value > attr->max_value)						\
			attr->max_value = value;							\
	}

STAT_UPDATE_INLINE_TEMPLATE(int8,   Int8GetDatum)
STAT_UPDATE_INLINE_TEMPLATE(int16,  Int16GetDatum)
STAT_UPDATE_INLINE_TEMPLATE(int32,  Int32GetDatum)
STAT_UPDATE_INLINE_TEMPLATE(int64,  Int64GetDatum)
STAT_UPDATE_INLINE_TEMPLATE(float4, Float4GetDatum)
STAT_UPDATE_INLINE_TEMPLATE(float8, Float8GetDatum)

/* ----------------------------------------------------------------
 *
 * setup_buffer handler for each data types
 *
 * ----------------------------------------------------------------
 */
static inline size_t
setup_arrow_buffer(ArrowBuffer *node, size_t offset, size_t length)
{
	memset(node, 0, sizeof(ArrowBuffer));
	node->tag = ArrowNodeTag__Buffer;
	node->offset = offset;
	node->length = MAXALIGN(length);

	return node->length;
}

static int
setup_buffer_inline_type(SQLattribute *attr,
						 ArrowBuffer *node, size_t *p_offset)
{
	size_t		offset = *p_offset;

	/* nullmap */
	if (attr->nullcount == 0)
		offset += setup_arrow_buffer(node, offset, 0);
	else
		offset += setup_arrow_buffer(node, offset, attr->nullmap.usage);
	/* inline values */
	offset += setup_arrow_buffer(node+1, offset, attr->values.usage);

	*p_offset = offset;
	return 2;	/* nullmap + values */
}

static int
setup_buffer_varlena_type(SQLattribute *attr,
						  ArrowBuffer *node, size_t *p_offset)
{
	size_t		offset = *p_offset;

	/* nullmap */
	if (attr->nullcount == 0)
		offset += setup_arrow_buffer(node, offset, 0);
	else
		offset += setup_arrow_buffer(node, offset, attr->nullmap.usage);
	/* index values */
	offset += setup_arrow_buffer(node+1, offset, attr->values.usage);
	/* extra buffer */
	offset += setup_arrow_buffer(node+2, offset, attr->extra.usage);

	*p_offset = offset;
	return 3;	/* nullmap + values (index) + extra buffer */
}

static int
setup_buffer_array_type(SQLattribute *attr,
						ArrowBuffer *node, size_t *p_offset)
{
	Elog("to be implemented");
	return -1;
}


static int
setup_buffer_composite_type(SQLattribute *attr,
							ArrowBuffer *node, size_t *p_offset)
{
	SQLtable   *subtypes = attr->subtypes;
	int			i, count = 1;

	/* nullmap */
	if (attr->nullcount == 0)
		*p_offset += setup_arrow_buffer(node, *p_offset, 0);
	else
		*p_offset += setup_arrow_buffer(node, *p_offset,
										attr->nullmap.usage);
	/* walk down the sub-types */
	for (i=0; i < subtypes->nfields; i++)
	{
		SQLattribute   *subattr = &subtypes->attrs[i];

		count += subattr->setup_buffer(subattr, node+count, p_offset);
	}
	return count;	/* nullmap + subtypes */
}

/* ----------------------------------------------------------------
 *
 * write buffer handler for each data types
 *
 * ----------------------------------------------------------------
 */
static inline void
write_buffer_common(int fdesc, const void *buffer, size_t length)
{
	ssize_t		nbytes;
	ssize_t		offset = 0;

	while (offset < length)
	{
		nbytes = write(fdesc, (const char *)buffer + offset, length - offset);
		if (nbytes < 0)
		{
			if (errno == EINTR)
				continue;
			Elog("failed on write(2): %m");
		}
		offset += nbytes;
	}

	if (length != MAXALIGN(length))
	{
		ssize_t	gap = MAXALIGN(length) - length;
		int64	zero = 0;

		offset = 0;
		while (offset < gap)
		{
			nbytes = write(fdesc, (const char *)&zero + offset, gap - offset);
			if (nbytes < 0)
			{
				if (errno == EINTR)
					continue;
				Elog("failed on write(2): %m");
			}
			offset += nbytes;
		}
	}
}

static void
write_buffer_inline_type(SQLattribute *attr, int fdesc)
{
	/* nullmap */
	if (attr->nullcount > 0)
		write_buffer_common(fdesc,
							attr->nullmap.ptr,
							attr->nullmap.usage);
	/* fixed length values */
	write_buffer_common(fdesc,
						attr->values.ptr,
						attr->values.usage);
}

static void
write_buffer_varlena_type(SQLattribute *attr, int fdesc)
{
	/* nullmap */
	if (attr->nullcount > 0)
		write_buffer_common(fdesc,
							attr->nullmap.ptr,
							attr->nullmap.usage);
	/* index values */
	write_buffer_common(fdesc,
						attr->values.ptr,
						attr->values.usage);
	/* extra buffer */
	write_buffer_common(fdesc,
						attr->extra.ptr,
						attr->extra.usage);
}

static void
write_buffer_array_type(SQLattribute *attr, int fdesc)
{
	Elog("not implemented yet");
}

static void
write_buffer_composite_type(SQLattribute *attr, int fdesc)
{
	SQLtable   *subtypes = attr->subtypes;
	int			i;

	/* nullmap */
	if (attr->nullcount > 0)
		write_buffer_common(fdesc,
							attr->nullmap.ptr,
							attr->nullmap.usage);
	/* sub-types */
	for (i=0; i < subtypes->nfields; i++)
	{
		SQLattribute   *subattr = &subtypes->attrs[i];

		subattr->write_buffer(subattr, fdesc);
	}
}

/* ----------------------------------------------------------------
 *
 * setup handler for each data types
 *
 * ----------------------------------------------------------------
 */
static void
assignArrowTypeInt(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag = ArrowNodeTag__Int;
	switch (attr->attlen)
	{
		case sizeof(char):
			attr->arrow_type.Int.bitWidth = 8;
			attr->put_value = put_inline_8b_value;
			attr->stat_update = stat_update_int8_value;
			break;
		case sizeof(short):
			attr->arrow_type.Int.bitWidth = 16;
			attr->put_value = put_inline_16b_value;
			attr->stat_update = stat_update_int16_value;
			break;
		case sizeof(int):
			attr->arrow_type.Int.bitWidth = 32;
			attr->put_value = put_inline_32b_value;
			attr->stat_update = stat_update_int32_value;
			break;
		case sizeof(long):
			attr->arrow_type.Int.bitWidth = 64;
			attr->put_value = put_inline_64b_value;
			attr->stat_update = stat_update_int64_value;
			break;
		default:
			Elog("unsupported Int width: %d", attr->attlen);
			break;
	}
	if (strcmp(attr->typname, "int2") == 0 ||
		strcmp(attr->typname, "int4") == 0 ||
		strcmp(attr->typname, "int8") == 0)
		attr->arrow_type.Int.is_signed = true;
	else
		attr->arrow_type.Int.is_signed = false;
	attr->setup_buffer = setup_buffer_inline_type;
	attr->write_buffer = write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
}

static void
assignArrowTypeFloatingPoint(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag = ArrowNodeTag__FloatingPoint;
	switch (attr->attlen)
	{
		case sizeof(short):		/* half */
			attr->arrow_type.FloatingPoint.precision = ArrowPrecision__Half;
			attr->put_value = put_inline_16b_value;
			break;
		case sizeof(float):
			attr->arrow_type.FloatingPoint.precision = ArrowPrecision__Single;
			attr->put_value = put_inline_32b_value;
			attr->stat_update = stat_update_float4_value;
			break;
		case sizeof(double):
			attr->arrow_type.FloatingPoint.precision = ArrowPrecision__Double;
			attr->put_value = put_inline_64b_value;
			attr->stat_update = stat_update_float8_value;
			break;
		default:
			Elog("unsupported floating point width: %d", attr->attlen);
			break;
	}
	attr->setup_buffer = setup_buffer_inline_type;
	attr->write_buffer = write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
}

static void
assignArrowTypeBinary(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__Binary;
	attr->put_value			= put_variable_value;
	attr->setup_buffer		= setup_buffer_varlena_type;
	attr->write_buffer		= write_buffer_varlena_type;

	*p_numBuffers += 3;		/* nullmap + index + extra */
}

static void
assignArrowTypeUtf8(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__Utf8;
	attr->put_value			= put_variable_value;
	attr->setup_buffer		= setup_buffer_varlena_type;
	attr->write_buffer		= write_buffer_varlena_type;

	*p_numBuffers += 3;		/* nullmap + index + extra */
}

static void
assignArrowTypeBool(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__Bool;
	attr->put_value			= put_inline_8b_value;
	attr->stat_update		= stat_update_int8_value;
	attr->setup_buffer		= setup_buffer_inline_type;
	attr->write_buffer		= write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
}

static void
assignArrowTypeDecimal(SQLattribute *attr, int *p_numBuffers)
{
#ifdef PG_INT128_TYPE
	int		typmod			= attr->atttypmod;
	int		precision		= 30;	/* default, if typmod == -1 */
	int		scale			= 11;	/* default, if typmod == -1 */

	if (typmod >= VARHDRSZ)
	{
		typmod -= VARHDRSZ;
		precision = (typmod >> 16) & 0xffff;
		scale = (typmod & 0xffff);
	}
	printf("precision=%d scale=%d\n", precision, scale);

	memset(&attr->arrow_type, 0, sizeof(ArrowType));
	attr->arrow_type.tag	= ArrowNodeTag__Decimal;
	attr->arrow_type.Decimal.precision = precision;
	attr->arrow_type.Decimal.scale = scale;
	attr->put_value			= put_decimal_value;
	attr->setup_buffer		= setup_buffer_inline_type;
	attr->write_buffer		= write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
#else
	/*
	 * MEMO: Numeric of PostgreSQL is mapped to Decimal128 in Apache Arrow.
	 * Due to implementation reason, we require int128 support by compiler.
	 */
	Elog("Numeric type of PostgreSQL is not supported in this build");
#endif
}

static void
assignArrowTypeDate(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__Date;
	attr->arrow_type.Date.unit = ArrowDateUnit__Day;
	attr->put_value			= put_date_value;
	attr->stat_update		= stat_update_int32_value;
	attr->setup_buffer		= setup_buffer_inline_type;
	attr->write_buffer		= write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
}

static void
assignArrowTypeTime(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__Time;
	attr->arrow_type.Time.unit = ArrowTimeUnit__MicroSecond;
	attr->arrow_type.Time.bitWidth = 64;
	attr->put_value			= put_inline_64b_value;
	attr->stat_update		= stat_update_int64_value;
	attr->setup_buffer		= setup_buffer_inline_type;
	attr->write_buffer		= write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
}

static void
assignArrowTypeTimestamp(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__Timestamp;
	attr->arrow_type.Timestamp.unit = ArrowTimeUnit__MicroSecond;
	attr->put_value			= put_timestamp_value;
	attr->stat_update		= stat_update_int64_value;
	attr->setup_buffer		= setup_buffer_inline_type;
	attr->write_buffer		= write_buffer_inline_type;

	*p_numBuffers += 2;		/* nullmap + values */
}

#if 0
static void
assignArrowTypeInterval(SQLattribute *attr, int *p_numBuffers)
{
	Elog("Interval is not supported yet");
}
#endif

static void
assignArrowTypeList(SQLattribute *attr, int *p_numBuffers)
{
	attr->arrow_type.tag	= ArrowNodeTag__List;
	attr->put_value			= put_array_value;
	attr->setup_buffer		= setup_buffer_array_type;
	attr->write_buffer		= write_buffer_array_type;

	*p_numBuffers += 999; //???
}

static void
assignArrowTypeStruct(SQLattribute *attr, int *p_numBuffers)
{
	assert(attr->subtypes != NULL);
	attr->arrow_type.tag	= ArrowNodeTag__Struct;
	attr->put_value			= put_composite_value;
	attr->setup_buffer		= setup_buffer_composite_type;
	attr->write_buffer		= write_buffer_composite_type;

	*p_numBuffers += 1;		/* only nullmap */
}

/*
 * assignArrowType
 */
void
assignArrowType(SQLattribute *attr, int *p_numBuffers)
{
	memset(&attr->arrow_type, 0, sizeof(ArrowType));
	if (attr->subtypes)
	{
		assignArrowTypeStruct(attr, p_numBuffers);
		return;
	}
	else if (attr->elemtype)
	{
		assignArrowTypeList(attr, p_numBuffers);
		return;
	}
	else if (strcmp(attr->typnamespace, "pg_catalog") == 0)
	{
		/* well known built-in data types? */
		if (strcmp(attr->typname, "bool") == 0)
		{
			assignArrowTypeBool(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "int2") == 0 ||
				 strcmp(attr->typname, "int4") == 0 ||
				 strcmp(attr->typname, "int8") == 0)
		{
			assignArrowTypeInt(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "float4") == 0 ||
				 strcmp(attr->typname, "float8") == 0)
		{
			assignArrowTypeFloatingPoint(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "date") == 0)
		{
			assignArrowTypeDate(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "time") == 0)
		{
			assignArrowTypeTime(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "timestamp") == 0 ||
				 strcmp(attr->typname, "timestamptz") == 0)
		{
			assignArrowTypeTimestamp(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "text") == 0 ||
				 strcmp(attr->typname, "varchar") == 0 ||
				 strcmp(attr->typname, "bpchar") == 0)
		{
			assignArrowTypeUtf8(attr, p_numBuffers);
			return;
		}
		else if (strcmp(attr->typname, "numeric") == 0)
		{
			assignArrowTypeDecimal(attr, p_numBuffers);
			return;
		}
	}
	/* elsewhere, we save the column just a bunch of binary data */
	if (attr->attlen > 0)
	{
		if (attr->attlen == sizeof(char) ||
			attr->attlen == sizeof(short) ||
			attr->attlen == sizeof(int) ||
			attr->attlen == sizeof(long))
		{
			assignArrowTypeInt(attr, p_numBuffers);
			return;
		}
		/*
		 * MEMO: Unfortunately, we have no portable way to pack user defined
		 * fixed-length binary data types, because their 'send' handler often
		 * manipulate its internal data representation.
		 * Please check box_send() for example. It sends four float8 (which
		 * is reordered to bit-endien) values in 32bytes. We cannot understand
		 * its binary format without proper knowledge.
		 */
	}
	else if (attr->attlen == -1)
	{
		assignArrowTypeBinary(attr, p_numBuffers);
		return;
	}
	Elog("PostgreSQL type: '%s.%s' is not supported",
		 attr->typnamespace,
		 attr->typname);
}
