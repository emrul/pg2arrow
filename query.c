/*
 * query.c - interaction to PostgreSQL server
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"

#define atooid(x)		((Oid) strtoul((x), NULL, 10))
#define InvalidOid		((Oid) 0)

/* forward declarations */
static SQLtable *
pgsql_create_composite_type(PGconn *conn, Oid comptype_relid);
static SQLattribute *
pgsql_create_array_element(PGconn *conn, Oid array_elemid);

static inline bool
pg_strtobool(const char *v)
{
	if (strcasecmp(v, "t") == 0 ||
		strcasecmp(v, "true") == 0 ||
		strcmp(v, "1") == 0)
		return true;
	else if (strcasecmp(v, "f") == 0 ||
			 strcasecmp(v, "false") == 0 ||
			 strcmp(v, "0") == 0)
		return false;
	Elog("unexpected boolean type literal: %s", v);
}

static inline char
pg_strtochar(const char *v)
{
	if (strlen(v) == 0)
		Elog("unexpected empty string");
	if (strlen(v) > 1)
		Elog("unexpected character string");
	return *v;
}

/*
 * SQLbuffer related routines
 */
static inline void
sql_buffer_init(SQLbuffer *buf)
{
	buf->ptr = NULL;
	buf->usage = 0;
	buf->length = 0;
}

static void
sql_buffer_expand(SQLbuffer *buf, size_t required)
{
	if (buf->length < required)
	{
		void	   *ptr;
		size_t		length;

		if (buf->ptr == NULL)
		{
			length = (1UL << 21);	/* start from 2MB */
			while (length < required)
				length *= 2;
			ptr = mmap(NULL, length, PROT_READ | PROT_WRITE,
					   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			if (ptr == MAP_FAILED)
				Elog("failed on mmap(len=%zu): %m", length);
			buf->ptr    = ptr;
			buf->usage  = 0;
			buf->length = length;
		}
		else
		{
			length = 2 * buf->length;
			while (length < required)
				length *= 2;
			ptr = mremap(buf->ptr, buf->length, length, MREMAP_MAYMOVE);
			if (ptr == MAP_FAILED)
				Elog("failed on mremap(len=%zu): %m", length);
			buf->ptr    = ptr;
			buf->length = length;
		}
	}
}

static inline void
sql_buffer_append(SQLbuffer *buf, const void *src, size_t len)
{
	sql_buffer_expand(buf, buf->usage + len);
	memcpy(buf->ptr + buf->usage, src, len);
	buf->usage += len;
	assert(buf->usage <= buf->length);
}

static inline void
sql_buffer_append_zero(SQLbuffer *buf, size_t len)
{
	sql_buffer_expand(buf, buf->usage + len);
	memset(buf->ptr + buf->usage, 0, len);
	buf->usage += len;
	assert(buf->usage <= buf->length);
}

static inline void
sql_buffer_setbit(SQLbuffer *buf, size_t index)
{
	size_t		required = BITMAPLEN(index+1);
	sql_buffer_expand(buf, required);
	((uint8 *)buf->ptr)[index>>3] |= (1 << (index & 7));
}

static inline void
sql_buffer_clrbit(SQLbuffer *buf, size_t index)
{
	size_t		required = BITMAPLEN(index+1);
	sql_buffer_expand(buf, required);
	((uint8 *)buf->ptr)[index>>3] &= ~(1 << (index & 7));
}

static inline void
sql_buffer_clear(SQLbuffer *buf)
{
	buf->usage = 0;
}

/*
 * type handlers for each Arrow type
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

static size_t
put_decimal_value(SQLattribute *attr, int row_index,
				  const char *addr, int sz)
{
	Elog("not supported now");
	return 0;
}

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

/*
 * attribute_assign_arrow_type
 */
static void
attribute_assign_arrow_type(SQLattribute *attr,
							const char *nspname,
							const char *typname)
{
	memset(&attr->arrow_type, 0, sizeof(ArrowType));
	if (attr->subtypes)
	{
		/* composite type */
		attr->arrow_type.tag = ArrowNodeTag__Struct;
		attr->put_value = put_composite_value;
		Elog("composite in PostgreSQL / Struct in Arrow is not supported now");
	}
	else if (attr->elemtype)
	{
		attr->arrow_type.tag = ArrowNodeTag__List;
		attr->put_value = put_array_value;
		Elog("array in PostgreSQL / List in Arrow is not supported now");
	}
	else if (strcmp(nspname, "pg_catalog") == 0)
	{
		/* built-in data type? */
		if (strcmp(typname, "bool") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Bool;
			attr->put_value = put_inline_8b_value;
			attr->stat_update = stat_update_int8_value;
			return;
		}
		else if (strcmp(typname, "int2") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 16;
			attr->arrow_type.Int.is_signed = true;
			attr->put_value = put_inline_16b_value;
			attr->stat_update = stat_update_int16_value;
		}
		else if (strcmp(typname, "int4") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 32;
			attr->arrow_type.Int.is_signed = true;
			attr->put_value = put_inline_32b_value;
			attr->stat_update = stat_update_int32_value;
		}
		else if (strcmp(typname, "int8") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 64;
			attr->arrow_type.Int.is_signed = true;
			attr->put_value = put_inline_64b_value;
			attr->stat_update = stat_update_int64_value;
		}
		else if (strcmp(typname, "float4") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__FloatingPoint;
			attr->arrow_type.FloatingPoint.precision = 32;
			attr->put_value = put_inline_32b_value;
			attr->stat_update = stat_update_float4_value;
		}
		else if (strcmp(typname, "float8") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__FloatingPoint;
			attr->arrow_type.FloatingPoint.precision = 64;
			attr->put_value = put_inline_64b_value;
			attr->stat_update = stat_update_float8_value;
		}
		else if (strcmp(typname, "date") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Date;
			attr->arrow_type.Date.unit = ArrowDateUnit__Day;
			attr->put_value = put_date_value;
			attr->stat_update = stat_update_int32_value;
		}
		else if (strcmp(typname, "time") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Time;
			attr->arrow_type.Time.unit = ArrowTimeUnit__MicroSecond;
			attr->arrow_type.Time.bitWidth = 64;
			attr->put_value = put_inline_64b_value;
			attr->stat_update = stat_update_int64_value;
		}
		else if (strcmp(typname, "timestamp") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Timestamp;
			attr->arrow_type.Timestamp.unit = ArrowTimeUnit__MicroSecond;
			attr->put_value = put_timestamp_value;
			attr->stat_update = stat_update_int64_value;
		}
		else if (strcmp(typname, "text") == 0 ||
				 strcmp(typname, "varchar") == 0 ||
				 strcmp(typname, "bpchar") == 0)
		{
			attr->arrow_type.tag = ArrowNodeTag__Utf8;
			attr->put_value = put_variable_value;
		}
		else if (strcmp(typname, "numeric") == 0 &&
				 attr->atttypmod >= VARHDRSZ)
		{
			int		typmod = attr->atttypmod - VARHDRSZ;
			int		precision = (typmod >> 16) & 0xffff;
			int		scale = (typmod & 0xffff);
			/*
			 * Memo: the upper 16bit of (typmod - VARHDRSZ) is precision,
			 * the lower 16bit is scale of the numeric data type.
			 */
			attr->arrow_type.tag = ArrowNodeTag__Decimal;
			attr->arrow_type.Decimal.precision = precision;
			attr->arrow_type.Decimal.scale = scale;
			attr->put_value = put_decimal_value;
		}
	}
	if (attr->put_value != NULL)
		return;
	/* elsewhere, use generic copy function */
	if (attr->attlen > 0)
	{
		if (attr->attlen == sizeof(uint8))
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 8;
			attr->arrow_type.Int.is_signed = false;
			attr->put_value = put_inline_8b_value;
		}
		else if (attr->attlen == sizeof(uint16))
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 16;
			attr->arrow_type.Int.is_signed = false;
			attr->put_value = put_inline_16b_value;
		}
		else if (attr->attlen == sizeof(uint32))
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 32;
			attr->arrow_type.Int.is_signed = false;
			attr->put_value = put_inline_32b_value;
		}
		else if (attr->attlen == sizeof(uint64))
		{
			attr->arrow_type.tag = ArrowNodeTag__Int;
			attr->arrow_type.Int.bitWidth = 64;
			attr->arrow_type.Int.is_signed = false;
			attr->put_value = put_inline_64b_value;
		}
		else
		{
			attr->arrow_type.tag = ArrowNodeTag__FixedSizeBinary;
			attr->arrow_type.FixedSizeBinary.byteWidth = attr->attlen;
			//FIXME: Is it right handler??
			attr->put_value = put_variable_value;
		}
	}
	else if (attr->attlen == -1)
	{
		attr->arrow_type.tag = ArrowNodeTag__Binary;
		attr->put_value = put_variable_value;
	}
	else
		Elog("cannot handle PG data type '%s'", typname);
}

/*
 * pgsql_setup_attribute
 */
static void
pgsql_setup_attribute(PGconn *conn,
					  SQLattribute *attr,
					  const char *attname,
					  Oid atttypid,
					  int atttypmod,
					  int attlen,
					  char attbyval,
					  char attalign,
					  char typtype,
					  Oid comp_typrelid,
					  Oid array_elemid,
					  const char *nspname,
					  const char *typname)
{
	attr->attname   = pstrdup(attname);
	attr->atttypid  = atttypid;
	attr->atttypmod = atttypmod;
	attr->attlen    = attlen;
	attr->attbyval  = attbyval;

	if (attalign == 'c')
		attr->attalign = sizeof(char);
	else if (attalign == 's')
		attr->attalign = sizeof(short);
	else if (attalign == 'i')
		attr->attalign = sizeof(int);
	else if (attalign == 'd')
		attr->attalign = sizeof(double);
	else
		Elog("unknown state of attalign: %c", attalign);

	attr->typnamespace = pstrdup(nspname);
	attr->typname = pstrdup(typname);
	attr->typtype = typtype;
	if (typtype == 'b')
	{
		if (array_elemid != InvalidOid)
			attr->elemtype = pgsql_create_array_element(conn, array_elemid);
	}
	else if (typtype == 'c')
	{
		/* composite data type */
		assert(comp_typrelid != 0);
		attr->subtypes = pgsql_create_composite_type(conn, comp_typrelid);
	}
#if 0
	else if (typtype == 'd')
	{
		//Domain type has identical definition to the base type
		//expect for its constraint.
	}
	else if (typtype == 'e')
	{
		//Enum type may be ideal for dictionary compression
	}
#endif
	else
		Elog("unknown state pf typtype: %c", typtype);

	attribute_assign_arrow_type(attr, nspname, typname);
	/* init statistics */
	attr->min_isnull = true;
	attr->max_isnull = true;
	attr->min_value  = 0UL;
	attr->max_value  = 0UL;
}

/*
 * pgsql_create_composite_type
 */
static SQLtable *
pgsql_create_composite_type(PGconn *conn, Oid comptype_relid)
{
	PGresult   *res;
	SQLtable   *table;
	char		query[4096];
	int			j, nfields;

	snprintf(query, sizeof(query),
			 "SELECT attname, attnum, atttypid, atttypmod, attlen,"
			 "       attbyval, attalign, typtype, typrelid, typelem,"
			 "       nspname, typname"
			 "  FROM pg_catalog.pg_attribute a,"
			 "       pg_catalog.pg_type t,"
			 "       pg_catalog.pg_namespace n"
			 " WHERE t.typnamespace = n.oid"
			 "   AND a.atttypid = t.oid"
			 "   AND a.attrelid = %u", comptype_relid);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		Elog("failed on pg_type system catalog query: %s",
			 PQresultErrorMessage(res));

	nfields = PQntuples(res);
	table = palloc0(offsetof(SQLtable, attrs[nfields]));
	table->nfields = nfields;
	for (j=0; j < nfields; j++)
	{
		const char *attname   = PQgetvalue(res, j, 0);
		const char *attnum    = PQgetvalue(res, j, 1);
		const char *atttypid  = PQgetvalue(res, j, 2);
		const char *atttypmod = PQgetvalue(res, j, 3);
		const char *attlen    = PQgetvalue(res, j, 4);
		const char *attbyval  = PQgetvalue(res, j, 5);
		const char *attalign  = PQgetvalue(res, j, 6);
		const char *typtype   = PQgetvalue(res, j, 7);
		const char *typrelid  = PQgetvalue(res, j, 8);
		const char *typelem   = PQgetvalue(res, j, 9);
		const char *nspname   = PQgetvalue(res, j, 10);
		const char *typname   = PQgetvalue(res, j, 11);
		int			index     = atoi(attnum);

		if (index < 1 || index > nfields)
			Elog("attribute number is out of range");
		pgsql_setup_attribute(conn,
							  &table->attrs[index-1],
							  attname,
							  atooid(atttypid),
							  atoi(atttypmod),
							  atoi(attlen),
							  pg_strtobool(attbyval),
							  pg_strtochar(attalign),
							  pg_strtochar(typtype),
							  atooid(typrelid),
							  atooid(typelem),
							  nspname, typname);
	}
	return table;
}

static SQLattribute *
pgsql_create_array_element(PGconn *conn, Oid array_elemid)
{
	SQLattribute   *attr = palloc0(sizeof(SQLattribute));
	PGresult	   *res;
	char			query[4096];
	const char     *nspname;
	const char	   *typname;
	const char	   *typlen;
	const char	   *typbyval;
	const char	   *typalign;
	const char	   *typtype;
	const char	   *typrelid;
	const char	   *typelem;

	snprintf(query, sizeof(query),
			 "SELECT nspname, typname,"
			 "       typlen, typbyval, typalign, typtype,"
			 "       typrelid, typelem,"
			 "  FROM pg_catalog.pg_type t,"
			 "       pg_catalog.pg_namespace n"
			 " WHERE t.typnamespace = n.oid"
			 "   AND t.oid = %u", array_elemid);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		Elog("failed on pg_type system catalog query: %s",
			 PQresultErrorMessage(res));
	if (PQntuples(res) != 1)
		Elog("unexpected number of result rows: %d", PQntuples(res));
	nspname  = PQgetvalue(res, 0, 0);
	typname  = PQgetvalue(res, 0, 1);
	typlen   = PQgetvalue(res, 0, 2);
	typbyval = PQgetvalue(res, 0, 3);
	typalign = PQgetvalue(res, 0, 4);
	typtype  = PQgetvalue(res, 0, 5);
	typrelid = PQgetvalue(res, 0, 6);
	typelem  = PQgetvalue(res, 0, 7);

	pgsql_setup_attribute(conn,
						  attr,
						  typname,
						  array_elemid,
						  -1,
						  atoi(typlen),
						  pg_strtobool(typbyval),
						  pg_strtochar(typalign),
						  pg_strtochar(typtype),
						  atooid(typrelid),
						  atooid(typelem),
						  nspname,
						  typname);
	return attr;
}

/*
 * pgsql_create_buffer
 */
SQLtable *
pgsql_create_buffer(PGconn *conn, PGresult *res, size_t segment_sz)
{
	int			j, nfields = PQnfields(res);
	SQLtable   *table;

	table = palloc0(offsetof(SQLtable, attrs[nfields]));
	table->segment_sz = segment_sz;
	table->nitems = 0;
	table->nfields = nfields;
	for (j=0; j < nfields; j++)
	{
		const char *attname = PQfname(res, j);
		Oid			atttypid = PQftype(res, j);
		int			atttypmod = PQfmod(res, j);
		PGresult   *__res;
		char		query[4096];
		const char *typlen;
		const char *typbyval;
		const char *typalign;
		const char *typtype;
		const char *typrelid;
		const char *typelem;
		const char *nspname;
		const char *typname;

		snprintf(query, sizeof(query),
				 "SELECT typlen, typbyval, typalign, typtype,"
				 "       typrelid, typelem, nspname, typname"
				 "  FROM pg_catalog.pg_type t,"
				 "       pg_catalog.pg_namespace n"
				 " WHERE t.typnamespace = n.oid"
				 "   AND t.oid = %u", atttypid);
		__res = PQexec(conn, query);
		if (PQresultStatus(__res) != PGRES_TUPLES_OK)
			Elog("failed on pg_type system catalog query: %s",
				 PQresultErrorMessage(res));
		if (PQntuples(__res) != 1)
			Elog("unexpected number of result rows: %d", PQntuples(__res));
		typlen   = PQgetvalue(__res, 0, 0);
		typbyval = PQgetvalue(__res, 0, 1);
		typalign = PQgetvalue(__res, 0, 2);
		typtype  = PQgetvalue(__res, 0, 3);
		typrelid = PQgetvalue(__res, 0, 4);
		typelem  = PQgetvalue(__res, 0, 5);
		nspname  = PQgetvalue(__res, 0, 6);
		typname  = PQgetvalue(__res, 0, 7);
		pgsql_setup_attribute(conn,
							  &table->attrs[j],
                              attname,
							  atttypid,
							  atttypmod,
							  atoi(typlen),
							  *typbyval,
							  *typalign,
							  *typtype,
							  atoi(typrelid),
							  atoi(typelem),
							  nspname, typname);
		PQclear(__res);
	}
	return table;
}

/*
 * pgsql_clear_attribute
 */
static void
pgsql_clear_attribute(SQLattribute *attr)
{
	attr->nullcount = 0;
	sql_buffer_clear(&attr->nullmap);
	sql_buffer_clear(&attr->values);
	sql_buffer_clear(&attr->extra);

	if (attr->subtypes)
	{
		SQLtable   *subtypes = attr->subtypes;
		int			j;

		for (j=0; j < subtypes->nfields; j++)
			pgsql_clear_attribute(&subtypes->attrs[j]);
	}
	if (attr->elemtype)
		pgsql_clear_attribute(attr->elemtype);
	/* clear statistics */
	attr->min_isnull = true;
	attr->max_isnull = true;
	attr->min_value  = 0UL;
	attr->max_value  = 0UL;
}

/*
 * pgsql_writeout_buffer
 */
void
pgsql_writeout_buffer(SQLtable *table)
{
	int		j;

	printf("writeout nitems=%zu\n", table->nitems);

	/* makes table/attributes empty */
	table->nitems = 0;
	for (j=0; j < table->nfields; j++)
		pgsql_clear_attribute(&table->attrs[j]);
}

/*
 * pgsql_append_results
 */
void
pgsql_append_results(SQLtable *table, PGresult *res)
{
	int		i, ntuples = PQntuples(res);
	int		j, nfields = PQnfields(res);
	size_t	usage;

	assert(nfields == table->nfields);
	for (i=0; i < ntuples; i++)
	{
	retry:
		usage = 0;
		for (j=0; j < nfields; j++)
		{
			SQLattribute   *attr = &table->attrs[j];
			const char	   *addr;
			size_t			sz;
			/* data must be binary format */
			assert(PQfformat(res, j) == 1);
			if (PQgetisnull(res, i, j))
			{
				addr = NULL;
				sz = 0;
			}
			else
			{
				addr = PQgetvalue(res, i, j);
				sz = PQgetlength(res, i, j);
			}
			usage += attr->put_value(attr, table->nitems, addr, sz);
		}
		/* check threshold to write out */
		if (usage > table->segment_sz)
		{
			if (table->nitems == 0)
				Elog("A result row is larger than size of record batch!!");
			/* fixup NULL-count if last row updated it */
			for (j=0; j < nfields; j++)
			{
				SQLattribute   *attr = &table->attrs[j];

				if (PQgetisnull(res, i, j))
				{
					assert(attr->nullcount > 0);
					attr->nullcount--;
				}
			}
			/* write out the bunch of query results */
			pgsql_writeout_buffer(table);
			goto retry;
		}
		/* update statistics */
		for (j=0; j < nfields; j++)
		{
			SQLattribute   *attr = &table->attrs[j];
			const char	   *addr;
			size_t			sz;

			if (!attr->stat_update)
				continue;
			if (PQgetisnull(res, i, j))
			{
				addr = NULL;
				sz = 0;
			}
			else
			{
				addr = PQgetvalue(res, i, j);
				sz = PQgetlength(res, i, j);
			}
			attr->stat_update(attr, addr, sz);
		}
		table->nitems++;
	}
}

/*
 * pgsql_dump_attribute
 */
static void
pgsql_dump_attribute(SQLattribute *attr, const char *label, int indent)
{
	int		j;

	for (j=0; j < indent; j++)
		putchar(' ');
	printf("%s {attname='%s', atttypid=%u, atttypmod=%d, attlen=%d,"
		   " attbyval=%s, attalign=%d, typtype=%c, arrow_type=",
		   label,
		   attr->attname, attr->atttypid, attr->atttypmod, attr->attlen,
		   attr->attbyval ? "true" : "false", attr->attalign, attr->typtype);
	dumpArrowNode((ArrowNode *)&attr->arrow_type, stdout);
	printf("}\n");

	if (attr->typtype == 'b')
	{
		if (attr->elemtype)
			pgsql_dump_attribute(attr->elemtype, "element", indent+2);
	}
	else if (attr->typtype == 'c')
	{
		SQLtable   *subtypes = attr->subtypes;
		char		label[64];

		for (j=0; j < subtypes->nfields; j++)
		{
			snprintf(label, sizeof(label), "subtype[%d]", j);
			pgsql_dump_attribute(&subtypes->attrs[j], label, indent+2);
		}
	}
}

/*
 * pgsql_dump_buffer
 */
void
pgsql_dump_buffer(SQLtable *table)
{
	int		j;
	char	label[64];

	printf("Dump of SQL buffer:\n"
		   "nfields: %d\n"
		   "nitems: %zu\n",
		   table->nfields,
		   table->nitems);
	for (j=0; j < table->nfields; j++)
	{
		snprintf(label, sizeof(label), "attr[%d]", j);
		pgsql_dump_attribute(&table->attrs[j], label, 0);
	}
}
