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

static SQLtable *pgsql_create_composite_type(PGconn *conn,
											 Oid comptype_relid);
static SQLattribute *pgsql_create_array_element(PGconn *conn,
												Oid array_elemid);

/*
 * pgsql_type_to_garrow_data_type
 */
static GArrowType
pgsql_type_to_garrow_data_type(SQLattribute *attr,
							   const char *nspname,
							   const char *typname)
{
	if (attr->subtypes)
		return GARROW_TYPE_STRUCT;
	if (attr->elemtype)
		return GARROW_TYPE_LIST;

	/* built-in data type? */
	if (strcmp(nspname, "pg_catalog") == 0)
	{
		if (strcmp(typname, "bool") == 0)
			return GARROW_TYPE_BOOLEAN;
		else if (strcmp(typname, "int2") == 0)
			return GARROW_TYPE_INT16;
		else if (strcmp(typname, "int4") == 0)
			return GARROW_TYPE_INT32;
		else if (strcmp(typname, "int8") == 0)
			return GARROW_TYPE_INT64;
		//else if (strcmp(typname, "float2") == 0)
		//	return GARROW_TYPE_HALF_FLOAT;
		else if (strcmp(typname, "float4") == 0)
			return GARROW_TYPE_FLOAT;
		else if (strcmp(typname, "float8") == 0)
			return GARROW_TYPE_DOUBLE;
		else if (strcmp(typname, "text") == 0 ||
				 strcmp(typname, "varchar") == 0 ||
				 strcmp(typname, "bpchar") == 0)
			return GARROW_TYPE_STRING;
		else if (strcmp(typname, "numeric") == 0 &&
				 attr->atttypmod >= VARHDRSZ)
		{
			/*
			 * Memo: the upper 16bit of (typmod - VARHDRSZ) is precision,
			 * the lower 16bit is scale of the numeric data type.
			 */
			return GARROW_TYPE_DECIMAL;
		}
	}

	if (attr->attlen > 0)
	{
		if (attr->attlen == sizeof(uchar))
			return GARROW_TYPE_UINT8;
		else if (attr->attlen == sizeof(ushort))
			return GARROW_TYPE_UINT16;
		else if (attr->attlen == sizeof(uint))
			return GARROW_TYPE_UINT32;
		else if (attr->attlen == sizeof(ulong))
			return GARROW_TYPE_UINT64;
		else
			return GARROW_TYPE_BINARY;
	}
	else if (attr->attlen == -1)
		return GARROW_TYPE_BINARY;
	else
		Elog("cannot handle PG data type '%s'", typname);
	return GARROW_TYPE_NA;	/* for compiler quiet */
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
	attr->attname   = pg_strdup(attname);
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

	attr->garrow_type = pgsql_type_to_garrow_data_type(attr,
													   nspname,
													   typname);
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
	table = pg_zalloc(offsetof(SQLtable, attrs[nfields]));
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
	SQLattribute   *attr = pg_zalloc(sizeof(SQLattribute));
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
pgsql_create_buffer(PGconn *conn, PGresult *res)
{
	int			j, nfields = PQnfields(res);
	SQLtable   *table;

	table = pg_zalloc(offsetof(SQLtable, attrs[nfields]));
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
 * pgsql_dump_attribute
 */
static void
pgsql_dump_attribute(SQLattribute *attr, const char *label, int indent)
{
	const char *garrow_type;
	int		j;

	switch (attr->garrow_type)
	{
		case GARROW_TYPE_BOOLEAN:    garrow_type = "boolean";    break;
		case GARROW_TYPE_UINT8:      garrow_type = "uint8";      break;
		case GARROW_TYPE_INT8:       garrow_type = "int8";       break;
		case GARROW_TYPE_UINT16:     garrow_type = "uint16";     break;
		case GARROW_TYPE_INT16:      garrow_type = "int16";      break;
		case GARROW_TYPE_UINT32:     garrow_type = "uint32";     break;
		case GARROW_TYPE_INT32:      garrow_type = "int32";      break;
		case GARROW_TYPE_UINT64:     garrow_type = "uint64";     break;
		case GARROW_TYPE_INT64:      garrow_type = "int64";      break;
		case GARROW_TYPE_HALF_FLOAT: garrow_type = "float2";     break;
		case GARROW_TYPE_FLOAT:      garrow_type = "float4";     break;
		case GARROW_TYPE_DOUBLE:     garrow_type = "float8";     break;
		case GARROW_TYPE_STRING:     garrow_type = "string";     break;
		case GARROW_TYPE_BINARY:     garrow_type = "binary";     break;
		case GARROW_TYPE_DATE32:     garrow_type = "date32";     break;
		case GARROW_TYPE_DATE64:     garrow_type = "date64";     break;
		case GARROW_TYPE_TIMESTAMP:  garrow_type = "timestamp";  break;
		case GARROW_TYPE_TIME32:     garrow_type = "time32";     break;
		case GARROW_TYPE_TIME64:     garrow_type = "time64";     break;
		case GARROW_TYPE_INTERVAL:   garrow_type = "interval";   break;
		case GARROW_TYPE_DECIMAL:    garrow_type = "decimal";    break;
		case GARROW_TYPE_LIST:       garrow_type = "list";       break;
		case GARROW_TYPE_STRUCT:     garrow_type = "struct";     break;
		case GARROW_TYPE_UNION:      garrow_type = "union";      break;
		case GARROW_TYPE_DICTIONARY: garrow_type = "dictionary"; break;
		default:   garrow_type = "unknown"; break;
	}

	for (j=0; j < indent; j++)
		putchar(' ');
	printf("%s {attname='%s', atttypid=%u, atttypmod=%d, attlen=%d,"
		   " attbyval=%s, attalign=%d, typtype=%c, arrow(%s)}\n",
		   label,
		   attr->attname, attr->atttypid, attr->atttypmod, attr->attlen,
		   attr->attbyval ? "true" : "false", attr->attalign, attr->typtype,
		   garrow_type);
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
		   "nitems: %zu\n"
		   "nrooms: %zu\n",
		   table->nfields,
		   table->nitems,
		   table->nrooms);
	for (j=0; j < table->nfields; j++)
	{
		snprintf(label, sizeof(label), "attr[%d]", j);
		pgsql_dump_attribute(&table->attrs[j], label, 0);
	}
}
