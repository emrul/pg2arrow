/*
 * pg2arrow.h
 *
 * common header file
 */
#ifndef PG2ARROW_H
#define PG2ARROW_H

#include "pg_config.h"
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>

#include <libpq-fe.h>
#include <arrow-glib/arrow-glib.h>

/* Basic type and label definitions */
typedef	char		bool;
#ifndef true
#define true		1
#endif
#ifndef false
#define false		0
#endif
typedef unsigned char	uchar;
typedef unsigned short	ushort;
typedef unsigned int	uint;
typedef unsigned long	ulong;

typedef struct SQLtable			SQLtable;
typedef struct SQLattribute		SQLattribute;

struct SQLattribute
{
	char	   *attname;
	Oid			atttypid;
	int			atttypmod;
	short		attlen;
	bool		attbyval;
	uchar		attalign;		/* 1, 2, 4 or 8 */
	char		typtype;		/* pg_type.typtype */
	SQLtable   *subtypes;		/* valid, if composite type */
	SQLattribute *elemtype;		/* valid, if array type */
	GArrowType	garrow_type;
	/* data buffer */
	char	   *nullmap;		/* != NULL, if any null values */
	char	   *values;			/* array of values */
};

struct SQLtable
{
	int			nfields;	/* number of attributes */
	size_t		nrooms;
	size_t		nitems;
	size_t		usage;
	SQLattribute attrs[1];	/* flexible length */
};

/* buffer */
SQLtable  *pgsql_create_buffer(PGconn *conn, PGresult *res);
void       pgsql_dump_buffer(SQLtable *table);

/*
 * Error message and exit
 */
#define Elog(fmt, ...)								\
	do {											\
		fprintf(stderr,fmt "\n", ##__VA_ARGS__);	\
		exit(1);									\
	} while(0)

/*
 * Memory operations
 */
static inline void *
pg_malloc(size_t sz)
{
	void   *ptr = malloc(sz);

	if (!ptr)
		Elog("out of memory");
	return ptr;
}

static inline void *
pg_zalloc(size_t sz)
{
	void   *ptr = malloc(sz);

	if (!ptr)
		Elog("out of memory");
	memset(ptr, 0, sz);
	return ptr;
}

static inline char *
pg_strdup(const char *str)
{
	char	   *temp = strdup(str);

	if (!temp)
		Elog("out of memory");
	return temp;
}

/*
 * support routines for varlena datum
 */
#define FLEXIBLE_ARRAY_MEMBER

typedef union
{
	struct		/* Normal varlena (4-byte length) */
	{
		uint	va_header;
		char	va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct		/* Compressed-in-line format */
	{
		uint	va_header;
		uint	va_rawsize;	/* Original data size (excludes header) */
		char	va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
    uchar		va_header;
    char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

#define VARHDRSZ		(sizeof(uint))
#define VARHDRSZ_SHORT	(sizeof(uchar))

/* little endian */
#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR)	(((varattrib_4b *) (PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)	(((varattrib_1b_e *) (PTR))->va_data)

#define VARDATA(PTR)		VARDATA_4B(PTR)
#define VARSIZE(PTR)		VARSIZE_4B(PTR)
#define VARSIZE_SHORT(PTR)	VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)	VARDATA_1B(PTR)

#define VARSIZE_ANY(PTR)										\
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) :				\
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) :						\
	  VARSIZE_4B(PTR)))

/* Size of a varlena data, excluding header */
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL :	\
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT :				\
	  VARSIZE_4B(PTR)-VARHDRSZ))

/*
 * support routines for composite data type
 */



#endif	/* PG2ARROW_H */
