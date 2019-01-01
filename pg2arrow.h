/*
 * pg2arrow.h
 *
 * common header file
 */
#ifndef PG2ARROW_H
#define PG2ARROW_H

#include "postgres.h"
#include "access/htup_details.h"
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
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

typedef struct SQLbuffer		SQLbuffer;
typedef struct SQLtable			SQLtable;
typedef struct SQLattribute		SQLattribute;

struct SQLbuffer
{
	char	   *ptr;
	uint32		usage;
	uint32		length;
};

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
	size_t (*put_value)(SQLattribute *attr, int row_index,
						const char *addr, int sz);
	long		nullcount;		/* number of null values */
	SQLbuffer	nullmap;		/* null bitmap */
	SQLbuffer	values;			/* main storage of values */
	SQLbuffer	extra;			/* extra buffer for varlena */
};

struct SQLtable
{
	size_t		segment_sz;		/* threshold of the memory usage */
	size_t		nrooms;			/* threshold of the nitems */
	size_t		nitems;			/* current number of rows */
	int			nfields;		/* number of attributes */
	SQLattribute attrs[1];		/* flexible length */
};

/* buffer */
SQLtable   *pgsql_create_buffer(PGconn *conn, PGresult *res,
								size_t segment_sz, size_t nrooms);
void		pgsql_append_results(SQLtable *table, PGresult *res);
void        pgsql_writeout_buffer(SQLtable *table);
void		pgsql_dump_buffer(SQLtable *table);

/*
 * Error message and exit
 */
#define Elog(fmt, ...)								\
	do {											\
		fprintf(stderr,"L%d: " fmt "\n", __LINE__, ##__VA_ARGS__);	\
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
 * support routines for composite data type
 */



#endif	/* PG2ARROW_H */
