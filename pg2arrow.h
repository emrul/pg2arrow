/*
 * pg2arrow.h
 *
 * common header file
 */
#ifndef PG2ARROW_H
#define PG2ARROW_H

#include "postgres.h"
#include "access/htup_details.h"
#include "datatype/timestamp.h"
#include "utils/date.h"
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
#include <unistd.h>
#include <time.h>

#include <libpq-fe.h>
#include "arrow_defs.h"

#define	ARROWALIGN(LEN)		TYPEALIGN(64, (LEN))

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
	uint8		attalign;		/* 1, 2, 4 or 8 */
	SQLtable   *subtypes;		/* valid, if composite type */
	SQLattribute *element;		/* valid, if array type */
	const char *typnamespace;	/* name of pg_type.typnamespace */
	const char *typname;		/* pg_type.typname */
	char		typtype;		/* pg_type.typtype */
	ArrowType	arrow_type;		/* type in apache arrow */
	const char *arrow_typename;	/* typename in apache arrow */
	/* data buffer and handler */
	void   (*put_value)(SQLattribute *attr,
						const char *addr, int sz);
	void   (*stat_update)(SQLattribute *attr,
						  const char *addr, int sz);
	size_t (*buffer_usage)(SQLattribute *attr);
	int	   (*setup_buffer)(SQLattribute *attr,
						   ArrowBuffer *node,
						   size_t *p_offset);
	void   (*write_buffer)(SQLattribute *attr, int fdesc);

	long		nitems;			/* number of rows */
	long		nullcount;		/* number of null values */
	SQLbuffer	nullmap;		/* null bitmap */
	SQLbuffer	values;			/* main storage of values */
	SQLbuffer	extra;			/* extra buffer for varlena */
	/* statistics */
	bool		min_isnull;
	bool		max_isnull;
	Datum		min_value;
	Datum		max_value;
};

struct SQLtable
{
	const char *filename;		/* output filename */
	int			fdesc;			/* output file descriptor */
	ArrowBlock *recordBatches;	/* recordBatches written in the past */
	int			numRecordBatches;
	ArrowBlock *dictionaries;	/* dictionaryBatches written in the past */
	int			numDictionaries;
	int			numFieldNodes;	/* # of FieldNode vector elements */
	int			numBuffers;		/* # of Buffer vector elements */
	size_t		segment_sz;		/* threshold of the memory usage */
	size_t		nitems;			/* current number of rows */
	int			nfields;		/* number of attributes */
	SQLattribute attrs[1];		/* flexible length */
};

/* pg2arrow.c */
extern int			shows_progress;
extern void			writeArrowRecordBatch(SQLtable *table,
										  size_t *p_metaLength,
										  size_t *p_bodyLength);
/* query.c */
extern SQLtable	   *pgsql_create_buffer(PGconn *conn, PGresult *res,
								size_t segment_sz);
extern void			pgsql_append_results(SQLtable *table, PGresult *res);
extern void 		pgsql_writeout_buffer(SQLtable *table);
extern void			pgsql_dump_buffer(SQLtable *table);
/* arrow_write.c */
extern ssize_t		writeFlatBufferMessage(int fdesc, ArrowMessage *message);
extern ssize_t		writeFlatBufferFooter(int fdesc, ArrowFooter *footer);
/* arrow_types.c */
extern void			assignArrowType(SQLattribute *attr, int *p_numBuffers);
/* arrow_read.c */
extern void			readArrowFile(const char *pathname);
/* arrow_dump.c */
extern void			dumpArrowNode(ArrowNode *node, FILE *out);

/*
 * Error message and exit
 */
#define Elog(fmt, ...)								\
	do {											\
		fprintf(stderr,"%s:%d  " fmt "\n",			\
				__FILE__,__LINE__, ##__VA_ARGS__);	\
		exit(1);									\
	} while(0)

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

static inline void
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

#endif	/* PG2ARROW_H */
