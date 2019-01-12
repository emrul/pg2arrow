/*
 * arrow_enums.h
 *
 * definitions for apache arrow format
 */
#ifndef _ARROW_ENUMS_H_
#define _ARROW_ENUMS_H_

/*
 * MetadataVersion : short
 */
typedef enum
{
	ArrowMetadataVersion__V1 = 0,		/* not supported */
	ArrowMetadataVersion__V2 = 1,		/* not supported */
	ArrowMetadataVersion__V3 = 2,		/* not supported */
	ArrowMetadataVersion__V4 = 3,
} ArrowMetadataVersion;

/*
 * MessageHeader : byte
 */
typedef enum
{
	ArrowMessageHeader__Schema			= 1,
	ArrowMessageHeader__DictionaryBatch	= 2,
	ArrowMessageHeader__RecordBatch		= 3,
	ArrowMessageHeader__Tensor			= 4,
	ArrowMessageHeader__SparseTensor	= 5,
} ArrowMessageHeader;

/*
 * Endianness : short
 */
typedef enum
{
	ArrowEndianness__Little		= 0,
	ArrowEndianness__Big		= 1,
} ArrowEndianness;

/*
 * Type : byte
 */
typedef enum
{
	ArrowType__Null				= 1,
	ArrowType__Int				= 2,
	ArrowType__FloatingPoint	= 3,
	ArrowType__Binary			= 4,
	ArrowType__Utf8				= 5,
	ArrowType__Bool				= 6,
	ArrowType__Decimal			= 7,
	ArrowType__Date				= 8,
	ArrowType__Time				= 9,
	ArrowType__Timestamp		= 10,
	ArrowType__Interval			= 11,
	ArrowType__List				= 12,
	ArrowType__Struct			= 13,
	ArrowType__Union			= 14,
	ArrowType__FixedSizeBinary	= 15,
	ArrowType__FixedSizeList	= 16,
	ArrowType__Map				= 17,
} ArrowTypeTag;

/*
 * DateUnit : short
 */
typedef enum
{
	ArrowDateUnit__Day			= 0,
	ArrowDateUnit__MilliSecond	= 1,
} ArrowDateUnit;

/*
 * TimeUnit : short
 */
typedef enum
{
	ArrowTimeUnit__Second		= 0,
	ArrowTimeUnit__MilliSecond	= 1,
	ArrowTimeUnit__MicroSecond	= 2,
	ArrowTimeUnit__NanoSecond	= 3,
} ArrowTimeUnit;

/*
 * IntervalUnit : short
 */
typedef enum
{
	ArrowIntervalUnit__Year_Month	= 0,
	ArrowIntervalUnit__Day_Time		= 1,
} ArrowIntervalUnit;

/*
 * Precision : short
 */
typedef enum
{
	ArrowPrecision__Half		= 0,
	ArrowPrecision__Single		= 1,
	ArrowPrecision__Double		= 2,
} ArrowPrecision;

/*
 * UnionMode : short
 */
typedef enum
{
	ArrowUnionMode__Sparse		= 0,
	ArrowUnionMode__Dense		= 1,
} ArrowUnionMode;

/*
 * ArrowNodeTag
 */
typedef enum
{
	/* types */
	ArrowNodeTag__Null,
	ArrowNodeTag__Int,
	ArrowNodeTag__FloatingPoint,
	ArrowNodeTag__Utf8,
	ArrowNodeTag__Binary,
	ArrowNodeTag__Bool,
	ArrowNodeTag__Decimal,
	ArrowNodeTag__Date,
	ArrowNodeTag__Time,
	ArrowNodeTag__Timestamp,
	ArrowNodeTag__Interval,
	ArrowNodeTag__List,
	ArrowNodeTag__Struct,
	ArrowNodeTag__Union,
	ArrowNodeTag__FixedSizeBinary,
	ArrowNodeTag__FixedSizeList,
	ArrowNodeTag__Map,
	/* others */
	ArrowNodeTag__KeyValue,
	ArrowNodeTag__DictionaryEncoding,
	ArrowNodeTag__Field,
	ArrowNodeTag__FieldNode,
	ArrowNodeTag__Buffer,
	ArrowNodeTag__Schema,
	ArrowNodeTag__RecordBatch,
	ArrowNodeTag__DictionaryBatch,
	ArrowNodeTag__Message,
} ArrowNodeTag;

/*
 * ArrowNode
 */
typedef struct
{
	ArrowNodeTag	tag;
} ArrowNode;

/* Null */
typedef ArrowNode	ArrowTypeNull;

/* Int */
typedef struct
{
	ArrowNodeTag	tag;
	int32			bitWidth;
	bool			is_signed;
} ArrowTypeInt;

/* FloatingPoint */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowPrecision	precision;
} ArrowTypeFloatingPoint;

/* Utf8 */
typedef ArrowNode	ArrowTypeUtf8;

/* Binary  */
typedef ArrowNode	ArrowTypeBinary;

/* Bool */
typedef ArrowNode	ArrowTypeBool;

/* Decimal */
typedef struct
{
	ArrowNodeTag	tag;
	int32			precision;
	int32			scale;
} ArrowTypeDecimal;

/* Date */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowDateUnit	unit;
} ArrowTypeDate;

/* Time */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowTimeUnit	unit;
	int32			bitWidth;
} ArrowTypeTime;

/* Timestamp */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowTimeUnit	unit;
	const char	   *timezone;
	int32			_timezone_len;
} ArrowTypeTimestamp;

/* Interval */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowIntervalUnit unit;
} ArrowTypeInterval;

/* List */
typedef ArrowNode	ArrowTypeList;

/* Struct */
typedef ArrowNode	ArrowTypeStruct;

/* Union */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowUnionMode	mode;
	int32		   *typeIds;
	int32			_num_typeIds;
} ArrowTypeUnion;

/* FixedSizeBinary */
typedef struct
{
	ArrowNodeTag	tag;
	int32			byteWidth;
} ArrowTypeFixedSizeBinary;

/* FixedSizeList */
typedef struct
{
	ArrowNodeTag	tag;
	int32			listSize;
} ArrowTypeFixedSizeList;

/* Map */
typedef struct
{
	ArrowNodeTag	tag;
	bool			keysSorted;
} ArrowTypeMap;

/*
 * ArrowType
 */
typedef union
{
	ArrowNodeTag			tag;
	ArrowTypeNull			Null;
	ArrowTypeInt			Int;
	ArrowTypeFloatingPoint	FloatingPoint;
	ArrowTypeUtf8			Utf8;
	ArrowTypeBinary			Binary;
	ArrowTypeBool			Bool;
	ArrowTypeDecimal		Decimal;
	ArrowTypeDate			Date;
	ArrowTypeTime			Time;
	ArrowTypeTimestamp		Timestamp;
	ArrowTypeInterval		Interval;
	ArrowTypeList			List;
	ArrowTypeStruct			Struct;
	ArrowTypeUnion			Union;
	ArrowTypeFixedSizeBinary FixedSizeBinary;
	ArrowTypeFixedSizeList	FixedSizeList;
	ArrowTypeMap			Map;
} ArrowType;

/*
 * Buffer
 */
typedef struct
{
	ArrowNodeTag	tag;
	int64			offset;
	int64			length;
} ArrowBuffer;

/*
 * KeyValue
 */
typedef struct
{
	ArrowNodeTag	tag;
	const char	   *key;
	const char	   *value;
	int				_key_len;
	int				_value_len;
} ArrowKeyValue;

/*
 * DictionaryEncoding
 */
typedef struct
{
	ArrowNodeTag	tag;
	int64			id;
	ArrowTypeInt	indexType;
	bool			isOrdered;
} ArrowDictionaryEncoding;

/*
 * Field
 */
typedef struct
{
	ArrowNodeTag	tag;
	const char	   *name;
	int				_name_len;
	bool			nullable;
	ArrowType		type;
	ArrowDictionaryEncoding dictionary;
	/* vector of nested data types */
	ArrowType	  **children;
	int				_num_children;
	/* vector of user defined metadata */
	ArrowKeyValue **custom_metadata;
	int				_num_custom_metadata;
} ArrowField;

/*
 * FieldNode
 */
typedef struct
{
	ArrowNodeTag	tag;
	uint64			length;
	uint64			null_count;
} ArrowFieldNode;

/*
 * Schema
 */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowEndianness	endianness;
	/* vector of Field */
	ArrowField	  **fields;
	int				_num_fields;
	/* List of KeyValue */
	ArrowKeyValue **custom_metadata;
	int				_num_custom_metadata;
} ArrowSchema;

/*
 * RecordBatch
 */
typedef struct
{
	ArrowNodeTag	tag;
	int64			length;
	/* vector of FieldNode */
	ArrowFieldNode **nodes;
	int				_num_nodes;
	/* vector of Buffer */
	ArrowBuffer	   **buffers;
	int				_num_buffers;
} ArrowRecordBatch;

/*
 * DictionaryBatch 
 */
typedef struct
{
	ArrowNodeTag	tag;
	int64			id;
	ArrowRecordBatch data;
	bool			isDelta;
} ArrowDictionaryBatch;

/*
 * ArrowMessageHeader
 */
typedef union
{
	ArrowNode		node;
	ArrowSchema		schema;
	ArrowDictionaryBatch dictionaryBatch;
	ArrowRecordBatch recordBatch;
} ArrowMessageBody;

/*
 * Message
 */
typedef struct
{
	ArrowNodeTag	tag;
	ArrowMetadataVersion version;
	ArrowMessageBody body;
	uint64			bodyLength;
} ArrowMessage;

#endif		/* _ARROW_DEFS_H_ */
