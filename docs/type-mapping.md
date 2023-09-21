# Type mapping between Avro and kdb+

Apache Avro support a [rich set of datatypes](https://avro.apache.org/docs/1.11.1/specification/) including basic scalars and composite types such as record, array, map and union.

It also support logical types which are annotations on other basic types and are used to indicate s specific interpretation of the data such as temporal values.

Note that Avro data is not self describing, it requires a schema to be defined and used during both the encoding and decoding process. 

## Record datatype

An Avro record contains [a set of fields where each field has its own datatype](https://avro.apache.org/docs/1.11.1/specification/#schema-record).

The kdb+ representation of a record is a 99h where the dictionary keys are a 11h and the dictionary values are a 0h.  Each item is the dictionary values mixed list has the kdb+ type corresponding to that field's datatype.

In order to prevent type promotion where all the fields have the same datatype (although an Avro map would be more suitable for this use case) a null symbol key with corresponding generic null (::) value should be added to the dictionary (and is ignored).  For consistency `avrokdb` also adds a null symbol key with corresponding generic null (::) as the first item in the dictionary when decoding a record. 

## Map datatype

An Avro map contains [a named set of values where each value has the same datatype](https://avro.apache.org/docs/1.11.1/specification/#maps).

The kdb+ representation of a map a 99h where the dictionary keys are a 11h.  The kdb+ type of the dictionary values list follows the type mapping used for arrays of the map's datatype.  

## Scalar datatypes

The type mappings between Avro scalars and kdb+ objects follow the convention:

| Scalar datatype | Scalar logical type | kdb+ type | Notes                                                        |
| --------------- | ------------------- | --------- | ------------------------------------------------------------ |
| bool            | none                | -1h       |                                                              |
| bytes           | none                | 4h        |                                                              |
| bytes           | decimal             | 0h        | mixed list of (1h;1h;4h) containing (precision; scale; 2's complement byte array) |
| double          | none                | -9h       |                                                              |
| enum            | none                | -11h      |                                                              |
| fixed           | none                | 4h        |                                                              |
| fixed           | decimal             | 0h        | mixed list of (1h;1h;4h) containing (precision; scale; 2's complement byte array) |
| fixed           | duration            | 6h        | int list containing (months, days, millis)                   |
| float           | none                | -8h       |                                                              |
| int             | none                | -6h       |                                                              |
| int             | date                | -14h      | epoch offsetting is automatically applied                    |
| int             | time-millis         | -19h      |                                                              |
| long            | none                | -7h       |                                                              |
| long            | time-micros         | -16h      | scaling is automatically applied                             |
| long            | timestamp-millis    | -12h      | epoch offsetting and scaling are automatically applied       |
| long            | timestamp-micros    | -12h      | epoch offsetting and scaling are automatically applied       |
| null            | none                | 101h      |                                                              |
| string          | none                | 10h       |                                                              |
| string          | uuid                | -2h       |                                                              |

### Additional notes

#### Duration

The Avro specification clearly details that the [duration logical type](https://avro.apache.org/docs/1.11.1/specification/#duration) cannot be converted to an equivalent number of milliseconds which means it cannot be represented by any of the kdb+ temporal types.  That is why is represented as an int list of (months, days, millis).  For example if you tried to add two one month durations together:

```q
q)`date$2000.02.01 + 2000.02.01
2000.03.03 // incorrect
q)(0 1 0) + (0 1 0)
0 2 0 //correct
```

#### Decimal

The [Avro decimal logical type](https://avro.apache.org/docs/1.11.1/specification/#decimal) encodes an arbitrary-precision signed decimal number without truncation or rounding.  The byte array contains the two’s-complement representation of the unscaled integer value in big-endian byte order and the base 10 scale and precision attributes are applied to it.  Because kdb+ has no native decimal support a decimal is mapped to its raw representation of (precision; scale; 2s complement byte array).

## Array datatype

An Avro array is [list of another Avro datatype](https://avro.apache.org/docs/1.11.1/specification/#arrays).  The type mappings between Avro arrays and kdb+ objects depend of the array's type and follow the convention:

| Array datatype | Array logical type | kdb+ type | Notes                                                        |
| -------------- | ------------------ | --------- | ------------------------------------------------------------ |
| array          | none               | 0h        | mixed list of the array type (kdb+ type depends on the sub-array's datatype) |
| bool           | none               | 1h        |                                                              |
| bytes          | none               | 0h        | mixed list of the bytes scalar type (4h)                     |
| bytes          | decimal            | 0h        | mixed list of the decimal scalar type (0h)                   |
| double         | none               | 9h        |                                                              |
| enum           | none               | 11h       |                                                              |
| fixed          | none               | 0h        | mixed list of the fixed scalar type (4h)                     |
| fixed          | decimal            | 0h        | mixed list of the decimal scalar type (0h)                   |
| fixed          | duration           | 0h        | mixed list of the duration scalar type (0h)                  |
| float          | none               | 8h        |                                                              |
| int            | none               | 6h        |                                                              |
| int            | date               | 14h       | epoch offsetting is automatically applied                    |
| int            | time-millis        | 19h       |                                                              |
| long           | none               | 7h        |                                                              |
| long           | time-micros        | 16h       | scaling is automatically applied                             |
| long           | timestamp-millis   | 12h       | epoch offsetting and scaling are automatically applied       |
| map            | none               | 0h        | mixed list of the map type (99h).  To prevent type promotion to a 98h, a generic null (101h) should be added to the mixed list (and is ignored). |
| long           | timestamp-micros   | 12h       | epoch offsetting and scaling are automatically applied       |
| null           | none               | 0h        | mixed list of the null scalar type (101h)                    |
| record         | none               | 0h        | mixed list of the record type (99h).  To prevent type promotion to a 98h, a generic null (101h) should be added to the mixed list (and is ignored). |
| string         | none               | 0h        | mixed list of the string scalar type (10h)                   |
| string         | uuid               | 2h        |                                                              |
| union          | none               | 0h        | mixed list of the union type (0h)                            |

### Additional notes

As described in the notes an array of records or an array of maps require a generic null (::) to be added to the mixed list while encoding to prevent type promotion.  For consistency `avrokdb` also adds a generic null (::) as the first item in the mixed list when decoding an array of records or an array of maps. 

## Union datatype

An Avro union specifies [a set of datatypes where only one can be set at any time](https://avro.apache.org/docs/1.11.1/specification/#unions).  Avro represents this as a branch selector (identifying the 'live' union datatype) and that datatype's value (the datum).

The kdb+ representation of a union is a two element mixed list of (branch selector; datum value).  The branch selector is a -5h, the datum value has the kdb+ type corresponding to that branch's datatype.

