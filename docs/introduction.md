# Avrokdb

## Introduction

Avrokdb supports the use of [Apache Avro](https://avro.apache.org/docs/) from within KDB-X.  Encoding converts a KDB-X object to Avro serialised using the specified Avro schema, decoding converts Avro serialised to a KDB-X object object using the specified Avro schema.  It support all the Avro datatypes:

- array
- bool
- bytes
- double
- enum
- fixed
- float
- int
- long
- map
- null
- string
- union

and all the Avro logical types:

* date
* decimal
* duration
* time-millis
* time-micros
* timestamp-millis
* timestamp-micros
* uuid

Full details of the type-mapping between Avro and KDB-X are described [here](./type-mapping.md).



## Functionality

Avrokdb allows you to:

* Compile Avro JSON schemas
* Encode a KDB-X object to Avro serialised data in either binary or JSON format
* Decode Avro serialised data in either binary or JSON format to a KDB-X object

See [here](./reference.md) for a full function reference.



## Avro schemas

Avro data is not self describing, it requires a schema to be defined and used during both the encoding and decoding process.  Details of how to define an Avro JSON schema can be found [here](https://avro.apache.org/docs/1.11.1/specification/). 



## Examples

See [example.q](../examples/example.q) for basic examples.

More detailed examples of all the Avro datatypes, logical types and nesting can be found in the [tests](../tests/) directory.

