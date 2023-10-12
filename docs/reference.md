# Function reference

These functions are exposed within the `.avrokdb` namespace, allowing users to convert data between Avro and kdb+.



## `.avrokdb`   Avro interface


object | use
-------|-------
[`schemaFromFile`](#schemaFromFile) | Create a compiled Avro schema from a JSON file
[`schemaFromString`](#schemaFromString) | Create a compiled Avro schema from a JSON string
[`getSchema`](#getSchema) | Return the JSON representation of an Avro compiled schema
[`printSchema`](#printSchema) | Display the JSON representation of an Avro compiled schema
[`encode`](#encode) | Encode kdb+ object to Avro serialised data
[`decode`](#decode) | Decode Avro serialised data to a kdb+ object



### `schemaFromFile`

*Create a compiled Avro schema from a JSON file*

```txt
.avrokdb.schemaFromFile[filename]
```

Where `filename` is a string to the Avro schema file.

The function returns a foreign containing the compiled Avro schema.  This will be garbage collected when its refcount drops to zero.

```q
q)schema:.avrokdb.schemaFromFile["schema.avsc"]
q)schema
foreign
q).avrokdb.printSchema[schema]
{
    "type": "enum",
    "name": "myenum",
    "symbols": [
        "AA",
        "BB",
        "CC"
    ]
}
```

### `schemaFromString`

*Create a compiled Avro schema from a JSON string*

```txt
.avrokdb.schemaFromString[json]
```

Where `json` is a string containing the JSON Avro schema.

The function returns a foreign containing the compiled Avro schema.  This will be garbage collected when its refcount drops to zero.

```q
q)json:"{ \"type\": \"enum\", \"name\": \"myenum\", \"symbols\": [\"AA\", \"BB\", \"CC\"] }"
q)schema:.avrokdb.schemaFromString[json]
q)schema
foreign
q).avrokdb.printSchema[schema]
{
    "type": "enum",
    "name": "myenum",
    "symbols": [
        "AA",
        "BB",
        "CC"
    ]
}
```

### `getSchema`

*Return the JSON representation of an Avro compiled schema*

```txt
.avrokdb.getSchema[schema]
```

Where `schema` is a foreign object containing a compiled Avro schema.

The function returns a string of the JSON schema.

```q
q)schema:.avrokdb.schemaFromFile["schema.avsc"]
q)schema
foreign
q).avrokdb.getSchema[schema]
"{\n    \"type\": \"enum\",\n    \"name\": \"myenum\",\n    \"symbols\": [\n        \"AA\",\n        \"BB\",\n        \"CC\"\n    ]\n}\n"
```

### `printSchema`

*Display the JSON representation of an Avro compiled schema*

```txt
.avrokdb.printSchema[schema]
```

Where `schema` is a foreign object containing a compiled Avro schema.

The function prints the JSON schema to stdout.

:warning: **For debugging use only**

The schema is displayed on stdout to preserve formatting and indentation.

```q
q)schema:.avrokdb.schemaFromFile["schema.avsc"]
q)schema
foreign
q).avrokdb.printSchema[schema]
{
    "type": "enum",
    "name": "myenum",
    "symbols": [
        "AA",
        "BB",
        "CC"
    ]
}
```

### `encode`

*Encode kdb+ object to Avro serialised data*

```txt
.avrokdb.encode[schema;input;options]
```

where:

* `schema` is a foreign object containing a compiled Avro schema.
* `input` is the kdb+ object to encode.  Must adhere to the appropriate [type mappings](./type-mapping.md) for the schema.
* `options` is a kdb+ dictionary of options or generic null (::) to use the defaults.  Dictionary key must be a 11h list.  Values list can be 7h, 11h or mixed list of -7|-11|4h.

The function returns Avro serialised data, either 4h for binary encoding or 10h for JSON encoding.

Supported options:

- `AVRO_FORMAT`- String identifying whether the kdb+ object should be encoded into Avro binary or JSON format.  Valid options `BINARY`, `JSON` or `PRETTY_JSON`, default `BINARY`.
- `MULTITHREADED` - Long flag.  By default avrokdb is optimised to reuse the existing encoder for this schema.  However, Avro encoders do not support concurrent access and therefore if running `encode` with `peach` this option **must** be set to non-zero to disable this optimisation.  Default 0.

```q
q)schema:.avrokdb.schemaFromFile["examples/scalars.avsc"];
q)input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;0b;0x0011;1.1;`AA;0x00112233;2.2e;3i;4;::;"aa";(0h;"abc"));
q).avrokdb.encode[schema;input;(enlist `AVRO_FORMAT)!enlist `BINARY]
0x000400119a9999999999f13f0000112233cdcc0c4006080461610006616263
q)-1 .avrokdb.encode[schema;input;(enlist `AVRO_FORMAT)!enlist `JSON_PRETTY];
{
  "a": false,
  "b": "\u0000\u0011",
  "c": 1.1000000000000001,
  "d": "AA",
  "e": "\u0000\u0011\"3",
  "f": 2.20000005,
  "g": 3,
  "h": 4,
  "i": null,
  "j": "aa",
  "k": {
    "string": "abc"
  }
}
```

### `decode`

*Decode Avro serialised data to a kdb+ object*

```txt
.avrokdb.encode[schema;data;options]
```

where:

* `schema` is a foreign object containing a compiled Avro schema.
* `input` is 4h or 10h list of Avro serialised data.
* `options` is a kdb+ dictionary of options or generic null (::) to use the defaults.  Dictionary key must be a 11h list.  Values list can be 7h, 11h or mixed list of -7|-11|4h.

The function returns a kdb+ object representing the Avro data having applied the appropriate [type mappings](./type-mapping.md) for the schema

Supported options:

- `AVRO_FORMAT`- String identifying whether the Avro serialised data is in binary or JSON format.  Valid options `BINARY` or `JSON`, default `BINARY`.
- `DECODE_OFFSET` - Long offset into the `data` buffer that decoding should begin from.  Can be used to skip over a header in the buffer.  Default 0. 
- `MULTITHREADED` - Long flag.  By default avrokdb is optimised to reuse the existing decoder for this schema.  However, Avro decoders do not support concurrent access and therefore if running `decode` with `peach` this option **must** be set to non-zero to disable this optimisation.  Default 0.

```q
q)schema:.avrokdb.schemaFromFile["examples/scalars.avsc"];
q)input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;0b;0x0011;1.1;`AA;0x00112233;2.2e;3i;4;::;"aa";(0h;"abc"));
q)serialised:.avrokdb.encode[schema;input;(enlist `AVRO_FORMAT)!enlist `BINARY];
q)show .avrokdb.decode[schema;serialised;(enlist `AVRO_FORMAT)!enlist `BINARY]
 | ::
a| 0b
b| 0x0011
c| 1.1
d| `AA
e| 0x00112233
f| 2.2e
g| 3i
h| 4
i| ::
j| "aa"
k| (0h;"abc")
```
