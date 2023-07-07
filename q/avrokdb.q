\d .avro

// Try to find avrodkb.so in a qpacker container
avrokdb_lib:`avrokdb;
if[not .z.o like "w*";
    potential_sos:@[system; "find /opt/kx/app -name 'avrokdb.so' 2>/dev/null"; ()];
    if [not 0 = num_sos:count potential_sos;
        avrokdbso_path:first potential_sos;
        avrokdb_lib:`$-3_avrokdbso_path;
        if [num_sos > 1;
            -1 .j.j(`component`time`level`message)!("avrokdb"; (string `date$.z.p), "T", (string .z.t), "z"; "INFO"; "Multiple avrokdb.so found, using ", avrokdbso_path)
            ]
        ]
    ]


/// @brief Create a compiled Avro schema from a JSON file
///
/// @param filename.  String containing the filename.
///
/// @return foreign containing the compiled Avro schema.  This will be garbage
/// collected when its refcount drops to zero.
schemaFromFile:avrokdb_lib 2:(`SchemaFromFile; 1);

/// @brief Create a compiled Avro schema from a JSON string
///
/// @param filename.  String containing the Avro JSON schema.
///
/// @return foreign containing the compiled Avro schema.  This will be garbage
/// collected when its refcount drops to zero.
schemaFromString:avrokdb_lib 2:(`SchemaFromString; 1);

/// @brief Return a pretty-printed JSON string detailing the Avro compiled
/// schema
///
/// @param schema.  Foreign object containing the Avro schema to display. 
///
/// @return String containing the Avro JSON schema
getSchema:avrokdb_lib 2:(`GetSchema; 1);
printSchema:{-1 getSchema[x];};


/// @brief Encode kdb+ object to Avro serialised data
///
/// Supported options:
///
/// * AVRO_FORMAT (string).  Describes whether the kdb+ object should be
/// encoded into Avro binary or JSON format.  Valid options "BINARY", "JSON"
/// or "PRETTY_JSON", default "BINARY".
///
/// @param schema.  Foreign object containing the Avro schema to use for
/// encoding. 
///
/// @param data.  Kdb+ object to encode.  Must adhere to the appropriate type
/// mapping for the schema.
///
/// @param options. kdb+ dictionary of options or generic null(::) to use the
/// defaults.  Dictionary key must be a 11h list.  Values list can be 7h, 11h
/// or mixed list of -7|-11|4h.
///
/// @return Avro serialised data, either 4h for binary encoding or 10h for
/// JSON encoding.
encode:avrokdb_lib 2:(`Encode; 3);

/// @brief Decode Avro serialised data to a kdb+ object
///
/// Supported options:
///
/// * AVRO_FORMAT (string).  Describes whether the Avro serialised data is in
/// binary or JSON format.  Valid options "BINARY" or "JSON", default
/// "BINARY".
///
/// @param schema.  Foreign object containing the Avro schema to use for
/// decoding. 
///
/// @param data.  4h or 10h list of Avro serialised data.
///
/// @param options. kdb+ dictionary of options or generic null(::) to use the
/// defaults.  Dictionary key must be a 11h list.  Values list can be 7h, 11h
/// or mixed list of -7|-11|4h.
///
/// @return kdb+ object representing the Avro data having applied the
/// appropriate type mappings
decode:avrokdb_lib 2:(`Decode; 3);


\d .
