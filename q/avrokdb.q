\d .avrokdb

// Create a compiled Avro schema from a JSON file
schemaFromFile:`avrokdb 2:(`SchemaFromFile; 1);

// Create a compiled Avro schema from a JSON string
schemaFromString:`avrokdb 2:(`SchemaFromString; 1);

// Return the JSON representation of an Avro compiled schema
getSchema:`avrokdb 2:(`GetSchema; 1);

// Display the JSON representation of an Avro compiled schema
printSchema:{-1 getSchema[x];};

// Encode kdb+ object to Avro serialised data
encode:`avrokdb 2:(`Encode; 3);

// Decode Avro serialised data to a kdb+ object
decode:`avrokdb 2:(`Decode; 3);
