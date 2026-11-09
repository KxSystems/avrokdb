clib:use`.clib

// Create a compiled Avro schema from a JSON file
schemaFromFile:clib.SchemaFromFile

// Create a compiled Avro schema from a JSON string
schemaFromString:clib.SchemaFromString

// Return the JSON representation of an Avro compiled schema
getSchema:clib.GetSchema

// Display the JSON representation of an Avro compiled schema
printSchema:{-1 getSchema[x];};

// Encode KDB-X object to Avro serialised data
encode:clib.Encode

// Decode Avro serialised data to a KDB-X object
decode:clib.Decode

export:([schemaFromFile;schemaFromString;getSchema;printSchema;encode;decode])
