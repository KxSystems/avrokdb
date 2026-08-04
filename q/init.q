if[5>.z.K;system"d .avrokdb"]
clib:$[5<=.z.K;use`.clib;(`avrokdb 2:(`kexport;1))[]]

// Create a compiled Avro schema from a JSON file
schemaFromFile:clib.SchemaFromFile

// Create a compiled Avro schema from a JSON string
schemaFromString:clib.SchemaFromString

// Return the JSON representation of an Avro compiled schema
getSchema:clib.GetSchema

// Display the JSON representation of an Avro compiled schema
printSchema:{-1 getSchema[x];};

// Encode kdb+ object to Avro serialised data
encode:clib.Encode

// Decode Avro serialised data to a kdb+ object
decode:clib.Decode

if[5<=.z.K;export:([schemaFromFile;schemaFromString;getSchema;printSchema;encode;decode])]
