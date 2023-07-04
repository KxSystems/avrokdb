\d .avro

// Quick hack for running with qpacker
orig_path:"";
// running as avrokdb.qpk dependency:
if[count key `:/opt/kx/app/avrokdb/; orig_path:first system "pwd"; system "cd /opt/kx/app/avrokdb/"];

init:`avrokdb 2:(`InitialiseAvroKdb; 1);

schemaFromFile:`avrokdb 2:(`SchemaFromFile; 1);
schemaFromString:`avrokdb 2:(`SchemaFromString; 1);
getSchema:`avrokdb 2:(`GetSchema; 1);
printSchema:{-1 getSchema[x];};

encode:`avrokdb 2:(`Encode; 3);
decode:`avrokdb 2:(`Decode; 3);

\d .

.avro.init[];


// Quick hack for running with qpacker
if[count .avro.orig_path; system "cd ", .avro.orig_path];
