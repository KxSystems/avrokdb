\d .avro

// Quick hack for running with qpacker
orig_path:"";
// running as avrokdb.qpk dependency:
if[count key `:/opt/kx/app/avrokdb/; orig_path:first system "pwd"; system "cd /opt/kx/app/avrokdb/"];

init:`avrokdb 2:(`InitialiseAvroKdb; 1);

deriveSchema:`avrokdb 2:(`DeriveSchema; 1);
readJsonSchema:`avrokdb 2:(`ReadJsonSchema; 1);
getSchema:`avrokdb 2:(`GetSchema; 1);
printSchema:{-1 getSchema[x];};
getTestSchema:`avrokdb 2:(`GetTestSchema; 1);

encode:`avrokdb 2:(`encode; 2);

decode:`avrokdb 2:(`decode; 2);

\d .

.avro.init[];


// Quick hack for running with qpacker
if[count .avro.orig_path; system "cd ", .avro.orig_path];
