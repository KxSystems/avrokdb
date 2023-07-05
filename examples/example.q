\l q/avrokdb.q

-1 "\n<----- Record of scalar types ----->\n";
sc:.avro.schemaFromFile["examples/scalars.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;0b;0x0011;1.1;`AA;0x00112233;2.2e;3i;4;::;"aa";(0h;"abc"));
-1 "<----- Input ----->";
show input;
-1 "<----- Binary Avro ----->";
binary_serialised:.avro.encode[sc;input;(enlist `AVRO_FORMAT)!enlist `BINARY];
show binary_serialised;
-1 "<----- JSON Avro ----->";
json_serialised:.avro.encode[sc;input;(enlist `AVRO_FORMAT)!enlist `JSON_PRETTY];
show json_serialised;
-1 "<----- Output ----->";
output:.avro.decode[sc;binary_serialised;(enlist `AVRO_FORMAT)!enlist `BINARY];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "\n<----- Record of arrays of scalar types ----->\n";
sc:.avro.schemaFromFile["examples/arrays.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;01b;(0x0011;0x1122);1.1 2.2;`AA`BB;(0x00112233;0x44112233);2.2 3.3e;3 4i;4 5;(::;::);("aa";"bb");((0h;"cc");(2h;33)));
-1 "<----- Input ----->";
show input;
-1 "<----- Binary Avro ----->";
binary_serialised:.avro.encode[sc;input;(enlist `AVRO_FORMAT)!enlist `BINARY];
show binary_serialised;
-1 "<----- JSON Avro ----->";
json_serialised:.avro.encode[sc;input;(enlist `AVRO_FORMAT)!enlist `JSON_PRETTY];
show json_serialised;
-1 "<----- Output ----->";
output:.avro.decode[sc;binary_serialised;(enlist `AVRO_FORMAT)!enlist `BINARY];
show output;
-1 "<----- Result ----->";
show input~output;
