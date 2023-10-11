\l q/avrokdb.q

array_len:10;
reps:1000000;

-1 "\n<----- Record of arrays of scalar types ----->\n";
sc:.avrokdb.schemaFromFile["examples/arrays.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;array_len#0b;array_len#enlist 0x0011;array_len#1.1;array_len#`AA;array_len#enlist 0x00112233;array_len#2.2e;array_len#3i;array_len#4; array_len#(::);array_len#enlist "aa";array_len#enlist(0h;"cc"));

-1 "<----- Input ----->";
show input;
-1 "<----- Binary encode ----->";
\t {binary_serialised::.avrokdb.encode[sc;input;(enlist `AVRO_FORMAT)!enlist `BINARY]}each til reps;

-1 "<----- Binary decode ----->";
\t {output::.avrokdb.decode[sc;binary_serialised;(enlist `AVRO_FORMAT)!enlist `BINARY]}each til reps;

binary_serialised:(5#0x00),binary_serialised;

-1 "<----- Binary decode (strip header)----->";
\t {output::.avrokdb.decode[sc;5_binary_serialised;(enlist `AVRO_FORMAT)!enlist `BINARY]}each til reps;

-1 "<----- Binary decode (offset)----->";
\t {output::.avrokdb.decode[sc;binary_serialised;(`AVRO_FORMAT`DECODE_OFFSET)!(`BINARY;5)]}each til reps;
