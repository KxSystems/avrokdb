\l q/avrokdb.q

sc:.avro.readJsonSchema["tests/simple.avsc"];
input:(`a`b`c`d`e`f`g`h`i`j)!(0b;0x0011;1.1;`AA;0x00112233;2.2e;3i;4;::;"aa");
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

