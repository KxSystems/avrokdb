\l q/avrokdb.q

sc:.avro.readJsonSchema["tests/simple_array.avsc"];
input:(`a`b`c`d`e`f`g`h`i`j)!(01b;(0x0011;0x1122);1.1 2.2;`AA`BB;(0x00112233;0x44112233);2.2 3.3e;3 4i;4 5;(::;::);("aa";"bb"));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

