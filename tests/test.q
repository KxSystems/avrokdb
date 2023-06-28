\l q/avrokdb.q

sc:.avro.readJsonSchema["tests/simple.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j)!(::;0b;0x0011;1.1;`AA;0x00112233;2.2e;3i;4;::;"aa");
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

sc:.avro.readJsonSchema["tests/simple_array.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j)!(::;01b;(0x0011;0x1122);1.1 2.2;`AA`BB;(0x00112233;0x44112233);2.2 3.3e;3 4i;4 5;(::;::);("aa";"bb"));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

sc:.avro.readJsonSchema["tests/nested_record.avsc"];
nested:(``c`d)!(::;1.1;`AA);
input:(``a`b)!(::;0b;nested);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

sc:.avro.readJsonSchema["tests/nested_record_single.avsc"];
nested:(``c`d)!(::;1.1;`AA);
input:(``b)!(::;nested);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

sc:.avro.readJsonSchema["tests/array_record_single.avsc"];
nested:(``b`c)!(::;1b;0x0011)
input:(``a)!(::;(::;nested;nested))
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

sc:.avro.readJsonSchema["tests/array_record.avsc"];
nested:(``b`c)!(::;1b;0x0011)
input:(``a`d)!(::;(::;nested;nested);`AA)
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;

sc:.avro.readJsonSchema["tests/simple_array_array.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j)!(::;enlist 01b;enlist (0x0011;0x1122);enlist 1.1 2.2;enlist `AA`BB;enlist (0x00112233;0x44112233);enlist 2.2 3.3e;enlist 3 4i;enlist 4 5;enlist (::;::);enlist ("aa";"bb"));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
show input~output;
