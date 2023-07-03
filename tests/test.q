\l q/avrokdb.q

-1 "<----- Single simple type ----->";
sc:.avro.readJsonSchema["tests/single_simple.avsc"];
input:`AA;
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Single array type ----->";
sc:.avro.readJsonSchema["tests/single_array.avsc"];
input:(0x00112233;0x44112233);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of simple types ----->";
sc:.avro.readJsonSchema["tests/simple.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;0b;0x0011;1.1;`AA;0x00112233;2.2e;3i;4;::;"aa";(0h;"abc"));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of simple array types ----->";
sc:.avro.readJsonSchema["tests/simple_array.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;01b;(0x0011;0x1122);1.1 2.2;`AA`BB;(0x00112233;0x44112233);2.2 3.3e;3 4i;4 5;(::;::);("aa";"bb");((0h;"cc");(2h;33)));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Single field nested record ----->";
sc:.avro.readJsonSchema["tests/nested_record_single.avsc"];
nested:(``c`d)!(::;1.1;`AA);
input:(``b)!(::;nested);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record including field of nested records ----->";
sc:.avro.readJsonSchema["tests/nested_record.avsc"];
nested:(``c`d)!(::;1.1;`AA);
input:(``a`b)!(::;0b;nested);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Single field record with array of records ----->";
sc:.avro.readJsonSchema["tests/array_record_single.avsc"];
nested:(``b`c)!(::;1b;0x0011);
input:(``a)!(::;(::;nested;nested));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record including field of array of records ----->";
sc:.avro.readJsonSchema["tests/array_record.avsc"];
nested:(``b`c)!(::;1b;0x0011);
input:(``a`d)!(::;(::;nested;nested);`AA);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record with single field containing array of array of records ----->";
sc:.avro.readJsonSchema["tests/array_array_record_single.avsc"];
nested:(``b`c)!(::;1b;0x0011)
input:(``a)!(::;enlist (::;nested;nested));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record with array of array simple types fields ----->";
sc:.avro.readJsonSchema["tests/simple_array_array.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;enlist 01b;enlist (0x0011;0x1122);enlist 1.1 2.2;enlist `AA`BB;enlist (0x00112233;0x44112233);enlist 2.2 3.3e;enlist 3 4i;enlist 4 5;enlist (::;::);enlist ("aa";"bb"); enlist ((0h;"cc");(2h;33)));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;


-1 "<----- Record with fields including array of array of records ----->";
sc:.avro.readJsonSchema["tests/array_array_record.avsc"];
nested:(``b`c)!(::;1b;0x0011);
input:(``a`d)!(::;((::;nested;nested);(::;nested;nested));`AA);
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of simple map types ----->";
sc:.avro.readJsonSchema["tests/simple_map.avsc"];
input:(``a`b`c`d`e`f`g`h`i`j`k)!(::;(`y`z)!01b;(`y`z)!(0x0011;0x1122);(`y`z)!1.1 2.2;(`y`z)!`AA`BB;(`y`z)!(0x00112233;0x44112233);(`y`z)!2.2 3.3e;(`y`z)!3 4i;(`y`z)!4 5;(`y`z)!(::;::);(`y`z)!("aa";"bb");(`y`z)!((0h;"cc");(2h;33)));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of arrays of maps ----->";
sc:.avro.readJsonSchema["tests/simple_array_map.avsc"];
input:(``b`c`k)!(::;(::;(`y`z)!(0x0011;0x1122);(`y`z)!(0x0011;0x1122));(::;(`y`z)!1.1 2.2;(`y`z)!1.1 2.2);(::;((`y`z)!((0h;"cc");(2h;33)));((`y`z)!((0h;"cc");(2h;33)))));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of map of arrays ----->";
sc:.avro.readJsonSchema["tests/simple_map_array.avsc"];
input:(``b`c`k)!(::;(`a`b)!((0x0011;0x223344);(0x0011;0x223344));((`c`d)!((1.1 2.2);(1.1 2.2)));(`f`h)!(((0h;"aa");(2h;123));((0h;"aa");(2h;123))));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of map of maps ----->";
sc:.avro.readJsonSchema["tests/simple_map_map.avsc"];
input:(``b`c`k)!(::;((``a`b)!(::;(`i`j)!(0x0011;0x223344);(`k`l)!(0x0011;0x223344)));((``c`d)!(::;((`m`n)!(1.1 2.2));(`o`p)!(1.1 2.2)));((``f`h)!(::;(`q`r)!((0h;"aa");(2h;123));(`s`t)!((0h;"aa");(2h;123)))));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of logical types ----->";
sc:.avro.readJsonSchema["tests/logical_single.avsc"];
input:(``a`b`c`d`e`f`g`h`i)!(::;(4i;2i;0x001122);first 1?0Ng;.z.d;.z.t;`timespan$123000;`timestamp$123000000;`timestamp$123000;(1 2 3i);(4i;2i;0x00112233));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;

-1 "<----- Record of array of logical types ----->";
sc:.avro.readJsonSchema["tests/logical_array.avsc"];
input:(``a`b`c`d`e`f`g`h`i)!(::;enlist (4i;2i;0x001122);2?0Ng;enlist .z.d;enlist .z.t;enlist `timespan$123000;enlist `timestamp$123000000;enlist `timestamp$123000;enlist (1 2 3i);enlist (4i;2i;0x00112233));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;


-1 "<----- Record of map of logical types ----->";
sc:.avro.readJsonSchema["tests/logical_maps.avsc"];
input:(``a`b`c`d`e`f`g`h`i)!(::;(enlist `j)!enlist (4i;2i;0x001122);(`k`l)!2?0Ng;(enlist `m)!enlist .z.d;(enlist `n)!enlist .z.t;(enlist `o)!enlist `timespan$123000;(enlist `p)!enlist `timestamp$123000000;(enlist `q)!enlist `timestamp$123000;(enlist `r)!enlist (1 2 3i);(enlist `s)!enlist (4i;2i;0x00112233));
serialised:.avro.encode[sc;input];
output:.avro.decode[sc;serialised];
show output;
-1 "<----- Result ----->";
show input~output;