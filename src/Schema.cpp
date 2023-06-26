#include <iostream>
#include <memory>
#include <string>
#include <fstream>

#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Node.hh>
#include <avro/Parser.hh>
#include <avro/Schema.hh>
#include <avro/SchemaResolution.hh>
#include <avro/Serializer.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>
#include "boost/make_shared.hpp"
#include "boost/shared_ptr.hpp"
#include <avro/buffer/BufferPrint.hh>
#include <avro/buffer/BufferStream.hh>

#include <avro/AvroSerialize.hh>
#include <avro/CustomAttributes.hh>
#include <avro/NodeConcepts.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>
#include <avro/Compiler.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "TypeCheck.h"

#include "k.h"


class AvroSchema
{
private:
  std::shared_ptr<avro::ValidSchema> avro_schema;


private:
  // Constructor is private and only used by KdbConstructor static function 
  AvroSchema(std::shared_ptr<avro::ValidSchema> avro_schema_) :
    avro_schema(avro_schema_)
  {};

public:
  // InvalidAvroSchema is thrown if an invalid foreign object is specified
  class InvalidAvroSchema : public std::invalid_argument
  {
  public:
    InvalidAvroSchema(std::string message) : std::invalid_argument(message.c_str())
    {};
  };

public:
  // KdbDestructor is registered as foreign object destructor callback
  static K KdbDestructor(K avro_schema);

  // Creates the AvroSchema object and wraps it in a kdb
  // foreign object
  static K KdbConstructor(std::shared_ptr<avro::ValidSchema> schema);

  // Returns the AvroSchema object that has been wrapped in a kdb foreign
  // object
  static AvroSchema* Get(K avro_schema);

  // Getter functions for object members
  std::shared_ptr<avro::ValidSchema> GetAvroSchema();
};


std::set<AvroSchema*> valid_avro_schema_data;
void AvroSchemaExitHandler()
{
  for (auto i : valid_avro_schema_data)
    delete i;

  valid_avro_schema_data.clear();
}

K AvroSchema::KdbDestructor(K avro_schema)
{
  KDB_EXCEPTION_TRY;

  auto* avro_schema_data = AvroSchema::Get(avro_schema);

  // Delete the ConnectionData object.  This will decrement the refcounts of the
  // shared pointers to allow them to be deleted. 
  delete avro_schema_data;
  valid_avro_schema_data.erase(avro_schema_data);

  return (K)0;

  KDB_EXCEPTION_CATCH;
}

K AvroSchema::KdbConstructor(std::shared_ptr<avro::ValidSchema> avro_schema)
{
  // Use a raw new rather than a smart point since kdb will be controlling the
  // lifetime via its refcount
  auto* avro_schema_data = new AvroSchema(avro_schema);
  valid_avro_schema_data.insert(avro_schema_data);
  K result = knk(2, AvroSchema::KdbDestructor, avro_schema_data);
  result->t = 112;

  return result;
}

AvroSchema* AvroSchema::Get(K avro_schema)
{
  if (avro_schema->t != 112)
    throw InvalidAvroSchema("avro_schema expected 112h");

  if (avro_schema->n != 2)
    throw InvalidAvroSchema("avro_schema length expected 2");

  auto* avro_schema_data = (AvroSchema*)kK(avro_schema)[1];
  if (valid_avro_schema_data.find(avro_schema_data) == valid_avro_schema_data.end())
    throw InvalidAvroSchema("foreign is not an avro_schema");

  return avro_schema_data;
}

std::shared_ptr<avro::ValidSchema> AvroSchema::GetAvroSchema()
{
  return avro_schema;
}

std::shared_ptr<avro::ValidSchema> GetAvroSchema(K avro_schema)
{
  return AvroSchema::Get(avro_schema)->GetAvroSchema();
}


avro::Schema DeriveSimpleSchema(const std::string name, K data)
{
  switch (data->t) {
  case 101:
    return avro::NullSchema();
  case -KB:
    return avro::BoolSchema();
  case KB:
    return avro::ArraySchema(avro::BoolSchema());
  case -KI:
    return avro::IntSchema();
  case KI:
    return avro::ArraySchema(avro::IntSchema());
  case -KJ:
    return avro::LongSchema();
  case KJ:
    return avro::ArraySchema(avro::LongSchema());
  case -KE:
    return avro::FloatSchema();
  case KE:
    return avro::ArraySchema(avro::FloatSchema());
  case -KF:
    return avro::DoubleSchema();
  case KF:
    return avro::ArraySchema(avro::DoubleSchema());
  case KG:
    return avro::BytesSchema();
  case KC:
    return avro::StringSchema();
  case KS:
  {
    auto enum_schema = avro::EnumSchema(name);
    for (auto i = 0; i < data->n; ++i)
      enum_schema.addSymbol(kS(data)[i]);
    return enum_schema;
  }
  case 0:
  {
    if (data->n == 0)
      throw TypeCheck("DeriveSchema, 0h list of length 0");
    KdbType first = kK(data)[0]->t;
    if (first != KC && first != KG)
      throw TypeCheck("DeriveSchema, 0h list doesn't contain 4|10h");
    for (auto i = 1; i < data->n; ++i) {
      if (first != kK(data)[i]->t)
        throw TypeCheck("DeriveSchema, 0h list contains mixed types");
    }
    if (first == KC)
      return avro::ArraySchema(avro::StringSchema());
    else // (first == KG)
      return avro::ArraySchema(avro::BytesSchema());
  }
  default:
    throw TypeCheck("DeriveSchema, invalid type: " + data->t);
  }
}

avro::Schema DeriveRecordSchema(const std::string name, K data)
{
  avro::RecordSchema record = avro::RecordSchema(name);
  K keys = kK(data)[0];
  K values = kK(data)[1];

  assert(keys->n == values->n);
  if (keys->t != KS)
    throw TypeCheck("Record keys not 11h");
  if (values->t != 0)
    throw TypeCheck("Record values not 0h");

  for (auto i = 0; i < keys->n; ++i) {
    std::string field_name = kS(keys)[i];
    K value = kK(values)[i];

    if (field_name.empty() && value->t == 101)
      continue;

    if (value->t == 99)
      record.addField(field_name, DeriveRecordSchema(field_name, value));
    else 
      record.addField(field_name, DeriveSimpleSchema(field_name, value));
  }
  
  return record;
}

K DeriveSchema(K data)
{
  KDB_EXCEPTION_TRY;

  return AvroSchema::KdbConstructor(std::make_shared<avro::ValidSchema>(DeriveRecordSchema("root", data)));

  KDB_EXCEPTION_CATCH;
}

K ReadJsonSchema(K filename)
{
  if (!IsKdbString(filename))
    return krr(S("ReadJsonSchema, filename expected -11|10h"));

  KDB_EXCEPTION_TRY;

  std::ifstream in(GetKdbString(filename));
  avro::ValidSchema schema;
  avro::compileJsonSchema(in, schema);

  return AvroSchema::KdbConstructor(std::make_shared<avro::ValidSchema>(schema));

  KDB_EXCEPTION_CATCH;
}

K PrintSchema(K schema)
{
  KDB_EXCEPTION_TRY;

  auto avro_schema = GetAvroSchema(schema);
  std::cout << avro_schema->toJson(true) << std::endl;

  return K(0);

  KDB_EXCEPTION_CATCH;
}


using namespace avro;

#define BOOST_CHECK_EQUAL(x, y) (x == y);

K GetTestSchema(K unused) 
{
  KDB_EXCEPTION_TRY;

  RecordSchema record("RootRecord");

  CustomAttributes customAttributeLong;
  customAttributeLong.addAttribute("extra_info_mylong", std::string("it's a long field"));
  // Validate that adding a custom attribute with same name is not allowed
  bool caught = false;
  try {
    customAttributeLong.addAttribute("extra_info_mylong", std::string("duplicate"));
  }
  catch (Exception& e) {
    std::cout << "(intentional) exception: " << e.what() << '\n';
    caught = true;
  }
  BOOST_CHECK_EQUAL(caught, true);
  // Add custom attribute for the field
  record.addField("mylong", LongSchema(), customAttributeLong);

  IntSchema intSchema;
  avro::MapSchema map = MapSchema(IntSchema());

  record.addField("mymap", map);

  ArraySchema array = ArraySchema(DoubleSchema());

  const std::string s("myarray");
  record.addField(s, array);

  EnumSchema myenum("ExampleEnum");
  myenum.addSymbol("zero");
  myenum.addSymbol("one");
  myenum.addSymbol("two");
  myenum.addSymbol("three");

  caught = false;
  try {
    myenum.addSymbol("three");
  }
  catch (Exception& e) {
    std::cout << "(intentional) exception: " << e.what() << '\n';
    caught = true;
  }
  BOOST_CHECK_EQUAL(caught, true);

  record.addField("myenum", myenum);

  UnionSchema onion;
  onion.addType(NullSchema());
  onion.addType(map);
  onion.addType(FloatSchema());

  record.addField("myunion", onion);

  RecordSchema nestedRecord("NestedRecord");
  nestedRecord.addField("floatInNested", FloatSchema());

  record.addField("nested", nestedRecord);

  record.addField("mybool", BoolSchema());
  FixedSchema fixed(16, "fixed16");
  record.addField("myfixed", fixed);

  caught = false;
  try {
    record.addField("mylong", LongSchema());
  }
  catch (Exception& e) {
    std::cout << "(intentional) exception: " << e.what() << '\n';
    caught = true;
  }
  BOOST_CHECK_EQUAL(caught, true);

  CustomAttributes customAttributeLong2;
  customAttributeLong2.addAttribute("extra_info_mylong2",
    std::string("it's a long field"));
  customAttributeLong2.addAttribute("more_info_mylong2",
    std::string("it's still a long field"));
  record.addField("mylong2", LongSchema(), customAttributeLong2);

  record.addField("anotherint", intSchema);

  return AvroSchema::KdbConstructor(std::make_shared<avro::ValidSchema>(record));

  KDB_EXCEPTION_CATCH;
}