#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <set>

#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#include <avro/LogicalType.hh>
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

K SchemaFromFile(K filename)
{
  if (!IsKdbString(filename))
    return krr(S("SchemaFromFile, filename expected -11|10h"));

  KDB_EXCEPTION_TRY;

  auto avro_schema = avro::compileJsonSchemaFromFile(GetKdbString(filename).c_str());

  return AvroSchema::KdbConstructor(std::make_shared<avro::ValidSchema>(avro_schema));

  KDB_EXCEPTION_CATCH;
}

K SchemaFromString(K schema)
{
  if (!IsKdbString(schema))
    return krr(S("SchemaFromString, schema expected -11|10h"));

  KDB_EXCEPTION_TRY;

  auto avro_schema = avro::compileJsonSchemaFromString(GetKdbString(schema));

  return AvroSchema::KdbConstructor(std::make_shared<avro::ValidSchema>(avro_schema));

  KDB_EXCEPTION_CATCH;
}

K GetSchema(K schema)
{
  KDB_EXCEPTION_TRY;

  auto avro_schema = GetAvroSchema(schema);
  const auto& schema_str = avro_schema->toJson(true);
  K result = ktn(KC, schema_str.length());
  std::memcpy(kG(result), schema_str.data(), schema_str.length());

  return result;

  KDB_EXCEPTION_CATCH;
}
