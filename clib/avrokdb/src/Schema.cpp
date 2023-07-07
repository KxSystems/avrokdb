#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <set>
#include <mutex>

#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#include <avro/LogicalType.hh>
#include <avro/Compiler.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "TypeCheck.h"
#include "GenericForeign.h"

#include "k.h"


K SchemaFromFile(K filename)
{
  if (!IsKdbString(filename))
    return krr(S("SchemaFromFile, filename expected -11|10h"));

  KDB_EXCEPTION_TRY;

  auto avro_schema = avro::compileJsonSchemaFromFile(GetKdbString(filename).c_str());

  return ForeignSet<avro::ValidSchema>::Foreign::KdbConstructor(std::make_shared<avro::ValidSchema>(avro_schema));

  KDB_EXCEPTION_CATCH;
}

K SchemaFromString(K schema)
{
  if (!IsKdbString(schema))
    return krr(S("SchemaFromString, schema expected -11|10h"));

  KDB_EXCEPTION_TRY;

  auto avro_schema = avro::compileJsonSchemaFromString(GetKdbString(schema));

  return ForeignSet<avro::ValidSchema>::Foreign::KdbConstructor(std::make_shared<avro::ValidSchema>(avro_schema));

  KDB_EXCEPTION_CATCH;
}

K GetSchema(K schema)
{
  KDB_EXCEPTION_TRY;

  auto avro_schema = GetForeign<avro::ValidSchema>(schema);
  const auto& schema_str = avro_schema->toJson(true);
  K result = ktn(KC, schema_str.length());
  std::memcpy(kG(result), schema_str.data(), schema_str.length());

  return result;

  KDB_EXCEPTION_CATCH;
}
