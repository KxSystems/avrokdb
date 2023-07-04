#pragma once

#include <memory>

#include "HelperFunctions.h"


void AvroSchemaExitHandler();

std::shared_ptr<avro::ValidSchema> GetAvroSchema(K avro_schema);


extern "C" {
  EXP K SchemaFromFile(K filename);
  EXP K SchemaFromString(K schema);
  EXP K GetSchema(K schema);
}
