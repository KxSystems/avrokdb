#pragma once

#include <memory>

#include "HelperFunctions.h"


void AvroSchemaExitHandler();

std::shared_ptr<avro::ValidSchema> GetAvroSchema(K avro_schema);


extern "C" {
  EXP K DeriveSchema(K data);
  EXP K ReadJsonSchema(K filename);
  EXP K PrintSchema(K schema);
  EXP K GetTestSchema(K unused);
}
