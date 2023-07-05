#pragma once

#include <memory>

#include "HelperFunctions.h"


void AvroSchemaExitHandler();

std::shared_ptr<avro::ValidSchema> GetAvroSchema(K avro_schema);


extern "C" {
  /// @brief Create a compiled Avro schema from a JSON file
  ///
  /// @param filename.  String containing the filename.
  ///
  /// @return foreign containing the compiled Avro schema.  This will be garbage
  /// collected when its refcount drops to zero.
  EXP K SchemaFromFile(K filename);

  /// @brief Create a compiled Avro schema from a JSON string
  ///
  /// @param filename.  String containing the Avro JSON schema.
  ///
  /// @return foreign containing the compiled Avro schema.  This will be garbage
  /// collected when its refcount drops to zero.
  EXP K SchemaFromString(K schema);

  /// @brief Return a pretty-printed JSON string detailing the Avro compiled
  /// schema
  ///
  /// @param schema.  Foreign object containing the Avro schema to display. 
  ///
  /// @return String containing the Avro JSON schema
  EXP K GetSchema(K schema);
}
