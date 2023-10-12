#pragma once

#include <memory>
#include <set>
#include <mutex>

#include "HelperFunctions.h"


// The structure that is stored in the avro foreign.
//
// Creating/destructing encoders and decoders is expensive so we create the
// various types here in advance for this schema and associate them with the
// foreign.  This allows these encoders/decoders to be reused on each subsequent
// encode or decode operation.
struct AvroForeign
{
  std::shared_ptr<avro::ValidSchema> schema;
  avro::EncoderPtr binary_encoder;
  avro::EncoderPtr json_encoder;
  avro::EncoderPtr json_pretty_encoder;
  avro::DecoderPtr binary_decoder;
  avro::DecoderPtr json_decoder;

  AvroForeign(const avro::ValidSchema& schema_) :
    schema(std::make_shared<avro::ValidSchema>(schema_)),
    binary_encoder(avro::validatingEncoder(schema_, avro::binaryEncoder())),
    json_encoder(avro::validatingEncoder(schema_, avro::jsonEncoder(schema_))),
    json_pretty_encoder(avro::validatingEncoder(schema_, avro::jsonPrettyEncoder(schema_))),
    binary_decoder(avro::validatingDecoder(schema_, avro::binaryDecoder())),
    json_decoder(avro::validatingDecoder(schema_, avro::jsonDecoder(schema_)))
  {}
};

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
