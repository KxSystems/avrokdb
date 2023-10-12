#pragma once

extern "C"
{
  /// @brief Encode kdb+ object to Avro serialised data
  ///
  /// Supported options:
  ///
  /// * AVRO_FORMAT (string).  Describes whether the kdb+ object should be
  /// encoded into Avro binary or JSON format.  Valid options "BINARY", "JSON"
  /// or "PRETTY_JSON", default "BINARY".
  ///
  /// * MULTITHREADED (long).  By default avrokdb is optimised to reuse the
  /// existing encoder for this schema.  However, Avro encoders do not support
  /// concurrent access and therefore if running encode with peach this option
  /// must be set to non-zero to disable this optimisation.  Default 0.
  /// 
  /// @param schema.  Foreign object containing the Avro schema to use for
  /// encoding. 
  ///
  /// @param data.  Kdb+ object to encode.  Must adhere to the appropriate type
  /// mapping for the schema.
  ///
  /// @param options. kdb+ dictionary of options or generic null(::) to use the
  /// defaults.  Dictionary key must be a 11h list.  Values list can be 7h, 11h
  /// or mixed list of -7|-11|4h.
  ///
  /// @return Avro serialised data, either 4h for binary encoding or 10h for
  /// JSON encoding.
  EXP K Encode(K schema, K data, K options);
}
