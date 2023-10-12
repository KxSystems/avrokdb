#pragma once

extern "C"
{
  /// @brief Decode Avro serialised data to a kdb+ object
  ///
  /// Supported options:
  ///
  /// * AVRO_FORMAT (string).  Describes whether the Avro serialised data is in
  /// binary or JSON format.  Valid options "BINARY" or "JSON", default
  /// "BINARY".
  ///
  /// * DECODE_OFFSET (long).  Offset into the `data` buffer that decoding
  /// should begin from.  Can be used to skip over a header in the buffer.
  /// Default 0. 
  ///
  /// * MULTITHREADED (long).  By default avrokdb is optimised to reuse the
  /// existing decoder for this schema.  However, Avro decoders do not support
  /// concurrent access and therefore if running decode with peach this option
  /// must be set to non-zero to disable this optimisation.  Default 0.
  ///
  /// @param schema.  Foreign object containing the Avro schema to use for
  /// decoding. 
  ///
  /// @param data.  4h or 10h list of Avro serialised data.
  ///
  /// @param options. kdb+ dictionary of options or generic null(::) to use the
  /// defaults.  Dictionary key must be a 11h list.  Values list can be 7h, 11h
  /// or mixed list of -7|-11|4h.
  ///
  /// @return kdb+ object representing the Avro data having applied the
  /// appropriate type mappings
  EXP K Decode(K schema, K data, K options);
}
