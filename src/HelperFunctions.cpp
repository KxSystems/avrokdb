#include "avro/LogicalType.hh"
#include "avro/Types.hh"
#include "avro/GenericDatum.hh"

#include "HelperFunctions.h"
#include "TypeCheck.h"


TemporalConversion::TemporalConversion(const std::string& field, const avro::LogicalType::Type logical_type)
{
  // Work out the correct epoch offsetting and scaling factors required for this
  // avro logical type
  switch (logical_type) {
  case avro::LogicalType::DATE:
    // Avro DATE <-> kdb date (KD)
    // DATE is int32_t days since the UNIX epoch
    // Kdb date is days since 2000.01.01
    // Requires: epoch offsetting
    offset = kdb_date_epoch_days;
    scalar = 1;
    break;
  case avro::LogicalType::TIME_MILLIS:
    // Avro TIME_MILLIS <-> kdb time (KT)
    // TIME_MILLIS is int32 representing milliseconds since midnight
    // Kdb time is milliseconds from midnight
    // Requires: no change
    offset = 0;
    scalar = 1;
    break;
  case avro::LogicalType::TIME_MICROS:
    // Avro TIME_MICROS <-> kdb timespan (KN)
    // TIME_MICROS is int64 representing either microseconds since midnight
    // Kdb timespan is nanoseconds from midnight
    // Requires: scaling
    offset = 0;
    scalar = 1000;
    break;
  case avro::LogicalType::TIMESTAMP_MILLIS:
    // Avro TIMESTAMP_MILLIS <-> kdb timestamp (KP)
    // TIMESTAMP_MILLIS is int64 of milliseconds since UNIX epoch)
    // Kdb timestamp is nano since 2000.01.01 00:00:00.0
    // Requires: epoch offsetting and scaling
    offset = kdb_timestamp_epoch_nano;
    scalar = ns_ms_scale;
    break;
  case avro::LogicalType::TIMESTAMP_MICROS:
    // Avro TIMESTAMP_MICROS <-> kdb timestamp (KP)
    // Timestamp is int64 of microseconds since UNIX epoch
    // Kdb timestamp is nano since 2000.01.01 00:00:00.0
    // Requires: epoch offsetting and scaling
    offset = kdb_timestamp_epoch_nano;
    scalar = ns_us_scale;
    break;
  default:
    TYPE_CHECK_UNSUPPORTED(field, std::to_string(logical_type));
  }
}