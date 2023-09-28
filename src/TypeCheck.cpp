#include <avro/Types.hh>
#include <avro/LogicalType.hh>
#include <avro/GenericDatum.hh>

#include "TypeCheck.h"


KdbType GetKdbArrayType(avro::Type type, avro::LogicalType::Type logical_type)
{
  switch (type) {
  case avro::AVRO_BOOL:
    return KB;
  case avro::AVRO_BYTES:
    // DECIMAL array is a mixed list of mixed lists of (precision; scale; bin_data)
    // AVRO_BYTES array is a mixed list of KG
    return 0;
  case avro::AVRO_DOUBLE:
    return KF;
  case avro::AVRO_ENUM:
    return KS;
  case avro::AVRO_FIXED:
    // DECIMAL array is a mixed list of mixed lists of (precision; scale; bin_data)
    // DURATION array is mixed list of int lists of (month day milli)
    // AVRO_FIXED array is a mixed list of KG
    return 0;
  case avro::AVRO_FLOAT:
    return KE;
  case avro::AVRO_INT:
    if (logical_type == avro::LogicalType::DATE)
      return KD;
    else if (logical_type == avro::LogicalType::TIME_MILLIS)
      return KT;
    else
      return KI;
  case avro::AVRO_LONG:
    if (logical_type == avro::LogicalType::TIME_MICROS)
      return KN;
    else if (logical_type == avro::LogicalType::TIMESTAMP_MILLIS || logical_type == avro::LogicalType::TIMESTAMP_MICROS)
      return KP;
    else
      return KJ;
  case avro::AVRO_MAP:
    // AVRO_MAP array is a mixed list of 99h
    return 0;
  case avro::AVRO_NULL:
    // AVRO_NULL array is a mixed list of (::)
    return 0;
  case avro::AVRO_RECORD:
    // AVRO_RECORD array is a mixed list of 99h
    return 0;
  case avro::AVRO_STRING:
    if (logical_type == avro::LogicalType::UUID)
      return UU;
    else {
      // AVRO_STRING array is a mixed list of KC
      return 0;
    }
  case avro::AVRO_UNION:
    // AVRO_UNION array is a mixed list of mixed lists of (branch selector; datum)
    return 0;
  case avro::AVRO_ARRAY:
    // AVRO_ARRAY array is a mixed list of datum lists
    return 0;
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    throw TypeCheck("GetKdbArrayType - unsupported type: " + std::to_string(type));
  }
}

KdbType GetKdbSimpleType(avro::Type type, avro::LogicalType::Type logical_type)
{
  switch (type) {
  case avro::AVRO_BOOL:
    return -KB;
  case avro::AVRO_BYTES:
    if (logical_type == avro::LogicalType::DECIMAL)
      // DECIMAL is a mixed list (precision; scale; bin_data)
      return 0;
    else
      return KG;
  case avro::AVRO_DOUBLE:
    return -KF;
  case avro::AVRO_ENUM:
    return -KS;
  case avro::AVRO_FIXED:
    if (logical_type == avro::LogicalType::DECIMAL)
      // DECIMAL is a mixed list of (precision; scale; bin_data)
      return 0;
    else if (logical_type == avro::LogicalType::DURATION)
      // DURATION is an int list of (month day milli)
      return KI;
    else
      return KG;
  case avro::AVRO_FLOAT:
    return -KE;
  case avro::AVRO_INT:
    if (logical_type == avro::LogicalType::DATE)
      return -KD;
    else if (logical_type == avro::LogicalType::TIME_MILLIS)
      return -KT;
    else
      return -KI;
  case avro::AVRO_LONG:
    if (logical_type == avro::LogicalType::TIME_MICROS)
      return -KN;
    else if (logical_type == avro::LogicalType::TIMESTAMP_MILLIS || logical_type == avro::LogicalType::TIMESTAMP_MICROS)
      return -KP;
    else
      return -KJ;
  case avro::AVRO_MAP:
    return 99;
  case avro::AVRO_NULL:
    return 101;
  case avro::AVRO_RECORD:
    return 99;
  case avro::AVRO_STRING:
    if (logical_type == avro::LogicalType::UUID)
      return -UU;
    else
      return KC;
  case avro::AVRO_UNION:
    // AVRO_UNION is a mixed list of (branch selector; datum)
    return 0;

  case avro::AVRO_ARRAY:
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    throw TypeCheck("GetKdbSimpleType - unsupported type: " + std::to_string(type));
  }
}

KdbType GetKdbType(const avro::GenericDatum& datum, bool decompose_union)
{
  avro::Type avro_type;
  avro::LogicalType::Type logical_type(avro::LogicalType::NONE);
  if (!decompose_union) {
    avro_type = GetRealType(datum);
    logical_type = GetRealLogicalType(datum).type();
  } else {
    avro_type = datum.type();
    logical_type = datum.logicalType().type();
  }

  switch (avro_type) {
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
    throw TypeCheck("GetKdbType - unsupported type: " + std::to_string(avro_type));
  case avro::AVRO_ARRAY:
  {
    const auto& avro_array = datum.value<avro::GenericArray>();
    auto array_value = avro_array.value();
    auto array_schema = avro_array.schema();
    assert(array_schema->leaves() == 1);
    return GetKdbArrayType(array_schema->leafAt(0)->type(), array_schema->leafAt(0)->logicalType().type());
  }
  default:
    return GetKdbSimpleType(avro_type, logical_type);
  }
}
