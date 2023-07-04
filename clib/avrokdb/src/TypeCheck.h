#pragma once

#include "HelperFunctions.h"


typedef signed char KdbType;

// Set of macros to assist with performing the type checks such the arguments
// required to generate the exception message are not evaluated unless the
// condition is met
#define TYPE_CHECK_DATUM(field, datatype, expected, received) \
  if (expected != received) throw TypeCheckDatum(field, datatype, expected, received);
#define TYPE_CHECK_ARRAY(field, datatype, expected, received) \
  if (expected != received) throw TypeCheckArray(field, datatype, expected, received);
#define TYPE_CHECK_MAP(field, datatype, expected, received) \
  if (expected != received) throw TypeCheckMap(field, datatype, expected, received);
#define TYPE_CHECK_FIXED(field, expected, received) \
  if (expected != received) throw TypeCheckFixed(field, expected, received);
#define TYPE_CHECK_KDB(field, datatype, what, expected, received) \
  if (expected != received) throw TypeCheckKdb(field, datatype, what, expected, received);
#define TYPE_CHECK_UNSUPPORTED(field, datatype) \
  throw TypeCheckUnsupported(field, datatype);
#define TYPE_CHECK_UNSUPPORTED_LEAF(field, datatype, leaf_datatype) \
  throw TypeCheckUnsupported(field, datatype, leaf_datatype);


  // Hierachy of TypeCheck exceptions with each derived type being using for a
  // specific check when converting from a kdb object to avro.
class TypeCheck : public std::invalid_argument
{
public:
  TypeCheck(std::string message) : std::invalid_argument(message.c_str()) {};
};

class TypeCheckDatum : public TypeCheck
{
public:
  TypeCheckDatum(const std::string& field, const std::string& datatype, int expected, int received) :
    TypeCheck("Invalid datum, field: '" + field + "', datatype: '" + datatype + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

class TypeCheckArray : public TypeCheck
{
public:
  TypeCheckArray(const std::string& field, const std::string& datatype, int expected, int received) :
    TypeCheck("Invalid array datum, field: '" + field + "', array datatype: '" + datatype + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

class TypeCheckMap : public TypeCheck
{
public:
  TypeCheckMap(const std::string& field, const std::string& datatype, int expected, int received) :
    TypeCheck("Invalid map datum, field: '" + field + "', map datatype: '" + datatype + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

class TypeCheckFixed : public TypeCheck
{
public:
  TypeCheckFixed(const std::string& field, size_t expected, size_t received) :
    TypeCheck("Invalid fixed length, field: '" + field + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

class TypeCheckUnsupported : public TypeCheck
{
public:
  TypeCheckUnsupported(const std::string& field, const std::string& datatype) :
    TypeCheck("Unsupported datatype, field: '" + field + "', datatype: '" + datatype + "'")
  {};
  TypeCheckUnsupported(const std::string& field, const std::string& datatype, const std::string& leaf_datatype) :
    TypeCheck("Unsupported datatype, field: '" + field + "', datatype: '" + datatype + "', leaf datatype: '" + leaf_datatype + "'")
  {};
};

class TypeCheckKdb : public TypeCheck
{
public:
  TypeCheckKdb(const std::string& field, const std::string& datatype, const std::string& what, int64_t expected, int64_t received) :
    TypeCheck("Invalid kdb+ mapping, field: '" + field + "', datatype: '" + datatype + "', " + what + " expected: " + std::to_string(expected) + ", received : " + std::to_string(received))
  {};
};

inline KdbType GetKdbArrayType(avro::Type type, avro::LogicalType::Type logical_type)
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
    throw TypeCheck("GetKdbArrayType - unsupported type: " + type);
  }
}

inline KdbType GetKdbSimpleType(avro::Type type, avro::LogicalType::Type logical_type)
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
    throw TypeCheck("GetKdbSimpleType - unsupported type: " + type);
  }
}

inline KdbType GetKdbType(const avro::GenericDatum& datum, bool decompose_union)
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
    throw TypeCheck("GetKdbType - unsupported type: " + avro_type);
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
