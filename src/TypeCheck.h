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
  TypeCheckFixed(const std::string& field, int64_t expected, int64_t received) :
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
  TypeCheckKdb(const std::string& field, const std::string& datatype, const std::string& what, int expected, int received) :
    TypeCheck("Invalid kdb+ mapping, field: '" + field + "', datatype: '" + datatype + "', " + what + " expected: " + std::to_string(expected) + ", received : " + std::to_string(received))
  {};
};

inline KdbType GetKdbArrayType(avro::Type type)
{
  switch (type) {
  case avro::AVRO_BOOL:
    return KB;
  case avro::AVRO_BYTES:
    return 0;
  case avro::AVRO_DOUBLE:
    return KF;
  case avro::AVRO_ENUM:
    return KS;
  case avro::AVRO_FIXED:
    return 0;
  case avro::AVRO_FLOAT:
    return KE;
  case avro::AVRO_INT:
    return KI;
  case avro::AVRO_LONG:
    return KJ;
  case avro::AVRO_MAP:
    return 0;
  case avro::AVRO_NULL:
    return 0;
  case avro::AVRO_RECORD:
    return 0;
  case avro::AVRO_STRING:
    return 0;
  case avro::AVRO_UNION:
    return 0;
  case avro::AVRO_ARRAY:
    return 0;
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    throw TypeCheck("GetKdbArrayType - unsupported type: " + type);
  }
}

inline KdbType GetKdbSimpleType(avro::Type type)
{
  switch (type) {
  case avro::AVRO_BOOL:
    return -KB;
  case avro::AVRO_BYTES:
    return KG;
  case avro::AVRO_DOUBLE:
    return -KF;
  case avro::AVRO_ENUM:
    return -KS;
  case avro::AVRO_FIXED:
    return KG;
  case avro::AVRO_FLOAT:
    return -KE;
  case avro::AVRO_INT:
    return -KI;
  case avro::AVRO_LONG:
    return -KJ;
  case avro::AVRO_MAP:
    return 99;
  case avro::AVRO_NULL:
    return 101;
  case avro::AVRO_RECORD:
    return 99;
  case avro::AVRO_STRING:
    return KC;
  case avro::AVRO_UNION:
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
  if (!decompose_union)
    avro_type = GetRealType(datum);
  else
    avro_type = datum.type();

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
    return GetKdbArrayType(array_schema->leafAt(0)->type());
  }
  default:
    return GetKdbSimpleType(avro_type);
  }
}
