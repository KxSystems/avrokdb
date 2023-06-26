#pragma once

#include "HelperFunctions.h"


typedef signed char KdbType;

// Set of macros to assist with performing the type checks such the arguments
// required to generate the exception message are not evaluated unless the
// condition is met
#define TYPE_CHECK_DATUM(condition, field, datatype, expected, received) \
  if (condition) throw TypeCheckDatum(field, datatype, expected, received);
#define TYPE_CHECK_ARRAY(condition, field, datatype, expected, received) \
  if (condition) throw TypeCheckArray(field, datatype, expected, received);
#define TYPE_CHECK_FIXED(condition, field, expected, received) \
  if (condition) TypeCheckFixed(field, expected, received);
#define TYPE_CHECK_UNSUPPORTED(field, datatype) \
  throw TypeCheckUnsupported(field, datatype);


  // Hierachy of TypeCheck exceptions with each derived type being using for a
  // specific check when converting from a kdb object to an arow arrow.
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
    TypeCheck("Unsupported datatype, field: '" + field + "' datatype: '" + datatype + "'")
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

  case avro::AVRO_ARRAY:
  case avro::AVRO_UNION:
  case avro::AVRO_UNKNOWN:
  default:
    throw TypeCheck("GetKdbSimpleType - unsupported type: " + type);
  }
}

inline KdbType GetKdbType(const avro::GenericDatum& datum)
{
  switch (datum.type()) {
  case avro::AVRO_UNION:
  case avro::AVRO_UNKNOWN:
    throw TypeCheck("GetKdbType - unsupported type: " + datum.type());
  case avro::AVRO_ARRAY:
  {
    const auto& avro_array = datum.value<avro::GenericArray>();
    auto array_value = avro_array.value();
    auto array_schema = avro_array.schema();
    assert(array_schema->leaves() == 1);
    return GetKdbArrayType(array_schema->leafAt(0)->type());
  }
  default:
    return GetKdbSimpleType(datum.type());
  }
}
