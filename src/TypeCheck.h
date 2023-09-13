#pragma once

#include "HelperFunctions.h"

typedef signed char KdbType;

// Map avro types and logical types to a kdb+ type
KdbType GetKdbArrayType(avro::Type type, avro::LogicalType::Type logical_type);
KdbType GetKdbSimpleType(avro::Type type, avro::LogicalType::Type logical_type);
KdbType GetKdbType(const avro::GenericDatum& datum, bool decompose_union);


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
