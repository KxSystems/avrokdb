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
    throw TypeCheck("GetKdbArrayType - unsupported type: " + type);
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
    throw TypeCheck("GetKdbSimpleType - unsupported type: " + type);
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

size_t InferUnionBranch(const UnionBranches& branches, K data)
{
  auto find_branch = [&branches, data](avro::Type type, avro::LogicalType::Type logical_type) {
    auto i = branches.find({ type, logical_type });
    if (i != branches.end())
      return i->second;
    else
      throw TypeCheck("Unable to find union branch for kdb+ type " + std::to_string((int)data->t));
  };

  switch (data->t) {
  case -KB:
    return find_branch(avro::AVRO_BOOL, avro::LogicalType::NONE);
  case KG:
  {
    auto bytes_branch = branches.find({ avro::AVRO_BYTES, avro::LogicalType::NONE });
    auto fixed_branch = branches.find({ avro::AVRO_FIXED, avro::LogicalType::NONE });
    if (bytes_branch != branches.end() && fixed_branch != branches.end())
      throw TypeCheck("Cannot infer union branch where union contains both bytes and fixes");

    if (bytes_branch != branches.end())
      return bytes_branch->second;
    else if (fixed_branch != branches.end())
      return fixed_branch->second;
    else
      throw TypeCheck("Unable to find union branch for kdb+ type " + std::to_string((int)data->t));
  }
  case -KF:
    return find_branch(avro::AVRO_DOUBLE, avro::LogicalType::NONE);
  case -KS:
    return find_branch(avro::AVRO_ENUM, avro::LogicalType::NONE);
  case -KE:
    return find_branch(avro::AVRO_FLOAT, avro::LogicalType::NONE);
  case -KD:
    return find_branch(avro::AVRO_INT, avro::LogicalType::DATE);
  case -KT:
    return find_branch(avro::AVRO_INT, avro::LogicalType::TIMESTAMP_MILLIS);
  case -KI:
    return find_branch(avro::AVRO_INT, avro::LogicalType::NONE);
  case -KN:
    return find_branch(avro::AVRO_LONG, avro::LogicalType::TIME_MICROS);
  case -KP:
  {
    auto millis_branch = branches.find({ avro::AVRO_LONG, avro::LogicalType::TIMESTAMP_MILLIS });
    auto micros_branch = branches.find({ avro::AVRO_LONG, avro::LogicalType::TIMESTAMP_MICROS });
    if (millis_branch != branches.end() && micros_branch != branches.end())
      throw TypeCheck("Cannot infer union branch where union contains both timestamp-millis and timestamp-micros");

    if (millis_branch != branches.end())
      return millis_branch->second;
    else if (micros_branch != branches.end())
      return micros_branch->second;
    else
      throw TypeCheck("Unable to find union branch for kdb+ type " + std::to_string((int)data->t));
  }
  case -KJ:
    return find_branch(avro::AVRO_LONG, avro::LogicalType::NONE);
  case 101:
    return find_branch(avro::AVRO_NULL, avro::LogicalType::NONE);
  case -UU:
    return find_branch(avro::AVRO_STRING, avro::LogicalType::UUID);
  case KC:
    return find_branch(avro::AVRO_STRING, avro::LogicalType::NONE);
  case 99:
  {
    auto map_branch = branches.find({ avro::AVRO_MAP, avro::LogicalType::NONE });
    auto record_branch = branches.find({ avro::AVRO_RECORD, avro::LogicalType::NONE });
    if (map_branch != branches.end() && record_branch != branches.end())
      throw TypeCheck("Cannot infer union branch where union contains both map and record");

    if (map_branch != branches.end())
      return map_branch->second;
    else if (record_branch != branches.end())
      return record_branch->second;
    else
      throw TypeCheck("Unable to find union branch for kdb+ type " + std::to_string((int)data->t));
  }
  case KB:
  case KF:
  case KS:
  case KE:
  case KD:
  case KT:
  case KN:
  case KP:
  case KJ:
  case UU:
    return find_branch(avro::AVRO_ARRAY, avro::LogicalType::NONE);
  case KI:
  {
    auto duration_branch = branches.find({ avro::AVRO_FIXED, avro::LogicalType::DURATION });
    auto array_branch = branches.find({ avro::AVRO_ARRAY, avro::LogicalType::NONE });
    if (duration_branch != branches.end() && array_branch != branches.end())
      throw TypeCheck("Cannot infer union branch where union contains both array and fixed(duration)");

    if (duration_branch != branches.end())
      return duration_branch->second;
    else if (array_branch != branches.end())
      return array_branch->second;
    else
      throw TypeCheck("Unable to find union branch for type " + std::to_string((int)data->t));
  }
  case 0:
  {
    auto decimal_bytes_branch = branches.find({ avro::AVRO_BYTES, avro::LogicalType::DECIMAL });
    auto decimal_fixed_branch = branches.find({ avro::AVRO_FIXED, avro::LogicalType::DECIMAL });
    auto union_branch = branches.find({ avro::AVRO_UNION, avro::LogicalType::NONE });
    auto array_branch = branches.find({ avro::AVRO_ARRAY, avro::LogicalType::NONE });
    size_t count = 0;
    if (decimal_bytes_branch != branches.end())
      ++count;
    if (decimal_fixed_branch != branches.end())
      ++count;
    if (union_branch != branches.end())
      ++count;
    if (array_branch != branches.end())
      ++count;

    if (count > 1)
      throw TypeCheck("Cannot infer union branch where union contains two of more of bytes(decimal), fixed(decimal), union or array");

    if (decimal_bytes_branch != branches.end())
      return decimal_bytes_branch->second;
    if (decimal_fixed_branch != branches.end())
      return decimal_fixed_branch->second;
    else if (union_branch != branches.end())
      return union_branch->second;
    else if (array_branch != branches.end())
      return array_branch->second;
    else
      throw TypeCheck("Unable to find union branch for kdb+ type " + std::to_string((int)data->t));
  }
  default:
    throw TypeCheck("Unable to find union branch for kdb+ type " + std::to_string((int)data->t));
  }
}