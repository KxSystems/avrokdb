#include <sstream>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/Decoder.hh>
#include <avro/Stream.hh>
#include <avro/LogicalType.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "Decode.h"
#include "TypeCheck.h"


K decodeMap(const std::string& field, const avro::GenericMap& avro_map);
K decodeRecord(const std::string& field, const avro::GenericRecord& record);
K decodeUnion(const std::string& field, const avro::GenericDatum& avro_union);

K decodeArray(const std::string& field, const avro::GenericArray& array_datum)
{
  auto array_schema = array_datum.schema();
  assert(array_schema->leaves() == 1);
  auto array_type = array_schema->leafAt(0)->type();
  auto array_logical_type = array_schema->leafAt(0)->logicalType();
  const auto& array_data = array_datum.value();

  size_t result_len = array_data.size();
  if (array_type == avro::AVRO_RECORD || array_type == avro::AVRO_MAP) {
    // We put a (::) at the start of an array of records/maps so need one more item
    ++result_len;
  }
  K result = ktn(GetKdbArrayType(array_type, array_logical_type.type()), result_len);
  size_t index = 0;

  switch (array_type) {
  case avro::AVRO_BOOL:
  {
    for (auto i : array_data)
      kG(result)[index++] = i.value<bool>();
    break;
  }
  case avro::AVRO_BYTES:
  {
    for (auto i : array_data) {
      const auto& bytes = i.value<std::vector<uint8_t>>();
      K k_bytes = ktn(KG, bytes.size());
      std::memcpy(kG(k_bytes), bytes.data(), bytes.size());
      kK(result)[index++] = k_bytes;
    }
    break;
  }
  case avro::AVRO_DOUBLE:
  {
    for (auto i : array_data)
      kF(result)[index++] = i.value<double>();
    break;
  }
  case avro::AVRO_ENUM:
  {
    for (auto i : array_data)
      kS(result)[index++] = ss((S)i.value<avro::GenericEnum>().symbol().c_str());
    break;
  }
  case avro::AVRO_FIXED:
  {
    for (auto i : array_data) {
      const auto& fixed = i.value<avro::GenericFixed>().value();
      K k_fixed = ktn(KG, fixed.size());
      std::memcpy(kG(k_fixed), fixed.data(), fixed.size());
      kK(result)[index++] = k_fixed;
    }
    break;
  }
  case avro::AVRO_FLOAT:
  {
    for (auto i : array_data)
      kE(result)[index++] = i.value<float>();
    break;
  }
  case avro::AVRO_INT:
  {
    for (auto i : array_data)
      kI(result)[index++] = i.value<int32_t>();
    break;
  }
  case avro::AVRO_LONG:
  {
    for (auto i : array_data)
      kJ(result)[index++] = i.value<int64_t>();
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i : array_data)
      kK(result)[index++] = identity();
    break;
  }
  case avro::AVRO_STRING:
  {
    for (auto i : array_data) {
      const auto& string = i.value<std::string>();
      K k_string = ktn(KC, string.length());
      std::memcpy(kG(k_string), string.c_str(), string.length());
      kK(result)[index++] = k_string;
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    kK(result)[index++] = identity();
    for (auto i : array_data)
      kK(result)[index++] = decodeRecord(field, i.value<avro::GenericRecord>());
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i : array_data)
      kK(result)[index++] = decodeArray(field, i.value<avro::GenericArray>());
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i : array_data)
      kK(result)[index++] = decodeUnion(field, i);
    break;
  }
  case avro::AVRO_MAP:
  {
    kK(result)[index++] = identity();
    for (auto i : array_data)
      kK(result)[index++] = decodeMap(field, i.value<avro::GenericMap>());
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED_LEAF(field, avro::toString(avro::AVRO_ARRAY), avro::toString(array_type));
  }

  return result;
}

K decodeMap(const std::string& field, const avro::GenericMap& map_datum)
{
  auto map_schema = map_datum.schema();
  assert(map_schema->leaves() == 2);
  auto map_type = map_schema->leafAt(1)->type();
  auto map_logical_type = map_schema->leafAt(0)->logicalType();
  const auto& map_data = map_datum.value();

  size_t result_len = map_data.size();
  if (map_type == avro::AVRO_RECORD || map_type == avro::AVRO_MAP) {
    // We put a (::) at the start of an array of records/maps so need one more item
    ++result_len;
  }

  K keys = ktn(KS, result_len);
  K values = ktn(GetKdbArrayType(map_type, map_logical_type.type()), result_len);
  size_t index = 0;

  switch (map_type) {
  case avro::AVRO_BOOL:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kG(values)[index++] = i.second.value<bool>();
    }
    break;
  }
  case avro::AVRO_BYTES:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      const auto& bytes = i.second.value<std::vector<uint8_t>>();
      K k_bytes = ktn(KG, bytes.size());
      std::memcpy(kG(k_bytes), bytes.data(), bytes.size());
      kK(values)[index++] = k_bytes;
    }
    break;
  }
  case avro::AVRO_DOUBLE:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kF(values)[index++] = i.second.value<double>();
    }
    break;
  }
  case avro::AVRO_ENUM:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kS(values)[index++] = ss((S)i.second.value<avro::GenericEnum>().symbol().c_str());
    }
    break;
  }
  case avro::AVRO_FIXED:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      const auto& fixed = i.second.value<avro::GenericFixed>().value();
      K k_fixed = ktn(KG, fixed.size());
      std::memcpy(kG(k_fixed), fixed.data(), fixed.size());
      kK(values)[index++] = k_fixed;
    }
    break;
  }
  case avro::AVRO_FLOAT:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kE(values)[index++] = i.second.value<float>();
    }
    break;
  }
  case avro::AVRO_INT:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kI(values)[index++] = i.second.value<int32_t>();
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kJ(values)[index++] = i.second.value<int64_t>();
    }
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = identity();
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      const auto& string = i.second.value<std::string>();
      K k_string = ktn(KC, string.length());
      std::memcpy(kG(k_string), string.c_str(), string.length());
      kK(values)[index++] = k_string;
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    kS(values)[index] = ss((S)"");
    kK(values)[index++] = identity();
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = decodeRecord(field, i.second.value<avro::GenericRecord>());
    }
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = decodeArray(field, i.second.value<avro::GenericArray>());
    }
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = decodeUnion(field, i.second);
    }
    break;
  }
  case avro::AVRO_MAP:
  {
    kS(keys)[index] = ss((S)"");
    kK(values)[index++] = identity();
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = decodeMap(field, i.second.value<avro::GenericMap>());
    }
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED_LEAF(field, avro::toString(avro::AVRO_MAP), avro::toString(map_type));
  }

  return xD(keys, values);
}

K decodeDatum(const std::string& field, const avro::GenericDatum& datum, bool decompose_union)
{
  avro::Type avro_type;
  avro::LogicalType logical_type(avro::LogicalType::NONE);
  if (!decompose_union) {
    avro_type = GetRealType(datum);
    logical_type = GetRealLogicalType(datum);
  } else {
    avro_type = datum.type();
    logical_type = datum.logicalType();
  }

  switch (avro_type) {
  case avro::AVRO_BOOL:
    return kb(datum.value<bool>());
  case avro::AVRO_BYTES:
  {
    const auto& bytes = datum.value<std::vector<uint8_t>>();
    K k_bytes = ktn(KG, bytes.size());
    std::memcpy(kG(k_bytes), bytes.data(), bytes.size());

    switch (logical_type.type()) {
    case avro::LogicalType::DECIMAL:
    {
      // DECIMAL is a mixed list of (precision; scale; bin_data)
      K result = ktn(0, 3);
      kK(result)[0] = ki(logical_type.precision());
      kK(result)[1] = ki(logical_type.scale());
      kK(result)[2] = k_bytes;
      return result;
    }
    default:
    {
      const auto& bytes = datum.value<std::vector<uint8_t>>();
      K result = ktn(KG, bytes.size());
      std::memcpy(kG(result), bytes.data(), bytes.size());
      return k_bytes;;
    }
    }
  }
  case avro::AVRO_DOUBLE:
    return kf(datum.value<double>());
  case avro::AVRO_ENUM:
    return ks(S(datum.value<avro::GenericEnum>().symbol().c_str()));
  case avro::AVRO_FIXED:
  {
    const auto& fixed = datum.value<avro::GenericFixed>().value();

    switch (logical_type.type()) {
    case avro::LogicalType::DECIMAL:
    {
      K k_bytes = ktn(KG, fixed.size());
      std::memcpy(kG(k_bytes), fixed.data(), fixed.size());

      // DECIMAL is a mixed list of (precision; scale; bin_data)
      K result = ktn(0, 3);
      kK(result)[0] = ki(logical_type.precision());
      kK(result)[1] = ki(logical_type.scale());
      kK(result)[2] = k_bytes;
      return result;
    }
    case avro::LogicalType::DURATION:
    {
      // DURATION is an int list of (month day milli)
      K result = ktn(KI, 3);
      uint32_t values[3];
      std::memcpy(values, fixed.data(), sizeof(uint32_t) * 3);
      kI(result)[0] = values[0];
      kI(result)[1] = values[1];
      kI(result)[2] = values[2];
      return result;
    }
    default:
    {
      K result = ktn(KG, fixed.size());
      std::memcpy(kG(result), fixed.data(), fixed.size());
      return result;
    }
    }
  }
  case avro::AVRO_FLOAT:
    return ke(datum.value<float>());
  case avro::AVRO_INT:
  {
    switch (logical_type.type()) {
    case avro::LogicalType::DATE:
    {
      TemporalConversion tc(field, logical_type.type());
      return kd(tc.AvroToKdb(datum.value<int32_t>()));
    }
    case avro::LogicalType::TIME_MILLIS:
    {
      TemporalConversion tc(field, logical_type.type());
      return kt(tc.AvroToKdb(datum.value<int32_t>()));
    }
    default:
      return ki(datum.value<int32_t>());
    }
  }
  case avro::AVRO_LONG:
  {
    switch (logical_type.type()) {
    case avro::LogicalType::TIME_MICROS:
    {
      TemporalConversion tc(field, logical_type.type());
      return ktj(-KN, tc.AvroToKdb(datum.value<int64_t>()));
    }
    case avro::LogicalType::TIMESTAMP_MILLIS:
    case avro::LogicalType::TIMESTAMP_MICROS:
    {
      TemporalConversion tc(field, logical_type.type());
      return ktj(-KP, tc.AvroToKdb(datum.value<int64_t>()));
    }
    default:
      return kj(datum.value<int64_t>());
    }
  }
  case avro::AVRO_NULL:
    return identity();;
  case avro::AVRO_STRING:
  {
    const auto& string = datum.value<std::string>();
    switch (logical_type.type()) {
    case avro::LogicalType::UUID:
    {
      TYPE_CHECK_KDB(field, avro::toString(avro_type), "avro uuid length", sizeof(U), string.length());
      U k_guid;
      std::memcpy(k_guid.g, string.data(), string.length());
      return ku(k_guid);
    }
    default:
    {
      K result = ktn(KC, string.length());
      std::memcpy(kG(result), string.c_str(), string.length());
      return result;
    }
    }
  }
  case avro::AVRO_RECORD:
    return decodeRecord(field, datum.value<avro::GenericRecord>());
  case avro::AVRO_ARRAY:
    return decodeArray(field, datum.value<avro::GenericArray>());
  case avro::AVRO_UNION:
    return decodeUnion(field, datum);
  case avro::AVRO_MAP:
    return decodeMap(field, datum.value<avro::GenericMap>());

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(avro_type));
  }
}

K decodeRecord(const std::string& field, const avro::GenericRecord& record)
{
  K keys = ktn(KS, record.fieldCount() + 1);
  kS(keys)[0] = ss((S)"");
  K values = ktn(0, record.fieldCount() + 1);
  kK(values)[0] = identity();

  size_t index = 1;
  for (auto i = 0; i < record.fieldCount(); ++i) {
    const auto& next = record.fieldAt(i);
    const auto& name = record.schema()->nameAt(i);
    kS(keys)[index] = ss((S)name.c_str());
    kK(values)[index] = decodeDatum(name, next, false);
    ++index;
  }

  return xD(keys, values);
}

K decodeUnion(const std::string& field, const avro::GenericDatum& avro_union)
{
  K result = ktn(0, 2);
  kK(result)[0] = kh((I)avro_union.unionBranch());
  kK(result)[1] = decodeDatum(field, avro_union, true);

  return result;
}

K decode(K schema, K data)
{
  if (data->t != KG)
    return krr((S)"data not 4h");

  KDB_EXCEPTION_TRY;

  auto avro_schema = GetAvroSchema(schema);

  auto decoder = avro::validatingDecoder(*avro_schema.get(), avro::binaryDecoder());

  std::istringstream iss;
  iss.str(std::string((char*)kG(data), data->n));

  auto istream = avro::istreamInputStream(iss);
  decoder->init(*istream);

  avro::GenericReader reader(*avro_schema.get(), decoder);

  avro::GenericDatum datum;
  reader.read(datum);
  reader.drain();

  return decodeDatum("", datum, false);

  KDB_EXCEPTION_CATCH;
}
