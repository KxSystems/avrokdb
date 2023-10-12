#include <sstream>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Stream.hh>
#include <avro/LogicalType.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "Encode.h"
#include "TypeCheck.h"
#include "KdbOptions.h"
#include "GenericForeign.h"


void EncodeArray(const std::string& field, avro::GenericArray& avro_array, K data);
void EncodeMap(const std::string& field, avro::GenericMap& avro_map, K data);
void EncodeRecord(const std::string& field, avro::GenericRecord& record, K data);
void EncodeUnion(const std::string& field, avro::GenericDatum& avro_union, K data);

std::vector<uint8_t> DecimalToBytes(const std::string& field, avro::Type avro_type, const avro::LogicalType& logical_type, K data)
{
  // DECIMAL is a mixed list of (precision; scale; bin_data)
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "decimal list length", 3, data->n);
  K precision = kK(data)[0];
  K scale = kK(data)[1];
  K k_bytes = kK(data)[2];
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "decimal precision type", -KI, precision->t);
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "decimal precision", logical_type.precision(), precision->i)
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "decimal scale type", -KI, scale->t);
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "decimal scale", logical_type.scale(), scale->i)
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "decimal data type", KG, k_bytes->t);

  std::vector<uint8_t> bytes(k_bytes->n);
  std::memcpy(bytes.data(), kG(k_bytes), k_bytes->n);
  return bytes;
}

std::vector<uint8_t> DurationToBytes(const std::string& field, avro::Type avro_type, K data)
{
  // DURATION is an int list of (month day milli)
  TYPE_CHECK_KDB(field, avro::toString(avro_type), "duration list length", 3, data->n);
  std::vector<uint8_t> fixed(sizeof(uint32_t) * 3);
  uint32_t values[3] = { (uint32_t)kI(data)[0], (uint32_t)kI(data)[1], (uint32_t)kI(data)[2] };
  std::memcpy(fixed.data(), values, sizeof(uint32_t) * 3);
  return fixed;
}

void EncodeDatum(const std::string& field, avro::GenericDatum& avro_datum, K data, bool decompose_union)
{
  avro::Type avro_type;
  avro::LogicalType logical_type(avro::LogicalType::NONE);
  if (!decompose_union) {
    avro_type = GetRealType(avro_datum);
    logical_type = GetRealLogicalType(avro_datum);
  }  else {
    avro_type = avro_datum.type();
    logical_type = avro_datum.logicalType();
  }

  TYPE_CHECK_DATUM(field, avro::toString(avro_type), GetKdbType(avro_datum, decompose_union), data->t);

  switch (avro_type) {
  case avro::AVRO_BOOL:
    avro_datum.value<bool>() = (bool)data->g;
    break;
  case avro::AVRO_BYTES:
  {
    if (logical_type.type() == avro::LogicalType::DECIMAL)
      avro_datum.value<std::vector<uint8_t>>() = DecimalToBytes(field, avro_type, logical_type, data);
    else {
      std::vector<uint8_t> bytes(data->n);
      std::memcpy(bytes.data(), kG(data), data->n);
      avro_datum.value<std::vector<uint8_t>>() = bytes;
    }
    break;
  }
  case avro::AVRO_DOUBLE:
    avro_datum.value<double>() = data->f;
    break;
  case avro::AVRO_ENUM:
    avro_datum.value<avro::GenericEnum>().set(data->s);
    break;
  case avro::AVRO_FIXED:
  {
    auto& avro_fixed = avro_datum.value<avro::GenericFixed>();
    const auto fixed_size = avro_fixed.schema()->fixedSize();

    if (logical_type.type() == avro::LogicalType::DECIMAL)
      avro_fixed.value() = DecimalToBytes(field, avro_type, logical_type, data);
    else if (logical_type.type() == avro::LogicalType::DURATION)
      avro_fixed.value() = DurationToBytes(field, avro_type, data);
    else {
      TYPE_CHECK_FIXED(field, fixed_size, (size_t)data->n);

      std::vector<uint8_t> fixed;
      fixed.resize(data->n);
      std::memcpy(fixed.data(), kG(data), data->n);
      avro_fixed.value() = fixed;
    }
    break;
  }
  case avro::AVRO_FLOAT:
    avro_datum.value<float>() = data->e;
    break;
  case avro::AVRO_INT:
  {
    if (logical_type.type() == avro::LogicalType::DATE || logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, logical_type.type());
      avro_datum.value<int32_t>() = tc.KdbToAvro(data->i);
    } else {
      avro_datum.value<int32_t>() = data->i;
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    if (logical_type.type() == avro::LogicalType::TIME_MICROS || logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
      TemporalConversion tc(field, logical_type.type());
      avro_datum.value<int64_t>() = tc.KdbToAvro(data->j);
    } else {
      avro_datum.value<int64_t>() = data->j;
    }
    break;
  }
  case avro::AVRO_NULL:
    break;
  case avro::AVRO_STRING:
  {
    if (logical_type.type() == avro::LogicalType::UUID)
      avro_datum.value<std::string>() = GuidToString(*(U*)kG(data));
    else
      avro_datum.value<std::string>() = std::string((char*)kG(data), data->n);
    break;
  }
  case avro::AVRO_RECORD:
  {
    auto& avro_record = avro_datum.value<avro::GenericRecord>();
    EncodeRecord(field, avro_record, data);
    break;
  }
  case avro::AVRO_ARRAY:
  {
    auto& avro_array = avro_datum.value<avro::GenericArray>();
    EncodeArray(field, avro_array, data);
    break;
  }
  case avro::AVRO_UNION:
  {
    EncodeUnion(field, avro_datum, data);
    break;
  }
  case avro::AVRO_MAP:
  {
    auto& avro_map = avro_datum.value<avro::GenericMap>();
    EncodeMap(field, avro_map, data);
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(avro_type));
  }
}

void EncodeArray(const std::string& field, avro::GenericArray& avro_array, K data)
{
  assert(avro_array.schema()->leaves() == 1);
  auto array_schema = avro_array.schema()->leafAt(0);
  auto array_type = array_schema->type();
  auto array_logical_type = array_schema->logicalType();
  auto& array_data = avro_array.value();

  switch (array_type) {
  case avro::AVRO_BOOL:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.emplace_back(std::move(avro::GenericDatum((bool)kG(data)[i])));
    break;
  }
  case avro::AVRO_BYTES:
  {
    if (array_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), 0, k_bytes->t);
        array_data.emplace_back(std::move(avro::GenericDatum(DecimalToBytes(field, array_type, array_logical_type, k_bytes))));
      }
    } else {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), KG, k_bytes->t);
        std::vector<uint8_t> bytes(k_bytes->n);
        std::memcpy(bytes.data(), kG(k_bytes), k_bytes->n);
        array_data.emplace_back(std::move(avro::GenericDatum(bytes)));
      }
    }
    break;
  }
  case avro::AVRO_DOUBLE:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.emplace_back(std::move(avro::GenericDatum(kF(data)[i])));
    break;
  }
  case avro::AVRO_ENUM:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.emplace_back(std::move(avro::GenericDatum(array_schema, avro::GenericEnum(array_schema, kS(data)[i]))));
    break;
  }
  case avro::AVRO_FIXED:
  {
    if (array_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), 0, k_bytes->t);
        array_data.emplace_back(std::move(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, DecimalToBytes(field, array_type, array_logical_type, k_bytes)))));
      }
    } else if (array_logical_type.type() == avro::LogicalType::DURATION) {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), KI, k_bytes->t);
        array_data.emplace_back(std::move(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, DurationToBytes(field, array_type, k_bytes)))));
      }
    } else {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        const auto fixed_size = array_schema->fixedSize();
        TYPE_CHECK_FIXED(field, fixed_size, (size_t)k_bytes->n);

        std::vector<uint8_t> fixed;
        fixed.resize(k_bytes->n);
        std::memcpy(fixed.data(), kG(k_bytes), k_bytes->n);
        array_data.emplace_back(std::move(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, fixed))));
      }
    }
    break;
  }
  case avro::AVRO_FLOAT:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.emplace_back(std::move(avro::GenericDatum(kE(data)[i])));
    break;
  }
  case avro::AVRO_INT:
  {
    if (array_logical_type.type() == avro::LogicalType::DATE || array_logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, array_logical_type.type());
      for (auto i = 0; i < data->n; ++i)
        array_data.emplace_back(std::move(avro::GenericDatum(tc.KdbToAvro(kI(data)[i]))));
    } else {
      for (auto i = 0; i < data->n; ++i)
        array_data.emplace_back(std::move(avro::GenericDatum(kI(data)[i])));
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    if (array_logical_type.type() == avro::LogicalType::TIME_MICROS || array_logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || array_logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
       TemporalConversion tc(field, array_logical_type.type());
      for (auto i = 0; i < data->n; ++i)
        array_data.emplace_back(std::move(avro::GenericDatum(tc.KdbToAvro<int64_t>(kJ(data)[i]))));
    } else {
      for (auto i = 0; i < data->n; ++i)
        array_data.emplace_back(std::move(avro::GenericDatum((int64_t)kJ(data)[i])));
    }
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_value = kK(data)[i];
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), 101, k_value->t);
      array_data.emplace_back(std::move(avro::GenericDatum()));
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    if (array_logical_type.type() == avro::LogicalType::UUID) {
      for (auto i = 0; i < data->n; ++i) {
        U k_uuid = kU(data)[i];
        array_data.emplace_back(std::move(avro::GenericDatum(GuidToString(k_uuid))));
      }
    } else {
      for (auto i = 0; i < data->n; ++i) {
        K k_string = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), KC, k_string->t);
        array_data.emplace_back(std::move(avro::GenericDatum(std::string((char*)kG(k_string), k_string->n))));
      }
    }
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_array = kK(data)[i];

      auto sub_array = array_schema;
      assert(sub_array->leaves() == 1);
      auto sub_array_type = GetKdbArrayType(sub_array->leafAt(0)->type(), sub_array->leafAt(0)->logicalType().type());
      TYPE_CHECK_ARRAY(field, avro::toString(sub_array->type()), sub_array_type, k_array->t);

      auto array_datum = avro::GenericArray(sub_array);
      EncodeArray(field, array_datum, k_array);
      array_data.emplace_back(std::move(avro::GenericDatum(sub_array, array_datum)));
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_record = kK(data)[i];
      if (k_record->t == 101)
        continue;
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), 99, k_record->t);

      auto record_datum = avro::GenericRecord(array_schema);
      EncodeRecord(field,  record_datum, k_record);
      array_data.emplace_back(std::move(avro::GenericDatum(array_schema, record_datum)));
    }
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_union = kK(data)[i];
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), 0, k_union->t);

      auto union_datum = avro::GenericDatum(array_schema);
      EncodeUnion(field, union_datum, k_union);
      array_data.emplace_back(std::move(union_datum));
    }
    break;
  }
  case avro::AVRO_MAP:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_map = kK(data)[i];
      if (k_map->t == 101)
        continue;
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), 99, k_map->t);

      auto map_datum = avro::GenericMap(array_schema);
      EncodeMap(field, map_datum, k_map);
      array_data.emplace_back(std::move(avro::GenericDatum(array_schema, map_datum)));
    }
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(array_type));
  }
}

void EncodeRecord(const std::string& field, avro::GenericRecord& record, K data)
{
  K keys = kK(data)[0];
  K values = kK(data)[1];
  TYPE_CHECK_KDB(field, avro::toString(avro::AVRO_RECORD), "dict keys", KS, keys->t);
  TYPE_CHECK_KDB(field, avro::toString(avro::AVRO_RECORD), "dict values", 0, values->t);
  assert(keys->n == values->n);

  for (auto i = 0; i < keys->n; ++i) {
    const std::string key = kS(keys)[i];
    K value = kK(values)[i];
    if (key == "" && value->t == 101)
      continue;

    auto& next = record.field(key);
    EncodeDatum(key, next, value, false);
  }
}

void EncodeMap(const std::string& field, avro::GenericMap& avro_map, K data)
{
  K keys = kK(data)[0];
  K values = kK(data)[1];
  TYPE_CHECK_KDB(field, avro::toString(avro::AVRO_MAP), "dict keys", KS, keys->t);
  assert(keys->n == values->n);

  assert(avro_map.schema()->leaves() == 2);
  auto map_schema = avro_map.schema()->leafAt(1);
  auto map_type = map_schema->type();
  auto map_logical_type = map_schema->logicalType();
  auto& map_data = avro_map.value();

  switch (map_type) {
  case avro::AVRO_BOOL:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum((bool)kG(values)[i]))));
    break;
  }
  case avro::AVRO_BYTES:
  {
    if (map_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i = 0; i < values->n; ++i) {
        K k_bytes = kK(values)[i];
        TYPE_CHECK_MAP(field, avro::toString(map_type), 0, k_bytes->t);
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(DecimalToBytes(field, map_type, map_logical_type, k_bytes)))));
      }
    } else {
      for (auto i = 0; i < values->n; ++i) {
        K k_bytes = kK(values)[i];
        TYPE_CHECK_MAP(field, avro::toString(map_type), KG, k_bytes->t);
        std::vector<uint8_t> bytes(k_bytes->n);
        std::memcpy(bytes.data(), kG(k_bytes), k_bytes->n);
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(bytes))));
      }
    }
    break;
  }
  case avro::AVRO_DOUBLE:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(kF(values)[i]))));
    break;
  }
  case avro::AVRO_ENUM:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(map_schema, avro::GenericEnum(map_schema, kS(values)[i])))));
    break;
  }
  case avro::AVRO_FIXED:
  {
    if (map_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i = 0; i < values->n; ++i) {
        K k_bytes = kK(values)[i];
        TYPE_CHECK_MAP(field, avro::toString(map_type), 0, k_bytes->t);
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(map_schema, avro::GenericFixed(map_schema, DecimalToBytes(field, map_type, map_logical_type, k_bytes))))));
      }
    } else if (map_logical_type.type() == avro::LogicalType::DURATION) {
      for (auto i = 0; i < values->n; ++i) {
        K k_bytes = kK(values)[i];
        TYPE_CHECK_MAP(field, avro::toString(map_type), KI, k_bytes->t);
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(map_schema, avro::GenericFixed(map_schema, DurationToBytes(field, map_type, k_bytes))))));
      }
    } else {
      for (auto i = 0; i < values->n; ++i) {
        K k_bytes = kK(values)[i];
        const auto fixed_size = map_schema->fixedSize();
        TYPE_CHECK_FIXED(field, fixed_size, (size_t)k_bytes->n);

        std::vector<uint8_t> fixed;
        fixed.resize(k_bytes->n);
        std::memcpy(fixed.data(), kG(k_bytes), k_bytes->n);
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(map_schema, avro::GenericFixed(map_schema, fixed)))));
      }
    }
    break;
  }
  case avro::AVRO_FLOAT:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(kE(values)[i]))));
    break;
  }
  case avro::AVRO_INT:
  {
    if (map_logical_type.type() == avro::LogicalType::DATE || map_logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, map_logical_type.type());
      for (auto i = 0; i < values->n; ++i)
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(tc.KdbToAvro(kI(values)[i])))));
    } else {
      for (auto i = 0; i < values->n; ++i)
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(kI(values)[i]))));
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    if (map_logical_type.type() == avro::LogicalType::TIME_MICROS || map_logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || map_logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
      TemporalConversion tc(field, map_logical_type.type());
      for (auto i = 0; i < values->n; ++i)
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(tc.KdbToAvro<int64_t>(kJ(values)[i])))));
    } else {
      for (auto i = 0; i < values->n; ++i)
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum((int64_t)kJ(values)[i]))));
    }
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_value = kK(values)[i];
      TYPE_CHECK_MAP(field, avro::toString(map_type), 101, k_value->t);
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum())));
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    if (map_logical_type.type() == avro::LogicalType::UUID) {
      for (auto i = 0; i < values->n; ++i) {
        U k_uuid = kU(values)[i];
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(GuidToString(k_uuid)))));
      }
    } else {
      for (auto i = 0; i < values->n; ++i) {
        K k_string = kK(values)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(map_type), KC, k_string->t);
        map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(std::string((char*)kG(k_string), k_string->n)))));
      }
    }
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_array = kK(values)[i];

      auto sub_array = map_schema;
      assert(sub_array->leaves() == 1);
      auto sub_map_type = GetKdbArrayType(sub_array->leafAt(0)->type(), sub_array->leafAt(0)->logicalType().type());
      TYPE_CHECK_MAP(field, avro::toString(sub_array->type()), sub_map_type, k_array->t);

      auto array_datum = avro::GenericArray(sub_array);
      EncodeArray(field, array_datum, k_array);
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(sub_array, array_datum))));
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_record = kK(values)[i];
      if (k_record->t == 101)
        continue;
      TYPE_CHECK_MAP(field, avro::toString(map_type), 99, k_record->t);

      auto record_datum = avro::GenericRecord(map_schema);
      EncodeRecord(field, record_datum, k_record);
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(map_schema, record_datum))));
    }
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_union = kK(values)[i];
      TYPE_CHECK_MAP(field, avro::toString(map_type), 0, k_union->t);

      auto union_datum = avro::GenericDatum(map_schema);
      EncodeUnion(field, union_datum, k_union);
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], union_datum)));
    }
    break;
  }
  case avro::AVRO_MAP:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_map = kK(values)[i];
      if (k_map->t == 101)
        continue;
      TYPE_CHECK_MAP(field, avro::toString(map_type), 99, k_map->t);

      auto map_datum = avro::GenericMap(map_schema);
      EncodeMap(field, map_datum, k_map);
      map_data.emplace_back(std::move(std::pair<std::string, avro::GenericDatum>(kS(keys)[i], avro::GenericDatum(map_schema, map_datum))));
    }
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(map_type));
  }
}

void EncodeUnion(const std::string& field, avro::GenericDatum& avro_union, K data)
{
  TYPE_CHECK_KDB(field, avro::toString(avro::AVRO_UNION), "mixed list length", 2, (int)data->n);

  K k_branch = kK(data)[0];
  K k_datum = kK(data)[1];

  // Even though a union branch is a size_t we're going to represent it as a -KH
  // Realistically no one will even have a union with > 16K branches
  // This avoids type promotion problems with a long union where (0; 123) would become (0 123)
  // Don't want yet to have to introduce yet more (::)
  // Avro don't have a short int type so it cannot be promoted
  TYPE_CHECK_KDB(field, avro::toString(avro::AVRO_UNION), "mixed list[0] branch selector", -KH, k_branch->t);

  avro_union.selectBranch(k_branch->h);
  EncodeDatum(field, avro_union, k_datum, true);
}

class KdbMemoryOutputStream : public avro::OutputStream {
public:
  const size_t chunkSize_;
  std::vector<uint8_t*> data_;
  size_t available_;
  size_t byteCount_;

  explicit KdbMemoryOutputStream(size_t chunkSize = 4 * 1024) : chunkSize_(chunkSize),
    available_(0), byteCount_(0) {}
  ~KdbMemoryOutputStream() final {
    for (std::vector<uint8_t*>::const_iterator it = data_.begin();
      it != data_.end(); ++it) {
      delete[] * it;
    }
  }

  bool next(uint8_t** data, size_t* len) final {
    if (available_ == 0) {
      data_.emplace_back(std::move(new uint8_t[chunkSize_]));
      available_ = chunkSize_;
    }
    *data = &data_.back()[chunkSize_ - available_];
    *len = available_;
    byteCount_ += available_;
    available_ = 0;
    return true;
  }

  void backup(size_t len) final {
    available_ += len;
    byteCount_ -= len;
  }

  uint64_t byteCount() const final {
    return byteCount_;
  }

  void flush() final {}

  K ToKdb(KdbType type) {
    K result = ktn(type, byteCount_);
    size_t index = 0;
    size_t remaining = byteCount_;
    for (const auto i : data_) {
      size_t bytes_from_chunk = remaining > chunkSize_ ? chunkSize_ : remaining;
      std::memcpy(kG(result) + index, i, bytes_from_chunk);
      index += bytes_from_chunk;
      remaining -= bytes_from_chunk;
      if (!remaining)
        break;
    }
    return result;
  }
};

K Encode(K schema, K data, K options)
{
  KDB_EXCEPTION_TRY;

  auto options_parser = KdbOptions(options, Options::string_options, Options::int_options);

  auto avro_foreign = GetForeign<AvroForeign>(schema);
  auto avro_schema = avro_foreign->schema;
  
  std::string avro_format = "BINARY";
  options_parser.GetStringOption(Options::AVRO_FORMAT, avro_format);

  int64_t multithreaded = 0;
  options_parser.GetIntOption(Options::MULTITHREADED, multithreaded);

  // Find the encoder to use.  Encoders don't support multithreaded use so if
  // running in this mode the encoder is created on the fly.  If running single
  // threaded we use the already created encoder in the foreign which is less
  // expensive.
  avro::EncoderPtr encoder;
  if (multithreaded) {
    avro::EncoderPtr base_encoder;
    if (avro_format == "BINARY")
      base_encoder = avro::binaryEncoder();
    else if (avro_format == "JSON")
      base_encoder = avro::jsonEncoder(*avro_schema.get());
    else if (avro_format == "JSON_PRETTY")
      base_encoder = avro::jsonPrettyEncoder(*avro_schema.get());
    else
      return krr((S)"Unsupported avro encoding type (should be BINARY, JSON or JSON_PRETTY)");

    encoder = avro::validatingEncoder(*avro_schema.get(), base_encoder);
  } else {
    if (avro_format == "BINARY")
      encoder = avro_foreign->binary_encoder;
    else if (avro_format == "JSON")
      encoder = avro_foreign->json_encoder;
    else if (avro_format == "JSON_PRETTY")
      encoder = avro_foreign->json_pretty_encoder;
    else
      return krr((S)"Unsupported avro encoding type (should be BINARY, JSON or JSON_PRETTY)");
  }

  auto datum = avro::GenericDatum(*avro_schema.get());
  EncodeDatum("", datum, data, false);

  KdbMemoryOutputStream ostream;
  encoder->init(ostream);

  avro::GenericWriter writer(*avro_schema.get(), encoder);
  writer.write(datum);

  encoder->flush();

  return ostream.ToKdb(avro_format == "BINARY" ? KG : KC);

  KDB_EXCEPTION_CATCH;
}
