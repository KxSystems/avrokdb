#include <sstream>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/Encoder.hh>
#include <avro/Stream.hh>
#include <avro/LogicalType.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "Encode.h"
#include "TypeCheck.h"


void encodeArray(const std::string& field, avro::GenericArray& avro_array, K data);
void encodeMap(const std::string& field, avro::GenericMap& avro_map, K data);
void encodeRecord(const std::string& field, avro::GenericRecord& record, K data);
void encodeUnion(const std::string& field, avro::GenericDatum& avro_union, K data);

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

void encodeDatum(const std::string& field, avro::GenericDatum& avro_datum, K data, bool decompose_union)
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
      TYPE_CHECK_FIXED(field, fixed_size, data->n);

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
      avro_datum.value<std::string>() = std::string((char*)kG(data), sizeof(U));
    else
      avro_datum.value<std::string>() = std::string((char*)kG(data), data->n);
    break;
  }
  case avro::AVRO_RECORD:
  {
    auto& avro_record = avro_datum.value<avro::GenericRecord>();
    encodeRecord(field, avro_record, data);
    break;
  }
  case avro::AVRO_ARRAY:
  {
    auto& avro_array = avro_datum.value<avro::GenericArray>();
    encodeArray(field, avro_array, data);
    break;
  }
  case avro::AVRO_UNION:
  {
    encodeUnion(field, avro_datum, data);
    break;
  }
  case avro::AVRO_MAP:
  {
    auto& avro_map = avro_datum.value<avro::GenericMap>();
    encodeMap(field, avro_map, data);
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(avro_type));
  }
}

void encodeArray(const std::string& field, avro::GenericArray& avro_array, K data)
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
      array_data.push_back(avro::GenericDatum((bool)kG(data)[i]));
    break;
  }
  case avro::AVRO_BYTES:
  {
    if (array_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), 0, k_bytes->t);
        array_data.push_back(avro::GenericDatum(DecimalToBytes(field, array_type, array_logical_type, k_bytes)));
      }
    } else {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), KG, k_bytes->t);
        std::vector<uint8_t> bytes(k_bytes->n);
        std::memcpy(bytes.data(), kG(k_bytes), k_bytes->n);
        array_data.push_back(avro::GenericDatum(bytes));
      }
    }
    break;
  }
  case avro::AVRO_DOUBLE:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.push_back(avro::GenericDatum(kF(data)[i]));
    break;
  }
  case avro::AVRO_ENUM:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.push_back(avro::GenericDatum(array_schema, avro::GenericEnum(array_schema, kS(data)[i])));
    break;
  }
  case avro::AVRO_FIXED:
  {
    if (array_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), 0, k_bytes->t);
        array_data.push_back(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, DecimalToBytes(field, array_type, array_logical_type, k_bytes))));
      }
    } else if (array_logical_type.type() == avro::LogicalType::DURATION) {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), KI, k_bytes->t);
        array_data.push_back(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, DurationToBytes(field, array_type, k_bytes))));
      }
    } else {
      for (auto i = 0; i < data->n; ++i) {
        K k_bytes = kK(data)[i];
        const auto fixed_size = array_schema->fixedSize();
        TYPE_CHECK_FIXED(field, fixed_size, k_bytes->n);

        std::vector<uint8_t> fixed;
        fixed.resize(k_bytes->n);
        std::memcpy(fixed.data(), kG(k_bytes), k_bytes->n);
        array_data.push_back(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, fixed)));
      }
    }
    break;
  }
  case avro::AVRO_FLOAT:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.push_back(avro::GenericDatum(kE(data)[i]));
    break;
  }
  case avro::AVRO_INT:
  {
    if (array_logical_type.type() == avro::LogicalType::DATE || array_logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, array_logical_type.type());
      for (auto i = 0; i < data->n; ++i)
        array_data.push_back(avro::GenericDatum(tc.KdbToAvro(kI(data)[i])));
    } else {
      for (auto i = 0; i < data->n; ++i)
        array_data.push_back(avro::GenericDatum(kI(data)[i]));
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    if (array_logical_type.type() == avro::LogicalType::TIME_MICROS || array_logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || array_logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
       TemporalConversion tc(field, array_logical_type.type());
      for (auto i = 0; i < data->n; ++i)
        array_data.push_back(avro::GenericDatum(tc.KdbToAvro(kJ(data)[i])));
    } else {
      for (auto i = 0; i < data->n; ++i)
        array_data.push_back(avro::GenericDatum(kJ(data)[i]));
    }
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_value = kK(data)[i];
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), 101, k_value->t);
      array_data.push_back(avro::GenericDatum());
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    if (array_logical_type.type() == avro::LogicalType::UUID) {
      for (auto i = 0; i < data->n; ++i) {
        U k_uuid = kU(data)[i];
        array_data.push_back(avro::GenericDatum(std::string((char*)k_uuid.g, sizeof(U))));
      }
    } else {
      for (auto i = 0; i < data->n; ++i) {
        K k_string = kK(data)[i];
        TYPE_CHECK_ARRAY(field, avro::toString(array_type), KC, k_string->t);
        array_data.push_back(avro::GenericDatum(std::string((char*)kG(k_string), k_string->n)));
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
      encodeArray(field, array_datum, k_array);
      array_data.push_back(avro::GenericDatum(sub_array, array_datum));
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
      encodeRecord(field,  record_datum, k_record);
      array_data.push_back(avro::GenericDatum(array_schema, record_datum));
    }
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_union = kK(data)[i];
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), 0, k_union->t);

      auto union_datum = avro::GenericDatum(array_schema);
      encodeUnion(field, union_datum, k_union);
      array_data.push_back(union_datum);
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
      encodeMap(field, map_datum, k_map);
      array_data.push_back(avro::GenericDatum(array_schema, map_datum));
    }
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(array_type));
  }
}

void encodeRecord(const std::string& field, avro::GenericRecord& record, K data)
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
    encodeDatum(key, next, value, false);
  }
}

void encodeMap(const std::string& field, avro::GenericMap& avro_map, K data)
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
      map_data.push_back({ kS(keys)[i], avro::GenericDatum((bool)kG(values)[i]) });
    break;
  }
  case avro::AVRO_BYTES:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_bytes = kK(values)[i];
      TYPE_CHECK_MAP(field, avro::toString(map_type), KG, k_bytes->t);
      std::vector<uint8_t> bytes(k_bytes->n);
      std::memcpy(bytes.data(), kG(k_bytes), k_bytes->n);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(bytes) });
    }
    break;
  }
  case avro::AVRO_DOUBLE:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(kF(values)[i]) });
    break;
  }
  case avro::AVRO_ENUM:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(map_schema, avro::GenericEnum(map_schema, kS(values)[i])) });
    break;
  }
  case avro::AVRO_FIXED:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_bytes = kK(values)[i];
      const auto fixed_size = map_schema->fixedSize();
      TYPE_CHECK_FIXED(field, fixed_size, k_bytes->n);

      std::vector<uint8_t> fixed;
      fixed.resize(k_bytes->n);
      std::memcpy(fixed.data(), kG(k_bytes), k_bytes->n);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(map_schema, avro::GenericFixed(map_schema, fixed)) });
    }
    break;
  }
  case avro::AVRO_FLOAT:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(kE(values)[i]) });
    break;
  }
  case avro::AVRO_INT:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(kI(values)[i]) });
    break;
  }
  case avro::AVRO_LONG:
  {
    for (auto i = 0; i < values->n; ++i)
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(kJ(values)[i]) });
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_value = kK(values)[i];
      TYPE_CHECK_MAP(field, avro::toString(map_type), 101, k_value->t);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum() });
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_string = kK(values)[i];
      TYPE_CHECK_MAP(field, avro::toString(map_type), KC, k_string->t);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(std::string((char*)kG(k_string), k_string->n)) });
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
      encodeArray(field, array_datum, k_array);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(sub_array, array_datum) });
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
      encodeRecord(field, record_datum, k_record);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(map_schema, record_datum) });
    }
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i = 0; i < values->n; ++i) {
      K k_union = kK(values)[i];
      TYPE_CHECK_MAP(field, avro::toString(map_type), 0, k_union->t);

      auto union_datum = avro::GenericDatum(map_schema);
      encodeUnion(field, union_datum, k_union);
      map_data.push_back({ kS(keys)[i], union_datum });
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
      encodeMap(field, map_datum, k_map);
      map_data.push_back({ kS(keys)[i], avro::GenericDatum(map_schema, map_datum) });
    }
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(map_type));
  }
}

void encodeUnion(const std::string& field, avro::GenericDatum& avro_union, K data)
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
  encodeDatum(field, avro_union, k_datum, true);
}

K encode(K schema, K data)
{
  KDB_EXCEPTION_TRY;

  auto avro_schema = GetAvroSchema(schema);

  auto datum = avro::GenericDatum(*avro_schema.get());
  encodeDatum("", datum, data, false);

  auto encoder = avro::validatingEncoder(*avro_schema.get(), avro::binaryEncoder());

  std::ostringstream oss;
  auto ostream = avro::ostreamOutputStream(oss);
  encoder->init(*ostream);

  avro::GenericWriter writer(*avro_schema.get(), encoder);
  writer.write(datum);

  encoder->flush();
  const auto str = oss.rdbuf()->str();

  K result = ktn(KG, str.length());
  std::memcpy(kG(result), str.c_str(), str.length());

  return result;

  KDB_EXCEPTION_CATCH;
}
