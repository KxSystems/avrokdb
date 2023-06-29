#include <sstream>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/Encoder.hh>
#include <avro/Stream.hh>
#include <boost/any.hpp>

#include "HelperFunctions.h"
#include "Schema.h"
#include "Encode.h"
#include "TypeCheck.h"


void encodeArray(const std::string& field, avro::GenericArray& avro_array, K data);
void encodeMap(const std::string& field, avro::GenericMap& avro_map, K data);
void encodeRecord(const std::string& field, avro::GenericRecord& record, K data);
void encodeUnion(const std::string& field, avro::GenericDatum& avro_union, K data);

void encodeDatum(const std::string& field, avro::GenericDatum& avro_datum, K data, bool decompose_union)
{
  avro::Type avro_type;
  if (!decompose_union)
    avro_type = GetRealType(avro_datum);
  else
    avro_type = avro_datum.type();

  TYPE_CHECK_DATUM(field, avro::toString(avro_type), GetKdbType(avro_datum, decompose_union), data->t);

  switch (avro_type) {
  case avro::AVRO_BOOL:
    avro_datum.value<bool>() = (bool)data->g;
    break;
  case avro::AVRO_BYTES:
  {
    std::vector<uint8_t> bytes(data->n);
    std::memcpy(bytes.data(), kG(data), data->n);
    avro_datum.value<std::vector<uint8_t>>() = bytes;
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
    TYPE_CHECK_FIXED(field, fixed_size, data->n);

    std::vector<uint8_t> fixed;
    fixed.resize(data->n);
    std::memcpy(fixed.data(), kG(data), data->n);
    avro_fixed.value() = fixed;
    break;
  }
  case avro::AVRO_FLOAT:
    avro_datum.value<float>() = data->e;
    break;
  case avro::AVRO_INT:
    avro_datum.value<int32_t>() = data->i;
    break;
  case avro::AVRO_LONG:
    avro_datum.value<int64_t>() = data->j;
    break;
  case avro::AVRO_NULL:
    break;
  case avro::AVRO_STRING:
    avro_datum.value<std::string>() = std::string((char*)kG(data), data->n);
    break;
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
    for (auto i = 0; i < data->n; ++i) {
      K k_bytes = kK(data)[i];
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), KG, k_bytes->t);
      std::vector<uint8_t> bytes(k_bytes->n);
      std::memcpy(bytes.data(), kG(k_bytes), k_bytes->n);
      array_data.push_back(avro::GenericDatum(bytes));
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
    for (auto i = 0; i < data->n; ++i) {
      K k_bytes = kK(data)[i];
      const auto fixed_size = array_schema->fixedSize();
      TYPE_CHECK_FIXED(field, fixed_size, k_bytes->n);

      std::vector<uint8_t> fixed;
      fixed.resize(k_bytes->n);
      std::memcpy(fixed.data(), kG(k_bytes), k_bytes->n);
      array_data.push_back(avro::GenericDatum(array_schema, avro::GenericFixed(array_schema, fixed)));
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
    for (auto i = 0; i < data->n; ++i)
      array_data.push_back(avro::GenericDatum(kI(data)[i]));
    break;
  }
  case avro::AVRO_LONG:
  {
    for (auto i = 0; i < data->n; ++i)
      array_data.push_back(avro::GenericDatum(kJ(data)[i]));
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
    for (auto i = 0; i < data->n; ++i) {
      K k_string = kK(data)[i];
      TYPE_CHECK_ARRAY(field, avro::toString(array_type), KC, k_string->t);
      array_data.push_back(avro::GenericDatum(std::string((char*)kG(k_string), k_string->n)));
    }
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_array = kK(data)[i];

      auto sub_array = array_schema;
      assert(sub_array->leaves() == 1);
      auto sub_array_type = GetKdbArrayType(sub_array->leafAt(0)->type());
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
  if (keys->t != KS)
    throw TypeCheck("Record keys not 11h");

  if (values->t != 0)
    throw TypeCheck("Record values not 0h");
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
  if (keys->t != KS)
    throw TypeCheck("Map keys not 11h");
  assert(keys->n == values->n);

  assert(avro_map.schema()->leaves() == 2);
  auto map_schema = avro_map.schema()->leafAt(1);
  auto map_type = map_schema->type();
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
      auto sub_map_type = GetKdbArrayType(sub_array->leafAt(0)->type());
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
  if (data->n != 2)
    throw TypeCheck("Union length not 2");
  K k_branch = kK(data)[0];
  K k_datum = kK(data)[1];

  // Even though a union branch is a size_t we're going to represent it as a -KH
  // Realistically no one will even have a union with > 16K branches
  // This avoids type promotion problems with a long union where (0; 123) would become (0 123)
  // Don't want yet to have to introduce yet more (::)
  // Avro don't have a short int type so it cannot be promoted
  if (k_branch->t != -KH)
    throw TypeCheck("Union branch not -5h");

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
