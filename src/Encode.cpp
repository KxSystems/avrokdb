#include <sstream>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/Encoder.hh>
#include <avro/Stream.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "Encode.h"
#include "TypeCheck.h"


void encodeRecord(avro::GenericRecord& record, K data);

void encodeSimple(const std::string& field, avro::GenericDatum& simple, K value)
{
  switch (simple.type()) {
  case avro::AVRO_BOOL:
    simple = (bool)value->g;
    break;
  case avro::AVRO_BYTES:
  {
    std::vector<uint8_t> bytes(value->n);
    std::memcpy(bytes.data(), kG(value), value->n);
    simple = bytes;
    break;
  }
  case avro::AVRO_DOUBLE:
    simple = value->f;
    break;
  case avro::AVRO_ENUM:
    simple.value<avro::GenericEnum>().set(value->s);
    break;
  case avro::AVRO_FIXED:
  {
    auto& avro_fixed = simple.value<avro::GenericFixed>();
    const auto fixed_size = avro_fixed.schema()->fixedSize();
    TYPE_CHECK_FIXED(fixed_size != value->n, field, fixed_size, value->n);

    std::vector<uint8_t> fixed;
    fixed.reserve(value->n);
    std::memcpy(fixed.data(), kG(value), value->n);
    avro_fixed.value() = fixed;
    break;
  }
  case avro::AVRO_FLOAT:
    simple = value->e;
    break;
  case avro::AVRO_INT:
    simple = value->i;
    break;
  case avro::AVRO_LONG:
    simple = value->j;
    break;
  case avro::AVRO_NULL:
    break;
  case avro::AVRO_STRING:
    simple = std::string((char*)kG(value), value->n);
    break;

  case avro::AVRO_ARRAY:
  case avro::AVRO_MAP:
  case avro::AVRO_RECORD:
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNION:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(simple.type()));
  }
}

void encodeArray(const std::string& field, avro::GenericArray& next, K data)
{
  auto array_schema = next.schema();
  assert(array_schema->leaves() == 1);
  auto array_type = array_schema->leafAt(0)->type();
  auto& array_data = next.value();

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
      TYPE_CHECK_ARRAY(k_bytes->t != KG, field, avro::toString(array_type), KG, k_bytes->t);
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
      array_data.push_back(avro::GenericDatum(array_schema->leafAt(0), avro::GenericEnum(array_schema->leafAt(0), kS(data)[i])));
    break;
  }
  case avro::AVRO_FIXED:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_bytes = kK(data)[i];
      const auto fixed_size = array_schema->leafAt(0)->fixedSize();
      TYPE_CHECK_FIXED(fixed_size != k_bytes->n, field, fixed_size, k_bytes->n);

      std::vector<uint8_t> fixed;
      fixed.reserve(k_bytes->n);
      std::memcpy(fixed.data(), kG(k_bytes), k_bytes->n);
      array_data.push_back(avro::GenericDatum(array_schema->leafAt(0), avro::GenericFixed(array_schema->leafAt(0), fixed)));
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
      TYPE_CHECK_ARRAY(k_value->t != 101, field, avro::toString(array_type), 101, k_value->t);
      array_data.push_back(avro::GenericDatum());
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_string = kK(data)[i];
      TYPE_CHECK_ARRAY(k_string->t != KC, field, avro::toString(array_type), KC, k_string->t);
      array_data.push_back(avro::GenericDatum(std::string((char*)kG(k_string), k_string->n)));
    }
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_array = kK(data)[i];

      auto sub_array = array_schema->leafAt(0);
      assert(sub_array->leaves() == 1);
      auto sub_array_type = GetKdbArrayType(sub_array->leafAt(0)->type());
      TYPE_CHECK_ARRAY(k_array->t != sub_array_type, field, avro::toString(sub_array->type()), sub_array_type, k_array->t);

      auto array_datum = avro::GenericArray(sub_array->leafAt(0));
      encodeArray(field, array_datum, k_array);
      array_data.push_back(avro::GenericDatum(array_schema->leafAt(0), array_datum));
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    for (auto i = 0; i < data->n; ++i) {
      K k_record = kK(data)[i];
      if (k_record->t == 101)
        continue;
      TYPE_CHECK_ARRAY(k_record->t != 99, field, avro::toString(avro::AVRO_RECORD), 99, k_record->t);

      auto record_datum = avro::GenericRecord(array_schema->leafAt(0));
      encodeRecord(record_datum, k_record);
      array_data.push_back(avro::GenericDatum(array_schema->leafAt(0), record_datum));
    }
    break;
  }

  case avro::AVRO_MAP:
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNION:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(array_type));
  }
}

void encodeRecord(avro::GenericRecord& record, K data)
{
  if (data->t != 99)
    throw TypeCheck("Record not 99h");

  K keys = kK(data)[0];
  K values = kK(data)[1];
  assert(keys->n == values->n);
  if (keys->t != KS)
    throw TypeCheck("Record keys not 11h");
  if (values->t != 0)
    throw TypeCheck("Record values not 0h");

  for (auto i = 0; i < keys->n; ++i) {
    const std::string key = kS(keys)[i];
    K value = kK(values)[i];
    if (key == "" && value->t == 101)
      continue;

    auto& next = record.field(key);
    TYPE_CHECK_DATUM(GetKdbType(next) == value->t, key, avro::toString(next.type()), GetKdbType(next), value->t);

    switch (next.type()) {
    case avro::AVRO_RECORD:
    {
      auto& next_record = next.value<avro::GenericRecord>();
      encodeRecord(next_record, value);
      break;
    }
    case avro::AVRO_ARRAY:
    {
      auto& next_array = next.value<avro::GenericArray>();
      encodeArray(key, next_array, value);
      break;
    }

    case avro::AVRO_MAP:
    case avro::AVRO_SYMBOLIC:
    case avro::AVRO_UNION:
    case avro::AVRO_UNKNOWN:
      TYPE_CHECK_UNSUPPORTED(key, avro::toString(next.type()));
    default:
      encodeSimple(key, next, value);
      break;
    }
  }
}

K encode(K schema, K data)
{
  if (data->t != 99)
    return krr((S)"data not 99h");

  KDB_EXCEPTION_TRY;

  auto avro_schema = GetAvroSchema(schema);

  auto datum = avro::GenericDatum(*avro_schema.get());
  if (datum.type() != avro::AVRO_RECORD)
    return krr(S("Schema not a record"));
  
  auto& record = datum.value<avro::GenericRecord>();

  encodeRecord(record, data);

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
