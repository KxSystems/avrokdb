#include <sstream>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>
#include <avro/Decoder.hh>
#include <avro/Stream.hh>

#include "HelperFunctions.h"
#include "Schema.h"
#include "Decode.h"
#include "TypeCheck.h"


K decodeRecord(const avro::GenericRecord& record);

K decodeArray(const avro::GenericArray& array_datum)
{
  auto array_schema = array_datum.schema();
  assert(array_schema->leaves() == 1);
  auto array_type = array_schema->leafAt(0)->type();
  const auto& array_data = array_datum.value();

  size_t result_len = array_data.size();
  if (array_type == avro::AVRO_RECORD) {
    // We put a (::) at the start of an array of records so need one more item
    ++result_len;
  }
  K result = ktn(GetKdbArrayType(array_type), result_len);
  /*K result = NULL;
  if (array_type != avro::AVRO_ENUM && array_type != avro::AVRO_FIXED)
    result = ktn(GetKdbArrayType(array_type), array_data.size());*/
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
    /*K keys = ktn(KS, 1);
    kS(keys)[0] = ss((S)array_schema->leafAt(0)->name().simpleName().c_str());
    K values = ktn(KS, array_data.size());*/

    for (auto i : array_data)
      //kS(values)[index++] = ss((S)i.value<avro::GenericEnum>().symbol().c_str());
      kS(result)[index++] = ss((S)i.value<avro::GenericEnum>().symbol().c_str());

    //result = xD(keys, knk(1, values));
    break;
  }
  case avro::AVRO_FIXED:
  {
    /*K keys = ktn(KS, 1);
    kS(keys)[0] = ss((S)array_schema->leafAt(0)->name().simpleName().c_str());
    K values = ktn(0, array_data.size());*/

    for (auto i : array_data) {
      const auto& fixed = i.value<avro::GenericFixed>().value();
      K k_fixed = ktn(KG, fixed.size());
      std::memcpy(kG(k_fixed), fixed.data(), fixed.size());
      //kK(values)[index++] = k_fixed;
      kK(result)[index++] = k_fixed;
    }

    //result = xD(keys, knk(1, values));
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
    for (auto i : array_data) {
      K id = ka(101);
      id->g = 0;
      kK(result)[index++] = id;
    }
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
    K id = ka(101);
    id->g = 0;
    kK(result)[index++] = id;
    for (auto i : array_data)
      kK(result)[index++] = decodeRecord(i.value<avro::GenericRecord>());
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i : array_data)
      kK(result)[index++] = decodeArray(i.value<avro::GenericArray>());
    break;
  }
  case avro::AVRO_MAP:
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNION:
  case avro::AVRO_UNKNOWN:
  default:
    throw TypeCheck("Unsupported type");
  }

  return result;
}

K decodeDatum(const avro::GenericDatum& datum)
{
  switch (datum.type()) {
  case avro::AVRO_BOOL:
    return kb(datum.value<bool>());
  case avro::AVRO_BYTES:
  {
    const auto& bytes = datum.value<std::vector<uint8_t>>();
    K result = ktn(KG, bytes.size());
    std::memcpy(kG(result), bytes.data(), bytes.size());
    return result;
  }
  case avro::AVRO_DOUBLE:
    return kf(datum.value<double>());
  case avro::AVRO_ENUM:
    return ks(S(datum.value<avro::GenericEnum>().symbol().c_str()));
  /* {
    const auto& avro_enum = datum.value<avro::GenericEnum>();
    K keys = ktn(KS, 1);
    kS(keys)[0] = ss((S)avro_enum.schema()->name().simpleName().c_str());
    K values = ktn(KS, 1);
    kS(values)[0] = ss((S)avro_enum.symbol().c_str());
    return xD(keys, values);
  } */
  case avro::AVRO_FIXED:
  {
    const auto & fixed = datum.value<avro::GenericFixed>().value();
    K result = ktn(KG, fixed.size());
    std::memcpy(kG(result), fixed.data(), fixed.size());
    return result;
    /*const auto& avro_fixed = datum.value<avro::GenericFixed>();
    K keys = ktn(KS, 1);
    kS(keys)[0] = ss((S)avro_fixed.schema()->name().simpleName().c_str());
    K values = ktn(0, 1);
    const auto& fixed = avro_fixed.value();
    K k_fixed = ktn(KG, fixed.size());
    std::memcpy(kG(k_fixed), fixed.data(), fixed.size());
    kK(values)[0] = k_fixed;
    return xD(keys, values);*/  
  }
  case avro::AVRO_FLOAT:
    return ke(datum.value<float>());
  case avro::AVRO_INT:
    return ki(datum.value<int32_t>());
  case avro::AVRO_LONG:
    return kj(datum.value<int64_t>());
  case avro::AVRO_NULL:
  {
    K id = ka(101);
    id->g = 0;
    return id;
  }
  case avro::AVRO_STRING:
  {
    const auto& string = datum.value<std::string>();
    K result = ktn(KC, string.length());
    std::memcpy(kG(result), string.c_str(), string.length());
    return result;
  }
  case avro::AVRO_RECORD:
    return decodeRecord(datum.value<avro::GenericRecord>());
  case avro::AVRO_ARRAY:
    return decodeArray(datum.value<avro::GenericArray>());

  case avro::AVRO_MAP:
  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNION:
  case avro::AVRO_UNKNOWN:
  default:
    throw TypeCheck("Unsupported type");
  }
}

K decodeRecord(const avro::GenericRecord& record)
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
    kK(values)[index] = decodeDatum(next);
    ++index;
  }

  return xD(keys, values);
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

  return decodeDatum(datum);

  KDB_EXCEPTION_CATCH;
}
