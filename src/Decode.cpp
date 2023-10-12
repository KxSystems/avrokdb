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
#include "Decode.h"
#include "TypeCheck.h"
#include "KdbOptions.h"
#include "GenericForeign.h"


K DecodeArray(const std::string& field, const avro::GenericArray& array_datum);
K DecodeMap(const std::string& field, const avro::GenericMap& avro_map);
K DecodeRecord(const std::string& field, const avro::GenericRecord& record);
K DecodeUnion(const std::string& field, const avro::GenericDatum& avro_union);


K DecimalFromBytes(const std::string& field, avro::LogicalType logical_type, const std::vector<uint8_t>& bytes)
{
  // DECIMAL is a mixed list of (precision; scale; bin_data)
  K result = ktn(0, 3);
  kK(result)[0] = ki(logical_type.precision());
  kK(result)[1] = ki(logical_type.scale());
  K k_bytes = ktn(KG, bytes.size());
  std::memcpy(kG(k_bytes), bytes.data(), bytes.size());
  kK(result)[2] = k_bytes;
  return result;
}


K DurationFromBytes(const std::string& field, const std::vector<uint8_t>& bytes)
{
  // DURATION is an int list of (month day milli)
  K result = ktn(KI, 3);
  uint32_t values[3];
  std::memcpy(values, bytes.data(), sizeof(uint32_t) * 3);
  kI(result)[0] = values[0];
  kI(result)[1] = values[1];
  kI(result)[2] = values[2];
  return result;
}

K DecodeDatum(const std::string& field, const avro::GenericDatum& datum, bool decompose_union)
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

    if (logical_type.type() == avro::LogicalType::DECIMAL)
      return DecimalFromBytes(field, logical_type, bytes);
    else {
      const auto& bytes = datum.value<std::vector<uint8_t>>();
      K k_bytes = ktn(KG, bytes.size());
      std::memcpy(kG(k_bytes), bytes.data(), bytes.size());
      return k_bytes;;
    }
  }
  case avro::AVRO_DOUBLE:
    return kf(datum.value<double>());
  case avro::AVRO_ENUM:
    return ks(S(datum.value<avro::GenericEnum>().symbol().c_str()));
  case avro::AVRO_FIXED:
  {
    const auto& fixed = datum.value<avro::GenericFixed>().value();

    if (logical_type.type() == avro::LogicalType::DECIMAL)
      return DecimalFromBytes(field, logical_type, fixed);
    else if (logical_type.type() == avro::LogicalType::DURATION)
      return DurationFromBytes(field, fixed);
    else {
      K result = ktn(KG, fixed.size());
      std::memcpy(kG(result), fixed.data(), fixed.size());
      return result;
    }
  }
  case avro::AVRO_FLOAT:
    return ke(datum.value<float>());
  case avro::AVRO_INT:
  {
    if (logical_type.type() == avro::LogicalType::DATE) {
      TemporalConversion tc(field, logical_type.type());
      return kd(tc.AvroToKdb(datum.value<int32_t>()));
    } else if (logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, logical_type.type());
      return kt(tc.AvroToKdb(datum.value<int32_t>()));
    } else {
      return ki(datum.value<int32_t>());
    }
  }
  case avro::AVRO_LONG:
  {
    if (logical_type.type() == avro::LogicalType::TIME_MICROS) {
      TemporalConversion tc(field, logical_type.type());
      return ktj(-KN, tc.AvroToKdb(datum.value<int64_t>()));
    } else if (logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
      TemporalConversion tc(field, logical_type.type());
      return ktj(-KP, tc.AvroToKdb(datum.value<int64_t>()));
    } else {
      return kj(datum.value<int64_t>());
    }
  }
  case avro::AVRO_NULL:
    return Identity();;
  case avro::AVRO_STRING:
  {
    const auto& string = datum.value<std::string>();
    if (logical_type.type() == avro::LogicalType::UUID) {
      TYPE_CHECK_KDB(field, avro::toString(avro_type), "avro uuid length", 36, string.length());
      U k_guid = StringToGuid(string);
      return ku(k_guid);
    } else {
      K result = ktn(KC, string.length());
      std::memcpy(kG(result), string.c_str(), string.length());
      return result;
    }
  }
  case avro::AVRO_RECORD:
    return DecodeRecord(field, datum.value<avro::GenericRecord>());
  case avro::AVRO_ARRAY:
    return DecodeArray(field, datum.value<avro::GenericArray>());
  case avro::AVRO_UNION:
    return DecodeUnion(field, datum);
  case avro::AVRO_MAP:
    return DecodeMap(field, datum.value<avro::GenericMap>());

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED(field, avro::toString(avro_type));
  }
}

K DecodeArray(const std::string& field, const avro::GenericArray& array_datum)
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
    if (array_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i : array_data) {
        const auto& bytes = i.value<std::vector<uint8_t>>();
        kK(result)[index++] = DecimalFromBytes(field, array_logical_type, bytes);
      }
    } else {
      for (auto i : array_data) {
        const auto& bytes = i.value<std::vector<uint8_t>>();
        K k_bytes = ktn(KG, bytes.size());
        std::memcpy(kG(k_bytes), bytes.data(), bytes.size());
        kK(result)[index++] = k_bytes;
      }
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
    if (array_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i : array_data) {
        const auto& fixed = i.value<avro::GenericFixed>().value();
        kK(result)[index++] = DecimalFromBytes(field, array_logical_type, fixed);
      }
    } else if (array_logical_type.type() == avro::LogicalType::DURATION) {
      for (auto i : array_data) {
        const auto& fixed = i.value<avro::GenericFixed>().value();
        kK(result)[index++] = DurationFromBytes(field, fixed);
      }
    } else {
      for (auto i : array_data) {
        const auto& fixed = i.value<avro::GenericFixed>().value();
        K k_fixed = ktn(KG, fixed.size());
        std::memcpy(kG(k_fixed), fixed.data(), fixed.size());
        kK(result)[index++] = k_fixed;
      }
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
    if (array_logical_type.type() == avro::LogicalType::DATE || array_logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, array_logical_type.type());
      for (auto i : array_data)
        kI(result)[index++] = tc.AvroToKdb(i.value<int32_t>());
    } else {
      for (auto i : array_data)
        kI(result)[index++] = i.value<int32_t>();
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    if (array_logical_type.type() == avro::LogicalType::TIME_MICROS || array_logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || array_logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
      TemporalConversion tc(field, array_logical_type.type());
      for (auto i : array_data)
        kJ(result)[index++] = tc.AvroToKdb(i.value<int64_t>());
    } else {
      for (auto i : array_data)
        kJ(result)[index++] = i.value<int64_t>();
    }
    break;
  }
  case avro::AVRO_NULL:
  {
    for (auto i : array_data)
      kK(result)[index++] = Identity();
    break;
  }
  case avro::AVRO_STRING:
  {
    if (array_logical_type.type() == avro::LogicalType::UUID) {
      for (auto i : array_data) {
        const auto& string = i.value<std::string>();
        TYPE_CHECK_KDB(field, avro::toString(array_type), "avro uuid length", 36, string.length());
        U k_guid = StringToGuid(string);
        kU(result)[index++] = k_guid;
      }
    } else {
      for (auto i : array_data) {
        const auto& string = i.value<std::string>();
        K k_string = ktn(KC, string.length());
        std::memcpy(kG(k_string), string.c_str(), string.length());
        kK(result)[index++] = k_string;
      }
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    kK(result)[index++] = Identity();
    for (auto i : array_data)
      kK(result)[index++] = DecodeRecord(field, i.value<avro::GenericRecord>());
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i : array_data)
      kK(result)[index++] = DecodeArray(field, i.value<avro::GenericArray>());
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i : array_data)
      kK(result)[index++] = DecodeUnion(field, i);
    break;
  }
  case avro::AVRO_MAP:
  {
    kK(result)[index++] = Identity();
    for (auto i : array_data)
      kK(result)[index++] = DecodeMap(field, i.value<avro::GenericMap>());
    break;
  }

  case avro::AVRO_SYMBOLIC:
  case avro::AVRO_UNKNOWN:
  default:
    TYPE_CHECK_UNSUPPORTED_LEAF(field, avro::toString(avro::AVRO_ARRAY), avro::toString(array_type));
  }

  return result;
}

K DecodeMap(const std::string& field, const avro::GenericMap& map_datum)
{
  auto map_schema = map_datum.schema();
  assert(map_schema->leaves() == 2);
  auto map_type = map_schema->leafAt(1)->type();
  auto map_logical_type = map_schema->leafAt(1)->logicalType();
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
    if (map_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& bytes = i.second.value<std::vector<uint8_t>>();
        kK(values)[index++] = DecimalFromBytes(field, map_logical_type, bytes);
      }
    } else {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& bytes = i.second.value<std::vector<uint8_t>>();
        K k_bytes = ktn(KG, bytes.size());
        std::memcpy(kG(k_bytes), bytes.data(), bytes.size());
        kK(values)[index++] = k_bytes;
      }
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
    if (map_logical_type.type() == avro::LogicalType::DECIMAL) {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& fixed = i.second.value<avro::GenericFixed>().value();
        kK(values)[index++] = DecimalFromBytes(field, map_logical_type, fixed);
      }
    } else if (map_logical_type.type() == avro::LogicalType::DURATION) {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& fixed = i.second.value<avro::GenericFixed>().value();
        kK(values)[index++] = DurationFromBytes(field, fixed);
      }
    } else {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& fixed = i.second.value<avro::GenericFixed>().value();
        K k_fixed = ktn(KG, fixed.size());
        std::memcpy(kG(k_fixed), fixed.data(), fixed.size());
        kK(values)[index++] = k_fixed;
      }
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
    if (map_logical_type.type() == avro::LogicalType::DATE || map_logical_type.type() == avro::LogicalType::TIME_MILLIS) {
      TemporalConversion tc(field, map_logical_type.type());
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        kI(values)[index++] = tc.AvroToKdb(i.second.value<int32_t>());
      }
    } else {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        kI(values)[index++] = i.second.value<int32_t>();
      }
    }
    break;
  }
  case avro::AVRO_LONG:
  {
    if (map_logical_type.type() == avro::LogicalType::TIME_MICROS || map_logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS || map_logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
      TemporalConversion tc(field, map_logical_type.type());
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        kJ(values)[index++] = tc.AvroToKdb(i.second.value<int64_t>());
      }
    } else {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        kJ(values)[index++] = i.second.value<int64_t>();
      }
    }
    break;

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
      kK(values)[index++] = Identity();
    }
    break;
  }
  case avro::AVRO_STRING:
  {
    if (map_logical_type.type() == avro::LogicalType::UUID) {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& string = i.second.value<std::string>();
        TYPE_CHECK_KDB(field, avro::toString(map_type), "avro uuid length", 36, string.length());
        U k_guid = StringToGuid(string);
        kU(values)[index++] = k_guid;
      }
    } else {
      for (auto i : map_data) {
        kS(keys)[index] = ss((S)i.first.c_str());
        const auto& string = i.second.value<std::string>();
        K k_string = ktn(KC, string.length());
        std::memcpy(kG(k_string), string.c_str(), string.length());
        kK(values)[index++] = k_string;
      }
    }
    break;
  }
  case avro::AVRO_RECORD:
  {
    kS(values)[index] = ss((S)"");
    kK(values)[index++] = Identity();
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = DecodeRecord(field, i.second.value<avro::GenericRecord>());
    }
    break;
  }
  case avro::AVRO_ARRAY:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = DecodeArray(field, i.second.value<avro::GenericArray>());
    }
    break;
  }
  case avro::AVRO_UNION:
  {
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = DecodeUnion(field, i.second);
    }
    break;
  }
  case avro::AVRO_MAP:
  {
    kS(keys)[index] = ss((S)"");
    kK(values)[index++] = Identity();
    for (auto i : map_data) {
      kS(keys)[index] = ss((S)i.first.c_str());
      kK(values)[index++] = DecodeMap(field, i.second.value<avro::GenericMap>());
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

K DecodeRecord(const std::string& field, const avro::GenericRecord& record)
{
  K keys = ktn(KS, record.fieldCount() + 1);
  kS(keys)[0] = ss((S)"");
  K values = ktn(0, record.fieldCount() + 1);
  kK(values)[0] = Identity();

  size_t index = 1;
  for (auto i = 0ull; i < record.fieldCount(); ++i) {
    const auto& next = record.fieldAt(i);
    const auto& name = record.schema()->nameAt(i);
    kS(keys)[index] = ss((S)name.c_str());
    kK(values)[index] = DecodeDatum(name, next, false);
    ++index;
  }

  return xD(keys, values);
}

K DecodeUnion(const std::string& field, const avro::GenericDatum& avro_union)
{
  K result = ktn(0, 2);
  kK(result)[0] = kh((I)avro_union.unionBranch());
  kK(result)[1] = DecodeDatum(field, avro_union, true);

  return result;
}

K Decode(K schema, K data, K options)
{
  if (data->t != KG && data->t != KC)
    return krr((S)"data not 4|10h");

  KDB_EXCEPTION_TRY;

  auto options_parser = KdbOptions(options, Options::string_options, Options::int_options);

  auto avro_foreign = GetForeign<AvroForeign>(schema);
  auto avro_schema = avro_foreign->schema;

  std::string avro_format = "BINARY";
  options_parser.GetStringOption(Options::AVRO_FORMAT, avro_format);

  int64_t decode_offset = 0;
  options_parser.GetIntOption(Options::DECODE_OFFSET, decode_offset);
  if (decode_offset > data->n)
    return krr((S)"Decode offset is greater than length of data");

  int64_t multithreaded = 0;
  options_parser.GetIntOption(Options::MULTITHREADED, multithreaded);

  // Find the decoder to use.  Decoders don't support multithreaded use so if
  // running in this mode the decoder is created on the fly.  If running single
  // threaded we use the already created decoder in the foreign which is less
  // expensive.
  avro::DecoderPtr decoder;
  if (multithreaded) {
    avro::DecoderPtr base_decoder;
    if (avro_format == "BINARY")
      base_decoder = avro::binaryDecoder();
    else if (avro_format == "JSON")
      base_decoder = avro::jsonDecoder(*avro_schema.get());
    else
      return krr((S)"Unsupported avro decoding type (should be BINARY or JSON)");

    decoder = avro::validatingDecoder(*avro_schema.get(), base_decoder);
  } else {
    if (avro_format == "BINARY")
      decoder = avro_foreign->binary_decoder;
    else if (avro_format == "JSON")
      decoder = avro_foreign->json_decoder;
    else
      return krr((S)"Unsupported avro decoding type (should be BINARY or JSON)");
  }

  auto istream = avro::memoryInputStream((const uint8_t*)kG(data) + decode_offset, data->n - decode_offset);
  decoder->init(*istream);

  avro::GenericReader reader(*avro_schema.get(), decoder);
  avro::GenericDatum datum;
  reader.read(datum);
  reader.drain();

  K result = DecodeDatum("", datum, false);

  return result;

  KDB_EXCEPTION_CATCH;
}

#include <fstream>
int main(int argc, char* argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: profile <schema file> <binary data file>";
    return 1;
  }
  K schema = SchemaFromFile(ks(argv[1]));
  std::ifstream input;
  input.open(argv[2]);

  K data = ktn(KG, 4096);
  input.read((char*)kG(data), data->n);

  for (auto i = 0; i < 1000000; ++i)
    r0(Decode(schema, data, Identity()));

  return 0;
}