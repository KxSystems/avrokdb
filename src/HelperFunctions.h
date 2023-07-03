#pragma once

#include <string>
#include <stdexcept>

#include <k.h>


//////////////////////
// WINDOWS BINDINGS //
//////////////////////

#ifdef _WIN32
#define EXP __declspec(dllexport)
#else
#define EXP
#endif // _WIN32


/////////////////
// KDB STRINGS //
/////////////////


inline bool IsKdbString(K str)
{
  return str != NULL && (str->t == -KS || str->t == KC);
}

inline const std::string GetKdbString(K str)
{
  return str->t == -KS ? str->s : std::string((S)kG(str), str->n);
}


////////////////////////
// EXCEPTION HANDLING //
////////////////////////

#define KDB_EXCEPTION_TRY \
  static char error_msg[1024]; \
  *error_msg = '\0'; \
  try {

#define KDB_EXCEPTION_CATCH \
  } catch (std::exception& e) {  \
    strncpy(error_msg, e.what(), sizeof(error_msg));  \
    error_msg[sizeof(error_msg) - 1] = '\0';  \
    return krr(error_msg);  \
  }


//////////////////////////////
// TEMPORAL TYPE CONVERSION //
//////////////////////////////

// Helper class which can convert any int32 or int64 arrow temporal type
// (including those with a parameterised TimeUnit) to an appropriate kdb type.
class TemporalConversion
{
private:
  // Epoch / scaling constants
  const static int32_t kdb_date_epoch_days = 10957;
  const static int64_t kdb_timestamp_epoch_nano = 946684800000000000LL;
  const static int64_t ns_us_scale = 1000LL;
  const static int64_t ns_ms_scale = ns_us_scale * 1000LL;
  const static int64_t ns_sec_scale = ns_ms_scale * 1000LL;
  const static int64_t day_as_ns = 86400000000000LL;

  int64_t offset = 0;
  int64_t scalar = 1;

public:
  // The constructor sets up the correct epoch offsetting and scaling factor
  // based the arrow datatype
  TemporalConversion(const std::string& field, const avro::LogicalType::Type logical_type);

  // Converts from an arrow temporal (either int32 or int64) to its kdb value,
  // applying the epoch offseting and scaling factor
  template <typename T>
  inline T AvroToKdb(T value)
  {
    return value * (T)scalar - (T)offset;
  }

  // Converts from a kdb temporal (either int32 or int64) to its arrow value,
  // applying the epoch offseting and scaling factor
  template <typename T>
  inline T KdbToAvro(T value)
  {
    return (value + (T)offset) / (T)scalar;
  }
};

/////////////////
// FLIP TABLES //
/////////////////

inline K DictFromTable(K table, size_t index)
{
  assert(table->t == 98);
  
  K keys = kK(table->k)[0];
  K values = kK(table->k)[1];

  //assert(values->n == 1);
  //values = kK(values)[0];

  //assert(keys->n == values->n);

  K result = ktn(0, keys->n);
  std::memset(kK(result), 0, sizeof(K) * keys->n);

  for (auto i = 0; i < values->n; ++i) {
    K input = kK(values)[i];
    switch (input->t) {
    case KB:
      kK(result)[i] = kb(kG(input)[index]);
      break;
    case KI:
      kK(result)[i] = ki(kI(input)[index]);
      break;
    case KJ:
      kK(result)[i] = kj(kJ(input)[index]);
      break;
    case KE:
      kK(result)[i] = ke(kE(input)[index]);
      break;
    case KF:
      kK(result)[i] = kf(kF(input)[index]);
      break;
    case KS:
      kK(result)[i] = ks(kS(input)[index]);
      break;
    case 0:
      kK(result)[i] = r1(kK(input)[index]);
      break;
    default:
      throw std::invalid_argument("DictFromTable unsupported type: " + input->t);
    }
  }

  return xD(r1(keys), result);
}

inline K identity()
{
  K id = ka(101);
  id->g = 0;
  return id;
}

inline avro::Type GetRealType(const avro::GenericDatum& datum)
{
  if (datum.isUnion())
    return avro::AVRO_UNION;
  return datum.type();
}

inline avro::LogicalType GetRealLogicalType(const avro::GenericDatum& datum)
{
  if (datum.isUnion())
    return avro::LogicalType(avro::LogicalType::NONE);
  return datum.logicalType();
}