#include <cstdlib>

#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>

#include "InitialiseKdb.h"
#include "Schema.h"


K InitialiseAvroKdb(K unused)
{
  std::atexit(AvroSchemaExitHandler);

  return (K)0;
}