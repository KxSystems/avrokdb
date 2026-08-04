
#include "k.h"
#include "Decode.h"
#include "Encode.h"

#ifdef _WIN32
#define EXP __declspec(dllexport)
#else
#define EXP __attribute__((visibility("default")))
#endif

extern "C" {
  K SchemaFromFile(K filename);
  K SchemaFromString(K schema);
  K GetSchema(K schema);
  EXP K1(kexport);
}

K1(kexport) {
  K n=ktn(KS,0),f=ktn(0,0);
  #define _(s,a) js(&n,ss((S)#s));jk(&f,dl((V*)s,a));
  _(SchemaFromFile,1)_(SchemaFromString,1)_(GetSchema,1)_(Encode,3)_(Decode,3)
  R xD(n, f);
}
