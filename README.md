# Avrokdb

## Introduction

Avrokdb supports the use of [Apache Avro](https://avro.apache.org/docs/) from within kdb+.  Encoding converts a kdb+ object to Avro serialised using the specified Avro schema, decoding converts Avro serialised to a kdb+ object object using the specified Avro schema.  It support all the Avro datatypes:

- array
- bool
- bytes
- double
- enum
- fixed
- float
- int
- long
- map
- null
- string
- union

and all the Avro logical types:

* date
* decimal
* duration
* time-millis
* time-micros
* timestamp-millis
* timestamp-micros
* uuid

Full details of the type-mapping between Avro and kdb+ are described [here](./docs/type-mapping.md).



## Functionality

Avrokdb allows you to:

* Compile Avro JSON schemas
* Encode a kdb+ object to Avro serialised data in either binary or JSON format
* Decode Avro serialised data in either binary or JSON format to a kdb+ object

See [avrodb.q](./q/avrokdb.q) for a full function reference.



## Avro schemas

Avro data is not self describing, it requires a schema to be defined and used during both the encoding and decoding process.  Details of how to define an Avro JSON schema can be found [here](https://avro.apache.org/docs/1.11.1/specification/). 



## Qpacker build

Clone the `avrokdb` repo then run:

```bash
qp build
```

This creates two qpacker targets:

```bash
qp run avrokdb // to run standalone
qp run tests   // to run the tests suite
```



## Qpacker install

A user can pick up the `avrokdb.qpk` file and add it as a dependency in the application:

```json
{
  "default": {
    "entry": ["myapp.q"],
    "depends": ["avrokdb"]
  }
}
```

You can get a pre-built `avrokdb.qpk` from the [packages page](https://gitlab.com/kxdev/interop/avrokdb/-/packages), or using q-packer:

```bash
qp pull gitlab.com/kxdev/interop/avrokdb/avrokdb 0.0.6
```



## Building from source (linux)

The [Dockerfile.qp](./clib/Dockerfile.qp) describes how to build `avrokdb.so` from source, including the dependencies (such as avrocpp and boost).

The base dependencies required to build avrocpp for yum package managers are (rockylinux:8):

* boost-devel.x86_64
* boost-filesystem.x86_64
* boost-iostreams.x86_64
* boost-program-options.x86_64
* snappy-devel.x86_64

The base dependencies required to build avrocpp for apt package managers are (ubuntu:20.04):

* libboost1.71-dev
* libboost-filesystem1.71-dev
* libboost-iostreams1.71-dev
* libboost-program-options1.71-dev
* libsnappy-dev

### Note

To avoid nasty C++ ABI problems (inevitably resulting in an unexplained coredump) you must set the `CMAKE_CXX_STANDARD` when building avrokdb to the same as was used to build the Boost libraries, e.g.:

- On rockylinux:8, yum installs boost built with C++11
- On ubuntu:20.04, apt installs boost built with C++17



## Building from source (Windows)

It is also possible to build avrokdb in Windows using [vcpkg](https://vcpkg.io/en/).

Download the vcpkg repo and run `bootstrap-vcpkg.bat` (full instructions are [here](https://github.com/microsoft/vcpkg)).

```bash
C:\Git> git clone https://github.com/microsoft/vcpkg.git
C:\Git> cd vcpkg
C:\Git\vcpkg> bootstrap-vcpkg.bat
```

Then install avrocpp and its dependencies:

```bash
C:\Git\vcpkg> vcpkg install avro-cpp:x64-windows
```

Then build avrokdb, point `$AVRO_INSTALL` to the the vcpkg directory containing the avrocpp package:

```bash
C:\Git\avrokdb\clib\avrokdb> mkdir build
C:\Git\avrokdb\clib\avrokdb> cd build
C:\Git\avrokdb\clib\avrokdb\build> cmake .. -DAVRO_INSTALL="C:/Git/vcpkg/packages/avro-cpp_x64-windows"
C:\Git\avrokdb\clib\avrokdb\build> cmake --build . --config Release
```

The `Release` directory will contain `avrokdb.dll` and all its dependencies which need to be made available to q (e.g. in `$QHOME/w64`):

```bash
C:\Git\avrokdb\clib\avrokdb\build>cd Release

C:\Git\avrokdb\clib\avrokdb\build\Release>dir
 Volume in drive C is Windows
 Volume Serial Number is 2241-3CD0

 Directory of C:\Git\avrokdb\clib\avrokdb\build\Release

05/07/2023  16:55    <DIR>          .
05/07/2023  16:55    <DIR>          ..
15/06/2023  11:02           781,824 avrocpp.dll
05/07/2023  16:55           361,984 avrokdb.dll
05/07/2023  16:55             1,266 avrokdb.exp
05/07/2023  16:55             2,630 avrokdb.lib
15/06/2023  11:01            78,848 boost_iostreams-vc143-mt-x64-1_81.dll
06/04/2023  14:30            75,264 bz2.dll
14/06/2023  17:20           183,808 liblzma.dll
06/04/2023  14:37            89,600 zlib1.dll
06/04/2023  14:38           637,440 zstd.dll
               9 File(s)      2,212,664 bytes
               2 Dir(s)  82,162,450,432 bytes free
```



## Examples

See [example.q](./examples/example.q) for a basic example.

More detailed examples of all the Avro datatypes and logical types can be found in the [tests](./tests/) directory.

