# avrokdb

![Avro](Apache_Avro_Logo.svg)

[![GitHub release (latest by date)](https://img.shields.io/github/v/release/kxsystems/avrokdb?include_prereleases)](https://github.com/kxsystems/avrokdb/releases) [![Travis (.com) branch](https://travis-ci.com/KxSystems/avrokdb.svg?branch=main)](https://travis-ci.com/KxSystems/avrokdb)


## Introduction

This interface allows kdb+ to users encode and decode Apache Avro serialized data.

This is part of the [*Fusion for kdb+*](http://code.kx.com/q/interfaces/fusion/) interface collection.



## New to kdb+ ?

Kdb+ is the world's fastest time-series database, optimized for  ingesting, analyzing and storing massive amounts of structured data. To  get started with kdb+, please visit https://code.kx.com/q/ for downloads and developer information. For general information, visit https://kx.com/



## New to Apache Avro?

Apache Avro is a data serialization system.

Avro provides:

- Rich data structures.
- A compact, fast, binary data format.

Avro relies on schemas which are defined with JSON. When Avro data is read, the schema used when writing has to be provided. This permits each datum to be written with no per-value overheads, making serialization both fast and small.



## Installation

### Requirements

- kdb+ ≥ 3.5 64-bit (Linux/macOS/Windows)

### Installing a release

It is recommended that a user install this interface using a release package. This is completed in a number of steps:

1. If running on Windows, ensure you have downloaded/installed the Avro C++ libraries following the instructions [here](#windows).  This step is not necessary on Linux or macOS because their `avrokdb` release packages are statically linked with `libavrocpp`.
2. [Download a release](https://github.com/KxSystems/avrokdb/releases) for your system architecture.
3. Install script `avrokdb.q` to `$QHOME`, and binary file `lib/arrowkdb.(so|dll)` to `$QHOME/[mlw](64)`, by executing the following from the unzipped release package directory:

```bash
## Linux/macOS
chmod +x install.sh && ./install.sh

## Windows
install.bat
```



## Building and installing from source

### Requirements

- kdb+ ≥ 3.5 64-bit (Linux/macOS/Windows)
- Apache Avro C++ libraries
- C++11 or later and 
- CMake ≥ 3.1.3

### Third-party library installation

#### Linux

On linux `avrocpp` should be built from source.

1. Install the `avrocpp` dependencies (boost and compression libraries).  

   With an `apt` package manager:

   ```bash
   sudo apt -y update
   sudo apt -y install libboost-dev libboost-filesystem-dev libboost-iostreams-dev libboost-program-options-dev libsnappy-dev
   ```

   With a `yum` package manager:

   ```bash
   sudo yum -y update
   sudo yum -y install boost-devel boost-filesystem boost-iostreams boost-program-options snappy-devel
   ```

2. Clone the avro repo and switch to the `release-1.11.2` tag (which is the one used to build the `avrokdb` linux package)

   ```bash
   git clone https://github.com/apache/avro.git
   cd avro
   git checkout refs/tags/release-1.11.2 --
   ```

3. Create cmake build and install directories:

   ```bash
   cd lang/c++
   mkdir build
   mkdir install
   export AVRO_INSTALL=$(pwd)/install
   cd build
   ```

4. Generate the cmake build scripts:

   ```bash
   cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_STANDARD=11 -DCMAKE_INSTALL_PREFIX=$AVRO_INSTALL ..
   ```

5. Build and install to `$AVRO_INSTALL`:

   ```bash
     cmake --build . --config Release
     cmake --build . --config Release --target install
   ```

6. Copy the shared object to `$QHOME/l64`:

   ```bash
   cp $AVRO_INSTALL/lib/libavrocpp.so $QHOME/l64
   ```

#### macOS

On macOS `avrocpp` (and its dependancy on `boost`) can be installed using `brew`:

```bash
brew install avro-cpp boost
```

#### Windows

On Windows `avrocpp` should be built using [vcpkg](https://vcpkg.io/en/):

1. Clone the `vcpkg` repo and bootstrap it:

   ```bash
   git clone https://github.com/microsoft/vcpkg.git
   git checkout refs/tags/2023.07.21 --
   cd vcpkg
   bootstrap-vcpkg.bat
   ```

2. Install `avrocpp`:

   ```bash
   vcpkg install avro-cpp:x64-windows
   set AVRO_INSTALL=%cd%\installed\x64-windows
   ```

3. Copy the DLLs to `$QHOME\w64`:

   ```bash
   cd installed\x64-windows\bin
   copy avrocpp.dll %QHOME%\w64
   copy boost_iostreams-vc143-mt-x64-*.dll %QHOME%\w64
   copy liblzma.dll %QHOME%\w64
   copy zstd.dll %QHOME%\w64
   copy bz2.dll %QHOME%\w64
   copy zlib1.dll %QHOME%\w64
   ```


### Building avrokdb

In order to successfully build and install this interface from source, the following environment variables must be set:

1. `AVRO_INSTALL` = Location of the Avro C++ API release (only required if `avrocpp` is not installed globally on the system, e.g. on Linux or Windows where `avrocpp` was built from source)
2. `BOOST_INSTALL` = Locaion of the Boost C++ library (only required if `boost` is not installed globally on the system)
3. `QHOME` = Q installation directory (directory containing `q.k`)

From a shell prompt (on Linux/macOS) or Visual Studio command prompt (on Windows), clone the `avrokdb` source from github:

```bash
git clone https://github.com/KxSystems/avrokdb.git
cd avrokdb
```

Create the cmake build directory and generate the build files (this will use the system's default cmake generator):

```bash
mkdir build
cd build

## Linux (using the Arrow installation which was build from source as above)
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_STANDARD=11 -DAVRO_INSTALL=$AVRO_INSTALL

## macOS
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_STANDARD=11

## Windows (using the Arrow installation which was build from source as above)
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_STANDARD=11 -DAVRO_INSTALL=%AVRO_INSTALL%
```

Start the build:

```bash
cmake --build . --config Release
```

Create the install package and deploy to `$QHOME`:

```bash
cmake --build . --config Release --target install
```



## Documentation

Documentation outlining the functionality available for this interface can be found in the [`docs`](docs/introduction.md) folder.



## Status

The avrokdb interface is provided here under an Apache 2.0 license.

If you find issues with the interface or have feature requests, please consider [raising an issue](https://github.com/KxSystems/avrokdb/issues).

If you wish to contribute to this project, please follow the [contribution guide](CONTRIBUTING.md).
