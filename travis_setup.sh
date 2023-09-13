#!/bin/bash

if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
  sudo apt update
  sudo apt install -y libboost1.71-dev libboost-filesystem1.71-dev libboost-iostreams1.71-dev libboost-program-options1.71-dev libsnappy-dev
  # Create avrocpp installation directory
  mkdir -p cbuild/install
  export AVRO_INSTALL=$(pwd)/cbuild/install
  cd cbuild
  # Build and install avrocpp
  git clone https://github.com/apache/avro.git
  cd avro
  git checkout refs/tags/release-1.11.2 --
  cd lang/c++
  mkdir build
  cd build
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_STANDARD=11 -DCMAKE_INSTALL_PREFIX=$AVRO_INSTALL ..
  cmake --build . --config Release
  cmake --build . --config Release --target install
  cd ../../..
  cd ..
elif [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
  # Create avrocpp installation directory
  mkdir -p cbuild/install
  export AVRO_INSTALL=$(pwd)/cbuild/install
elif [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
  # Create arrow installation directory
  mkdir -p cbuild/install
  export ARROW_INSTALL=$(pwd)/cbuild/install  
  cd cbuild
  # Build and install avrocpp using vcpkg
  git clone https://github.com/microsoft/vcpkg.git
  cd vcpkg
  ./bootstrap-vcpkg.bat
  ./vcpkg install avro-cpp:x64-windows
  cp -r installed/x64-windows/* cbuild/install
  cd ..
else
  echo "$TRAVIS_OS_NAME is currently not supported"  
fi
