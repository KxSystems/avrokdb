#!/bin/bash

if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
  sudo apt update
  sudo apt install -y libboost-dev libboost-filesystem-dev libboost-iostreams-dev libboost-program-options-dev libsnappy-dev
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
  brew install avro-cpp  
  cp -r /usr/local/opt/avro-cpp/* $AVRO_INSTALL
  cp -r /usr/local/opt/boost/* $AVRO_INSTALL
elif [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
  # Create avrocpp installation directory
  mkdir -p cbuild/install
  export AVRO_INSTALL=$(pwd)/cbuild/install
  cd cbuild
  # Build and install avrocpp using vcpkg
  git clone https://github.com/microsoft/vcpkg.git
  cd vcpkg
  git checkout refs/tags/2023.08.09 -- 
  ./bootstrap-vcpkg.bat
  ./vcpkg install boost-iostreams:x64-windows boost-filesystem:x64-windows boost-program-options:x64-windows snappy:x64-windows
  ./vcpkg install avro-cpp:x64-windows
  cp -r installed/x64-windows/* $AVRO_INSTALL
  cd ..
  cd ..
else
  echo "$TRAVIS_OS_NAME is currently not supported"  
fi
