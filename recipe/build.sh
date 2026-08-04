#!/bin/bash
set -e
set -x

mkdir -p build
cd build

BUILD_TYPE=RelWithDebInfo

# Determine OSX architecture from the target platform
if [[ "$target_platform" == "osx-arm64" ]]; then
    OSX_ARCH="arm64"
elif [[ "$target_platform" == "osx-64" ]]; then
    OSX_ARCH="x86_64"
else
    OSX_ARCH=$(uname -m)
fi

cmake .. -DQMOD=ON \
      -DCMAKE_INSTALL_PREFIX=$PREFIX/lib/q/mod/kx \
      -DAVRO_INSTALL=$BUILD_PREFIX \
      -DBOOST_INSTALL=$BUILD_PREFIX \
      -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
      -DCMAKE_CXX_STANDARD=17 \
      -DCMAKE_CXX_STANDARD_REQUIRED=ON \
      -DCMAKE_OSX_ARCHITECTURES=$OSX_ARCH

cmake --build . --config $BUILD_TYPE

cmake --build . --config $BUILD_TYPE --target install
