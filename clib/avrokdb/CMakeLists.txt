if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

cmake_minimum_required(VERSION 3.1.3)
project(avrokdb CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -DKXVER=3")

## !!! NB !!!
## To avoid nasty C++ ABI problems (inevitably resulting in an unexplained coredump) you must set the CMAKE_CXX_STANDARD to the same as was used to build the Boost libraries
## So on rockylinux:8, yum installs boost built with C++11
## But on ubuntu:20.06, apt install boost built with C++17
## These must match...
#set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(MY_LIBRARY_NAME avrokdb)
file(GLOB SRC_FILES src/*.cpp)
set_source_files_properties(${SRC_FILES} PROPERTIES LANGUAGE CXX)

message(STATUS "Generator : ${CMAKE_GENERATOR}")
message(STATUS "Build Tool : ${CMAKE_BUILD_TOOL}")
message(STATUS "Avro install : ${AVRO_INSTALL}")

# When building protobuf from source using cmake it statically links against the MSVC runtime.
# Since we are statically linking againat libprotobuf.lib, we must use the same runtime.
if(MSVC)
  foreach(flag_var
      CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE
      CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
    if(${flag_var} MATCHES "/MD")
      string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
    endif(${flag_var} MATCHES "/MD")
  endforeach()
endif()

include_directories (
	"${AVRO_INSTALL}/include"
    ${_VCPKG_ROOT_DIR}/installed/${VCPKG_TARGET_TRIPLET}/include  # where avro-cpp has been installed using vcpkg
    ${CMAKE_BINARY_DIR} # For 'k.h', downloaded below
)
#if (NOT ${AVRO_LIBRARY})
    message(STATUS "Looking for avrocpp.so")
	find_library(AVRO_LIBRARY
		NAMES avrocpp_s avrocpp
		HINTS "${AVRO_INSTALL}/lib/"
	)
#endif()

message(STATUS "Avro library : ${AVRO_LIBRARY}")

file(DOWNLOAD "https://github.com/KxSystems/kdb/raw/master/c/c/k.h" "${CMAKE_BINARY_DIR}/k.h" )

if (MSVC)
    set(INSTALL_SCRIPT "install.bat")
    file(DOWNLOAD "https://github.com/KxSystems/kdb/raw/master/w64/q.lib" "${CMAKE_BINARY_DIR}/q.lib" )
    set(LINK_LIBS "${CMAKE_BINARY_DIR}/q.lib")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3 /D WIN32_LEAN_AND_MEAN /D _CRT_SECURE_NO_WARNINGS /D _CRT_NONSTDC_NO_DEPRECATE /D _WINSOCK_DEPRECATED_NO_WARNINGS") 
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W3 /D WIN32_LEAN_AND_MEAN /D _CRT_SECURE_NO_WARNINGS /D _CRT_NONSTDC_NO_DEPRECATE /D _WINSOCK_DEPRECATED_NO_WARNINGS")
else()
    set(INSTALL_SCRIPT "install.sh")
    set(LINK_LIBS "")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-strict-aliasing")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-strict-aliasing")
endif()
    
add_library (${MY_LIBRARY_NAME} SHARED ${SRC_FILES})

IF(APPLE)
   set_target_properties(${MY_LIBRARY_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
   set_target_properties(${MY_LIBRARY_NAME} PROPERTIES SUFFIX .so)
endif()

if (NOT MSVC)  
  set(STATIC_CPP "-static-libgcc -static-libstdc++")
endif()

target_link_libraries(${MY_LIBRARY_NAME} ${STATIC_CPP} ${AVRO_LIBRARY} ${LINK_LIBS})
set_target_properties(${MY_LIBRARY_NAME} PROPERTIES PREFIX "")

#include(GNUInstallDirs)
if(WIN32)
  install(TARGETS ${MY_LIBRARY_NAME}
    RUNTIME DESTINATION ".")
else()
  install(TARGETS ${MY_LIBRARY_NAME}
    LIBRARY DESTINATION ".")
endif() 
