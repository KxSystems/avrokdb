if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

cmake_minimum_required(VERSION 3.5)
project(kxavro CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -DKXVER=3")

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(MY_MODULE_NAME avro)

if(CMAKE_SYSTEM_NAME STREQUAL Linux)
    set(A l)
elseif(CMAKE_SYSTEM_NAME STREQUAL Darwin)
    set(A m)
elseif(CMAKE_SYSTEM_NAME STREQUAL Windows)
    set(A w)
else()
    message(FATAL_ERROR "Unsupported OS")
endif()

# CMAKE_OSX_ARCHITECTURES is only set for explicit/universal macOS builds; otherwise
# the build targets the host, reported in CMAKE_SYSTEM_PROCESSOR.
if(APPLE AND CMAKE_OSX_ARCHITECTURES)
    set(TARGET_ARCH "${CMAKE_OSX_ARCHITECTURES}")
else()
    set(TARGET_ARCH "${CMAKE_SYSTEM_PROCESSOR}")
endif()

if(TARGET_ARCH MATCHES "aarch64|arm64")
    set(A "${A}a64")
elseif(TARGET_ARCH MATCHES "x86_64|AMD64|x64")
    set(A "${A}i64")
else()
    message(FATAL_ERROR "Unsupported target architecture: ${TARGET_ARCH}")
endif()

message(STATUS "A: ${A}")

set(MY_LIBRARY_NAME "clib.${A}")
file(GLOB SRC_FILES src/*.cpp)
set_source_files_properties(${SRC_FILES} PROPERTIES LANGUAGE CXX)

message(STATUS "Generator : ${CMAKE_GENERATOR}")
message(STATUS "Build Tool : ${CMAKE_BUILD_TOOL}")

include_directories (
    ${CMAKE_BINARY_DIR} # For 'k.h', downloaded below
)

file(DOWNLOAD "https://github.com/KxSystems/kdb/raw/master/c/c/k.h" "${CMAKE_BINARY_DIR}/k.h" )

if (MSVC)
    file(DOWNLOAD "https://github.com/KxSystems/kdb/raw/master/w64/q.lib" "${CMAKE_BINARY_DIR}/q.lib" )
    set(LINK_LIBS "${CMAKE_BINARY_DIR}/q.lib")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3 /D WIN32_LEAN_AND_MEAN /D _CRT_SECURE_NO_WARNINGS /D _CRT_NONSTDC_NO_DEPRECATE /D _WINSOCK_DEPRECATED_NO_WARNINGS") 
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W3 /D WIN32_LEAN_AND_MEAN /D _CRT_SECURE_NO_WARNINGS /D _CRT_NONSTDC_NO_DEPRECATE /D _WINSOCK_DEPRECATED_NO_WARNINGS")
    add_compile_options(/wd4710 /wd4711 /utf-8)
else()
    set(LINK_LIBS "")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-strict-aliasing")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-strict-aliasing")
endif()

add_library (${MY_LIBRARY_NAME} SHARED ${SRC_FILES})

find_package(Boost REQUIRED)
find_package(avro-cpp)
if(avro-cpp_FOUND)
    message(STATUS "avro-cpp found")
   target_link_libraries(${MY_LIBRARY_NAME} avro-cpp::avrocpp ${LINK_LIBS})
else()
    message(STATUS "No avro-cpp cmake, searching manually")
    find_path(AVRO_INCLUDE_DIR avro/Encoder.hh)
    if(NOT AVRO_INCLUDE_DIR)
        # older version
        find_path(AVRO_INCLUDE_DIR avro/schema.h)
    endif()
    find_library(AVRO_LIBRARY avrocpp)
    if(NOT AVRO_LIBRARY)
        # older version
        find_library(AVRO_LIBRARY avro)
    endif()
    message(STATUS ${AVRO_INCLUDE_DIR})
    message(STATUS ${AVRO_LIBRARY})
    if(NOT AVRO_INCLUDE_DIR OR NOT AVRO_LIBRARY)
        message(FATAL_ERROR "avro-cpp not found")
    endif()

    target_include_directories(${MY_LIBRARY_NAME} PRIVATE ${AVRO_INCLUDE_DIR})
    target_link_libraries(${MY_LIBRARY_NAME} PRIVATE ${AVRO_LIBRARY})
endif()

if(LINUX)
    target_compile_options(${MY_LIBRARY_NAME} PRIVATE "-fvisibility=hidden")
endif()

if(APPLE)
   set_target_properties(${MY_LIBRARY_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
   set_target_properties(${MY_LIBRARY_NAME} PROPERTIES SUFFIX .so)
   set (CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(${MY_LIBRARY_NAME} PRIVATE
        -g3 -O0 -fprofile-arcs -ftest-coverage -fno-inline
    )

    target_link_options(${MY_LIBRARY_NAME} PRIVATE
        --coverage
    )
endif()

set_target_properties(${MY_LIBRARY_NAME}
    PROPERTIES
    PREFIX ""
    INSTALL_RPATH "$ORIGIN"
)

# Install as a KDB-X module: <CMAKE_INSTALL_PREFIX>/avro/{clib.*, init.q}
install(TARGETS ${MY_LIBRARY_NAME} DESTINATION ${MY_MODULE_NAME})
install(FILES q/init.q DESTINATION ${MY_MODULE_NAME})
