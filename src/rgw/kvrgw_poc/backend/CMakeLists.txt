cmake_minimum_required(VERSION 3.16)
project(kv-rgw-backend LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(FDB_ROOT "${CMAKE_SOURCE_DIR}/../third_party/fdb")
set(FDB_INCLUDE_DIR "${FDB_ROOT}/usr/include")
set(FDB_LIB_DIR "${FDB_ROOT}/usr/lib64")

find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(YAML_CPP REQUIRED yaml-cpp)

set(PROTO_FILE "${CMAKE_SOURCE_DIR}/../proto/kvrgw.proto")
set(GENERATED_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")

set(PROTO_SRCS "${GENERATED_DIR}/kvrgw.pb.cc")
set(PROTO_HDRS "${GENERATED_DIR}/kvrgw.pb.h")

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
  COMMAND protobuf::protoc
  ARGS --cpp_out=${GENERATED_DIR}
       -I ${CMAKE_SOURCE_DIR}/../proto
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf sources from kvrgw.proto"
)

add_library(kvrgw_pb ${PROTO_SRCS})
target_include_directories(kvrgw_pb PUBLIC "${GENERATED_DIR}")
target_link_libraries(kvrgw_pb PUBLIC protobuf::libprotobuf)
set_target_properties(kvrgw_pb PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(KVRGW_LIB_SRCS
  src/kvrgw_runtime.cpp
  src/kvrgw_c_api.cpp
  src/kv_store.cpp
  src/keys.cpp
  src/object_value.cpp
  src/tenant_value.cpp
  src/gc_value.cpp
  src/ref_tag.cpp
  src/data_store.cpp
  src/byte_range.cpp
  src/id_tag.cpp
  src/id_meta.cpp
  src/service_impl.cpp
  src/bucket_policy.cpp
  src/sweeper.cpp
  src/gc_worker.cpp
  src/gc_config_state.cpp
  src/ref_count_ops.cpp
  src/tier_config_state.cpp
  src/error_codes.cpp
  src/err_insertion.cpp
  src/typed_ids.cpp
  src/admin_server.cpp
)

add_library(kvrgw SHARED ${KVRGW_LIB_SRCS})
target_include_directories(kvrgw PUBLIC
  "${GENERATED_DIR}"
  "${CMAKE_SOURCE_DIR}/include"
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(kvrgw PUBLIC "${FDB_LIB_DIR}")
target_link_libraries(kvrgw PUBLIC
  kvrgw_pb
  "${FDB_LIB_DIR}/libfdb_c.so"
  OpenSSL::Crypto
  ${YAML_CPP_LIBRARIES}
  Threads::Threads
)
target_compile_definitions(kvrgw PUBLIC FDB_API_VERSION=730)
set_target_properties(kvrgw PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  BUILD_RPATH "${FDB_LIB_DIR}"
  INSTALL_RPATH "${FDB_LIB_DIR}"
)

add_executable(kv-rgw-backend
  src/main.cpp
  src/perf_driver.cpp
)

target_include_directories(kv-rgw-backend PRIVATE
  "${GENERATED_DIR}"
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)

target_link_directories(kv-rgw-backend PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(kv-rgw-backend PRIVATE
  kvrgw
)

set_target_properties(kv-rgw-backend PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

add_executable(object_value_test tests/object_value_test.cpp src/object_value.cpp src/tenant_value.cpp src/id_tag.cpp src/id_meta.cpp src/typed_ids.cpp)
target_include_directories(object_value_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
)
target_link_libraries(object_value_test PRIVATE OpenSSL::Crypto)

add_executable(l_key_test tests/l_key_test.cpp src/keys.cpp)
target_include_directories(l_key_test PRIVATE "${CMAKE_SOURCE_DIR}/src")

add_executable(data_store_test tests/data_store_test.cpp src/data_store.cpp src/ref_tag.cpp)
target_include_directories(data_store_test PRIVATE "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(data_store_test PRIVATE OpenSSL::Crypto)

add_executable(kv_range_test
  tests/kv_range_test.cpp
  src/kv_store.cpp
  src/keys.cpp
  src/object_value.cpp
  src/id_tag.cpp
  src/id_meta.cpp
  src/typed_ids.cpp
)
target_include_directories(kv_range_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  "${GENERATED_DIR}"
)
target_link_directories(kv_range_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(kv_range_test PRIVATE
  "${FDB_LIB_DIR}/libfdb_c.so"
  OpenSSL::Crypto
  kvrgw_pb
)
target_compile_definitions(kv_range_test PRIVATE FDB_API_VERSION=730)
set_target_properties(kv_range_test PROPERTIES BUILD_RPATH "${FDB_LIB_DIR}")

add_executable(gc_ctl
  tools/gc_ctl.cpp
  src/kv_store.cpp
  src/keys.cpp
  src/data_store.cpp
  src/ref_tag.cpp
  src/gc_value.cpp
  src/object_value.cpp
  src/id_tag.cpp
  src/id_meta.cpp
  src/typed_ids.cpp
)
target_include_directories(gc_ctl PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
)
target_link_directories(gc_ctl PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(gc_ctl PRIVATE
  "${FDB_LIB_DIR}/libfdb_c.so"
  OpenSSL::Crypto
)
target_compile_definitions(gc_ctl PRIVATE FDB_API_VERSION=730)
set_target_properties(gc_ctl PROPERTIES BUILD_RPATH "${FDB_LIB_DIR}")

add_executable(gc_config_state_test tests/gc_config_state_test.cpp src/gc_config_state.cpp)
target_include_directories(gc_config_state_test PRIVATE "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(gc_config_state_test PRIVATE OpenSSL::Crypto)

add_executable(fdb_error_test
  tests/fdb_error_test.cpp
  src/kv_store.cpp
  src/keys.cpp
)
target_include_directories(fdb_error_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  "${GENERATED_DIR}"
)
target_link_directories(fdb_error_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(fdb_error_test PRIVATE "${FDB_LIB_DIR}/libfdb_c.so" kvrgw_pb)
target_compile_definitions(fdb_error_test PRIVATE FDB_API_VERSION=730)
set_target_properties(fdb_error_test PROPERTIES BUILD_RPATH "${FDB_LIB_DIR}")

add_executable(data_store_error_test tests/data_store_error_test.cpp src/data_store.cpp src/ref_tag.cpp)
target_include_directories(data_store_error_test PRIVATE "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(data_store_error_test PRIVATE OpenSSL::Crypto)

add_executable(err_insertion_test tests/err_insertion_test.cpp src/err_insertion.cpp)
target_include_directories(err_insertion_test PRIVATE "${CMAKE_SOURCE_DIR}/src")

add_executable(kvrgw_c_abi_test tests/kvrgw_c_abi_test.c)
target_include_directories(kvrgw_c_abi_test PRIVATE "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvrgw_c_abi_test PRIVATE kvrgw)
set_target_properties(kvrgw_c_abi_test PROPERTIES
  LINKER_LANGUAGE CXX
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

enable_testing()
add_test(NAME object_value_test COMMAND object_value_test)
add_test(NAME l_key_test COMMAND l_key_test)
add_test(NAME data_store_test COMMAND data_store_test)
add_test(NAME kv_range_test COMMAND kv_range_test)
add_test(NAME gc_config_state_test COMMAND gc_config_state_test)
add_test(NAME fdb_error_test COMMAND fdb_error_test)
add_test(NAME data_store_error_test COMMAND data_store_error_test)
add_test(NAME err_insertion_test COMMAND err_insertion_test)
