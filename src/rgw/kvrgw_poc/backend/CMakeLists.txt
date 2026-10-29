cmake_minimum_required(VERSION 3.16)
project(kv-rgw-backend LANGUAGES C CXX)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "12")
  message(FATAL_ERROR
    "GCC 12 or later is required for C++23 <format> support.\n"
    "Source gcc-toolset-12 first:  source /opt/rh/gcc-toolset-12/enable")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(FDB_ROOT "${CMAKE_SOURCE_DIR}/../third_party/fdb")
set(FDB_INCLUDE_DIR "${FDB_ROOT}/usr/include")
set(FDB_LIB_DIR "${FDB_ROOT}/usr/lib64")

find_package(OpenSSL REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(YAML_CPP REQUIRED yaml-cpp)

set(KVRGW_LIB_SRCS
  src/kvrgw_runtime.cpp
  src/kvrgw_c_api.cpp
  src/kv_store.cpp
  src/l_keys.cpp
  src/keys.cpp
  src/object_value.cpp
  src/tenant_value.cpp
  src/gc_value.cpp
  src/multipart_value.cpp
  src/multipart_manifest.cpp
  src/ref_tag.cpp
  src/data_store.cpp
  src/byte_range.cpp
  src/id_tag.cpp
  src/id_meta.cpp
  src/service_impl.cpp
  src/service_impl_multipart.cpp
  src/bucket_policy.cpp
  src/sweeper.cpp
  src/gc_worker.cpp
  src/gc_config_state.cpp
  src/ref_count_ops.cpp
  src/tier_config_state.cpp
  src/error_codes.cpp
  src/err_insertion.cpp
  src/typed_ids.cpp
  src/etag.cpp
  src/admin_server.cpp
)

add_library(kvrgw SHARED ${KVRGW_LIB_SRCS})
target_include_directories(kvrgw PUBLIC
  "${CMAKE_SOURCE_DIR}/include"
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(kvrgw PUBLIC "${FDB_LIB_DIR}")
target_link_libraries(kvrgw PUBLIC
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

# Go frontend (frontend/) is a separate Go module, not a CMake project, but it
# cgo-links against libkvrgw.so (see frontend/kvrgwc/kvrgwc.go's #cgo LDFLAGS,
# which point at ${SRCDIR}/../../build == this CMAKE_BINARY_DIR). Wiring it in
# here as an ALL custom target means a single `cmake --build build` covers
# backend + tests + frontend. go/cmake invoke this as a subprocess with its
# own WORKING_DIRECTORY, so the caller's shell cwd is never touched.
add_custom_target(frontend ALL
  COMMAND go mod tidy
  COMMAND go build -o "${CMAKE_BINARY_DIR}/kv-rgw-frontend" .
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/../frontend"
  DEPENDS kvrgw
  COMMENT "Building Go frontend (cgo-links libkvrgw.so)"
)

add_executable(kv-rgw-backend
  src/main.cpp
  src/perf_driver.cpp
)

target_include_directories(kv-rgw-backend PRIVATE
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

add_executable(object_value_test tests/object_value_test.cpp src/object_value.cpp src/ref_tag.cpp src/tenant_value.cpp src/id_tag.cpp src/id_meta.cpp src/typed_ids.cpp src/etag.cpp src/error_codes.cpp)
target_include_directories(object_value_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
)
target_link_libraries(object_value_test PRIVATE OpenSSL::Crypto "${FDB_LIB_DIR}/libfdb_c.so")
target_compile_definitions(object_value_test PRIVATE FDB_API_VERSION=730)

add_executable(l_key_test tests/l_key_test.cpp src/keys.cpp src/l_keys.cpp src/kv_store.cpp src/ref_tag.cpp src/typed_ids.cpp src/etag.cpp src/error_codes.cpp)
target_include_directories(l_key_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
)
target_link_libraries(l_key_test PRIVATE OpenSSL::Crypto "${FDB_LIB_DIR}/libfdb_c.so")
target_compile_definitions(l_key_test PRIVATE FDB_API_VERSION=730)

add_executable(data_store_test tests/data_store_test.cpp src/data_store.cpp src/ref_tag.cpp)
target_include_directories(data_store_test PRIVATE "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(data_store_test PRIVATE OpenSSL::Crypto)

add_executable(kv_range_test
  tests/kv_range_test.cpp
  src/kv_store.cpp
  src/l_keys.cpp
  src/keys.cpp
  src/object_value.cpp
  src/ref_tag.cpp
  src/id_tag.cpp
  src/id_meta.cpp
  src/typed_ids.cpp
  src/etag.cpp
)
target_include_directories(kv_range_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
)
target_link_directories(kv_range_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(kv_range_test PRIVATE
  "${FDB_LIB_DIR}/libfdb_c.so"
  OpenSSL::Crypto
)
target_compile_definitions(kv_range_test PRIVATE FDB_API_VERSION=730)
set_target_properties(kv_range_test PROPERTIES BUILD_RPATH "${FDB_LIB_DIR}")

add_executable(gc_ctl
  tools/gc_ctl.cpp
  src/kv_store.cpp
  src/l_keys.cpp
  src/keys.cpp
  src/data_store.cpp
  src/ref_tag.cpp
  src/gc_value.cpp
  src/object_value.cpp
  src/id_tag.cpp
  src/id_meta.cpp
  src/typed_ids.cpp
  src/etag.cpp
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

add_executable(multipart_key_test tests/multipart_key_test.cpp src/keys.cpp src/l_keys.cpp src/kv_store.cpp src/object_value.cpp src/gc_value.cpp src/multipart_value.cpp src/multipart_manifest.cpp src/ref_tag.cpp src/id_tag.cpp src/id_meta.cpp src/typed_ids.cpp src/etag.cpp src/error_codes.cpp)
target_include_directories(multipart_key_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
)
target_link_directories(multipart_key_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(multipart_key_test PRIVATE OpenSSL::Crypto "${FDB_LIB_DIR}/libfdb_c.so")
target_compile_definitions(multipart_key_test PRIVATE FDB_API_VERSION=730)
set_target_properties(multipart_key_test PROPERTIES BUILD_RPATH "${FDB_LIB_DIR}")

add_executable(multipart_manifest_test tests/multipart_manifest_test.cpp src/multipart_manifest.cpp)
target_include_directories(multipart_manifest_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
)
target_link_libraries(multipart_manifest_test PRIVATE OpenSSL::Crypto)

add_executable(gc_config_state_test tests/gc_config_state_test.cpp src/gc_config_state.cpp)
target_include_directories(gc_config_state_test PRIVATE "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(gc_config_state_test PRIVATE OpenSSL::Crypto)

add_executable(fdb_error_test
  tests/fdb_error_test.cpp
  src/kv_store.cpp
  src/l_keys.cpp
  src/keys.cpp
  src/ref_tag.cpp
  src/typed_ids.cpp
  src/etag.cpp
)
target_include_directories(fdb_error_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
)
target_link_directories(fdb_error_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(fdb_error_test PRIVATE OpenSSL::Crypto "${FDB_LIB_DIR}/libfdb_c.so")
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

add_executable(batch_mixed_test
  tests/batch_mixed_test.cpp
)
target_include_directories(batch_mixed_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(batch_mixed_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(batch_mixed_test PRIVATE kvrgw)
set_target_properties(batch_mixed_test PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

add_executable(put_condition_test
  tests/put_condition_test.cpp
)
target_include_directories(put_condition_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(put_condition_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(put_condition_test PRIVATE kvrgw)
set_target_properties(put_condition_test PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

# Ad hoc Phase 2/3 (Initiate/UploadPart/List*/Complete/Abort) smoke test
# against a live FDB cluster -- run manually.
add_executable(multipart_phase2_test
  tests/multipart_phase2_test.cpp
)
target_include_directories(multipart_phase2_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(multipart_phase2_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(multipart_phase2_test PRIVATE kvrgw)
set_target_properties(multipart_phase2_test PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

# GC / Sweeper integration tests for multipart (scenarios 4-7 from
# plans/multipart_test_plan.md) -- isolated binary so its KVRGW_GC_INTERVAL_SEC=1
# env-var override doesn't affect other tests; run manually against a live FDB cluster.
add_executable(multipart_gc_test
  tests/multipart_gc_test.cpp
)
target_include_directories(multipart_gc_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(multipart_gc_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(multipart_gc_test PRIVATE kvrgw)
set_target_properties(multipart_gc_test PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

# CHUNK_MULTIPART_REF zero-copy server-side-copy tests (copy, copy-of-copy,
# delete-original-first, delete-copy-first) -- isolated binary, own
# KVRGW_GC_INTERVAL_SEC override; run manually against a live FDB cluster.
add_executable(multipart_ref_copy_test
  tests/multipart_ref_copy_test.cpp
)
target_include_directories(multipart_ref_copy_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(multipart_ref_copy_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(multipart_ref_copy_test PRIVATE kvrgw)
set_target_properties(multipart_ref_copy_test PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

add_executable(multipart_goa_test
  tests/multipart_goa_test.cpp
)
target_include_directories(multipart_goa_test PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${FDB_INCLUDE_DIR}"
  ${YAML_CPP_INCLUDE_DIRS}
)
target_link_directories(multipart_goa_test PRIVATE "${FDB_LIB_DIR}")
target_link_libraries(multipart_goa_test PRIVATE kvrgw)
set_target_properties(multipart_goa_test PROPERTIES
  BUILD_RPATH "${FDB_LIB_DIR};$ORIGIN"
  INSTALL_RPATH "${FDB_LIB_DIR};$ORIGIN"
)

enable_testing()
add_test(NAME object_value_test COMMAND object_value_test)
add_test(NAME l_key_test COMMAND l_key_test)
add_test(NAME multipart_key_test COMMAND multipart_key_test)
add_test(NAME multipart_manifest_test COMMAND multipart_manifest_test)
add_test(NAME data_store_test COMMAND data_store_test)
add_test(NAME kv_range_test COMMAND kv_range_test)
add_test(NAME gc_config_state_test COMMAND gc_config_state_test)
add_test(NAME fdb_error_test COMMAND fdb_error_test)
add_test(NAME data_store_error_test COMMAND data_store_error_test)
add_test(NAME err_insertion_test COMMAND err_insertion_test)
add_test(NAME batch_mixed_test COMMAND batch_mixed_test)
add_test(NAME put_condition_test COMMAND put_condition_test)
