set(HDRHISTOGRAM_PATH ${CMAKE_SOURCE_DIR}/HdrHistogram_c)
add_subdirectory(${HDRHISTOGRAM_PATH} ${CMAKE_BINARY_DIR}/HdrHistogram_c_build)
include_directories(${HDRHISTOGRAM_PATH}/include)
add_executable(crimson-store-nbd
	store_nbd/store-nbd.cc
	store_nbd/tm_driver.cc
	store_nbd/fs_driver.cc
	store_nbd/block_driver.cc
  )
target_link_libraries(crimson-store-nbd
  crimson-os)
install(TARGETS crimson-store-nbd DESTINATION bin)

add_executable(crimson-store-bench
	store_bench/store-bench.cc
  )
target_link_libraries(crimson-store-bench
  crimson-os
  hdr_histogram_static)
install(TARGETS crimson-store-bench DESTINATION bin)

add_executable(perf-crimson-msgr perf_crimson_msgr.cc)
target_link_libraries(perf-crimson-msgr crimson)

add_executable(perf-async-msgr perf_async_msgr.cc)
target_link_libraries(perf-async-msgr ceph-common global ${ALLOC_LIBS})

add_executable(perf-staged-fltree perf_staged_fltree.cc)
if(WITH_TESTS)
target_link_libraries(perf-staged-fltree crimson-seastore crimson::gtest)
else()
target_link_libraries(perf-staged-fltree crimson-seastore)
endif()

add_executable(crimson-objectstore-tool
  objectstore/objectstore_tool.cc
  objectstore/crimson_objectstore_tool.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/lsan_suppressions.cc)
target_link_libraries(crimson-objectstore-tool
  legacy-option-headers
  crimson
  crimson-os)
install(TARGETS crimson-objectstore-tool DESTINATION bin)
