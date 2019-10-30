add_executable(perf_crimson_msgr perf_crimson_msgr.cc)
target_link_libraries(perf_crimson_msgr crimson)

add_executable(perf_async_msgr perf_async_msgr.cc)
target_link_libraries(perf_async_msgr ceph-common global ${ALLOC_LIBS})
