add_executable(perf-crimson-msgr perf_crimson_msgr.cc)
target_link_libraries(perf-crimson-msgr crimson)

add_executable(perf-async-msgr perf_async_msgr.cc)
target_link_libraries(perf-async-msgr ceph-common global ${ALLOC_LIBS})

add_executable(perf-staged-fltree perf_staged_fltree.cc)
target_link_libraries(perf-staged-fltree crimson-seastore)
