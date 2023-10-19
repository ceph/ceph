if(ALLOCATOR STREQUAL "tcmalloc")
  add_library(heap_profiler STATIC
    heap_profiler.cc)
  target_link_libraries(heap_profiler
    gperftools::tcmalloc)
else()
  add_library(heap_profiler STATIC
    disabled_heap_profiler.cc)
endif()

option(WITH_PROFILER "build extra profiler binaries" OFF)

if(WITH_PROFILER)
  find_package(gperftools 2.6.2 REQUIRED profiler)
  add_library(cpu_profiler STATIC
    cpu_profiler.cc)
  target_link_libraries(cpu_profiler
    gperftools::profiler)
else()
  add_library(cpu_profiler STATIC
    disabled_stubs.cc)
endif()
