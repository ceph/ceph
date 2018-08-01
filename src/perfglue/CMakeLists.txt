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
  if(GPERFTOOLS_USE_STATIC_LIBS)
    if(NOT TARGET gperftools::profiler)
      include(Buildgperftools)
    endif()
  else()
    find_package(gperftools REQUIRED profiler)
  endif()
  add_library(cpu_profiler STATIC
    cpu_profiler.cc)
  target_link_libraries(cpu_profiler
    gperftools::profiler)
else()
  add_library(cpu_profiler STATIC
    disabled_stubs.cc)
endif()
