if(ALLOCATOR STREQUAL "tcmalloc")
  add_library(heap_profiler STATIC
    heap_profiler.cc)
  target_link_libraries(heap_profiler
    gperftools::tcmalloc)
else()
  add_library(heap_profiler STATIC
    disabled_heap_profiler.cc)
endif()
