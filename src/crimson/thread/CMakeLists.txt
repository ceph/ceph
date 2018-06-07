set(crimson_thread_srcs
  ThreadPool.cc
  Throttle.cc)
add_library(crimson_thread_objs OBJECT ${crimson_thread_srcs})
target_compile_definitions(crimson_thread_objs
  PUBLIC $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_COMPILE_DEFINITIONS>)
target_include_directories(crimson_thread_objs
  PUBLIC $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_INCLUDE_DIRECTORIES>)
