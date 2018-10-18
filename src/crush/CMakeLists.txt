set(crush_srcs
  builder.c
  mapper.c
  crush.c
  hash.c
  CrushWrapper.cc
  CrushCompiler.cc
  CrushTester.cc
  CrushLocation.cc)

add_library(crush_objs OBJECT ${crush_srcs})

if(WITH_SEASTAR)
  add_library(crimson-crush OBJECT ${crush_srcs})
  target_compile_definitions(crimson-crush PRIVATE
    "WITH_SEASTAR=1")
  target_include_directories(crimson-crush PRIVATE
    $<TARGET_PROPERTY:Seastar::seastar,INTERFACE_INCLUDE_DIRECTORIES>)
endif()
