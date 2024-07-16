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
target_link_libraries(crush_objs PUBLIC legacy-option-headers)
