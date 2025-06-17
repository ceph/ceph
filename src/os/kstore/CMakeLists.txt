add_library(kstore STATIC
  KStore.cc
  kstore_types.cc)
target_compile_definitions(kstore
  PUBLIC WITH_KSTORE)
target_link_libraries(kstore
  PUBLIC kv)
