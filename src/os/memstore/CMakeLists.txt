include(CheckCXXCompilerFlag)

add_library(memstore
  MemStore.cc)
target_link_libraries(memstore PRIVATE os)
check_cxx_compiler_flag("-Wno-maybe-uninitialized"
  HAS_WARNING_MAYBE_UNINITIALIZED)
if(HAS_WARNING_MAYBE_UNINITIALIZED)
  target_compile_options(os
    PRIVATE "-Wno-maybe-uninitialized")
endif()
