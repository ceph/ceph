# make check tests for erasure-code directory

set(EC_LIBS_PATH_FLAG "-DEC_LIBS_PATH=${CMAKE_BINARY_DIR}/src/test/erasure-code")

add_library(ec_example SHARED ErasureCodePluginExample.cc)
add_dependencies(ec_example ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_example pthread ${EXTRA_LIBS})

add_library(ec_missing_entry_point SHARED ErasureCodePluginMissingEntryPoint.cc)
add_dependencies(ec_missing_entry_point ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_missing_entry_point pthread ${EXTRA_LIBS})

add_library(ec_missing_version SHARED ErasureCodePluginMissingVersion.cc)
add_dependencies(ec_missing_version ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_missing_version pthread ${EXTRA_LIBS})

add_library(ec_hangs SHARED ErasureCodePluginHangs.cc)
add_dependencies(ec_hangs ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_hangs pthread ${EXTRA_LIBS})

add_library(ec_fail_to_initialize SHARED ErasureCodePluginFailToInitialize.cc)
add_dependencies(ec_fail_to_initialize ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_fail_to_initialize pthread ${EXTRA_LIBS})

add_library(ec_fail_to_register SHARED ErasureCodePluginFailToRegister.cc)
add_dependencies(ec_fail_to_register ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_fail_to_register pthread ${EXTRA_LIBS})

# unittest_erasure_code_plugin
add_executable(unittest_erasure_code_plugin EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePlugin.cc
  )
add_dependencies(unittest_erasure_code_plugin
  ec_example
  ec_missing_entry_point
  ec_missing_version
  ec_hangs
  ec_fail_to_initialize
  ec_fail_to_register)
add_test(unittest_erasure_code_plugin unittest_erasure_code_plugin)
add_dependencies(check unittest_erasure_code_plugin)
target_link_libraries(unittest_erasure_code_plugin
  global
  osd
  dl
  ec_example
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin PROPERTIES COMPILE_FLAGS
  "${EC_LIBS_PATH_FLAG} ${UNITTEST_CXX_FLAGS}")

# unittest_erasure_code
add_executable(unittest_erasure_code EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCode.cc
  )
add_test(unittest_erasure_code unittest_erasure_code)
add_dependencies(check unittest_erasure_code)
target_link_libraries(unittest_erasure_code
  global
  osd
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_library(ec_test_jerasure_neon SHARED TestJerasurePluginNEON.cc)
add_dependencies(ec_test_jerasure_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_jerasure_neon pthread ${EXTRA_LIBS})

add_library(ec_test_jerasure_sse4 SHARED TestJerasurePluginSSE4.cc)
add_dependencies(ec_test_jerasure_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_jerasure_sse4 pthread ${EXTRA_LIBS})

add_library(ec_test_jerasure_sse3 SHARED TestJerasurePluginSSE3.cc)
add_dependencies(ec_test_jerasure_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_jerasure_sse3 pthread ${EXTRA_LIBS})

add_library(ec_test_jerasure_generic SHARED TestJerasurePluginGeneric.cc)
add_dependencies(ec_test_jerasure_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_jerasure_generic pthread ${EXTRA_LIBS})

# unittest_erasure_code_jerasure_plugin
add_executable(unittest_erasure_code_plugin_jerasure EXCLUDE_FROM_ALL
  TestErasureCodePluginJerasure.cc
  )
add_dependencies(unittest_erasure_code_plugin_jerasure
  ec_jerasure
  ec_jerasure_sse3
  ec_jerasure_sse4
  ec_test_jerasure_neon 
  ec_test_jerasure_sse4
  ec_test_jerasure_sse3
  ec_test_jerasure_generic)
add_test(unittest_erasure_code_plugin_jerasure unittest_erasure_code_jerasure_plugin)
add_dependencies(check unittest_erasure_code_plugin_jerasure)
target_link_libraries(unittest_erasure_code_plugin_jerasure
  global
  osd
  ec_jerasure_generic
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin_jerasure PROPERTIES COMPILE_FLAGS
  "${EC_LIBS_PATH_FLAG} ${UNITTEST_CXX_FLAGS}")

if(HAVE_BETTER_YASM_ELF64)

#unittest_erasure_code_isa
add_executable(unittest_erasure_code_isa EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeIsa.cc
)
add_test(unittest_erasure_code_isa unittest_erasure_code_isa)
add_dependencies(check unittest_erasure_code_isa)
target_link_libraries(unittest_erasure_code_isa
  global
  osd
  common
  ec_isa
  erasure_code
  dl
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_isa PROPERTIES COMPILE_FLAGS
${UNITTEST_CXX_FLAGS})

#unittest_erasure_code_plugin_isa
add_executable(unittest_erasure_code_plugin_isa EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePluginIsa.cc
)
add_test(unittest_erasure_code_plugin_isa unittest_erasure_code_plugin_isa)
add_dependencies(check unittest_erasure_code_plugin_isa)
target_link_libraries(unittest_erasure_code_plugin_isa
  global
  osd
  common
  ec_isa
  crush
  dl
  erasure_code
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin_isa PROPERTIES COMPILE_FLAGS
  "${EC_LIBS_PATH_FLAG} ${UNITTEST_CXX_FLAGS}")
endif(HAVE_BETTER_YASM_ELF64)

# unittest_erasure_code_lrc
add_executable(unittest_erasure_code_lrc EXCLUDE_FROM_ALL
  TestErasureCodeLrc.cc
  ${lrc_srcs}
  )
add_dependencies(unittest_erasure_code_lrc ec_jerasure)
add_test(unittest_erasure_code_lrc unittest_erasure_code_lrc)
add_dependencies(check unittest_erasure_code_lrc)
target_link_libraries(unittest_erasure_code_lrc
  global
  osd
  dl
  ec_lrc
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_lrc PROPERTIES COMPILE_FLAGS
  "${EC_LIBS_PATH_FLAG} ${UNITTEST_CXX_FLAGS}")

# unittest_erasure_code_plugin_lrc
add_executable(unittest_erasure_code_plugin_lrc EXCLUDE_FROM_ALL
  TestErasureCodePluginLrc.cc
  )
add_test(unittest_erasure_code_plugin_lrc unittest_erasure_code_plugin_lrc)
add_dependencies(check unittest_erasure_code_plugin_lrc)
add_dependencies(unittest_erasure_code_plugin_lrc 
  ec_lrc
  ec_jerasure
  ec_jerasure_sse3
  ec_jerasure_sse4
  ec_jerasure_generic)
target_link_libraries(unittest_erasure_code_plugin_lrc
  global
  osd
  dl
  ec_lrc
  ec_jerasure_generic
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin_lrc PROPERTIES COMPILE_FLAGS
  "${EC_LIBS_PATH_FLAG} ${UNITTEST_CXX_FLAGS}")

add_library(ec_test_shec_neon SHARED TestShecPluginNEON.cc)
add_dependencies(ec_test_shec_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_shec_neon pthread ${EXTRA_LIBS})

add_library(ec_test_shec_sse4 SHARED TestShecPluginSSE4.cc)
add_dependencies(ec_test_shec_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_shec_sse4 pthread ${EXTRA_LIBS})

add_library(ec_test_shec_sse3 SHARED TestShecPluginSSE3.cc)
add_dependencies(ec_test_shec_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_shec_sse3 pthread ${EXTRA_LIBS})

add_library(ec_test_shec_generic SHARED TestShecPluginGeneric.cc)
add_dependencies(ec_test_shec_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_test_shec_generic pthread ${EXTRA_LIBS})

# unittest_erasure_code_plugin_shec
add_executable(unittest_erasure_code_plugin_shec EXCLUDE_FROM_ALL
  TestErasureCodePluginShec.cc
  )
add_test(unittest_erasure_code_plugin_shec unittest_erasure_code_plugin_shec)
add_dependencies(check unittest_erasure_code_plugin_shec)
add_dependencies(unittest_erasure_code_plugin_shec 
  ec_shec
  ec_shec_sse3
  ec_shec_sse4
  #ec_shec_neon
  ec_test_shec_neon
  ec_test_shec_sse3
  ec_test_shec_sse4
  ec_test_shec_generic)
target_link_libraries(unittest_erasure_code_plugin_shec
  global
  osd
  dl
  common
  ec_shec_generic
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin_shec PROPERTIES COMPILE_FLAGS
  "${EC_LIBS_PATH_FLAG} ${UNITTEST_CXX_FLAGS}")


# unittest_erasure_code_example
add_executable(unittest_erasure_code_example EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeExample.cc
)
add_test(unittest_erasure_code_example unittest_erasure_code_example)
add_dependencies(check unittest_erasure_code_example)
target_link_libraries(unittest_erasure_code_example
  global
  osd
  dl
  common
  erasure_code
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_example PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/include)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code//jerasure/gf-complete/include)

# unittest_erasure_code_jerasure
add_executable(unittest_erasure_code_jerasure EXCLUDE_FROM_ALL
  TestErasureCodeJerasure.cc
  )
add_test(unittest_erasure_code_jerasure unittest_erasure_code_jerasure)
add_dependencies(check unittest_erasure_code_jerasure)
target_link_libraries(unittest_erasure_code_jerasure
  global
  osd
  common
  ec_jerasure_generic
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_jerasure PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/shec)

# unittest_erasure_code_shec
add_executable(unittest_erasure_code_shec
  TestErasureCodeShec.cc
  )
add_test(unittest_erasure_code_shec unittest_erasure_code_shec)
add_dependencies(check unittest_erasure_code_shec)
target_link_libraries(unittest_erasure_code_shec
  global
  osd
  dl
  common
  ec_shec_generic
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_shec PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_erasure_code_shec_all
add_executable(unittest_erasure_code_shec_all EXCLUDE_FROM_ALL
  TestErasureCodeShec_all.cc
  )
add_test(unittest_erasure_code_shec_all unittest_erasure_code_shec_all)
add_dependencies(check unittest_erasure_code_shec_all)
target_link_libraries(unittest_erasure_code_shec_all
  global
  osd
  dl
  common
  ec_shec_generic
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_shec_all PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_erasure_code_shec_thread
add_executable(unittest_erasure_code_shec_thread EXCLUDE_FROM_ALL
  TestErasureCodeShec_thread.cc
  )
add_test(unittest_erasure_code_shec_thread unittest_erasure_code_shec_thread)
add_dependencies(check unittest_erasure_code_shec_thread)
target_link_libraries(unittest_erasure_code_shec_thread
  global
  osd
  dl
  common
  ec_shec_generic
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_shec_thread PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_erasure_code_shec_arguments
add_executable(unittest_erasure_code_shec_arguments EXCLUDE_FROM_ALL
  TestErasureCodeShec_arguments.cc
  )
add_test(unittest_erasure_code_shec_arguments unittest_erasure_code_shec_arguments)
add_dependencies(check unittest_erasure_code_shec_arguments)
target_link_libraries(unittest_erasure_code_shec_arguments
  global
  osd
  dl
  common
  ec_shec_generic
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_shec_arguments PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_custom_target(ec_symlinks  COMMAND
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/shec/libec_shec_generic.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/shec/libec_shec.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/shec/libec_shec_sse3.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/shec/libec_shec_sse4.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/lrc/libec_lrc.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/jerasure/libec_jerasure.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/jerasure/libec_jerasure_sse4.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/jerasure/libec_jerasure_sse3.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/jerasure/libec_jerasure_generic.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/
  ln -sf ${CMAKE_BINARY_DIR}/src/erasure-code/isa/libec_isa.so ${CMAKE_BINARY_DIR}/src/test/erasure-code/
COMMENT "Symlinks for Erasure Code libraries have been created")
add_dependencies(check ec_symlinks)

