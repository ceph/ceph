
add_ceph_test(test-erasure-code.sh ${CMAKE_CURRENT_SOURCE_DIR}/test-erasure-code.sh)
add_ceph_test(test-erasure-eio.sh ${CMAKE_CURRENT_SOURCE_DIR}/test-erasure-eio.sh)

add_executable(ceph_erasure_code_benchmark 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  ceph_erasure_code_benchmark.cc)
target_link_libraries(ceph_erasure_code_benchmark osd common ${Boost_PROGRAM_OPTIONS_LIBRARY} global dl)

add_executable(ceph_erasure_code_non_regression ceph_erasure_code_non_regression.cc)
target_link_libraries(ceph_erasure_code_non_regression osd common ${Boost_PROGRAM_OPTIONS_LIBRARY} global dl)

add_executable(ceph_erasure_code ceph_erasure_code.cc)
target_link_libraries(ceph_erasure_code osd common ${Boost_PROGRAM_OPTIONS_LIBRARY} global dl)

add_library(ec_example SHARED 
  ErasureCodePluginExample.cc
  $<TARGET_OBJECTS:erasure_code_objs>)
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
add_ceph_unittest(unittest_erasure_code_plugin ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin)
target_link_libraries(unittest_erasure_code_plugin
  global
  osd
  dl
  ec_example
  common
  )
add_dependencies(unittest_erasure_code_plugin
  ec_example
  ec_missing_entry_point
  ec_missing_version
  ec_hangs
  ec_fail_to_initialize
  ec_fail_to_register)

# unittest_erasure_code
add_executable(unittest_erasure_code EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCode.cc
  )
add_ceph_unittest(unittest_erasure_code ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code)
target_link_libraries(unittest_erasure_code
  global
  osd
  common
  )

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

# unittest_erasure_code_plugin_jerasure
add_executable(unittest_erasure_code_plugin_jerasure EXCLUDE_FROM_ALL
  TestErasureCodePluginJerasure.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_jerasure ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_jerasure)
target_link_libraries(unittest_erasure_code_plugin_jerasure
  global
  osd
  ec_jerasure_generic
  common)
add_dependencies(unittest_erasure_code_plugin_jerasure
  ec_jerasure
  ec_jerasure_sse3
  ec_jerasure_sse4
  ec_test_jerasure_neon 
  ec_test_jerasure_sse4
  ec_test_jerasure_sse3
  ec_test_jerasure_generic)

if(HAVE_BETTER_YASM_ELF64)

#unittest_erasure_code_isa
add_executable(unittest_erasure_code_isa EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeIsa.cc
  )
add_ceph_unittest(unittest_erasure_code_isa ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_isa)
target_link_libraries(unittest_erasure_code_isa
  global
  osd
  common
  ec_isa
  erasure_code
  )

#unittest_erasure_code_plugin_isa
add_executable(unittest_erasure_code_plugin_isa EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePluginIsa.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_isa ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_isa)
target_link_libraries(unittest_erasure_code_plugin_isa
  global
  osd
  common
  ec_isa
  crush
  dl
  erasure_code
  )
endif(HAVE_BETTER_YASM_ELF64)

# unittest_erasure_code_lrc
add_executable(unittest_erasure_code_lrc EXCLUDE_FROM_ALL
  TestErasureCodeLrc.cc
  ${lrc_srcs}
  )
add_ceph_unittest(unittest_erasure_code_lrc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_lrc)
target_link_libraries(unittest_erasure_code_lrc
  global
  osd
  dl
  ec_lrc
  common
  )

# unittest_erasure_code_plugin_lrc
add_executable(unittest_erasure_code_plugin_lrc EXCLUDE_FROM_ALL
  TestErasureCodePluginLrc.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_lrc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_lrc)
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
  common)

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
add_ceph_unittest(unittest_erasure_code_plugin_shec ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_shec)
target_link_libraries(unittest_erasure_code_plugin_shec
  global
  osd
  dl
  common
  ec_shec_generic)
add_dependencies(unittest_erasure_code_plugin_shec 
  ec_shec
  ec_shec_sse3
  ec_shec_sse4
  #ec_shec_neon
  ec_test_shec_neon
  ec_test_shec_sse3
  ec_test_shec_sse4
  ec_test_shec_generic)

# unittest_erasure_code_example
add_executable(unittest_erasure_code_example EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeExample.cc
)
add_ceph_unittest(unittest_erasure_code_example ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_example)
target_link_libraries(unittest_erasure_code_example
  global
  osd
  dl
  common
  erasure_code
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_erasure_code_example PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/include)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code//jerasure/gf-complete/include)

# unittest_erasure_code_jerasure
add_executable(unittest_erasure_code_jerasure EXCLUDE_FROM_ALL
  TestErasureCodeJerasure.cc
  )
add_ceph_unittest(unittest_erasure_code_jerasure ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_jerasure)
target_link_libraries(unittest_erasure_code_jerasure
  global
  osd
  common
  ec_jerasure_generic
  )

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/shec)

# unittest_erasure_code_shec
add_executable(unittest_erasure_code_shec EXCLUDE_FROM_ALL
  TestErasureCodeShec.cc
  )
add_ceph_unittest(unittest_erasure_code_shec ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec)
target_link_libraries(unittest_erasure_code_shec
  global
  osd
  dl
  common
  ec_shec_generic
  )

# unittest_erasure_code_shec_all
add_executable(unittest_erasure_code_shec_all EXCLUDE_FROM_ALL
  TestErasureCodeShec_all.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_all ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_all)
target_link_libraries(unittest_erasure_code_shec_all
  global
  osd
  dl
  common
  ec_shec_generic
  )

# unittest_erasure_code_shec_thread
add_executable(unittest_erasure_code_shec_thread EXCLUDE_FROM_ALL
  TestErasureCodeShec_thread.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_thread ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_thread)
target_link_libraries(unittest_erasure_code_shec_thread
  global
  osd
  dl
  common
  ec_shec_generic
  )


# unittest_erasure_code_shec_arguments
add_executable(unittest_erasure_code_shec_arguments EXCLUDE_FROM_ALL
  TestErasureCodeShec_arguments.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_arguments ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_arguments)
target_link_libraries(unittest_erasure_code_shec_arguments
  global
  osd
  dl
  common
  ec_shec_generic
  )
