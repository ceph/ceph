add_ceph_test(test-erasure-code-plugins.sh ${CMAKE_CURRENT_SOURCE_DIR}/test-erasure-code-plugins.sh)

add_ceph_test(test-erasure-code.sh ${CMAKE_CURRENT_SOURCE_DIR}/test-erasure-code.sh)
#add_ceph_test(test-erasure-eio.sh ${CMAKE_CURRENT_SOURCE_DIR}/test-erasure-eio.sh)

add_executable(ceph_erasure_code_benchmark 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  ceph_erasure_code_benchmark.cc)
target_link_libraries(ceph_erasure_code_benchmark common ${Boost_PROGRAM_OPTIONS_LIBRARY} global ${CMAKE_DL_LIBS})
install(TARGETS ceph_erasure_code_benchmark
  DESTINATION bin)

add_executable(ceph_erasure_code_non_regression ceph_erasure_code_non_regression.cc)
target_link_libraries(ceph_erasure_code_non_regression common ${Boost_PROGRAM_OPTIONS_LIBRARY} global ${CMAKE_DL_LIBS})

add_executable(ceph_erasure_code ceph_erasure_code.cc)
target_link_libraries(ceph_erasure_code common ${Boost_PROGRAM_OPTIONS_LIBRARY} global ${CMAKE_DL_LIBS})
install(TARGETS ceph_erasure_code
  DESTINATION bin)

add_library(ec_example SHARED 
  ErasureCodePluginExample.cc
  $<TARGET_OBJECTS:erasure_code_objs>)
add_dependencies(ec_example ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_example pthread ${EXTRALIBS})

add_library(ec_missing_entry_point SHARED ErasureCodePluginMissingEntryPoint.cc)
add_dependencies(ec_missing_entry_point ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_missing_entry_point pthread ${EXTRALIBS})

add_library(ec_missing_version SHARED ErasureCodePluginMissingVersion.cc)
add_dependencies(ec_missing_version ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_missing_version pthread ${EXTRALIBS})

add_library(ec_hangs SHARED ErasureCodePluginHangs.cc)
add_dependencies(ec_hangs ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_hangs pthread ${EXTRALIBS})

add_library(ec_fail_to_initialize SHARED ErasureCodePluginFailToInitialize.cc)
add_dependencies(ec_fail_to_initialize ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_fail_to_initialize pthread ${EXTRALIBS})

add_library(ec_fail_to_register SHARED ErasureCodePluginFailToRegister.cc)
add_dependencies(ec_fail_to_register ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_fail_to_register pthread ${EXTRALIBS})

# unittest_erasure_code_plugin
add_executable(unittest_erasure_code_plugin
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePlugin.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin)
target_link_libraries(unittest_erasure_code_plugin
  global
  ${CMAKE_DL_LIBS}
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
add_executable(unittest_erasure_code
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCode.cc
  )
add_ceph_unittest(unittest_erasure_code ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code)
target_link_libraries(unittest_erasure_code
  global
  common
  )

# unittest_erasure_code_plugin_jerasure
add_executable(unittest_erasure_code_plugin_jerasure
  TestErasureCodePluginJerasure.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_jerasure ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_jerasure)
target_link_libraries(unittest_erasure_code_plugin_jerasure
  global
  common)
add_dependencies(unittest_erasure_code_plugin_jerasure
  ec_jerasure)

if(HAVE_BETTER_YASM_ELF64)

#unittest_erasure_code_isa
add_executable(unittest_erasure_code_isa
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeIsa.cc
  )
add_ceph_unittest(unittest_erasure_code_isa ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_isa)
target_link_libraries(unittest_erasure_code_isa
  global
  common
  ec_isa
  erasure_code
  )

#unittest_erasure_code_plugin_isa
add_executable(unittest_erasure_code_plugin_isa
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePluginIsa.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_isa ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_isa)
target_link_libraries(unittest_erasure_code_plugin_isa
  global
  common
  crush
  ${CMAKE_DL_LIBS}
  erasure_code
  )
add_dependencies(unittest_erasure_code_plugin_isa
  ec_isa)
endif(HAVE_BETTER_YASM_ELF64)

# unittest_erasure_code_lrc
add_executable(unittest_erasure_code_lrc
  TestErasureCodeLrc.cc)
add_ceph_unittest(unittest_erasure_code_lrc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_lrc)
target_link_libraries(unittest_erasure_code_lrc
  global
  ${CMAKE_DL_LIBS}
  ec_lrc
  common
  )

# unittest_erasure_code_plugin_lrc
add_executable(unittest_erasure_code_plugin_lrc
  TestErasureCodePluginLrc.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_lrc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_lrc)
add_dependencies(unittest_erasure_code_plugin_lrc 
  ec_lrc
  ec_jerasure)
target_link_libraries(unittest_erasure_code_plugin_lrc
  global
  ${CMAKE_DL_LIBS}
  common)

# unittest_erasure_code_plugin_shec
add_executable(unittest_erasure_code_plugin_shec
  TestErasureCodePluginShec.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_shec ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_shec)
target_link_libraries(unittest_erasure_code_plugin_shec
  global
  ${CMAKE_DL_LIBS}
  common)
add_dependencies(unittest_erasure_code_plugin_shec 
  ec_shec)

# unittest_erasure_code_example
add_executable(unittest_erasure_code_example
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeExample.cc
)
add_ceph_unittest(unittest_erasure_code_example ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_example)
target_link_libraries(unittest_erasure_code_example
  global
  ${CMAKE_DL_LIBS}
  common
  erasure_code
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_erasure_code_example PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/include)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code//jerasure/gf-complete/include)

# unittest_erasure_code_jerasure
add_executable(unittest_erasure_code_jerasure
  TestErasureCodeJerasure.cc
  )
add_ceph_unittest(unittest_erasure_code_jerasure ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_jerasure)
target_link_libraries(unittest_erasure_code_jerasure
  global
  common
  ec_jerasure
  )

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/shec)

# unittest_erasure_code_shec
add_executable(unittest_erasure_code_shec
  TestErasureCodeShec.cc
  )
add_ceph_unittest(unittest_erasure_code_shec ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec)
target_link_libraries(unittest_erasure_code_shec
  global
  ${CMAKE_DL_LIBS}
  common
  ec_shec
  )

# unittest_erasure_code_shec_all
add_executable(unittest_erasure_code_shec_all
  TestErasureCodeShec_all.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_all ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_all)
target_link_libraries(unittest_erasure_code_shec_all
  global
  ${CMAKE_DL_LIBS}
  common
  ec_shec
  )

# unittest_erasure_code_shec_thread
add_executable(unittest_erasure_code_shec_thread
  TestErasureCodeShec_thread.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_thread ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_thread)
target_link_libraries(unittest_erasure_code_shec_thread
  global
  ${CMAKE_DL_LIBS}
  common
  ec_shec
  )


# unittest_erasure_code_shec_arguments
add_executable(unittest_erasure_code_shec_arguments
  TestErasureCodeShec_arguments.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_arguments ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_arguments)
target_link_libraries(unittest_erasure_code_shec_arguments
  global
  ${CMAKE_DL_LIBS}
  common
  ec_shec
  )
