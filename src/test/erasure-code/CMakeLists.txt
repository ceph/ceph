# make check tests for erasure-code directory
# unittest_erasure_code_plugin
add_executable(unittest_erasure_code_plugin EXCLUDE_FROM_ALL
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePlugin.cc
  )
add_test(unittest_erasure_code_plugin unittest_erasure_code_plugin)
add_dependencies(check unittest_erasure_code_plugin)
target_link_libraries(unittest_erasure_code_plugin
  global
  osd
  common
  dl
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

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

# unittest_erasure_code_jerasure_plugin
add_executable(unittest_erasure_code_jerasure_plugin EXCLUDE_FROM_ALL
  TestErasureCodePluginJerasure.cc
  )
add_test(unittest_erasure_code_jerasure_plugin unittest_erasure_code_jerasure_plugin)
add_dependencies(check unittest_erasure_code_jerasure_plugin)
target_link_libraries(unittest_erasure_code_jerasure_plugin
  global
  osd
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_jerasure_plugin PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

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
${UNITTEST_CXX_FLAGS})
endif(HAVE_BETTER_YASM_ELF64)

# unittest_erasure_code_lrc
add_executable(unittest_erasure_code_lrc EXCLUDE_FROM_ALL
  TestErasureCodeLrc.cc
  ${lrc_srcs}
  )
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
  ${UNITTEST_CXX_FLAGS})

# unittest_erasure_code_plugin_lrc
add_executable(unittest_erasure_code_plugin_lrc EXCLUDE_FROM_ALL
  TestErasureCodePluginLrc.cc
  )
add_test(unittest_erasure_code_plugin_lrc unittest_erasure_code_plugin_lrc)
add_dependencies(check unittest_erasure_code_plugin_lrc)
target_link_libraries(unittest_erasure_code_plugin_lrc
  global
  osd
  dl
  common
  ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_erasure_code_plugin_lrc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_erasure_code_plugin_shec
add_executable(unittest_erasure_code_plugin_shec EXCLUDE_FROM_ALL
  TestErasureCodePluginShec.cc
  )
add_test(unittest_erasure_code_plugin_shec unittest_erasure_code_plugin_shec)
add_dependencies(check unittest_erasure_code_plugin_shec)
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
  ${UNITTEST_CXX_FLAGS})

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

