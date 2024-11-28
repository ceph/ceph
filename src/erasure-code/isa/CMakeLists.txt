# build isa-l from its makefile and expose as target ISAL::ISAL
include(BuildISAL)
build_isal()

# ISA
set(isa_srcs
  ErasureCodeIsa.cc
  ErasureCodeIsaTableCache.cc
  ErasureCodePluginIsa.cc
  xor_op.cc
)

add_library(ec_isa SHARED
  ${isa_srcs}
  $<TARGET_OBJECTS:erasure_code_objs>)
target_link_libraries(ec_isa ISAL::ISAL ${EXTRALIBS})
set_target_properties(ec_isa PROPERTIES
  INSTALL_RPATH "")
install(TARGETS ec_isa DESTINATION ${erasure_plugin_dir})
