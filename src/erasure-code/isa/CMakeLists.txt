# build isa-l from its makefile and expose as target ISAL::ISAL
include(BuildISAL)
build_isal()

# ISA
set(isa_srcs
  ErasureCodeIsa.cc
  ErasureCodeIsaTableCache.cc
  ErasureCodePluginIsa.cc
)

# x86-64 specific check for GFNI support. If the CPU supports GFNI then 
# we need to use an 8 byte stride size for parity delta writes (otherwise 32).
if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64|AMD64")

  find_program(ASSEMBLER_PATH NAMES nasm yasm)
  if(NOT ASSEMBLER_PATH)
    message(FATAL_ERROR "nasm/yasm not found! Required for ErasureCodeIsaCoefficientSize.asm")
  endif()

  set(ASM_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/ErasureCodeIsaCoefficientSize.asm")
  set(ASM_OBJECT "${CMAKE_CURRENT_BINARY_DIR}/ErasureCodeIsaCoefficientSize.o")
  set(ISAL_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/src/isa-l/include/")

  if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set(ASM_FORMAT "macho64")
  else()
    set(ASM_FORMAT "elf64")
  endif()

  add_custom_command(
    OUTPUT ${ASM_OBJECT}
    COMMAND ${ASSEMBLER_PATH} -f ${ASM_FORMAT} -I ${ISAL_INCLUDE_DIR} ${ASM_SOURCE} -o ${ASM_OBJECT}
    DEPENDS ${ASM_SOURCE}
    COMMENT "Assembling ErasureCodeIsaCoefficientSize"
  )

  list(APPEND isa_srcs ${ASM_OBJECT})

else()
  # Not x86-64 so no need to check for GFNI support
  list(APPEND isa_srcs ErasureCodeIsaCoefficientSize.cc)

endif()


add_library(ec_isa SHARED
  ${isa_srcs}
  $<TARGET_OBJECTS:erasure_code_objs>)
target_include_directories(ec_isa PRIVATE "${CMAKE_SOURCE_DIR}/src/isa-l/include")
target_link_libraries(ec_isa ISAL::ISAL ${EXTRALIBS})
set_target_properties(ec_isa PROPERTIES
  INSTALL_RPATH "")
install(TARGETS ec_isa DESTINATION ${erasure_plugin_dir})
