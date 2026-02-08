# eBPF tracing tools
# Requires: clang, libbpf, libelf, libdw

if(NOT WITH_EBPF_TRACE)
  return()
endif()

set(ARCH ${CMAKE_SYSTEM_PROCESSOR})
if(ARCH STREQUAL "x86_64")
  set(ARCH "x86")
elseif(ARCH STREQUAL "aarch64")
  set(ARCH "arm64")
elseif(ARCH STREQUAL "ppc64le")
  set(ARCH "powerpc")
elseif(ARCH MATCHES "mips.*")
  set(ARCH "mips")
endif()

# Build libbpf and bpftool using ExternalProject
include(Buildlibbpf)
include(Buildbpftool)
build_libbpf()
build_bpftool()

# libbpf requires zlib
find_package(ZLIB REQUIRED)

# Paths
set(EBPF_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(EBPF_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

# Include paths for BPF compilation
# LIBBPF_INCLUDE_DIR contains installed headers (bpf/*.h) from bundled libbpf build
set(EBPF_INCLUDES
  -I${EBPF_SOURCE_DIR}
  -I${EBPF_BINARY_DIR}
  -I${LIBBPF_INCLUDE_DIR}
  -I${LIBBPF_UAPI_INCLUDE_DIR}
  -I${LIBBPF_SRC_DIR})

# Add architecture-specific system includes for BPF compilation
# When clang targets BPF, it doesn't automatically search host arch-specific paths
# like /usr/include/x86_64-linux-gnu where asm/types.h resides
if(CMAKE_LIBRARY_ARCHITECTURE)
  set(ARCH_SYSINCLUDE "-isystem/usr/include/${CMAKE_LIBRARY_ARCHITECTURE}")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  set(ARCH_SYSINCLUDE "-isystem/usr/include/x86_64-linux-gnu")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(ARCH_SYSINCLUDE "-isystem/usr/include/aarch64-linux-gnu")
else()
  set(ARCH_SYSINCLUDE "")
endif()

# Get clang BPF system includes
execute_process(
  COMMAND ${CLANG_EXECUTABLE} -v -E -
  INPUT_FILE /dev/null
  ERROR_VARIABLE CLANG_STDERR
  OUTPUT_QUIET
)
string(REGEX MATCHALL "[ \t]([^\n]+)" CLANG_INCLUDE_DIRS "${CLANG_STDERR}")
set(CLANG_BPF_SYS_INCLUDES "")
set(IN_SEARCH_LIST FALSE)
foreach(LINE ${CLANG_INCLUDE_DIRS})
  string(STRIP "${LINE}" LINE)
  if(LINE MATCHES "^#include <...> search starts here:")
    set(IN_SEARCH_LIST TRUE)
  elseif(LINE MATCHES "^End of search list")
    set(IN_SEARCH_LIST FALSE)
  elseif(IN_SEARCH_LIST AND LINE MATCHES "^/")
    list(APPEND CLANG_BPF_SYS_INCLUDES "-idirafter${LINE}")
  endif()
endforeach()

# Generate bpf_ceph_exported.h from source headers
add_custom_command(
  OUTPUT ${EBPF_BINARY_DIR}/bpf_ceph_exported.h
  COMMAND ${EBPF_SOURCE_DIR}/gen_bpf_types.sh ${EBPF_BINARY_DIR}/bpf_ceph_exported.h
  DEPENDS
    ${CMAKE_SOURCE_DIR}/src/include/ceph_fs.h
    ${CMAKE_SOURCE_DIR}/src/msg/Message.h
    ${CMAKE_SOURCE_DIR}/src/include/rados.h
    ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.h
    ${EBPF_SOURCE_DIR}/gen_bpf_types.sh
    ${EBPF_SOURCE_DIR}/bpf_ceph_types.h
  COMMENT "Generating bpf_ceph_exported.h from Ceph headers"
  VERBATIM
)

add_custom_target(bpf_ceph_export_target
  DEPENDS ${EBPF_BINARY_DIR}/bpf_ceph_exported.h
)

# Build BPF object file
add_custom_command(
  OUTPUT ${EBPF_BINARY_DIR}/radostrace.bpf.o
  COMMAND ${CLANG_EXECUTABLE} -g -O2 -target bpf
    -D__TARGET_ARCH_${ARCH}
    ${EBPF_INCLUDES}
    ${ARCH_SYSINCLUDE}
    ${CLANG_BPF_SYS_INCLUDES}
    -c ${EBPF_SOURCE_DIR}/radostrace.bpf.c
    -o ${EBPF_BINARY_DIR}/radostrace.tmp.bpf.o
  COMMAND ${BPFTOOL_EXECUTABLE} gen object
    ${EBPF_BINARY_DIR}/radostrace.bpf.o
    ${EBPF_BINARY_DIR}/radostrace.tmp.bpf.o
  DEPENDS
    ${EBPF_SOURCE_DIR}/radostrace.bpf.c
    ${EBPF_BINARY_DIR}/bpf_ceph_exported.h
    ${EBPF_SOURCE_DIR}/bpf_ceph_types.h
    ${EBPF_SOURCE_DIR}/bpf_utils.h
    bpftool_ext
    libbpf_ext
    bpf_ceph_export_target
  COMMENT "Compiling BPF program radostrace.bpf.c"
  VERBATIM
)

# Generate BPF skeleton
add_custom_command(
  OUTPUT ${EBPF_BINARY_DIR}/radostrace.skel.h
  COMMAND ${BPFTOOL_EXECUTABLE} gen skeleton
    ${EBPF_BINARY_DIR}/radostrace.bpf.o
    > ${EBPF_BINARY_DIR}/radostrace.skel.h
  DEPENDS ${EBPF_BINARY_DIR}/radostrace.bpf.o
  COMMENT "Generating BPF skeleton radostrace.skel.h"
  VERBATIM
)

# Build dwarf_parser object
# Note: dwarf_parser only uses elfutils (libdw/libdwfl) for DWARF parsing,
# it does NOT use libbpf
add_library(dwarf_parser OBJECT
  ${EBPF_SOURCE_DIR}/dwarf_parser.cc
  ${EBPF_SOURCE_DIR}/dwarf_parser.h
)

target_include_directories(dwarf_parser PRIVATE
  ${EBPF_SOURCE_DIR}
  ${EBPF_BINARY_DIR}
)

# Build radostrace executable
add_executable(radostrace
  ${EBPF_SOURCE_DIR}/radostrace.cc
  ${EBPF_SOURCE_DIR}/dwarf_parser.h
  ${EBPF_BINARY_DIR}/bpf_ceph_exported.h
  ${EBPF_SOURCE_DIR}/bpf_ceph_types.h
  ${EBPF_SOURCE_DIR}/bpf_utils.h
  ${EBPF_BINARY_DIR}/radostrace.skel.h
  $<TARGET_OBJECTS:dwarf_parser>
)

target_include_directories(radostrace PRIVATE
  ${EBPF_SOURCE_DIR}
  ${EBPF_BINARY_DIR}
  ${LIBBPF_INCLUDE_DIR}
  ${LIBBPF_UAPI_INCLUDE_DIR}
  ${LIBBPF_SRC_DIR}
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(radostrace
  libbpf::libbpf
  LibElf::LibElf
  LibDw::LibDw
  ZLIB::ZLIB
  ${CMAKE_DL_LIBS}
  common
)

add_dependencies(radostrace
  libbpf_ext
  bpftool_ext
  bpf_ceph_export_target
)

# Install radostrace
install(TARGETS radostrace DESTINATION ${CMAKE_INSTALL_BINDIR})

message(STATUS "eBPF tracing (radostrace) will be built")
