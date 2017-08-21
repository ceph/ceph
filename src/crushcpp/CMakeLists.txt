cmake_minimum_required(VERSION 2.8.11)

project(libcrush)
set(VERSION 1.0.0)

set(CMAKE_C_FLAGS_NONE "${CMAKE_C_FLAGS_NONE} -Wall -O2 -g")
add_compile_options(-D__STANDALONE_CRUSH__)
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-std=c++11> $<$<COMPILE_LANGUAGE:C>:-std=c99>)

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/placeholders)
include_directories(${CMAKE_BINARY_DIR}/crush)

include(CheckIncludeFiles)

CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("linux/types.h" HAVE_LINUX_TYPES_H)

configure_file(
  ${CMAKE_SOURCE_DIR}/crush/config-h.in.cmake
  ${CMAKE_BINARY_DIR}/crush/acconfig.h
)

set(crushcpp_srcs
  crush/crush.c
crush/builder.c
crush/mapper.c
crush/hash.c
crush/CrushCompiler.cc
crush/CrushWrapper.cc

common/simple_spin.cc
common/errno.cc
common/buffer.cc
common/safe_io.c
common/armor.c
common/escape.c
common/page.cc
common/Formatter.cc
common/HTMLFormatter.cc
common/mempool.cc
common/strtol.cc
)

set(CMAKE_INSTALL_LIBDIR ${CMAKE_INSTALL_PREFIX}/lib CACHE PATH "libdir")
set(CMAKE_INSTALL_INCLUDEDIR ${CMAKE_INSTALL_PREFIX}/include CACHE PATH "includedir")
set(CMAKE_INSTALL_DATADIR ${CMAKE_INSTALL_PREFIX}/share CACHE PATH "datadir")

add_library(crushcpp SHARED ${crushcpp_srcs})
set_target_properties(crushcpp PROPERTIES
    VERSION 1.0.0
    SOVERSION 1
    )
install(TARGETS crushcpp DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY crushcpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h")
install(FILES ${CMAKE_BINARY_DIR}/crush/acconfig.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/crushcpp/)

set(crushtool_srcs crushtool.cc)
add_executable(crushtool ${crushtool_srcs})
target_link_libraries(crushtool crushcpp)
install(TARGETS crushtool DESTINATION bin)

add_subdirectory(test)
add_subdirectory(googletest)
enable_testing()

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME}-${VERSION})
add_custom_target(dist
  COMMAND git-archive-all/git-archive-all.sh --format tar.gz --prefix ${CMAKE_PROJECT_NAME}-${VERSION}/ ${CMAKE_BINARY_DIR}/${CMAKE_PROJECT_NAME}-${VERSION}.tar.gz
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
