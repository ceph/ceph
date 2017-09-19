function(do_build_rocksdb)
    set(ROCKSDB_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

  if(ALLOCATOR STREQUAL "jemalloc")
    list(APPEND ROCKSDB_CMAKE_ARGS -DWITH_JEMALLOC=ON)
  endif()

  if (WITH_CCACHE AND CCACHE_FOUND)
    list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_CXX_COMPILER=ccache)
    list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_CXX_COMPILER_ARG1=${CMAKE_CXX_COMPILER})
  else()
    list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
  endif()

  list(APPEND ROCKSDB_CMAKE_ARGS -DPORTABLE=ON)
  list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND ROCKSDB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

  if (CMAKE_CXX_COMPILER_ID STREQUAL Clang)
    list(APPEND ROCKSDB_CMAKE_ARGS -DFAIL_ON_WARNINGS=OFF)
  endif()

  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  ExternalProject_Add(rocksdb_ext
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb
    CMAKE_ARGS ${ROCKSDB_CMAKE_ARGS}
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/rocksdb
    BUILD_COMMAND $(MAKE) rocksdb
    INSTALL_COMMAND "true")

  # force rocksdb make to be called on each time
  ExternalProject_Add_Step(rocksdb_ext forcebuild
    DEPENDEES configure
    DEPENDERS build
    COMMAND "true"
    ALWAYS 1)
endfunction()

macro(build_rocksdb)
  do_build_rocksdb()
  add_library(rocksdb STATIC IMPORTED)
  add_dependencies(rocksdb rocksdb_ext)
  set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/rocksdb/librocksdb.a")
  set(ROCKSDB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include)
  set(ROCKSDB_LIBRARIES rocksdb)
endmacro()
