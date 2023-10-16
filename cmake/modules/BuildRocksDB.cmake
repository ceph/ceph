function(build_rocksdb)
  set(rocksdb_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
  list(APPEND rocksdb_CMAKE_ARGS -DWITH_GFLAGS=OFF)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  string(REPLACE ";" "!" CMAKE_PREFIX_PATH_ALT_SEP "${CMAKE_PREFIX_PATH}")
  list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND rocksdb_CMAKE_ARGS
         -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND rocksdb_CMAKE_ARGS -DWITH_LIBURING=${WITH_LIBURING})
  if(WITH_LIBURING)
    list(APPEND rocksdb_CMAKE_ARGS -During_INCLUDE_DIR=${URING_INCLUDE_DIR})
    list(APPEND rocksdb_CMAKE_ARGS -During_LIBRARIES=${URING_LIBRARY_DIR})
    list(APPEND rocksdb_INTERFACE_LINK_LIBRARIES uring::uring)
  endif()

  if(ALLOCATOR STREQUAL "jemalloc")
    list(APPEND rocksdb_CMAKE_ARGS -DWITH_JEMALLOC=ON)
    list(APPEND rocksdb_INTERFACE_LINK_LIBRARIES JeMalloc::JeMalloc)
  endif()

  list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

  list(APPEND rocksdb_CMAKE_ARGS -DWITH_SNAPPY=${SNAPPY_FOUND})
  if(SNAPPY_FOUND)
    list(APPEND rocksdb_INTERFACE_LINK_LIBRARIES snappy::snappy)
  endif()
  # libsnappy is a C++ library, we need to force rocksdb to link against
  # libsnappy statically.
  if(SNAPPY_FOUND AND WITH_STATIC_LIBSTDCXX)
    list(APPEND rocksdb_CMAKE_ARGS -DWITH_SNAPPY_STATIC_LIB=ON)
  endif()

  list(APPEND rocksdb_CMAKE_ARGS -DWITH_LZ4=${LZ4_FOUND})
  if(LZ4_FOUND)
    list(APPEND rocksdb_INTERFACE_LINK_LIBRARIES LZ4::LZ4)
    # When cross compiling, cmake may fail to locate lz4.
    list(APPEND rocksdb_CMAKE_ARGS -Dlz4_INCLUDE_DIRS=${LZ4_INCLUDE_DIR})
    list(APPEND rocksdb_CMAKE_ARGS -Dlz4_LIBRARIES=${LZ4_LIBRARY})
  endif()

  list(APPEND rocksdb_CMAKE_ARGS -DWITH_ZLIB=${ZLIB_FOUND})
  if(ZLIB_FOUND)
    list(APPEND rocksdb_INTERFACE_LINK_LIBRARIES ZLIB::ZLIB)
  endif()

  list(APPEND rocksdb_CMAKE_ARGS -DPORTABLE=ON)
  list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
  list(APPEND rocksdb_CMAKE_ARGS -DFAIL_ON_WARNINGS=OFF)
  list(APPEND rocksdb_CMAKE_ARGS -DUSE_RTTI=1)
  CHECK_C_COMPILER_FLAG("-Wno-stringop-truncation" HAS_WARNING_STRINGOP_TRUNCATION)
  if(HAS_WARNING_STRINGOP_TRUNCATION)
    list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_C_FLAGS=-Wno-stringop-truncation)
  endif()
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-Wno-deprecated-copy" HAS_WARNING_DEPRECATED_COPY)
  if(HAS_WARNING_DEPRECATED_COPY)
    set(rocksdb_CXX_FLAGS -Wno-deprecated-copy)
  endif()
  check_cxx_compiler_flag("-Wno-pessimizing-move" HAS_WARNING_PESSIMIZING_MOVE)
  if(HAS_WARNING_PESSIMIZING_MOVE)
    set(rocksdb_CXX_FLAGS "${rocksdb_CXX_FLAGS} -Wno-pessimizing-move")
  endif()
  if(rocksdb_CXX_FLAGS)
    list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_CXX_FLAGS='${rocksdb_CXX_FLAGS}')
  endif()
  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  set(rocksdb_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rocksdb")
  set(rocksdb_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/rocksdb")
  set(rocksdb_LIBRARY "${rocksdb_BINARY_DIR}/librocksdb.a")
  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE) rocksdb)
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target rocksdb)
  endif()

  ExternalProject_Add(rocksdb_ext
    SOURCE_DIR "${rocksdb_SOURCE_DIR}"
    CMAKE_ARGS ${rocksdb_CMAKE_ARGS}
    BINARY_DIR "${rocksdb_BINARY_DIR}"
    BUILD_COMMAND "${make_cmd}"
    BUILD_BYPRODUCTS "${rocksdb_LIBRARY}"
    INSTALL_COMMAND ""
    LIST_SEPARATOR !)

  add_library(RocksDB::RocksDB STATIC IMPORTED)
  add_dependencies(RocksDB::RocksDB rocksdb_ext)
  set(rocksdb_INCLUDE_DIR "${rocksdb_SOURCE_DIR}/include")
  foreach(ver "MAJOR" "MINOR" "PATCH")
    file(STRINGS "${rocksdb_INCLUDE_DIR}/rocksdb/version.h" ROCKSDB_VER_${ver}_LINE
      REGEX "^#define[ \t]+ROCKSDB_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+ROCKSDB_${ver}[ \t]+([0-9]+)$"
      "\\1" ROCKSDB_VERSION_${ver} "${ROCKSDB_VER_${ver}_LINE}")
    unset(ROCKDB_VER_${ver}_LINE)
  endforeach()
  set(rocksdb_VERSION_STRING
    "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}")
  set_target_properties(RocksDB::RocksDB PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${rocksdb_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${rocksdb_INTERFACE_LINK_LIBRARIES}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${rocksdb_LIBRARY}"
    VERSION "${rocksdb_VERSION_STRING}")
endfunction()
