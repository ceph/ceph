include(CheckCXXSourceRuns)

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
  list(APPEND ROCKSDB_CMAKE_ARGS -DFAIL_ON_WARNINGS=OFF)

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

function(check_aligned_alloc)
  set(SAVE_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
  set(CMAKE_REQUIRED_FLAGS "-std=c++11 -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  if(LINUX)
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -D_GNU_SOURCE")
  endif()
  set(SAVE_CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
  set(CMAKE_REQUIRED_LIBRARIES ${GPERFTOOLS_TCMALLOC_LIBRARY})
  CHECK_CXX_SOURCE_RUNS("
#include <stdlib.h>

int main()
{
#if defined(_ISOC11_SOURCE)
  void* buf = aligned_alloc(64, 42);
  free(buf);
#endif
}
" HAVE_ALIGNED_ALLOC)
  set(CMAKE_REQUIRED_FLAGS ${SAVE_CMAKE_REQUIRED_FLAGS})
  set(CMAKE_REQUIRED_LIBRARIES ${SAVE_CMAKE_REQUIRED_LIBRARIES})

  if(NOT HAVE_ALIGNED_ALLOC)
    message(SEND_ERROR
      "Incompatible tcmalloc v${TCMALLOC_VERSION_STRING} and rocksdb "
      "v${ROCKSDB_VERSION_STRING}, please install gperf-tools 2.5 (not 2.5.93) "
      "or >= 2.6.2, or switch to another allocator using "
      "'cmake -DALLOCATOR=libc'.")
  endif()
endfunction()

macro(build_rocksdb)
  do_build_rocksdb()
  add_library(rocksdb STATIC IMPORTED)
  add_dependencies(rocksdb rocksdb_ext)
  set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/rocksdb/librocksdb.a")
  set(ROCKSDB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include)
  set(ROCKSDB_LIBRARIES rocksdb)
  foreach(ver "MAJOR" "MINOR" "PATCH")
    file(STRINGS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h" ROCKSDB_VER_${ver}_LINE
      REGEX "^#define[ \t]+ROCKSDB_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+ROCKSDB_${ver}[ \t]+([0-9]+)$"
      "\\1" ROCKSDB_VERSION_${ver} "${ROCKSDB_VER_${ver}_LINE}")
    unset(ROCKDB_VER_${ver}_LINE)
  endforeach()
  set(ROCKSDB_VERSION_STRING
    "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}")

  if(ALLOCATOR MATCHES "tcmalloc(_minimal)?")
    # see http://tracker.ceph.com/issues/21422
    if(ROCKSDB_VERSION_STRING VERSION_GREATER 5.7 AND
        TCMALLOC_VERSION_STRING VERSION_GREATER 2.5 AND
        TCMALLOC_VERSION_STRING VERSION_LESS 2.6.2)
      check_aligned_alloc()
    endif()
  endif()
endmacro()
