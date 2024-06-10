# apache arrow and its parquet library are used in radosgw for s3 select

function(build_arrow)
  # only enable the parquet component
  set(arrow_CMAKE_ARGS -DARROW_PARQUET=ON)

  # only use preinstalled dependencies for arrow, don't fetch/build any
  list(APPEND arrow_CMAKE_ARGS -DARROW_DEPENDENCY_SOURCE=SYSTEM)

  # only build static version of arrow and parquet
  list(APPEND arrow_CMAKE_ARGS -DARROW_BUILD_SHARED=OFF)
  list(APPEND arrow_CMAKE_ARGS -DARROW_BUILD_STATIC=ON)

  # arrow only supports its own bundled version of jemalloc, so can't
  # share the version ceph is using
  list(APPEND arrow_CMAKE_ARGS -DARROW_JEMALLOC=OFF)

  # transitive dependencies
  list(APPEND arrow_INTERFACE_LINK_LIBRARIES thrift)

  if (NOT WITH_SYSTEM_UTF8PROC)
    # forward utf8proc_ROOT from build_utf8proc()
    list(APPEND arrow_CMAKE_ARGS -Dutf8proc_ROOT=${utf8proc_ROOT})
    # non-system utf8proc is bundled as a static library
    list(APPEND arrow_CMAKE_ARGS -DARROW_UTF8PROC_USE_SHARED=OFF)
    # make sure utf8proc submodule builds first, so arrow can find its byproducts
    list(APPEND arrow_DEPENDS utf8proc::utf8proc)
  endif()

  list(APPEND arrow_CMAKE_ARGS -DARROW_WITH_BROTLI=${HAVE_BROTLI})
  if (HAVE_BROTLI) # optional, off by default
    list(APPEND arrow_INTERFACE_LINK_LIBRARIES ${brotli_libs})
  endif (HAVE_BROTLI)

  list(APPEND arrow_CMAKE_ARGS -DARROW_WITH_BZ2=OFF)

  list(APPEND arrow_CMAKE_ARGS -DARROW_WITH_LZ4=${HAVE_LZ4})
  if (HAVE_LZ4) # optional, on by default
    list(APPEND arrow_INTERFACE_LINK_LIBRARIES LZ4::LZ4)
  endif (HAVE_LZ4)

  list(APPEND arrow_CMAKE_ARGS -DARROW_WITH_SNAPPY=ON) # required
  list(APPEND arrow_INTERFACE_LINK_LIBRARIES snappy::snappy)

  if(WITH_RADOSGW_ARROW_FLIGHT)
    message("building arrow flight; make sure grpc-plugins is installed on the system")
    list(APPEND arrow_CMAKE_ARGS
      -DARROW_FLIGHT=ON -DARROW_WITH_RE2=OFF)
    find_package(gRPC REQUIRED)
    find_package(Protobuf REQUIRED)
    find_package(c-ares 1.13.0 QUIET REQUIRED)
  endif(WITH_RADOSGW_ARROW_FLIGHT)

  list(APPEND arrow_CMAKE_ARGS -DARROW_WITH_ZLIB=ON) # required
  list(APPEND arrow_INTERFACE_LINK_LIBRARIES ZLIB::ZLIB)

  list(APPEND arrow_CMAKE_ARGS -DARROW_WITH_ZSTD=${WITH_SYSTEM_ZSTD})
  if (WITH_SYSTEM_ZSTD)
    find_package(Zstd 1.4.4 REQUIRED)
    list(APPEND arrow_INTERFACE_LINK_LIBRARIES Zstd::Zstd)
  endif (WITH_SYSTEM_ZSTD)

  list(APPEND arrow_CMAKE_ARGS -DBOOST_ROOT=${BOOST_ROOT})
  list(APPEND arrow_CMAKE_ARGS -DBOOST_INCLUDEDIR=${Boost_INCLUDE_DIR})
  list(APPEND arrow_CMAKE_ARGS -DBOOST_LIBRARYDIR=${BOOST_LIBRARYDIR})

  if (NOT WITH_SYSTEM_BOOST)
    # make sure boost submodule builds first, so arrow can find its byproducts
    list(APPEND arrow_DEPENDS Boost)
  endif()

  # since Arrow 15.0.0 needs xsimd>=8.1.0 and since Ubuntu Jammy
  # Jellyfish only provides 7.6.0, we'll have arrow build it as source
  list(APPEND arrow_CMAKE_ARGS -Dxsimd_SOURCE=BUNDLED)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  string(REPLACE ";" "!" CMAKE_PREFIX_PATH_ALT_SEP "${CMAKE_PREFIX_PATH}")
  list(APPEND arrow_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND arrow_CMAKE_ARGS
         -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND arrow_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
  list(APPEND arrow_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
  list(APPEND arrow_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  if(CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE STREQUAL "None")
    list(APPEND arrow_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
  else()
    list(APPEND arrow_CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release)
  endif()
  # don't add -Werror or debug package builds fail with:
  #warning _FORTIFY_SOURCE requires compiling with optimization (-O)
  list(APPEND arrow_CMAKE_ARGS -DBUILD_WARNING_LEVEL=PRODUCTION)

  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  set(arrow_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/arrow/cpp")
  set(arrow_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow/cpp")

  set(arrow_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow")
  list(APPEND arrow_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${arrow_INSTALL_PREFIX})

  set(arrow_INSTALL_LIBDIR "lib") # force lib so we don't have to guess between lib/lib64
  list(APPEND arrow_CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=${arrow_INSTALL_LIBDIR})
  set(arrow_LIBRARY_DIR "${arrow_INSTALL_PREFIX}/${arrow_INSTALL_LIBDIR}")

  set(arrow_LIBRARY "${arrow_LIBRARY_DIR}/libarrow.a")
  set(parquet_LIBRARY "${arrow_LIBRARY_DIR}/libparquet.a")

  set(arrow_INCLUDE_DIR "${arrow_INSTALL_PREFIX}/include")
  # this include directory won't exist until the install step, but the
  # imported targets need it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${arrow_INCLUDE_DIR}")

  set(arrow_BYPRODUCTS ${arrow_LIBRARY})
  list(APPEND arrow_BYPRODUCTS ${parquet_LIBRARY})

  if(WITH_RADOSGW_ARROW_FLIGHT)
    set(arrow_flight_LIBRARY "${arrow_LIBRARY_DIR}/libarrow_flight.a")
    list(APPEND arrow_BYPRODUCTS ${arrow_flight_LIBRARY})
  endif(WITH_RADOSGW_ARROW_FLIGHT)

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE))
    set(install_cmd $(MAKE) install)
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR>)
    set(install_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target install)
  endif()

  # clear the DESTDIR environment variable from debian/rules,
  # because it messes with the internal install paths of arrow's bundled deps
  set(NO_DESTDIR_COMMAND ${CMAKE_COMMAND} -E env --unset=DESTDIR)

  ExternalProject_Add(arrow_ext
    SOURCE_DIR "${arrow_SOURCE_DIR}"
    CMAKE_ARGS ${arrow_CMAKE_ARGS}
    BINARY_DIR "${arrow_BINARY_DIR}"
    BUILD_COMMAND ${NO_DESTDIR_COMMAND} ${make_cmd}
    BUILD_BYPRODUCTS "${arrow_BYPRODUCTS}"
    INSTALL_COMMAND ${NO_DESTDIR_COMMAND} ${install_cmd}
    INSTALL_DIR "${arrow_INSTALL_PREFIX}"
    DEPENDS "${arrow_DEPENDS}"
    LIST_SEPARATOR !)

  add_library(Arrow::Arrow STATIC IMPORTED)
  add_dependencies(Arrow::Arrow arrow_ext)
  set_target_properties(Arrow::Arrow PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${arrow_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${arrow_INTERFACE_LINK_LIBRARIES}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${arrow_LIBRARY}")

  add_library(Arrow::Parquet STATIC IMPORTED)
  add_dependencies(Arrow::Parquet arrow_ext)
  target_link_libraries(Arrow::Parquet INTERFACE Arrow::Arrow)
  set_target_properties(Arrow::Parquet PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${parquet_LIBRARY}")

  if(WITH_RADOSGW_ARROW_FLIGHT)
    add_library(Arrow::Flight STATIC IMPORTED)
    add_dependencies(Arrow::Flight arrow_ext)
    target_link_libraries(Arrow::Flight INTERFACE Arrow::Arrow gRPC::grpc++)
    set_target_properties(Arrow::Flight PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${arrow_INCLUDE_DIR}" # flight is accessed via "arrow/flight"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${arrow_flight_LIBRARY}")
  endif(WITH_RADOSGW_ARROW_FLIGHT)
endfunction()
