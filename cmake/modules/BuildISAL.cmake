# use an ExternalProject to build isa-l using its makefile
function(build_isal)
  set(isal_BINARY_DIR ${CMAKE_BINARY_DIR}/src/isa-l)
  set(isal_INSTALL_DIR ${isal_BINARY_DIR}/install)
  set(isal_INCLUDE_DIR "${isal_INSTALL_DIR}/include")
  set(isal_LIBRARY "${isal_INSTALL_DIR}/lib/libisal.a")

  # this include directory won't exist until the install step, but the
  # imported targets need it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${isal_INCLUDE_DIR}")

  set(configure_cmd env CC=${CMAKE_C_COMPILER} ./configure --prefix=${isal_INSTALL_DIR})
  # build a static library with -fPIC that we can link into crypto/compressor plugins
  list(APPEND configure_cmd --with-pic --enable-static --disable-shared)

  # clear the DESTDIR environment variable from debian/rules,
  # because it messes with the internal install paths of arrow's bundled deps
  set(NO_DESTDIR_COMMAND ${CMAKE_COMMAND} -E env --unset=DESTDIR)

  if(CMAKE_C_COMPILER_ID MATCHES "Clang" AND HAVE_ARMV8_SIMD)
    list(APPEND configure_cmd CFLAGS=-no-integrated-as)
  endif()

  include(ExternalProject)
  ExternalProject_Add(isal_ext
    SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/isa-l"
    CONFIGURE_COMMAND ./autogen.sh COMMAND ${configure_cmd}
    BUILD_COMMAND ${NO_DESTDIR_COMMAND} make -j3
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${isal_LIBRARY}
    INSTALL_COMMAND ${NO_DESTDIR_COMMAND} make install
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # add imported library target ISAL::Crypto
  add_library(ISAL::ISAL STATIC IMPORTED GLOBAL)
  add_dependencies(ISAL::ISAL isal_ext)
  set_target_properties(ISAL::ISAL PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${isal_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION ${isal_LIBRARY})
endfunction()
