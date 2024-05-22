function(build_qatzip)
  set(QATzip_BINARY_DIR ${CMAKE_BINARY_DIR}/src/qatzip)
  set(QATzip_INSTALL_DIR ${QATzip_BINARY_DIR}/install)
  set(QATzip_INCLUDE_DIR ${QATzip_INSTALL_DIR}/include)
  set(QATzip_LIBRARY ${QATzip_INSTALL_DIR}/lib/libqatzip.a)

  # this include directory won't exist until the install step, but the
  # imported targets need it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${QATzip_INCLUDE_DIR}")

  set(configure_cmd env CC=${CMAKE_C_COMPILER} ./configure --prefix=${QATzip_INSTALL_DIR})
  # build a static library with -fPIC that we can link into crypto/compressor plugins
  list(APPEND configure_cmd --with-pic --enable-static --disable-shared)
  if(QATDRV_INCLUDE_DIR)
    list(APPEND configure_cmd --with-ICP_ROOT=${QATDRV_INCLUDE_DIR})
  endif()
  if(QAT_INCLUDE_DIR)
    list(APPEND configure_cmd CFLAGS=-I${QAT_INCLUDE_DIR})
  endif()
  if(QAT_LIBRARY_DIR)
    list(APPEND configure_cmd LDFLAGS=-L${QAT_LIBRARY_DIR})
  endif()

  # clear the DESTDIR environment variable from debian/rules,
  # because it messes with the internal install paths of arrow's bundled deps
  set(NO_DESTDIR_COMMAND ${CMAKE_COMMAND} -E env --unset=DESTDIR)

  include(ExternalProject)
  ExternalProject_Add(qatzip_ext
    SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/qatzip"
    CONFIGURE_COMMAND ./autogen.sh COMMAND ${configure_cmd}
    BUILD_COMMAND ${NO_DESTDIR_COMMAND} make -j3
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${QATzip_LIBRARY}
    INSTALL_COMMAND ${NO_DESTDIR_COMMAND} make install
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # export vars for find_package(QATzip)
  set(QATzip_LIBRARIES ${QATzip_LIBRARY} PARENT_SCOPE)
  set(QATzip_INCLUDE_DIR ${QATzip_INCLUDE_DIR} PARENT_SCOPE)
  set(QATzip_INTERFACE_LINK_LIBRARIES QAT::qat QAT::usdm LZ4::LZ4 PARENT_SCOPE)
endfunction()
