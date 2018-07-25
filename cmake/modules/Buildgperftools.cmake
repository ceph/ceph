if(EXISTS ${CMAKE_SOURCE_DIR}/src/gperftools/configure)
  set(gperftools_SOURCE_DIR
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/gperftools)
else()
  set(gperftools_SOURCE_DIR
    URL https://github.com/gperftools/gperftools/releases/download/gperftools-2.7/gperftools-2.7.tar.gz
    URL_HASH SHA256=1ee8c8699a0eff6b6a203e59b43330536b22bbcbe6448f54c7091e5efb0763c9)
endif()

set(gperftools_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/gperftools)
include(ExternalProject)
# override the $DESTDIR specified by debian's dh-make, as it sets $DESTDIR
# environment variable globally, which instructs GNU automake to install the
# artifacts to the specified $DESTDIR instead of <INSTALL_DIR>, where the
# headers and libraries are expected. if $DESTDIR is specified, the artifacts
# will be installed into ${DESTDIR}/${prefix}. and the default ${prefix} is
# /user/local, so pass an empty string to "configure"
ExternalProject_Add(gperftools_ext
  ${gperftools_SOURCE_DIR}
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --disable-libunwind --disable-stacktrace-via-backtrace --enable-frame-pointers --prefix= CXXFLAGS=-fPIC
  BUILD_COMMAND $(MAKE)
  INSTALL_DIR ${gperftools_ROOT_DIR}
  INSTALL_COMMAND $(MAKE) install DESTDIR=<INSTALL_DIR>)

# create the directory so cmake won't complain when looking at the imported
# target
file(MAKE_DIRECTORY ${gperftools_ROOT_DIR}/include)

foreach(component tcmalloc tcmalloc_minimal profiler)
  add_library(gperftools::${component} STATIC IMPORTED)
  set_target_properties(gperftools::${component} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${gperftools_ROOT_DIR}/include
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION ${gperftools_ROOT_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${component}${CMAKE_STATIC_LIBRARY_SUFFIX})
  add_dependencies(gperftools::${component} gperftools_ext)
endforeach()

find_package(Threads)
foreach(component tcmalloc profiler)
  set_target_properties(gperftools::${component} PROPERTIES
    INTERFACE_LINK_LIBRARIES "Threads::Threads")
endforeach()
