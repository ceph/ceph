function(build_thrift)
  find_package(thrift)
  if(NOT ${thrift_FOUND})
    set(thrift_FOUND false PARENT_SCOPE)
    set(thrift_DOWNLOAD_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing")
    set(thrift_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/thrift")
    set(thrift_BINARY_DIR "${CMAKE_BINARY_DIR}/external/thrift")

    set(thrift_CMAKE_ARGS  -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DBUILD_JAVA=OFF
        -DBUILD_PYTHON=OFF
        -DBUILD_TESTING=OFF
            -DBUILD_TUTORIALS=OFF
            -DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external/lib
        -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/external
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib)

    if(EXISTS "/opt/ceph/include/boost/")
      message(STATUS "thrift will be using system boost")
      set(dependencies "")
      list(APPEND thrift_CMAKE_ARGS -DBOOST_ROOT=/opt/ceph)
      list(APPEND thrift_CMAKE_ARGS -DCMAKE_FIND_ROOT_PATH=/opt/ceph)
    else()
      message(STATUS "thrift will be using external build boost")
      set(dependencies Boost)
      list(APPEND thrift_CMAKE_ARGS  -DCMAKE_FIND_ROOT_PATH=${CMAKE_BINARY_DIR}/boost)
      list(APPEND thrift_CMAKE_ARGS  -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/external)
    endif()

    if(CMAKE_MAKE_PROGRAM MATCHES "make")
      # try to inherit command line arguments passed by parent "make" job
      set(make_cmd $(MAKE))
    else()
      set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target thrift)
    endif()

    set(install_cmd $(MAKE) install DESTDIR=)

    include(ExternalProject)
    ExternalProject_Add(thrift
      URL http://archive.apache.org/dist/thrift/0.11.0/thrift-0.11.0.tar.gz
      URL_HASH SHA1=bdf159ef455c6d3c71e95dba15a6d05f6aaca2a9
      INSTALL_DIR "${CMAKE_BINARY_DIR}/external"
      DOWNLOAD_DIR ${thrift_DOWNLOAD_DIR}
      SOURCE_DIR ${thrift_SOURCE_DIR}
      PREFIX "${CMAKE_BINARY_DIR}/external/thrift"
      CMAKE_ARGS ${thrift_CMAKE_ARGS}
      BINARY_DIR ${thrift_BINARY_DIR}
      BUILD_COMMAND ${make_cmd}
      INSTALL_COMMAND ${install_cmd}
      DEPENDS ${dependencies}
      )
  else()
    set(thrift_FOUND true PARENT_SCOPE)
    message(STATUS "PREINSTALLED thrift")
  endif()
endfunction()
