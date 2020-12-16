function(build_thrift)
  set(thrift_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/thrift")
  set(thrift_BINARY_DIR "${CMAKE_BINARY_DIR}/external/thrift")

  set(thrift_CMAKE_ARGS  -DCMAKE_BUILD_TYPE=Release
			 -DCMAKE_POSITION_INDEPENDENT_CODE=ON
			 -DBUILD_JAVA=OFF
			 -DBUILD_PYTHON=OFF
			 -DBUILD_TESTING=OFF
			 -DBUILD_TUTORIALS=OFF
			 -DBUILD_C_GLIB=OFF
			 -DBUILD_HASKELL=OFF
			 -DWITH_LIBEVENT=OFF
			 -DWITH_ZLIB=OFF
			 -DBoost_INCLUDE_DIRS=${CMAKE_BINARY_DIR}/boost/include
			 -DCMAKE_INSTALL_PREFIX="${CMAKE_BINARY_DIR}/boost;${CMAKE_BINARY_DIR}/boost/include;${CMAKE_BINARY_DIR}/external"
			 -DCMAKE_FIND_ROOT_PATH="${CMAKE_BINARY_DIR}/boost;${CMAKE_BINARY_DIR}/boost/include;${CMAKE_BINARY_DIR}/external"
			 -DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external/lib
			 -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
			 -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/external
			 -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib)

  if(WITH_SYSTEM_BOOST)
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
    SOURCE_DIR ${thrift_SOURCE_DIR}
    PREFIX "${CMAKE_BINARY_DIR}/external/thrift"
    CMAKE_ARGS ${thrift_CMAKE_ARGS}
    BINARY_DIR ${thrift_BINARY_DIR}
    BUILD_COMMAND ${make_cmd}
    INSTALL_COMMAND ${install_cmd}
    DEPENDS ${dependencies}
    )
endfunction()
