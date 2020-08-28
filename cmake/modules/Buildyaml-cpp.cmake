###############################################################################
#Ceph - scalable distributed file system
#
#Copyright (C) 2020 Red Hat Inc.
#
#This is free software; you can redistribute it and/or
#modify it under the terms of the GNU Lesser General Public
#License version 2.1, as published by the Free Software
#Foundation.  See file COPYING.
################################################################################

function(build_yamlcpp)
  set(yaml-cpp_DOWNLOAD_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing")
  set(yaml-cpp_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/yaml-cpp")
  set(yaml-cpp_BINARY_DIR "${CMAKE_BINARY_DIR}/external/yaml-cpp")

  set(yaml-cpp_CMAKE_ARGS -DBUILD_SHARED_LIBS=ON
		          -DYAML_CPP_BUILD_TESTS=OFF
			  -DYAML_CPP_BUILD_CONTRIB=OFF
        		  -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/external
        		  -DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external/lib
			  -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
			  -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib
			  -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/external)

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd "$(MAKE)")
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target yaml-cpp)
  endif()
set(install_cmd $(MAKE) install DESTDIR=)

  include(ExternalProject)
  ExternalProject_Add(yaml-cpp
    GIT_REPOSITORY "https://github.com/jbeder/yaml-cpp.git"
    GIT_TAG "yaml-cpp-0.6.2"
    UPDATE_COMMAND ""
    INSTALL_DIR "${CMAKE_BINARY_DIR}/external"
    DOWNLOAD_DIR ${yaml-cpp_DOWNLOAD_DIR}
    SOURCE_DIR ${yaml-cpp_SOURCE_DIR}
    PREFIX "${CMAKE_BINARY_DIR}/external/yaml-cpp"
    CMAKE_ARGS ${yaml-cpp_CMAKE_ARGS}
    BUILD_COMMAND ${make_cmd}
    INSTALL_COMMAND ${install_cmd}
    )
endfunction()