# This module builds Jaeger after it's dependencies are installed and discovered
# opentracing: is built using cmake/modules/Buildopentracing.cmake
# Thrift: build using cmake/modules/Buildthrift.cmake
# yaml-cpp, nlhomann-json: are installed locally and then discovered using
# Find<package>.cmake
# Boost Libraries used for building thrift are build and provided by
# cmake/modules/BuildBoost.cmake

function(build_jaeger)
  set(Jaeger_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/jaeger-client-cpp")
  set(Jaeger_INSTALL_DIR "${CMAKE_BINARY_DIR}/external")
  set(Jaeger_BINARY_DIR "${Jaeger_INSTALL_DIR}/Jaeger")

  file(MAKE_DIRECTORY "${Jaeger_INSTALL_DIR}")
  set(Jaeger_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON
			-DBUILD_SHARED_LIBS=ON
			-DHUNTER_ENABLED=OFF
			-DBUILD_TESTING=OFF
			-DJAEGERTRACING_BUILD_EXAMPLES=OFF
			-DCMAKE_PREFIX_PATH="${CMAKE_BINARY_DIR}/external;${CMAKE_BINARY_DIR}/boost"
			-DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external
			-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
			-DOpenTracing_DIR=${CMAKE_SOURCE_DIR}/src/jaegertracing/opentracing-cpp
			-Dnlohmann_json_DIR=/usr/lib
			-DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/boost\;${CMAKE_BINARY_DIR}/boost/include\;${CMAKE_BINARY_DIR}/external
                       -DCMAKE_FIND_ROOT_PATH=${CMAKE_BINARY_DIR}/boost\;${CMAKE_BINARY_DIR}/boost/include\;${CMAKE_BINARY_DIR}/external
			-DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib
			-Dthrift_HOME=${CMAKE_BINARY_DIR}/external
			-DOpenTracing_HOME=${CMAKE_BINARY_DIR}/external)

  set(dependencies opentracing thrift)
  include(BuildOpenTracing)
  build_opentracing()
  include(Buildthrift)
  build_thrift()
  if(NOT yaml-cpp_FOUND)
    include(Buildyaml-cpp)
    build_yamlcpp()
    add_library(yaml-cpp::yaml-cpp SHARED IMPORTED)
    add_dependencies(yaml-cpp::yaml-cpp yaml-cpp)
    set_library_properties_for_external_project(yaml-cpp::yaml-cpp yaml-cpp)
    list(APPEND dependencies "yaml-cpp")
  endif()

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE))
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --config $<CONFIG> --target Jaeger)
  endif()
  set(install_cmd $(MAKE) install DESTDIR=)

  include(ExternalProject)
  ExternalProject_Add(Jaeger
    SOURCE_DIR ${Jaeger_SOURCE_DIR}
    UPDATE_COMMAND ""
    INSTALL_DIR "external"
    PREFIX ${Jaeger_INSTALL_DIR}
    CMAKE_ARGS ${Jaeger_CMAKE_ARGS}
    BINARY_DIR ${Jaeger_BINARY_DIR}
    BUILD_COMMAND ${make_cmd}
    INSTALL_COMMAND ${install_cmd}
    DEPENDS "${dependencies}"
    )
endfunction()
