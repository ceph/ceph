# This module builds Jaeger after it's dependencies are installed and discovered
# opentracing: is built using cmake/modules/Buildopentracing.cmake
# Thrift: build using cmake/modules/Buildthrift.cmake
# yaml-cpp, nlhomann-json: are installed locally and then discovered using
# Find<package>.cmake
# Boost Libraries used for building thrift are build and provided by
# cmake/modules/BuildBoost.cmake
include(BuildOpenTracing)

# will do all linking and path setting
function (set_library_properties_for_external_project _target _lib)
  # Manually create the directory, it will be created as part of the build,
  # but this runs in the configuration phase, and CMake generates an error if
  # we add an include directory that does not exist yet.
  set(_libfullname "${CMAKE_SHARED_LIBRARY_PREFIX}${_lib}${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(_libpath "${CMAKE_BINARY_DIR}/external/lib/${_libfullname}")
  set(_includepath "${CMAKE_BINARY_DIR}/external/include")
  message(STATUS "Configuring ${_target} with ${_libpath}")
  add_library(${_target} SHARED IMPORTED)
  add_dependencies(${_target} opentracing)

  file(MAKE_DIRECTORY "${_includepath}")
  set_target_properties(${_target} PROPERTIES
    INTERFACE_LINK_LIBRARIES "${_libpath}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${_libpath}"
    INTERFACE_INCLUDE_DIRECTORIES "${_includepath}")
endfunction ()

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
			-DCMAKE_FIND_ROOT_PATH=${CMAKE_BINARY_DIR}/external\;${CMAKE_BINARY_DIR}/boost\;${CMAKE_BINARY_DIR}/boost/include
			-DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/boost\;${CMAKE_BINARY_DIR}/boost/include\;${CMAKE_BINARY_DIR}/external
			-DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib
			-DBOOST_INCLUDEDIR=${CMAKE_BINARY_DIR}/boost/include
			-Dthrift_HOME=${CMAKE_BINARY_DIR}/external
			-DOpenTracing_HOME=${CMAKE_BINARY_DIR}/external)

  # build these libraries along with jaeger
  set(dependencies opentracing)
  include(BuildOpenTracing)
  build_opentracing()
  find_package(thrift 0.13.0)
  if(NOT thrift_FOUND)
    include(Buildthrift)
    build_thrift()
    list(APPEND dependencies thrift)
    execute_process(COMMAND bash -c "grep -q 'thrift' debian/libjaeger.install || echo 'usr/lib/libthrift.so.*' >> debian/libjaeger.install"
		    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  endif()

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE) jaegertracing)
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target jaegertracing)
  endif()
  set(install_cmd ${CMAKE_MAKE_PROGRAM} install)

  include(ExternalProject)
  ExternalProject_Add(jaegertracing
    SOURCE_DIR ${Jaeger_SOURCE_DIR}
    UPDATE_COMMAND ""
    INSTALL_DIR "external"
    PREFIX ${Jaeger_INSTALL_DIR}
    CMAKE_ARGS ${Jaeger_CMAKE_ARGS}
    BINARY_DIR ${Jaeger_BINARY_DIR}
    BUILD_COMMAND ${make_cmd}
    INSTALL_COMMAND ${install_cmd}
    DEPENDS "${dependencies}"
    BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/external/lib/libjaegertracing.so
    )
endfunction()

build_jaeger()
set_library_properties_for_external_project(opentracing::libopentracing
  opentracing)
set_library_properties_for_external_project(jaegertracing::libjaegertracing
jaegertracing)
if(NOT thrift_FOUND)
set_library_properties_for_external_project(thrift::libthrift thrift)
set(jaeger_base ${jaeger_base} thrift::libthrift PARENT_SCOPE)
endif()
