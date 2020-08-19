function(build_opentracing)
  find_package(OpenTracing)
  if(NOT ${OpenTracing_FOUND})
    set(OpenTracing_FOUND false PARENT_SCOPE)
    set(OpenTracing_DOWNLOAD_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing")
    set(OpenTracing_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/opentracing-cpp")
    set(OpenTracing_BINARY_DIR "${CMAKE_BINARY_DIR}/external/opentracing-cpp")

    set(OpenTracing_CMAKE_ARGS  -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                                -DBUILD_MOCKTRACER=OFF
                                -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/external
                                -DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external
              -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
                                -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib
                                -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/external)

    if(CMAKE_MAKE_PROGRAM MATCHES "make")
      # try to inherit command line arguments passed by parent "make" job
      set(make_cmd "$(MAKE)")
    else()
      set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target OpenTracing)
    endif()
    set(install_cmd $(MAKE) install DESTDIR=)

    include(ExternalProject)
    ExternalProject_Add(OpenTracing
      GIT_REPOSITORY "https://github.com/opentracing/opentracing-cpp.git"
      GIT_TAG "v1.5.0"
      UPDATE_COMMAND ""
      INSTALL_DIR "${CMAKE_BINARY_DIR}/external"
      DOWNLOAD_DIR ${OpenTracing_DOWNLOAD_DIR}
      SOURCE_DIR ${OpenTracing_SOURCE_DIR}
      PREFIX "${CMAKE_BINARY_DIR}/external/opentracing-cpp"
      CMAKE_ARGS ${OpenTracing_CMAKE_ARGS}
      BUILD_IN_SOURCE 1
      BUILD_COMMAND ${make_cmd}
      INSTALL_COMMAND ${install_cmd}
      )
  else()
    set(OpenTracing_FOUND true PARENT_SCOPE)
    message(STATUS "PREINSTALLED Opentracing")
  endif()
endfunction()
