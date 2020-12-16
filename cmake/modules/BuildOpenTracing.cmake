function(build_opentracing)
  set(opentracing_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/opentracing-cpp")
  set(opentracing_BINARY_DIR "${CMAKE_BINARY_DIR}/external/opentracing-cpp")

  set(opentracing_CMAKE_ARGS  -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                              -DBUILD_MOCKTRACER=OFF
                              -DENABLE_LINTING=OFF
                              -DBUILD_STATIC_LIBS=OFF
                              -DBUILD_TESTING=OFF
                              -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/external
                              -DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external
                              -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
                              -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib
                              -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/external)

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE) )
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target opentracing)
  endif()
  set(install_cmd $(MAKE) install DESTDIR=)

  include(ExternalProject)
  ExternalProject_Add(opentracing
    SOURCE_DIR ${opentracing_SOURCE_DIR}
    UPDATE_COMMAND ""
    INSTALL_DIR "external"
    PREFIX "external/opentracing-cpp"
    CMAKE_ARGS ${opentracing_CMAKE_ARGS}
    BUILD_IN_SOURCE 1
    BUILD_COMMAND ${make_cmd}
    INSTALL_COMMAND ${install_cmd}
    )
endfunction()
