function(build_nlohmann_json)
  # find_package(nlohmann_json)
  set(nlohmann_json_FOUND false)
  if(NOT ${nlohmann_json_FOUND})
    message(STATUS "build nlohmann_json")
    set(nlohmann_json_FOUND false PARENT_SCOPE)
    set(nlohmann_json_DOWNLOAD_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing")
    set(nlohmann_json_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/jaegertracing/nlohmann_json")
    set(nlohmann_json_BINARY_DIR "${CMAKE_BINARY_DIR}/external/nlohmann_json")
    set(nlohmann_json_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                                  -DBUILD_MOCKTRACER=OFF
                                  -DBUILD_SHARED_LIBS=ON
                                  -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/external
                                  -DCMAKE_INSTALL_RPATH=${CMAKE_BINARY_DIR}/external
                -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE
                                  -DCMAKE_INSTALL_LIBDIR=${CMAKE_BINARY_DIR}/external/lib
                                  -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/external)

    if(CMAKE_MAKE_PROGRAM MATCHES "make")
      message(STATUS "MATCHES NOHLMANN_JSON")
      # try to inherit command line arguments passed by parent "make" job
      set(make_cmd "$(MAKE)")
    else()
      set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> --target nlohmann_json)
    endif()
    set(install_cmd $(MAKE) install DESTDIR=)

    include(ExternalProject)
    ExternalProject_Add(nlohmann_json
      GIT_REPOSITORY "https://github.com/nlohmann/json.git"
      GIT_TAG "v3.7.2"
      UPDATE_COMMAND ""
      INSTALL_DIR "${CMAKE_BINARY_DIR}/external"
      DOWNLOAD_DIR ${nlohmann_json_DOWNLOAD_DIR}
      SOURCE_DIR ${nlohmann_json_SOURCE_DIR}
      PREFIX "${CMAKE_BINARY_DIR}/external/nlohmann_json"
      CMAKE_ARGS ${nlohmann_json_CMAKE_ARGS}
      BUILD_IN_SOURCE 1
      BUILD_COMMAND ${make_cmd}
      INSTALL_COMMAND ${install_cmd}
      )
  else()
    set(nlohmann_json_FOUND true PARENT_SCOPE)
    message(STATUS "PREINSTALLED nlohmann_json_FOUND")
  endif()
endfunction()
