function(build_ubbd)
  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  include(ExternalProject)
  ExternalProject_Add(ubbd_ext
    UPDATE_COMMAND "" # this disables rebuild on each run
    GIT_REPOSITORY "https://github.com/DataTravelGuide/ubbd.git"
    GIT_CONFIG advice.detachedHead=false
    GIT_SHALLOW 1
    GIT_TAG "v0.1.4"
    SOURCE_DIR "${CMAKE_BINARY_DIR}/src/ubbd"
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ${make_cmd} libubbd
    INSTALL_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)
endfunction()
