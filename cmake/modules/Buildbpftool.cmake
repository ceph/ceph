# Build bpftool from source
function(build_bpftool)
  include(ExternalProject)

  set(bpftool_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/tracing/CephBPF/bpftool")
  set(bpftool_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/bpftool")
  set(bpftool_EXECUTABLE "${bpftool_BINARY_DIR}/bootstrap/bpftool")

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    set(make_cmd $(MAKE))
  else()
    set(make_cmd make)
  endif()

  ExternalProject_Add(bpftool_ext
    SOURCE_DIR "${bpftool_SOURCE_DIR}/src"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ${make_cmd}
      OUTPUT=${bpftool_BINARY_DIR}/
      bootstrap
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "${bpftool_EXECUTABLE}"
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # Export variable to parent scope
  set(BPFTOOL_EXECUTABLE "${bpftool_EXECUTABLE}" PARENT_SCOPE)
endfunction()
