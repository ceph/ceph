# Build libbpf from source

function(build_libbpf)
  include(ExternalProject)

  set(libbpf_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/tracing/CephBPF/libbpf")
  set(libbpf_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/libbpf")
  set(libbpf_INSTALL_DIR "${libbpf_BINARY_DIR}/install")
  set(libbpf_LIBRARY "${libbpf_INSTALL_DIR}/libbpf.a")

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    set(make_cmd $(MAKE))
  else()
    set(make_cmd make)
  endif()

  ExternalProject_Add(libbpf_ext
    SOURCE_DIR "${libbpf_SOURCE_DIR}/src"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ${make_cmd}
      CC=${CMAKE_C_COMPILER}
      BUILD_STATIC_ONLY=1
      OBJDIR=${libbpf_BINARY_DIR}/build
      DESTDIR=${libbpf_INSTALL_DIR}
      INCLUDEDIR= LIBDIR= UAPIDIR=
      install
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "${libbpf_LIBRARY}"
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # Create include directory before defining imported target
  file(MAKE_DIRECTORY "${libbpf_INSTALL_DIR}/bpf")
  file(MAKE_DIRECTORY "${libbpf_SOURCE_DIR}/include/uapi")

  add_library(libbpf::libbpf STATIC IMPORTED GLOBAL)
  add_dependencies(libbpf::libbpf libbpf_ext)
  set_target_properties(libbpf::libbpf PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${libbpf_INSTALL_DIR};${libbpf_SOURCE_DIR}/include/uapi"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${libbpf_LIBRARY}")

  # Export variables to parent scope
  set(LIBBPF_INCLUDE_DIR "${libbpf_INSTALL_DIR}" PARENT_SCOPE)
  set(LIBBPF_UAPI_INCLUDE_DIR "${libbpf_SOURCE_DIR}/include/uapi" PARENT_SCOPE)
  set(LIBBPF_SRC_DIR "${libbpf_SOURCE_DIR}/src" PARENT_SCOPE)
endfunction()
