function(build_c_ares)
  include(ExternalProject)
  set(C-ARES_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/c-ares")
  set(C-ARES_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/c-ares")
  ExternalProject_Add(c-ares_ext
    SOURCE_DIR "${C-ARES_SOURCE_DIR}"
    CMAKE_ARGS
     -DCARES_STATIC=ON
     -DCARES_SHARED=OFF
     -DCARES_INSTALL=OFF
    BINARY_DIR "${C-ARES_BINARY_DIR}"
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
    INSTALL_COMMAND "")
  add_library(c-ares::cares STATIC IMPORTED)
  add_dependencies(c-ares::cares c-ares_ext)
  set_target_properties(c-ares::cares PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${C-ARES_SOURCE_DIR};${C-ARES_BINARY_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${C-ARES_BINARY_DIR}/lib/libcares.a")
  # to appease find_package()
  add_custom_target(c-ares DEPENDS c-ares::cares)
  # to be compatible with old Seastar
  add_library(c-ares::c-ares ALIAS c-ares::cares)
endfunction()
