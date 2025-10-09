# libzstd - build it statically
function(build_Zstd)
  set(ZSTD_C_FLAGS "-fPIC -Wno-unused-variable -O3")

  include(ExternalProject)
  ExternalProject_Add(zstd_ext
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/zstd/build/cmake
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
               -DCMAKE_C_FLAGS=${ZSTD_C_FLAGS}
               -DCMAKE_AR=${CMAKE_AR}
               -DCMAKE_POSITION_INDEPENDENT_CODE=${ENABLE_SHARED}
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/libzstd
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target libzstd_static
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/libzstd/lib/libzstd.a"
    INSTALL_COMMAND "")
  add_library(Zstd::Zstd STATIC IMPORTED)
  set_target_properties(Zstd::Zstd PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src/zstd/lib"
    IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/libzstd/lib/libzstd.a")
  add_dependencies(Zstd::Zstd zstd_ext)
endfunction()
