cmake_minimum_required(VERSION 2.8)

# set(CMAKE_MODULE_PATH
#   ${CMAKE_MODULE_PATH}
#  "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")

if(CMAKE_BUILD_TYPE MATCHES Release)
  set(my_compile_flags "-O3 -Wno-write-strings")
else()
  set(my_compile_flags "-g -O0 -Wno-write-strings")
endif()

add_subdirectory(src)
