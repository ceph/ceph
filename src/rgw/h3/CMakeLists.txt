# this is a separate cmake project so Ceph can build it as an ExternalProject

cmake_minimum_required(VERSION 3.16)
project(h3)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(OpenSSL REQUIRED)
find_package(Quiche 0.16.0 REQUIRED)
find_package(Boost 1.79 REQUIRED COMPONENTS system)

add_library(rgw_h3_quiche_headers INTERFACE)
target_include_directories(rgw_h3_quiche_headers INTERFACE "include")
# XXX: only required for common/ceph_time.h
target_include_directories(rgw_h3_quiche_headers INTERFACE "../..")
# TODO: update for system fmt
target_include_directories(rgw_h3_quiche_headers INTERFACE "../../fmt/include")

add_subdirectory(src)
