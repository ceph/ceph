#need to update cmake version here
cmake_minimum_required(VERSION 3.14.0)
project(dbstore)

option(USE_SQLITE "Enable SQLITE DB" ON)

set (dbstore_include_dirs
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/common")

IF(USE_SQLITE)
  add_subdirectory(sqlite)
  list(APPEND dbstore_include_dirs "${CMAKE_CURRENT_SOURCE_DIR}/sqlite")
  add_compile_definitions(SQLITE_ENABLED=1)
ENDIF()

set(dbstore_srcs
    common/dbstore_log.h
    common/dbstore.h
    common/dbstore.cc
    config/store.cc)
IF(USE_SQLITE)
  list(APPEND dbstore_srcs
      config/sqlite.cc
      sqlite/connection.cc
      sqlite/error.cc
      sqlite/statement.cc)
endif()

set(dbstore_mgr_srcs
    dbstore_mgr.h
    dbstore_mgr.cc
    )

add_library(dbstore_lib ${dbstore_srcs})
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(dbstore_lib PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()
target_include_directories(dbstore_lib
    PUBLIC
    ${dbstore_include_dirs}
    "${CMAKE_SOURCE_DIR}/src/rgw"
    "${CMAKE_SOURCE_DIR}/src/s3select/rapidjson/include"
    PRIVATE
    "${LUA_INCLUDE_DIR}")

set(link_targets Boost::context)
if(WITH_JAEGER)
  list(APPEND link_targets jaeger_base)
endif()

target_link_libraries(dbstore_lib PUBLIC ${link_targets})

set (dbstore_libs dbstore_lib)

IF(USE_SQLITE)
  list(APPEND dbstore_libs
    rgw_common
    sqlite_db)
ENDIF()

# add pthread library
list(APPEND dbstore_libs pthread)

find_package(gtest QUIET)
if(WITH_TESTS)
    add_subdirectory(tests)
else()
	message(WARNING "Gtest not enabled")
endif()

add_library(dbstore STATIC ${dbstore_mgr_srcs})
target_link_libraries(dbstore ${dbstore_libs})
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(dbstore PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()

# testing purpose
set(dbstore_main_srcs
    dbstore_main.cc)

list(APPEND dbstore_libs dbstore)
add_executable(dbstore-bin ${dbstore_main_srcs})
add_dependencies(dbstore-bin dbstore)
target_link_libraries(dbstore-bin ${dbstore_libs})
if(COMPILER_SUPPORTS_VLA_ERROR)
  target_compile_options(dbstore-bin PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-Werror=vla>)
endif()
