macro(_build_gtest gtest_root)
  include(ExternalProject)
  ExternalProject_Add(googletest
    SOURCE_DIR ${gtest_root}
    CMAKE_ARGS -DBUILD_GMOCK=OFF -DBUILD_GTEST=ON
    INSTALL_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  ExternalProject_Get_Property(googletest source_dir)
  set(GTEST_INCLUDE_DIRS ${source_dir}/googletest/include)
  set(GMOCK_INCLUDE_DIRS ${source_dir}/googlemock/include)

  find_package(Threads REQUIRED)

  ExternalProject_Get_Property(googletest binary_dir)
  set(GTEST_LIBRARY_PATH ${binary_dir}/googletest/${CMAKE_FIND_LIBRARY_PREFIXES}gtest.a)
  set(GTEST_LIBRARY gtest)
  add_library(${GTEST_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GTEST_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIRS}"
    IMPORTED_LOCATION ${GTEST_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GTEST_LIBRARY} googletest)
  set(GTEST_LIBRARIES ${GTEST_LIBRARY})

  set(GTEST_MAIN_LIBRARY_PATH ${binary_dir}/googletest/${CMAKE_FIND_LIBRARY_PREFIXES}gtest_main.a)
  set(GTEST_MAIN_LIBRARY gtest_main)
  add_library(${GTEST_MAIN_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GTEST_MAIN_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIRS}"
    IMPORTED_LOCATION ${GTEST_MAIN_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GTEST_MAIN_LIBRARY} googletest)

  set(GMOCK_LIBRARY_PATH ${binary_dir}/googlemock/${CMAKE_FIND_LIBRARY_PREFIXES}gmock.a)
  set(GMOCK_LIBRARY gmock)
  add_library(${GMOCK_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GMOCK_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GMOCK_INCLUDE_DIRS}"
    IMPORTED_LOCATION "${GMOCK_LIBRARY_PATH}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GMOCK_LIBRARY} googletest)

  set(GMOCK_MAIN_LIBRARY_PATH ${binary_dir}/googlemock/${CMAKE_FIND_LIBRARY_PREFIXES}gmock_main.a)
  set(GMOCK_MAIN_LIBRARY gmock_main)
  add_library(${GMOCK_MAIN_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GMOCK_MAIN_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GMOCK_INCLUDE_DIRS}"
    IMPORTED_LOCATION ${GMOCK_MAIN_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GMOCK_MAIN_LIBRARY} ${GTEST_LIBRARY})
endmacro()

find_path(GTEST_ROOT src/gtest.cc
  HINTS $ENV{GTEST_ROOT}
  PATHS /usr/src/googletest/googletest /usr/src/gtest)

if(EXISTS ${GTEST_ROOT})
  message(STATUS "Found googletest: ${GTEST_ROOT}")
  _build_gtest(${GTEST_ROOT})
else()
  message(SEND_ERROR "Could NOT find googletest")
endif()
