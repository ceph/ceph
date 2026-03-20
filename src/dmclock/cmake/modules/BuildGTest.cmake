macro(_build_gtest gtest_root)
  include(ExternalProject)
  set(gtest_library ${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(gtest_main_library ${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(gmock_library ${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(gmock_main_library ${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX})

  ExternalProject_Add(googletest
    SOURCE_DIR ${gtest_root}
    CMAKE_ARGS -DBUILD_GMOCK=OFF -DBUILD_GTEST=ON
    BUILD_BYPRODUCTS
      <BINARY_DIR>/lib/${gtest_library}
      <BINARY_DIR>/lib/${gtest_main_library}
      <BINARY_DIR>/lib/${gmock_library}
      <BINARY_DIR>/lib/${gmock_main_library}
    INSTALL_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  ExternalProject_Get_Property(googletest source_dir)
  find_path(GTEST_INCLUDE_DIRS
    NAMES gtest/gtest.h
    PATHS ${source_dir}/include /usr/include)
  find_path(GMOCK_INCLUDE_DIRS
    NAMES gmock/gmock.h
    PATHS ${source_dir}/googlemock/include /usr/include)

  find_package(Threads REQUIRED)

  ExternalProject_Get_Property(googletest binary_dir)
  set(GTEST_LIBRARY_PATH ${binary_dir}/lib/${gtest_library})
  unset(gtest_library)
  set(GTEST_LIBRARY GTest::GTest)
  add_library(${GTEST_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GTEST_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIRS}"
    IMPORTED_LOCATION ${GTEST_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GTEST_LIBRARY} googletest)
  set(GTEST_LIBRARIES ${GTEST_LIBRARY})

  set(GTEST_MAIN_LIBRARY_PATH ${binary_dir}/lib/${gtest_main_library})
  unset(gtest_main_library)
  set(GTEST_MAIN_LIBRARY GTest::Main)
  add_library(${GTEST_MAIN_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GTEST_MAIN_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIRS}"
    IMPORTED_LOCATION ${GTEST_MAIN_LIBRARY_PATH}
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GTEST_MAIN_LIBRARY} googletest)

  set(GMOCK_LIBRARY_PATH ${binary_dir}/lib/${gmock_library})
  unset(gmock_library)
  set(GMOCK_LIBRARY GMock::GMock)
  add_library(${GMOCK_LIBRARY} STATIC IMPORTED)
  set_target_properties(${GMOCK_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GMOCK_INCLUDE_DIRS}"
    IMPORTED_LOCATION "${GMOCK_LIBRARY_PATH}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(${GMOCK_LIBRARY} googletest)

  set(GMOCK_MAIN_LIBRARY_PATH ${binary_dir}/lib/${gmock_main_library})
  unset(gmock_main_library)
  set(GMOCK_MAIN_LIBRARY GMock::Main)
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
