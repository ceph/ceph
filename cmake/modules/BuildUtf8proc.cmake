# utf8proc is a dependency of the arrow submodule

function(build_utf8proc)
  # only build static version
  list(APPEND utf8proc_CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  string(REPLACE ";" "!" CMAKE_PREFIX_PATH_ALT_SEP "${CMAKE_PREFIX_PATH}")
  list(APPEND utf8proc_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND utf8proc_CMAKE_ARGS
         -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND utf8proc_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
  list(APPEND utf8proc_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND utf8proc_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

  set(utf8proc_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/utf8proc")
  set(utf8proc_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/utf8proc")

  set(utf8proc_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/utf8proc/install")
  list(APPEND utf8proc_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${utf8proc_INSTALL_PREFIX})

  # expose the install path as utf8proc_ROOT for find_package()
  set(utf8proc_ROOT "${utf8proc_INSTALL_PREFIX}" PARENT_SCOPE)

  set(utf8proc_INSTALL_LIBDIR "lib") # force lib so we don't have to guess between lib/lib64
  list(APPEND utf8proc_CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=${utf8proc_INSTALL_LIBDIR})
  set(utf8proc_LIBRARY_DIR "${utf8proc_INSTALL_PREFIX}/${utf8proc_INSTALL_LIBDIR}")

  set(utf8proc_LIBRARY "${utf8proc_LIBRARY_DIR}/libutf8proc.a")

  set(utf8proc_INCLUDE_DIR "${utf8proc_INSTALL_PREFIX}/include")
  # this include directory won't exist until the install step, but the
  # imported target needs it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${utf8proc_INCLUDE_DIR}")

  set(utf8proc_BYPRODUCTS ${utf8proc_LIBRARY})

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE))
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR>)
  endif()

  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  ExternalProject_Add(utf8proc_ext
    SOURCE_DIR "${utf8proc_SOURCE_DIR}"
    CMAKE_ARGS ${utf8proc_CMAKE_ARGS}
    BINARY_DIR "${utf8proc_BINARY_DIR}"
    BUILD_COMMAND "${make_cmd}"
    BUILD_BYPRODUCTS "${utf8proc_BYPRODUCTS}"
    INSTALL_DIR "${utf8proc_INSTALL_PREFIX}"
    DEPENDS "${utf8proc_DEPENDS}"
    LIST_SEPARATOR !)

  add_library(utf8proc::utf8proc STATIC IMPORTED)
  add_dependencies(utf8proc::utf8proc utf8proc_ext)
  set_target_properties(utf8proc::utf8proc PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${utf8proc_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${utf8proc_INTERFACE_LINK_LIBRARIES}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${utf8proc_LIBRARY}")
endfunction()
