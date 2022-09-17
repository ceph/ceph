find_package(PkgConfig QUIET)
pkg_search_module(PC_QUICHE quiche)

find_path(QUICHE_INCLUDE_DIR quiche.h
  HINTS ${PC_QUICHE_INCLUDEDIR} ${PC_QUICHE_INCLUDE_DIRS})
find_library(QUICHE_LIBRARY NAMES quiche
  HINTS ${PC_QUICHE_LIBDIR} ${PC_QUICHE_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Quiche
  REQUIRED_VARS QUICHE_LIBRARY QUICHE_INCLUDE_DIR)

if (Quiche_FOUND AND NOT TARGET Quiche::Quiche)
  add_library(Quiche::Quiche STATIC IMPORTED)
  set_target_properties(Quiche::Quiche PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${QUICHE_INCLUDE_DIR}"
    IMPORTED_LOCATION "${QUICHE_LIBRARY}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C")
endif()
