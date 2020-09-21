find_package(PkgConfig QUIET)

pkg_search_module(PC_rdkafka
  rdkafka)

find_path(rdkafka_INCLUDE_DIR
  NAMES librdkafka/rdkafka.h
  PATHS ${PC_rdkafka_INCLUDE_DIRS})

find_library(rdkafka_LIBRARY
  NAMES rdkafka
  PATHS ${PC_rdkafka_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKafka
  REQUIRED_VARS rdkafka_INCLUDE_DIR rdkafka_LIBRARY
  VERSION_VAR PC_rdkafka_VERSION)

if(RDKafka_FOUND)
  set(RDKafka_VERSION ${PC_rdkafka_VERSION})
  string(REPLACE "." ";" version_list ${PC_rdkafka_VERSION})
  list(GET version_list 0 RDKafka_VERSION_MAJOR)
  list(GET version_list 1 RDKafka_VERSION_MINOR)
  list(GET version_list 2 RDKafka_VERSION_PATCH)

  if(NOT TARGET RDKafka::RDKafka)
    add_library(RDKafka::RDKafka UNKNOWN IMPORTED)
    set_target_properties(RDKafka::RDKafka PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${rdkafka_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${rdkafka_LIBRARY}")
  endif()
endif()
