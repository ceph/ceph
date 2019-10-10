find_path(rdkafka_INCLUDE_DIR
  NAMES librdkafka/rdkafka.h)

find_library(rdkafka_LIBRARY
  NAMES rdkafka)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(RDKafka DEFAULT_MSG
  rdkafka_INCLUDE_DIR
  rdkafka_LIBRARY)

if(RDKafka_FOUND AND NOT (TARGET RDKafka::RDKafka))
  add_library(RDKafka::RDKafka UNKNOWN IMPORTED)
  set_target_properties(RDKafka::RDKafka PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${rdkafka_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${rdkafka_LIBRARY}")
endif()
