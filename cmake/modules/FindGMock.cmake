find_path(GMock_INCLUDE_DIR NAMES gmock/gmock.h)
find_library(GMock_GMock_LIBRARY NAMES gmock)
find_library(GMock_Main_LIBRARY NAMES gmock_main)

find_package_handle_standard_args(GMock
  REQUIRED_VARS
    GMock_GMock_LIBRARY
    GMock_Main_LIBRARY
    GMock_INCLUDE_DIR)

if(GMock_FOUND)
  foreach(c GMock Main)
    if(NOT TARGET GMock::${c})
      add_library(GMock::${c} UNKNOWN IMPORTED)
      set_target_properties(GMock::${c} PROPERTIES
        IMPORTED_LOCATION "${GMock_${c}_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${GMock_INCLUDE_DIR}"
        IMPORTED_LINK_INTERFACE_LANGUAGES "CXX")
    endif()
  endforeach()
endif()
