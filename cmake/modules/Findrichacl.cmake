# - Find richacl
#
# RICHACL_FOUND - True if found.
# RICHACL_INCLUDE_DIR - where to find sys/richacl.h
# RICHACL_LIBS - link to these to use richacl

find_path(RICHACL_INCLUDE_DIR sys/richacl.h)
find_library(RICHACL_LIBRARY NAMES richacl)

if (RICHACL_INCLUDE_DIR AND RICHACL_LIBRARY)
  set(RICHACL_FOUND TRUE)
  set(RICHACL_LIBS ${RICHACL_LIBRARY})
else ()
  set(RICHACL_FOUND FALSE)
  set(RICHACL_LIBS})
endif ()

if (RICHACL_FOUND)
  message(STATUS "Found richacl: ${RICHACL_LIBS}")
else ()
  message(STATUS "Failed to find librichacl")
  if (RICHACL_FIND_REQUIRED)
    message(FATAL_ERROR "Missing required librichacl")
  endif ()
endif ()

# handle the QUIETLY and REQUIRED arguments and set RICHACL_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(richacl DEFAULT_MSG RICHACL_INCLUDE_DIR RICHACL_LIBS)

mark_as_advanced(RICHACL_INCLUDE_DIR RICHACL_LIBS)
