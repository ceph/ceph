# - Find Jerasure and GF-Complete
# Find the jerasure and gf-complete libraries and includes
#
# Jerasure_INCLUDE_DIR - where to find jerasure.h
# Jerasure_SUBHEADER_DIR - where to find galois.h, cauchy.h, etc.
# Jerasure_LIBRARY - the jerasure library
# GFComplete_INCLUDE_DIR - where to find gf_complete.h
# GFComplete_LIBRARY - the gf-complete library
# Jerasure_FOUND - True if both jerasure and gf-complete found.

find_path(Jerasure_INCLUDE_DIR jerasure.h)

# jerasure sub-headers (galois.h, cauchy.h, etc.) may be installed in a
# jerasure/ subdirectory. The main jerasure.h includes them with bare
# #include "galois.h", so this directory must be on the include path.
find_path(Jerasure_SUBHEADER_DIR galois.h
  PATH_SUFFIXES jerasure)

find_library(Jerasure_LIBRARY NAMES Jerasure)

find_path(GFComplete_INCLUDE_DIR gf_complete.h)

find_library(GFComplete_LIBRARY NAMES gf_complete)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jerasure
  REQUIRED_VARS
    Jerasure_INCLUDE_DIR
    Jerasure_SUBHEADER_DIR
    Jerasure_LIBRARY
    GFComplete_INCLUDE_DIR
    GFComplete_LIBRARY)

if(Jerasure_FOUND)
  set(Jerasure_INCLUDE_DIRS
    ${Jerasure_INCLUDE_DIR}
    ${Jerasure_SUBHEADER_DIR}
    ${GFComplete_INCLUDE_DIR})
  set(Jerasure_LIBRARIES
    ${Jerasure_LIBRARY}
    ${GFComplete_LIBRARY})

  if(NOT TARGET Jerasure::jerasure)
    add_library(Jerasure::jerasure UNKNOWN IMPORTED GLOBAL)
    set_target_properties(Jerasure::jerasure PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${Jerasure_INCLUDE_DIR};${Jerasure_SUBHEADER_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${Jerasure_LIBRARY}"
      INTERFACE_LINK_LIBRARIES "${GFComplete_LIBRARY}")
  endif()

  if(NOT TARGET Jerasure::GFComplete)
    add_library(Jerasure::GFComplete UNKNOWN IMPORTED GLOBAL)
    set_target_properties(Jerasure::GFComplete PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${GFComplete_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${GFComplete_LIBRARY}")
  endif()
endif()

mark_as_advanced(
  Jerasure_INCLUDE_DIR
  Jerasure_SUBHEADER_DIR
  Jerasure_LIBRARY
  GFComplete_INCLUDE_DIR
  GFComplete_LIBRARY)
