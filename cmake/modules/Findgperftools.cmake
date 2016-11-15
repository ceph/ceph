# Try to find gperftools
# Once done, this will define
#
# GPERFTOOLS_FOUND - system has Profiler
# GPERFTOOLS_INCLUDE_DIR - the Profiler include directories
# Tcmalloc_INCLUDE_DIR - where to find Tcmalloc.h
# GPERFTOOLS_TCMALLOC_LIBRARY - link it to use tcmalloc
# GPERFTOOLS_TCMALLOC_MINIMAL_LIBRARY - link it to use tcmalloc_minimal
# GPERFTOOLS_PROFILER_LIBRARY - link it to use Profiler

find_path(GPERFTOOLS_INCLUDE_DIR gperftools/profiler.h)
find_path(Tcmalloc_INCLUDE_DIR gperftools/tcmalloc.h)

foreach(component tcmalloc tcmalloc_minimal profiler)
  string(TOUPPER ${component} COMPONENT)
  find_library(GPERFTOOLS_${COMPONENT}_LIBRARY ${component})
  list(APPEND GPERFTOOLS_LIBRARIES GPERFTOOLS_${COMPONENT}_LIBRARY)
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gperftools DEFAULT_MSG GPERFTOOLS_LIBRARIES GPERFTOOLS_INCLUDE_DIR)

mark_as_advanced(GPERFTOOLS_LIBRARIES GPERFTOOLS_INCLUDE_DIR)
