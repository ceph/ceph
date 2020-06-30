# - Find Babeltrace
# This module defines the following variables:
#    BABELTRACE_FOUND       = Was Babeltrace found or not?
#    BABELTRACE_EXECUTABLE  = The path to lttng command
#    BABELTRACE_LIBRARIES   = The list of libraries to link to when using Babeltrace
#    BABELTRACE_INCLUDE_DIR = The path to Babeltrace include directory
#

find_path(BABELTRACE_INCLUDE_DIR
  NAMES babeltrace/babeltrace.h babeltrace/ctf/events.h babeltrace/ctf/iterator.h)

find_library(BABELTRACE_LIBRARY
  NAMES babeltrace babeltrace-ctf)

find_program(BABELTRACE_EXECUTABLE
  NAMES babeltrace babeltrace-ctf)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(babeltrace DEFAULT_MSG
  BABELTRACE_INCLUDE_DIR BABELTRACE_LIBRARY)
set(BABELTRACE_LIBRARIES ${BABELTRACE_LIBRARY})
mark_as_advanced(BABELTRACE_INCLUDE_DIR BABELTRACE_LIBRARY)
