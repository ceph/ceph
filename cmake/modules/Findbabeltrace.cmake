# - Find Babeltrace
# This module accepts the following optional variables:
#    BABELTRACE_PATH_HINT   = A hint on BABELTRACE install path.
#
# This module defines the following variables:
#    BABELTRACE_FOUND       = Was Babeltrace found or not?
#    BABELTRACE_EXECUTABLE  = The path to lttng command
#    BABELTRACE_LIBRARIES   = The list of libraries to link to when using Babeltrace
#    BABELTRACE_INCLUDE_DIR = The path to Babeltrace include directory
#
# On can set BABELTRACE_PATH_HINT before using find_package(Babeltrace) and the
# module with use the PATH as a hint to find Babeltrace.
#
# The hint can be given on the command line too:
#   cmake -DBABELTRACE_PATH_HINT=/DATA/ERIC/Babeltrace /path/to/source

if(BABELTRACE_PATH_HINT)
  message(STATUS "FindBabeltrace: using PATH HINT: ${BABELTRACE_PATH_HINT}")
else()
  set(BABELTRACE_PATH_HINT)
endif()

#One can add his/her own builtin PATH.
#FILE(TO_CMAKE_PATH "/DATA/ERIC/Babeltrace" MYPATH)
#list(APPEND BABELTRACE_PATH_HINT ${MYPATH})

find_path(BABELTRACE_INCLUDE_DIR
          NAMES babeltrace/babeltrace.h babeltrace/ctf/events.h babeltrace/ctf/iterator.h
          PATHS ${BABELTRACE_PATH_HINT}
          PATH_SUFFIXES include
          DOC "The Babeltrace include headers")

find_path(BABELTRACE_LIBRARY_DIR
          NAMES libbabeltrace.so
          NAMES libbabeltrace-ctf.so
          PATHS ${BABELTRACE_PATH_HINT}
          PATH_SUFFIXES lib lib64
          DOC "The Babeltrace libraries")

find_library(BABELTRACE NAMES babeltrace babeltrace-ctf PATHS ${BABELTRACE_LIBRARY_DIR})

set(BABELTRACE_LIBRARIES ${BABELTRACE})

message(STATUS "Looking for Babeltrace...")
set(BABELTRACE_NAMES "babeltrace;babeltrace-ctf")
# FIND_PROGRAM twice using NO_DEFAULT_PATH on first shot
find_program(BABELTRACE_EXECUTABLE
  NAMES ${BABELTRACE_NAMES}
  PATHS ${BABELTRACE_PATH_HINT}/bin /bin
  NO_DEFAULT_PATH
  DOC "The BABELTRACE command line tool")

# handle the QUIETLY and REQUIRED arguments and set PRELUDE_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(BABELTRACE
                                  REQUIRED_VARS BABELTRACE_INCLUDE_DIR BABELTRACE_LIBRARY_DIR)
# VERSION FPHSA options not handled by CMake version < 2.8.2)
#                                  VERSION_VAR)
mark_as_advanced(BABELTRACE_INCLUDE_DIR)
mark_as_advanced(BABELTRACE_LIBRARY_DIR)
