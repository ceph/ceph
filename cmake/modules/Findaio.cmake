# - Find AIO
#
# AIO_INCLUDE - Where to find AIO/aio.h
# AIO_LIBS - List of libraries when using AIO.
# AIO_FOUND - True if AIO found.

get_filename_component(module_file_path ${CMAKE_CURRENT_LIST_FILE} PATH)

# Look for the header file.
find_path(AIO_INCLUDE 
NAMES aio.h 
PATHS /usr/include  $ENV{AIO_ROOT}/include /opt/local/include /usr/local/include 
DOC "Path in which the file AIO/aio.h is located." )

mark_as_advanced(AIO_INCLUDE)

# Look for the library.
# Does this work on UNIX systems? (LINUX)
find_library(AIO_LIBS 
NAMES aio
PATHS /usr/lib /usr/lib/x86_64-linux-gnu $ENV{AIO_ROOT}/lib 
DOC "Path to AIO library.")

mark_as_advanced(AIO_LIBS)

# Copy the results to the output variables.
if (AIO_INCLUDE AND AIO_LIBS)
  message(STATUS "Found AIO in ${AIO_INCLUDE} ${AIO_LIBS}")
  set(AIO_FOUND 1)
  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_LIBRARY ${AIO_LIBS} pthread)
  set(CMAKE_REQUIRED_INCLUDES ${AIO_INCLUDE})
 else ()
   set(AIO_FOUND 0)
 endif ()

 # Report the results.
 if (NOT AIO_FOUND)
   set(AIO_DIR_MESSAGE "AIO was not found. Make sure AIO_LIBS and AIO_INCLUDE are set.")
   if (AIO_FIND_REQUIRED)
     message(FATAL_ERROR "${AIO_DIR_MESSAGE}")
   elseif (NOT AIO_FIND_QUIETLY)
     message(STATUS "${AIO_DIR_MESSAGE}")
   endif ()
 endif ()

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(aio DEFAULT_MSG AIO_LIBS AIO_INCLUDE)
