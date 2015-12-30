# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#

# - Find Snappy
# Find the snappy compression library and includes
#
# SNAPPY_INCLUDE_DIR - where to find snappy.h, etc.
# SNAPPY_LIBRARIES - List of libraries when using snappy.
# SNAPPY_FOUND - True if snappy found.

find_path(SNAPPY_INCLUDE_DIR snappy.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include
  /opt/local/include
  /usr/local/include
)

set(SNAPPY_NAMES ${SNAPPY_NAMES} snappy)
find_library(SNAPPY_LIBRARY NAMES ${SNAPPY_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /usr/local/lib
    /opt/local/lib
    /usr/lib
    )

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
  set(SNAPPY_FOUND TRUE)
  set( SNAPPY_LIBRARIES ${SNAPPY_LIBRARY} )
else ()
  set(SNAPPY_FOUND FALSE)
  set( SNAPPY_LIBRARIES )
endif ()

if (SNAPPY_FOUND)
  message(STATUS "Found Snappy: ${SNAPPY_LIBRARY}")
else ()
  message(STATUS "Not Found Snappy: ${SNAPPY_LIBRARY}")
  if (SNAPPY_FIND_REQUIRED)
    message(STATUS "Looked for Snappy libraries named ${SNAPPY_NAMES}.")
    message(FATAL_ERROR "Could NOT find Snappy library")
  endif ()
endif ()

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(snappy DEFAULT_MSG SNAPPY_LIBRARIES SNAPPY_INCLUDE_DIR)

mark_as_advanced(
  SNAPPY_LIBRARY
  SNAPPY_I
)
