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

# - Find libudev
# Find the udev library and includes
#
# UDEV_INCLUDE_DIR - where to find libudev.h, etc.
# UDEV_LIBRARIES - List of libraries when using udev.
# UDEV_FOUND - True if udev found.

find_path(UDEV_INCLUDE_DIR libudev.h)

set(UDEV_NAMES ${UDEV_NAMES} udev)
find_library(UDEV_LIBRARY NAMES ${UDEV_NAMES})

if(UDEV_INCLUDE_DIR AND UDEV_LIBRARY)
  set(UDEV_FOUND TRUE)
  set(UDEV_LIBRARIES ${UDEV_LIBRARY})
else()
  set(UDEV_FOUND FALSE)
  set(UDEV_LIBRARIES)
endif()

if(UDEV_FOUND)
  message(STATUS "Found libudev: ${UDEV_LIBRARY}")
else()
  message(STATUS "Not Found libudev: ${UDEV_LIBRARY}")
  if(UDEV_FIND_REQUIRED)
    message(STATUS "Looked for libudev named ${UDEV_NAMES}.")
    message(FATAL_ERROR "Could NOT find libudev")
  endif()
endif()

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(udev DEFAULT_MSG UDEV_LIBRARIES UDEV_INCLUDE_DIR)

mark_as_advanced(
  UDEV_LIBRARY
  UDEV_INCLUDE_DIR
)
