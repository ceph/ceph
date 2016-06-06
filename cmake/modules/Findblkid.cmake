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

# - Find libblkid
# Find the blkid library and includes
#
# BLKID_INCLUDE_DIR - where to find blkid.h, etc.
# BLKID_LIBRARY - List of libraries when using blkid.
# BLKID_FOUND - TRUE if blkid found.

find_path(BLKID_INCLUDE_DIR blkid/blkid.h)

set(BLKID_NAMES ${BLKID_NAMES} blkid)
find_library(BLKID_LIBRARY NAMES ${BLKID_NAMES})

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(blkid DEFAULT_MSG BLKID_LIBRARY BLKID_INCLUDE_DIR)

mark_as_advanced(
  BLKID_LIBRARY
  BLKID_INCLUDE_DIR
)
