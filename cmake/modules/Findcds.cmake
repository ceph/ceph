# - Find cds
#
# CDS_INCLUDE_DIR - where to find cds/init.h
# FIO_FOUND - True if found.

find_path(CDS_INC_DIR cds/init.h NO_DEFAULT_PATH PATHS
  /usr/include
  /opt/local/include
  /usr/local/include
  /opt/cds
)

if (CDS_INC_DIR)
  set(CDS_FOUND TRUE)
else ()
  set(CDS_FOUND FALSE)
endif ()

if (CDS_FOUND)
  message(STATUS "Found cds: ${CDS_INC_DIR}")
else ()
  message(STATUS "Failed to find cds/init.h")
  if (CDS_FIND_REQUIRED)
    message(FATAL_ERROR "Missing required cds/init.h")
  endif ()
endif ()

find_library(CDS_LIBS 
  NAMES cds
  PATHS /usr/lib /usr/lib/x86_64-linux-gnu /opt/cds/bin/gcc-amd64-linux-64
)

mark_as_advanced(
  CDS_INC_DIR
  CDS_LIBS
)

