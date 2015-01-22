# - Find fio
#
# FIO_INCLUDE_DIR - where to find fio.h
# FIO_FOUND - True if found.

find_path(FIO_INCLUDE_DIR fio.h NO_DEFAULT_PATH PATHS
  /usr/include
  /opt/local/include
  /usr/local/include
)

if (FIO_INCLUDE_DIR)
  set(FIO_FOUND TRUE)
else ()
  set(FIO_FOUND FALSE)
endif ()

if (FIO_FOUND)
  message(STATUS "Found fio: ${FIO_INCLUDE_DIR}")
else ()
  message(STATUS "Failed to find fio.h")
  if (FIO_FIND_REQUIRED)
    message(FATAL_ERROR "Missing required fio.h")
  endif ()
endif ()

mark_as_advanced(
  FIO_INCLUDE_DIR
)
