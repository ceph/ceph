set(CMAKE_SYSTEM_NAME Windows)
set(TOOLCHAIN_PREFIX x86_64-w64-mingw32)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

# We'll need to use posix threads in order to use
# C++11 features, such as std::thread.
set(CMAKE_C_COMPILER ${TOOLCHAIN_PREFIX}-gcc-posix)
set(CMAKE_CXX_COMPILER ${TOOLCHAIN_PREFIX}-g++-posix)
set(CMAKE_RC_COMPILER ${TOOLCHAIN_PREFIX}-windres)

set(CMAKE_FIND_ROOT_PATH /usr/${TOOLCHAIN_PREFIX} /usr/lib/gcc/${TOOLCHAIN_PREFIX}/7.3-posix)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# TODO: consider switching this to "ONLY". The issue with
# that is that all our libs should then be under
# CMAKE_FIND_ROOT_PATH and CMAKE_PREFIX_PATH would be ignored.
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)

# Some functions (e.g. localtime_r) will not be available unless we set
# the following flag.
add_definitions(-D_POSIX=1)
add_definitions(-D_POSIX_C_SOURCE=1)
add_definitions(-D_POSIX_=1)
add_definitions(-D_POSIX_THREADS=1)
