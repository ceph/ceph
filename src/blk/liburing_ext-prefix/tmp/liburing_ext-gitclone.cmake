
if(NOT "/home/sh3ll/ceph/ceph/src/blk/liburing_ext-prefix/src/liburing_ext-stamp/liburing_ext-gitinfo.txt" IS_NEWER_THAN "/home/sh3ll/ceph/ceph/src/blk/liburing_ext-prefix/src/liburing_ext-stamp/liburing_ext-gitclone-lastrun.txt")
  message(STATUS "Avoiding repeated git clone, stamp file is up to date: '/home/sh3ll/ceph/ceph/src/blk/liburing_ext-prefix/src/liburing_ext-stamp/liburing_ext-gitclone-lastrun.txt'")
  return()
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} -E remove_directory "/home/sh3ll/ceph/ceph/src/liburing"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to remove directory: '/home/sh3ll/ceph/ceph/src/liburing'")
endif()

# try the clone 3 times in case there is an odd git clone issue
set(error_code 1)
set(number_of_tries 0)
while(error_code AND number_of_tries LESS 3)
  execute_process(
    COMMAND "/usr/bin/git"  clone --no-checkout --depth 1 --no-single-branch --config advice.detachedHead=false "https://github.com/axboe/liburing.git" "liburing"
    WORKING_DIRECTORY "/home/sh3ll/ceph/ceph/src"
    RESULT_VARIABLE error_code
    )
  math(EXPR number_of_tries "${number_of_tries} + 1")
endwhile()
if(number_of_tries GREATER 1)
  message(STATUS "Had to git clone more than once:
          ${number_of_tries} times.")
endif()
if(error_code)
  message(FATAL_ERROR "Failed to clone repository: 'https://github.com/axboe/liburing.git'")
endif()

execute_process(
  COMMAND "/usr/bin/git"  checkout liburing-0.7 --
  WORKING_DIRECTORY "/home/sh3ll/ceph/ceph/src/liburing"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to checkout tag: 'liburing-0.7'")
endif()

set(init_submodules TRUE)
if(init_submodules)
  execute_process(
    COMMAND "/usr/bin/git"  submodule update --recursive --init 
    WORKING_DIRECTORY "/home/sh3ll/ceph/ceph/src/liburing"
    RESULT_VARIABLE error_code
    )
endif()
if(error_code)
  message(FATAL_ERROR "Failed to update submodules in: '/home/sh3ll/ceph/ceph/src/liburing'")
endif()

# Complete success, update the script-last-run stamp file:
#
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy
    "/home/sh3ll/ceph/ceph/src/blk/liburing_ext-prefix/src/liburing_ext-stamp/liburing_ext-gitinfo.txt"
    "/home/sh3ll/ceph/ceph/src/blk/liburing_ext-prefix/src/liburing_ext-stamp/liburing_ext-gitclone-lastrun.txt"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to copy script-last-run stamp file: '/home/sh3ll/ceph/ceph/src/blk/liburing_ext-prefix/src/liburing_ext-stamp/liburing_ext-gitclone-lastrun.txt'")
endif()

