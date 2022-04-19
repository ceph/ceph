# Install script for directory: /home/sh3ll/ceph/ceph/src/tools/ceph-dencoder

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so"
         RPATH "/usr/local/lib/ceph")
  endif()
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/ceph/denc/denc-mod-common.so")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
file(INSTALL DESTINATION "/usr/local/lib/ceph/denc" TYPE SHARED_LIBRARY FILES "/home/sh3ll/ceph/ceph/lib/denc-mod-common.so")
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so"
         OLD_RPATH ":::::::::::::::::::"
         NEW_RPATH "/usr/local/lib/ceph")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-common.so")
    endif()
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so"
         RPATH "/usr/local/lib/ceph")
  endif()
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/ceph/denc/denc-mod-osd.so")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
file(INSTALL DESTINATION "/usr/local/lib/ceph/denc" TYPE SHARED_LIBRARY FILES "/home/sh3ll/ceph/ceph/lib/denc-mod-osd.so")
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so"
         OLD_RPATH ":::::::::::::::::::"
         NEW_RPATH "/usr/local/lib/ceph")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-osd.so")
    endif()
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so"
         RPATH "/usr/local/lib/ceph")
  endif()
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/lib/ceph/denc/denc-mod-rgw.so")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
file(INSTALL DESTINATION "/usr/local/lib/ceph/denc" TYPE SHARED_LIBRARY FILES "/home/sh3ll/ceph/ceph/lib/denc-mod-rgw.so")
  if(EXISTS "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so"
         OLD_RPATH "/home/sh3ll/ceph/ceph/lib:"
         NEW_RPATH "/usr/local/lib/ceph")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}/usr/local/lib/ceph/denc/denc-mod-rgw.so")
    endif()
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder"
         RPATH "/usr/local/lib/ceph")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE EXECUTABLE FILES "/home/sh3ll/ceph/ceph/bin/ceph-dencoder")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder"
         OLD_RPATH "/home/sh3ll/ceph/ceph/lib:"
         NEW_RPATH "/usr/local/lib/ceph")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/ceph-dencoder")
    endif()
  endif()
endif()

