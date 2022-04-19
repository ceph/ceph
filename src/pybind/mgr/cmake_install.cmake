# Install script for directory: /home/sh3ll/ceph/ceph/src/pybind/mgr

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

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for the subdirectory.
  include("/home/sh3ll/ceph/ceph/src/pybind/mgr/dashboard/cmake_install.cmake")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for the subdirectory.
  include("/home/sh3ll/ceph/ceph/src/pybind/mgr/rook/cmake_install.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/share/ceph/mgr/alerts;/usr/local/share/ceph/mgr/balancer;/usr/local/share/ceph/mgr/cephadm;/usr/local/share/ceph/mgr/crash;/usr/local/share/ceph/mgr/devicehealth;/usr/local/share/ceph/mgr/diskprediction_local;/usr/local/share/ceph/mgr/influx;/usr/local/share/ceph/mgr/insights;/usr/local/share/ceph/mgr/iostat;/usr/local/share/ceph/mgr/k8sevents;/usr/local/share/ceph/mgr/localpool;/usr/local/share/ceph/mgr/mds_autoscaler;/usr/local/share/ceph/mgr/mirroring;/usr/local/share/ceph/mgr/nfs;/usr/local/share/ceph/mgr/orchestrator;/usr/local/share/ceph/mgr/osd_perf_query;/usr/local/share/ceph/mgr/osd_support;/usr/local/share/ceph/mgr/pg_autoscaler;/usr/local/share/ceph/mgr/progress;/usr/local/share/ceph/mgr/prometheus;/usr/local/share/ceph/mgr/rbd_support;/usr/local/share/ceph/mgr/restful;/usr/local/share/ceph/mgr/selftest;/usr/local/share/ceph/mgr/snap_schedule;/usr/local/share/ceph/mgr/stats;/usr/local/share/ceph/mgr/status;/usr/local/share/ceph/mgr/telegraf;/usr/local/share/ceph/mgr/telemetry;/usr/local/share/ceph/mgr/test_orchestrator;/usr/local/share/ceph/mgr/volumes;/usr/local/share/ceph/mgr/zabbix")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
file(INSTALL DESTINATION "/usr/local/share/ceph/mgr" TYPE DIRECTORY FILES
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/alerts"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/balancer"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/cephadm"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/crash"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/devicehealth"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/diskprediction_local"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/influx"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/insights"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/iostat"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/k8sevents"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/localpool"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/mds_autoscaler"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/mirroring"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/nfs"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/orchestrator"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/osd_perf_query"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/osd_support"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/pg_autoscaler"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/progress"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/prometheus"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/rbd_support"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/restful"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/selftest"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/snap_schedule"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/stats"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/status"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/telegraf"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/telemetry"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/test_orchestrator"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/volumes"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/zabbix"
    REGEX "/CMakeLists\\.txt$" EXCLUDE REGEX "/\\.gitignore$" EXCLUDE REGEX "/tox\\.ini$" EXCLUDE REGEX "/requirements[^/]*\\.txt$" EXCLUDE REGEX "/constraints[^/]*\\.txt$" EXCLUDE REGEX "/tests\\/[^/]*$" EXCLUDE)
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/usr/local/share/ceph/mgr/mgr_module.py;/usr/local/share/ceph/mgr/mgr_util.py")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
file(INSTALL DESTINATION "/usr/local/share/ceph/mgr" TYPE FILE FILES
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/mgr_module.py"
    "/home/sh3ll/ceph/ceph/src/pybind/mgr/mgr_util.py"
    )
endif()

