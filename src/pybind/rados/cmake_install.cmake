# Install script for directory: /home/sh3ll/ceph/ceph/src/pybind/rados

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
  
    set(ENV{CC} " /usr/bin/cc")
    set(ENV{LDSHARED} " /usr/bin/cc -shared")
    set(ENV{CPPFLAGS} "-iquote/home/sh3ll/ceph/ceph/src/include
                        -D'void0=dead_function(void)'                         -D'__Pyx_check_single_interpreter(ARG)=ARG ## 0'                         -fno-var-tracking-assignments")
    set(ENV{LDFLAGS} "-L/home/sh3ll/ceph/ceph/lib")
    set(ENV{CYTHON_BUILD_DIR} "/home/sh3ll/ceph/ceph/src/pybind/rados")
    set(ENV{CEPH_LIBDIR} "/home/sh3ll/ceph/ceph/lib")

    set(options --prefix=/usr/local)
    if(DEFINED ENV{DESTDIR})
      if(EXISTS /etc/debian_version)
        list(APPEND options --install-layout=deb)
      endif()
      list(APPEND options --root=$ENV{DESTDIR})
    else()
      list(APPEND options --root=/)
    endif()
    execute_process(
       COMMAND
           /usr/bin/python3.8 /home/sh3ll/ceph/ceph/src/pybind/rados/setup.py
           build  --build-base /home/sh3ll/ceph/ceph/lib/cython_modules
           --build-platlib /home/sh3ll/ceph/ceph/lib/cython_modules/lib.3
           build_ext --cython-c-in-temp --build-temp /home/sh3ll/ceph/ceph/src/pybind/rados --cython-include-dirs /home/sh3ll/ceph/ceph/src/pybind/rados
           install ${options} --single-version-externally-managed --record /dev/null
           egg_info --egg-base /home/sh3ll/ceph/ceph/src/pybind/rados
           
       WORKING_DIRECTORY "/home/sh3ll/ceph/ceph/src/pybind/rados"
       RESULT_VARIABLE install_res)
    if(NOT "${install_res}" STREQUAL 0)
      message(FATAL_ERROR "Failed to build and install cython_rados python module")
    endif()
  
endif()

