if (CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(dpdk_env  "x86_64-native-linuxapp-gcc")
  MESSAGE(STATUS "current platform: Linux ")
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  set(dpdk_env "x86_64-native-bsdapp-gcc")
  MESSAGE(STATUS "current platform: FreeBSD")
else ()
  set(dpdk_env "x86_64-native-linuxapp-gcc")
  MESSAGE(STATUS "other platform: ${CMAKE_SYSTEM_NAME}")
endif (CMAKE_SYSTEM_NAME MATCHES "Linux")

include(ExternalProject) 
ExternalProject_Add(build_dpdk
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
  CONFIGURE_COMMAND ""
  BUILD_COMMAND make EXTRA_CFLAGS=-fPIC
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND "")                                                                                                                                                                        

ExternalProject_Add_Step(build_dpdk before_build
  DEPENDERS configure
  DEPENDERS build
  COMMAND make config T=${dpdk_env}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
  ALWAYS 1)
