if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "i386")
    set(dpdk_env  "x86_x32-native-linuxapp-gcc")
  else()
    set(dpdk_env  "x86_64-native-linuxapp-gcc")
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  set(dpdk_env "x86_64-native-bsdapp-gcc")
else()
  set(dpdk_env "x86_64-native-linuxapp-gcc")
endif (CMAKE_SYSTEM_NAME MATCHES "Linux")

include(ExternalProject) 
ExternalProject_Add(build_dpdk
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
  CONFIGURE_COMMAND ""
  BUILD_COMMAND $(MAKE) EXTRA_CFLAGS=-fPIC
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND "")                                                                                                                                                                        

ExternalProject_Add_Step(build_dpdk before_build
  DEPENDERS configure
  DEPENDERS build
  COMMAND $(MAKE) config T=${dpdk_env}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
  ALWAYS 1)
