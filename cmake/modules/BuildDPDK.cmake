function(do_build_dpdk dpdk_dir)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "i386")
    set(arch "x86_64")
    set(machine "default")
    set(machine_tmpl "native")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "i686")
    set(arch "i686")
    set(machine "default")
    set(machine_tmpl "native")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64|AMD64")
    set(arch "x86_64")
    set(machine "default")
    set(machine_tmpl "native")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM")
    set(arch "arm")
    set(machine "armv7a")
    set(machine_tmpl "armv7a")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64")
    set(arch "arm64")
    set(machine "armv8a")
    set(machine_tmpl "armv8a")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(powerpc|ppc)64")
    set(arch "ppc_64")
    set(machine "power8")
    set(machine_tmpl "power8")
  else()
    message(FATAL_ERROR "not able to build DPDK support: "
      "unknown arch \"${CMAKE_SYSTEM_PROCESSOR}\"")
  endif()

  if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(execenv "linuxapp")
  elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    set(execenv "bsdapp")
  else()
    message(FATAL_ERROR "not able to build DPDK support: "
      "unsupported OS \"${CMAKE_SYSTEM_NAME}\"")
  endif()

  if(CMAKE_C_COMPILER_ID STREQUAL GNU)
    set(toolchain "gcc")
  elseif(CMAKE_C_COMPILER_ID STREQUAL Clang)
    set(toolchain "clang")
  elseif(CMAKE_C_COMPILER_ID STREQUAL Intel)
    set(toolchain "icc")
  else()
    message(FATAL_ERROR "not able to build DPDK support: "
      "unknown compiler \"${CMAKE_C_COMPILER_ID}\"")
  endif()

  set(target "${arch}-${machine_tmpl}-${execenv}-${toolchain}")

  execute_process(
    COMMAND ${CMAKE_MAKE_PROGRAM} showconfigs
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
    OUTPUT_VARIABLE supported_targets
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(REPLACE "\n" ";" supported_targets "${supported_targets}")
  list(FIND supported_targets ${target} found)
  if(found EQUAL -1)
    message(FATAL_ERROR "not able to build DPDK support: "
      "unsupported target. "
      "\"${target}\" not listed in ${supported_targets}")
  endif()

  include(ExternalProject)
  ExternalProject_Add(dpdk-ext
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
    CONFIGURE_COMMAND $(MAKE) config O=${dpdk_dir} T=${target}
    BUILD_COMMAND env CC=${CMAKE_C_COMPILER} $(MAKE) O=${dpdk_dir} EXTRA_CFLAGS=-fPIC
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND "true")
  ExternalProject_Add_Step(dpdk-ext patch-config
    COMMAND ${CMAKE_MODULE_PATH}/patch-dpdk-conf.sh ${dpdk_dir} ${machine}
    DEPENDEES configure
    DEPENDERS build)
  # easier to adjust the config
  ExternalProject_Add_StepTargets(dpdk-ext configure patch-config build)
endfunction()

macro(build_dpdk)
  set(DPDK_DIR ${CMAKE_BINARY_DIR}/src/dpdk)
  do_build_dpdk(${DPDK_DIR})
  set(DPDK_INCLUDE_DIR ${DPDK_DIR}/include)
  # create the directory so cmake won't complain when looking at the imported
  # target
  file(MAKE_DIRECTORY ${DPDK_INCLUDE_DIR})
  foreach(c
      pci bus_pci
      eal
      mempool mempool_ring mempool_stack ring)
    add_library(dpdk::${c} STATIC IMPORTED)
    add_dependencies(dpdk::${c} dpdk-ext)
    set(dpdk_${c}_LIBRARY
      "${DPDK_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}rte_${c}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set_target_properties(dpdk::${c} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES ${DPDK_INCLUDE_DIR}
      IMPORTED_LOCATION "${dpdk_${c}_LIBRARY}")
    list(APPEND DPDK_LIBRARIES dpdk::${c})
  endforeach()
endmacro()
