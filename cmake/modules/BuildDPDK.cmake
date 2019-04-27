function(do_build_dpdk dpdk_dir)
  find_program (MAKE_EXECUTABLE NAMES make gmake)
  # mk/machine/native/rte.vars.mk
  # rte_cflags are extracted from mk/machine/${machine}/rte.vars.mk
  # only 3 of them have -march=<arch> defined, so copying them here.
  # we need to pass the -march=<arch> to ${cc} as some headers in dpdk
  # require it to compile. for instance, dpdk/include/rte_memcpy.h.
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
    set(rte_cflags "-march=core2")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM")
    set(arch "arm")
    set(machine "armv7a")
    set(machine_tmpl "armv7a")
    set(rte_cflags "-march=armv7-a")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64")
    set(arch "arm64")
    set(machine "armv8a")
    set(machine_tmpl "armv8a")
    set(rte_cflags "-march=armv8-a+crc")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(powerpc|ppc)64")
    set(arch "ppc_64")
    set(machine "power8")
    set(machine_tmpl "power8")
  else()
    message(FATAL_ERROR "not able to build DPDK support: "
      "unknown arch \"${CMAKE_SYSTEM_PROCESSOR}\"")
  endif()
  set(dpdk_rte_CFLAGS "${rte_cflags}" CACHE INTERNAL "")
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
    COMMAND ${MAKE_EXECUTABLE} showconfigs
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

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd "$(MAKE)")
  else()
    set(make_cmd "${MAKE_EXECUTABLE}")
  endif()

  include(ExternalProject)
  ExternalProject_Add(dpdk-ext
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk/dpdk
    CONFIGURE_COMMAND ${make_cmd} config O=${dpdk_dir} T=${target}
    BUILD_COMMAND env CC=${CMAKE_C_COMPILER} ${make_cmd} O=${dpdk_dir} EXTRA_CFLAGS=-fPIC
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND "true")
  ExternalProject_Add_Step(dpdk-ext patch-config
    COMMAND ${CMAKE_MODULE_PATH}/patch-dpdk-conf.sh ${dpdk_dir} ${machine} ${arch}
    DEPENDEES configure
    DEPENDERS build)
  # easier to adjust the config
  ExternalProject_Add_StepTargets(dpdk-ext configure patch-config build)
endfunction()

function(build_dpdk dpdk_dir)
  do_build_dpdk(${dpdk_dir})
  set(DPDK_INCLUDE_DIR ${dpdk_dir}/include)
  # create the directory so cmake won't complain when looking at the imported
  # target
  file(MAKE_DIRECTORY ${DPDK_INCLUDE_DIR})

  if(NOT TARGET dpdk::cflags)
    add_library(dpdk::cflags INTERFACE IMPORTED)
    if (dpdk_rte_CFLAGS)
      set_target_properties(dpdk::cflags PROPERTIES
        INTERFACE_COMPILE_OPTIONS "${dpdk_rte_CFLAGS}")
    endif()
  endif()

  foreach(c
      bus_pci
      eal
      ethdev
      kvargs
      mbuf
      mempool
      mempool_ring
      pci
      ring)
    add_library(dpdk::${c} STATIC IMPORTED)
    add_dependencies(dpdk::${c} dpdk-ext)
    set(dpdk_${c}_LIBRARY
      "${dpdk_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}rte_${c}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set_target_properties(dpdk::${c} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES ${DPDK_INCLUDE_DIR}
      INTERFACE_LINK_LIBRARIES dpdk::cflags
      IMPORTED_LOCATION "${dpdk_${c}_LIBRARY}")
    list(APPEND DPDK_LIBRARIES dpdk::${c})
    list(APPEND DPDK_ARCHIVES "${dpdk_${c}_LIBRARY}")
  endforeach()

  add_library(dpdk::dpdk INTERFACE IMPORTED)
  add_dependencies(dpdk::dpdk
    ${DPDK_LIBRARIES})
  # workaround for https://gitlab.kitware.com/cmake/cmake/issues/16947
  set_target_properties(dpdk::dpdk PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${DPDK_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES
    "-Wl,--whole-archive $<JOIN:${DPDK_ARCHIVES}, > -Wl,--no-whole-archive")
  if(dpdk_rte_CFLAGS)
    set_target_properties(dpdk::dpdk PROPERTIES
      INTERFACE_COMPILE_OPTIONS "${dpdk_rte_CFLAGS}")
  endif()
endfunction()
