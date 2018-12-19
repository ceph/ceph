set(arch_srcs
  probe.cc)

if(HAVE_ARM)
  list(APPEND arch_srcs arm.c)
elseif(HAVE_INTEL)
  list(APPEND arch_srcs intel.c)
elseif(HAVE_POWER8)
  list(APPEND arch_srcs ppc.c)
endif()

add_library(arch STATIC ${arch_srcs})
