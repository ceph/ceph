
include(Distutils)
distutils_install_module(ceph_volume
  INSTALL_SCRIPT ${CMAKE_INSTALL_FULL_SBINDIR})

if(FREEBSD)
  add_subdirectory(plugin/zfs)
endif()

