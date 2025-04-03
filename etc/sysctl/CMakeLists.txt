if(CMAKE_SIZEOF_VOID_P EQUAL 8)
  # 4194304 is the maximum limit possible on 64-bit CONFIG_BASE_FULL kernels.
  # Keep the default for 32-bit systems.
  set(sysctl_pid_max "kernel.pid_max = 4194304")
endif()

if(WITH_CRIMSON)
  configure_file(90-crimson-osd.conf.in
    ${CMAKE_CURRENT_SOURCE_DIR}/90-ceph-osd.conf
    @ONLY)
else()
  configure_file(90-ceph-osd.conf.in
    ${CMAKE_CURRENT_SOURCE_DIR}/90-ceph-osd.conf
    @ONLY)
endif()
