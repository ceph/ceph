if(WITH_SELINUX)
  install(FILES ceph_selinux.8
    DESTINATION ${CEPH_MAN_DIR}/man8)
endif()
