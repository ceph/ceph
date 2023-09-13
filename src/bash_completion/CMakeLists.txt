set(completions
  ceph
  rados)

if(WITH_RBD)
  list(APPEND completions rbd)
endif()

if(WITH_RADOSGW)
  list(APPEND completions radosgw-admin)
endif()

install(FILES ${completions}
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/bash_completion.d)

