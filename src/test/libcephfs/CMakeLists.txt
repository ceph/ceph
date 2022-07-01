if(${WITH_CEPHFS})
  add_executable(ceph_test_libcephfs
    test.cc
    readdir_r_cb.cc
    caps.cc
    multiclient.cc
    flock.cc
    recordlock.cc
    acl.cc
    main.cc
    deleg.cc
    monconfig.cc
    vxattr.cc
  )
  target_link_libraries(ceph_test_libcephfs
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_reclaim
    reclaim.cc
  )
  target_link_libraries(ceph_test_libcephfs_reclaim
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs_reclaim
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_lazyio
    lazyio.cc
  )
  target_link_libraries(ceph_test_libcephfs_lazyio
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )   
  install(TARGETS ceph_test_libcephfs_lazyio
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_access
    test.cc
    access.cc
  )
  target_link_libraries(ceph_test_libcephfs_access
    ceph-common
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs_access
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(${WITH_CEPHFS})  

