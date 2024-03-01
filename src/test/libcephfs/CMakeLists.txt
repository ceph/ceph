if(WITH_LIBCEPHFS)
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

  add_executable(ceph_test_libcephfs_snapdiff
    main.cc
    snapdiff.cc
  )
  target_link_libraries(ceph_test_libcephfs_snapdiff
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs_snapdiff
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_suidsgid
    suidsgid.cc
  )
  target_link_libraries(ceph_test_libcephfs_suidsgid
    ceph-common
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs_suidsgid
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_newops
    main.cc
    newops.cc
  )
  target_link_libraries(ceph_test_libcephfs_newops
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs_newops
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  # uses fork, not available on Windows
  if(NOT WIN32)
    add_executable(ceph_test_libcephfs_reclaim
      reclaim.cc
    )
  target_link_libraries(ceph_test_libcephfs_reclaim
      ceph-common
      cephfs
      ${UNITTEST_LIBS}
      ${EXTRALIBS}
      ${CMAKE_DL_LIBS}
      )
    install(TARGETS ceph_test_libcephfs_reclaim
      DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif(NOT WIN32)

  add_executable(ceph_test_libcephfs_lazyio
    lazyio.cc
  )
target_link_libraries(ceph_test_libcephfs_lazyio
    ceph-common
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )   
  install(TARGETS ceph_test_libcephfs_lazyio
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_access
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
endif(WITH_LIBCEPHFS)
