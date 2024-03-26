if(${WITH_CEPHFS})
  add_executable(ceph_test_client
    main.cc
    alternate_name.cc
    ops.cc
    nonblocking.cc
    commands.cc
    )
  target_link_libraries(ceph_test_client
    client
    global
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_client
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(${WITH_CEPHFS})
