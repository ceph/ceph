if(WITH_LIBCEPHSQLITE)
  add_executable(ceph_test_libcephsqlite
    main.cc
    ${CMAKE_SOURCE_DIR}/src/SimpleRADOSStriper.cc
  )
  target_link_libraries(ceph_test_libcephsqlite
    cephsqlite
    cls_lock_client
    librados
    ceph-common
    SQLite3::SQLite3
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephsqlite DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(WITH_LIBCEPHSQLITE)
