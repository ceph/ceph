if(WITH_LIBCEPHSQLITE)
  add_executable(ceph_test_libcephsqlite
    main.cc
  )
  target_link_libraries(ceph_test_libcephsqlite
    cephsqlite
    librados
    ceph-common
    SQLite3::SQLite3
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephsqlite DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(WITH_LIBCEPHSQLITE)
