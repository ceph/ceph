if(WITH_LIBCEPHFSSQLITE)
    add_executable(ceph_test_libcephfssqlite
            main.cc
            )
    target_link_libraries(ceph_test_libcephfssqlite
            cephfssqlite
            client
            ceph-common
            SQLite3::SQLite3
            ${UNITTEST_LIBS}
            ${EXTRALIBS}
            ${CMAKE_DL_LIBS}
            )
    install(TARGETS ceph_test_libcephfssqlite DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(WITH_LIBCEPHFSSQLITE)