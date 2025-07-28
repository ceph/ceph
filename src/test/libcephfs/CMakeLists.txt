# Note:
#
#    target_link_options(ceph_test_libcephfs PRIVATE -Wl,--copy-dt-needed-entries)
#
# This is done due to this linker error:
#
#    FAILED: bin/ceph_test_libcephfs_newops
#    : && /usr/bin/g++-11 -Og -g -rdynamic -pie src/test/libcephfs/CMakeFiles/ceph_test_libcephfs_newops.dir/main.cc.o src/test/libcephfs/CMakeFiles/ceph_test_libcephfs_newops.dir/newops.cc.o -o bin/ceph_test_libcephfs_newops  -Wl,-rpath,/home/ubuntu/ceph/build/lib:  lib/libcephfs.so.2.0.0  lib/libgmock_main.a  lib/libgmock.a  lib/libgtest.a  -ldl  -ldl  /usr/lib/x86_64-linux-gnu/librt.a  -lresolv  -ldl  lib/libceph-common.so.2  lib/libjson_spirit.a  lib/libcommon_utf8.a  lib/liberasure_code.a  lib/libextblkdev.a  -lcap  boost/lib/libboost_thread.a  boost/lib/libboost_chrono.a  boost/lib/libboost_atomic.a  boost/lib/libboost_system.a  boost/lib/libboost_random.a  boost/lib/libboost_program_options.a  boost/lib/libboost_date_time.a  boost/lib/libboost_iostreams.a  boost/lib/libboost_regex.a  lib/libfmtd.a  /usr/lib/x86_64-linux-gnu/libblkid.so  /usr/lib/x86_64-linux-gnu/libcrypto.so  /usr/lib/x86_64-linux-gnu/libudev.so  /usr/lib/x86_64-linux-gnu/libz.so  src/opentelemetry-cpp/sdk/src/trace/libopentelemetry_trace.a  src/opentelemetry-cpp/sdk/src/resource/libopentelemetry_resources.a  src/opentelemetry-cpp/sdk/src/common/libopentelemetry_common.a  src/opentelemetry-cpp/exporters/jaeger/libopentelemetry_exporter_jaeger_trace.a  src/opentelemetry-cpp/ext/src/http/client/curl/libopentelemetry_http_client_curl.a  /usr/lib/x86_64-linux-gnu/libcurl.so  /usr/lib/x86_64-linux-gnu/libthrift.so  -lresolv  -ldl   -Wl,--as-needed -latomic && :
#    /usr/bin/ld: lib/libcephfs.so.2.0.0: undefined reference to symbol '_ZN4ceph18__ceph_assert_failERKNS_11assert_dataE'
#    /usr/bin/ld: lib/libceph-common.so.2: error adding symbols: DSO missing from command line
#    collect2: error: ld returned 1 exit status
#
# Despite the presence of libceph-common.so.2 on the linker invocation, this
# error persists. I (Patrick) have not found a resolution for this except to
# make the linker behave in traditional manner where library dependencies are
# assumed to be available. It seems like a linker bug and I have no explanation
# for why it only affects a few test executables.

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
    client_cache.cc
  )
  target_link_libraries(ceph_test_libcephfs
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
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
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs_snapdiff PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
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
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs_suidsgid PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
  install(TARGETS ceph_test_libcephfs_suidsgid
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_vxattr
    vxattr.cc
    main.cc
  )
  target_link_libraries(ceph_test_libcephfs_vxattr
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs_vxattr PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
  install(TARGETS ceph_test_libcephfs_vxattr
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
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs_newops PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
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
    if(NOT WIN32)
      target_link_options(ceph_test_libcephfs_reclaim PRIVATE -Wl,--copy-dt-needed-entries)
    endif()
    install(TARGETS ceph_test_libcephfs_reclaim
      DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif(NOT WIN32)

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
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs_lazyio PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
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
  if(NOT WIN32)
    target_link_options(ceph_test_libcephfs_access PRIVATE -Wl,--copy-dt-needed-entries)
  endif()
  install(TARGETS ceph_test_libcephfs_access
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_libcephfs_perfcounters
    perfcounters.cc
    main.cc
  )
  target_link_libraries(ceph_test_libcephfs_perfcounters
    ceph-common
    cephfs
    librados
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_libcephfs_perfcounters
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(WITH_LIBCEPHFS)
