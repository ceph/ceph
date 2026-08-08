# mold does not implement --copy-dt-needed-entries and exits with a fatal
# error when passed the flag; it also does not exhibit the BFD linker
# behavior the flag works around (see src/test/libcephfs/CMakeLists.txt)
if(USING_MOLD_LINKER)
  set(copy_dt_needed_entries "")
else()
  set(copy_dt_needed_entries "-Wl,--copy-dt-needed-entries")
endif()

if(${WITH_CEPHFS})
  # unittest_mds_types
  add_executable(unittest_mds_types
    mds_types.cc
    )
  add_ceph_unittest(unittest_mds_types)
  target_link_libraries(unittest_mds_types global)

  add_executable(ceph_test_trim_caps
    test_trim_caps.cc
  )
  target_link_libraries(ceph_test_trim_caps
      ceph-common
      cephfs
  )
  if(NOT WIN32)
    target_link_options(ceph_test_trim_caps PRIVATE ${copy_dt_needed_entries})
  endif()
  install(TARGETS ceph_test_trim_caps DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_executable(ceph_test_ino_release_cb
    test_ino_release_cb.cc
  )
  target_link_libraries(ceph_test_ino_release_cb
      ceph-common
      cephfs
  )
  if(NOT WIN32)
    target_link_options(ceph_test_ino_release_cb PRIVATE ${copy_dt_needed_entries})
  endif()
  install(TARGETS ceph_test_ino_release_cb DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(${WITH_CEPHFS})
