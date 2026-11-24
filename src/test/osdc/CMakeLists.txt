add_executable(ceph_test_objectcacher_stress
  object_cacher_stress.cc
  FakeWriteback.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_stress
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_objectcacher_misc
  object_cacher_misc.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_misc
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_misc
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_splitop
# SplitOp.cc is compiled into libcommon (see src/CMakeLists.txt), which global
# pulls in, so this needs no further libraries.
add_executable(unittest_splitop
  test_splitop.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_splitop)
target_link_libraries(unittest_splitop global ${BLKID_LIBRARIES})

# unittest_objecter_direct_reads
add_executable(unittest_objecter_direct_reads
  test_objecter_direct_reads.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_objecter_direct_reads)
target_link_libraries(unittest_objecter_direct_reads osdc global ${BLKID_LIBRARIES})

# unittest_crimson_core_route
# SessionRoutes on its own; crimson_core_route/session_routes.cc is
# compiled into libcommon (see src/CMakeLists.txt)
add_executable(unittest_crimson_core_route
  test_crimson_core_route.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_crimson_core_route)
target_link_libraries(unittest_crimson_core_route global ${BLKID_LIBRARIES})

# unittest_objclass_read_flags
# Drives librados' own IoCtxImpl and public API, so it needs librados_impl
# and librados as well as osdc, and cls_lock_client for a real object class
# client to call through.
add_executable(unittest_objclass_read_flags
  test_objclass_read_flags.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_objclass_read_flags)
target_link_libraries(unittest_objclass_read_flags
  librados_impl librados cls_lock_client osdc global ${BLKID_LIBRARIES})
