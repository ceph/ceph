add_executable(ceph_test_objectcacher_stress
  object_cacher_stress.cc
  FakeWriteback.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(ceph_test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

