# smalliobench
set(smalliobench_srcs
  small_io_bench.cc
  rados_backend.cc
  detailed_stat_collector.cc
  bencher.cc
  )
add_executable(ceph_smalliobench
  ${smalliobench_srcs}
  )
target_link_libraries(ceph_smalliobench librados ${Boost_PROGRAM_OPTIONS_LIBRARY} global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

# ceph_smalliobenchrbd
if(WITH_RBD)
  set(smalliobenchrbd_srcs
    small_io_bench_rbd.cc
    rbd_backend.cc
    detailed_stat_collector.cc
    bencher.cc
  )
  add_executable(ceph_smalliobenchrbd
    ${smalliobenchrbd_srcs}
    $<TARGET_OBJECTS:common_texttable_obj>)
  target_link_libraries(ceph_smalliobenchrbd
    librbd
    librados
    os
    global
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    udev
    ${BLKID_LIBRARIES}
    ${CMAKE_DL_LIBS}
    keyutils
    )
  install(TARGETS
    ceph_smalliobenchrbd
    DESTINATION bin)
endif(WITH_RBD)

# ceph_smalliobenchfs
set(ceph_smalliobenchfs_srcs
  small_io_bench_fs.cc
  testfilestore_backend.cc
  detailed_stat_collector.cc
  bencher.cc
  )
add_executable(ceph_smalliobenchfs
  ${ceph_smalliobenchfs_srcs}
  )
target_link_libraries(ceph_smalliobenchfs librados ${Boost_PROGRAM_OPTIONS_LIBRARY} os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

# ceph_smalliobenchdumb
set(smalliobenchdumb_srcs
  small_io_bench_dumb.cc
  dumb_backend.cc
  detailed_stat_collector.cc
  bencher.cc
  )
add_executable(ceph_smalliobenchdumb
  ${smalliobenchdumb_srcs}
  )
target_link_libraries(ceph_smalliobenchdumb librados ${Boost_PROGRAM_OPTIONS_LIBRARY} os global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

# ceph_tpbench
set(tpbench_srcs
  tp_bench.cc
  detailed_stat_collector.cc)
add_executable(ceph_tpbench
  ${tpbench_srcs}
  )
target_link_libraries(ceph_tpbench librados ${Boost_PROGRAM_OPTIONS_LIBRARY} global
  ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

install(TARGETS
  ceph_smalliobench
  ceph_smalliobenchfs
  ceph_smalliobenchdumb
  ceph_tpbench
  DESTINATION bin)
