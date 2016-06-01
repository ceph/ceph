## System tests

set(libsystest_srcs 
  cross_process_sem.cc
  systest_runnable.cc
  systest_settings.cc
  st_rados_create_pool.cc
  st_rados_delete_pool.cc
  st_rados_list_objects.cc
  st_rados_watch.cc
  st_rados_notify.cc)
add_library(systest STATIC ${libsystest_srcs})

# test_rados_list_parallel
add_executable(test_rados_list_parallel
  rados_list_parallel.cc
  )
target_link_libraries(test_rados_list_parallel librados systest global pthread
  rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

# test_rados_open_pools_parallel
set(test_rados_open_pools_parallel_srcs 
  rados_open_pools_parallel.cc)
add_executable(test_rados_open_pools_parallel
  ${test_rados_open_pools_parallel_srcs}
  )
target_link_libraries(test_rados_open_pools_parallel librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

# test_rados_delete_pools_parallel
set(test_rados_delete_pools_parallel_srcs
  rados_delete_pools_parallel.cc
  st_rados_create_pool.cc
  st_rados_delete_pool.cc
  st_rados_list_objects.cc
  )
add_executable(test_rados_delete_pools_parallel
  ${test_rados_delete_pools_parallel_srcs}
  )
target_link_libraries(test_rados_delete_pools_parallel librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

# test_rados_watch_notify
set(test_rados_watch_notify_srcs
  rados_watch_notify.cc
  st_rados_create_pool.cc
  st_rados_delete_pool.cc
  st_rados_delete_objs.cc
  st_rados_watch.cc
  st_rados_notify.cc
  )
add_executable(test_rados_watch_notify
  ${test_rados_watch_notify_srcs}
  )
target_link_libraries(test_rados_watch_notify librados systest global
  pthread rt ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS}) 

