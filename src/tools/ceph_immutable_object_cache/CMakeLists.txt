add_executable(ceph-immutable-object-cache
  ${CMAKE_SOURCE_DIR}/src/librbd/cache/SharedPersistentObjectCacherFile.cc
  ObjectCacheStore.cc
  CacheController.cc
  CacheServer.cc
  CacheSession.cc
  main.cc)
target_link_libraries(ceph-immutable-object-cache
  librados
  global)
install(TARGETS ceph-immutable-object-cache DESTINATION bin)
