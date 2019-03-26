set(ceph_immutable_object_cache_files
  ObjectCacheStore.cc
  CacheController.cc
  CacheServer.cc
  CacheClient.cc
  CacheSession.cc
  SimplePolicy.cc
  Types.cc
  )
add_library(ceph_immutable_object_cache_lib STATIC ${ceph_immutable_object_cache_files})

add_executable(ceph-immutable-object-cache
  main.cc)
target_link_libraries(ceph-immutable-object-cache
  ceph_immutable_object_cache_lib
  librados
  StdFilesystem::filesystem
  global)
install(TARGETS ceph-immutable-object-cache DESTINATION bin)
