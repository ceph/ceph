set(ceph_immutable_object_cache_files
  ObjectCacheFile.cc
  ObjectCacheStore.cc
  CacheController.cc
  CacheServer.cc
  CacheClient.cc
  CacheSession.cc
  SimplePolicy.cc)
add_library(ceph_immutable_object_cache_lib STATIC ${ceph_immutable_object_cache_files})

add_executable(ceph-immutable-object-cache
  ObjectCacheFile.cc
  ObjectCacheStore.cc
  CacheController.cc
  CacheServer.cc
  CacheSession.cc
  SimplePolicy.cc
  main.cc)
target_link_libraries(ceph-immutable-object-cache
  librados-cxx
  stdc++fs
  global)
install(TARGETS ceph-immutable-object-cache DESTINATION bin)
