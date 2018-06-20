add_executable(rbd-cache
  ${CMAKE_SOURCE_DIR}/src/librbd/cache/SharedPersistentObjectCacherFile.cc
  ObjectCacheStore.cc
  CacheController.cc
  main.cc)
target_link_libraries(rbd-cache
  librados
  global)
install(TARGETS rbd-cache DESTINATION bin)
