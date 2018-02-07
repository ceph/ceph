add_executable(rbd-cache
  main.cc)
target_link_libraries(rbd-cache
  rbd_api
  rbd_internal
  rbd_types
  journal
  librados
  osdc
  cls_rbd_client
  cls_lock_client
  cls_journal_client
  global)
install(TARGETS rbd-cache DESTINATION bin)
