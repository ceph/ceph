add_executable(rbd-ggate
  Driver.cc
  Server.cc
  Watcher.cc
  debug.cc
  ggate_drv.c
  main.cc)
target_link_libraries(rbd-ggate geom librbd librados global)
install(TARGETS rbd-ggate DESTINATION bin)
