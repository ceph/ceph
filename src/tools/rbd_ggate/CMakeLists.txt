add_executable(rbd-ggate
  Driver.cc
  Server.cc
  Watcher.cc
  debug.cc
  ggate_drv.c
  main.cc)
add_dependencies(rbd-ggate prebuild-targets)
target_link_libraries(rbd-ggate geom librbd librados global)
install(TARGETS rbd-ggate DESTINATION bin)
