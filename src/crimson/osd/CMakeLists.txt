add_executable(crimson-osd
  main.cc
  osd.cc
  chained_dispatchers.cc)
target_link_libraries(crimson-osd
  crimson-common crimson-os crimson ceph-common)

