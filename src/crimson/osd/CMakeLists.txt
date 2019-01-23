add_executable(crimson-osd
  chained_dispatchers.cc
  main.cc
  osd.cc
  osd_meta.cc)
target_link_libraries(crimson-osd
  crimson-common crimson-os crimson)
