add_executable(crimson-osd
  chained_dispatchers.cc
  heartbeat.cc
  main.cc
  osd.cc
  osd_meta.cc
  pg.cc
  pg_meta.cc
  recovery_machine.cc
  recovery_state.cc
  recovery_states.cc)
target_link_libraries(crimson-osd
  crimson-common crimson-os crimson fmt::fmt)
