add_library(crimson-seastore
  seastore_types.cc
  segment_manager/ephemeral.cc
  segment_manager.cc
  journal.cc
	)
target_link_libraries(crimson-seastore
  crimson)
