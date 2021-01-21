add_library(crimson-poseidonstore STATIC
  poseidonstore.cc
	)
target_link_libraries(crimson-poseidonstore
  crimson
  crimson-os)

