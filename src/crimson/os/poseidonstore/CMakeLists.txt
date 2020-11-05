add_library(crimson-poseidonstore STATIC
  poseidonstore.cc
  device_manager/memory.cc
  device_manager.cc
  wal.cc
  poseidonstore_types.cc
	)
target_link_libraries(crimson-poseidonstore
  crimson
  crimson-os)

