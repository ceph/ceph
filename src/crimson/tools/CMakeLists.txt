add_executable(crimson-store-nbd
	store_nbd/store-nbd.cc
	store_nbd/tm_driver.cc
	store_nbd/fs_driver.cc
	store_nbd/block_driver.cc
  )
target_link_libraries(crimson-store-nbd
  crimson-os)
install(TARGETS crimson-store-nbd DESTINATION bin)
