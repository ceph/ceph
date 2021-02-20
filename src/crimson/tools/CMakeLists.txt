add_executable(crimson-store-nbd
	store-nbd.cc
  )
target_link_libraries(crimson-store-nbd
  crimson-seastore)
install(TARGETS crimson-store-nbd DESTINATION bin)
