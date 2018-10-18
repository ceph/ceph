add_custom_target(crypto_plugins)

if(HAVE_INTEL AND HAVE_BETTER_YASM_ELF64 AND (NOT APPLE))
  add_subdirectory(isa-l)
endif()
