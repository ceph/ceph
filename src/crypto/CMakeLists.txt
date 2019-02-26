add_custom_target(crypto_plugins)
set(crypto_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/crypto)

add_subdirectory(openssl)

if(HAVE_INTEL AND HAVE_BETTER_YASM_ELF64 AND (NOT APPLE))
  add_subdirectory(isa-l)
endif()

if(WITH_QAT)
  add_subdirectory(qat)
endif()
