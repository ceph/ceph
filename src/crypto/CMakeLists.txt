add_custom_target(crypto_plugins)
set(crypto_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/crypto)

add_subdirectory(openssl)

if(HAVE_INTEL AND HAVE_NASM_X64_AVX2 AND (NOT APPLE))
  add_subdirectory(isa-l)
endif()

if(WITH_QATDRV)
  add_subdirectory(qat)
endif()
