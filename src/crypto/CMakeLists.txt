# crypto plugins are loaded at runtime by rgw_crypt.cc
add_custom_target(crypto_plugins ALL)
set(crypto_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/crypto)

add_subdirectory(openssl)

if(HAVE_INTEL AND HAVE_NASM_X64_AVX2 AND (NOT APPLE))
  add_subdirectory(isa-l)
endif()

if(HAVE_QAT)
  add_subdirectory(qat)
endif()
