##
# Make file for QAT linux driver project
##

set(qatdrv_root_dir "${CMAKE_BINARY_DIR}/qatdrv")
set(qatdrv_url "https://01.org/sites/default/files/downloads/intelr-quickassist-technology/qat1.7.l.4.2.0-00012.tar.gz")
set(qatdrv_url_hash "SHA256=47990b3283ded748799dba42d4b0e1bdc0be3cf3978bd587533cd12788b03856")
set(qatdrv_config_args "--enable-qat-uio")

include(ExternalProject)
ExternalProject_Add(QatDrv
  URL ${qatdrv_url}
  URL_HASH  ${qatdrv_url_hash}
  CONFIGURE_COMMAND ${qatdrv_env} ./configure ${qatdrv_config_args}

# Temporarily forcing single thread as multi-threaded make is causing build
# failues.
  BUILD_COMMAND make -j1 quickassist-all
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ""
  TEST_COMMAND ""
  PREFIX ${qatdrv_root_dir})

set(QatDrv_INCLUDE_DIRS
  ${qatdrv_root_dir}/src/QatDrv/quickassist/include
  ${qatdrv_root_dir}/src/QatDrv/quickassist/lookaside/access_layer/include
  ${qatdrv_root_dir}/src/QatDrv/quickassist/include/lac
  ${qatdrv_root_dir}/src/QatDrv/quickassist/utilities/libusdm_drv
  ${qatdrv_root_dir}/src/QatDrv/quickassist/utilities/libusdm_drv/linux/include)

set(QatDrv_LIBRARIES
  ${qatdrv_root_dir}/src/QatDrv/build/libqat_s.so
  ${qatdrv_root_dir}/src/QatDrv/build/libusdm_drv_s.so)
