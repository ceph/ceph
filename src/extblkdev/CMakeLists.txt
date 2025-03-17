## extended block device plugins

set(extblkdev_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/extblkdev)

add_subdirectory(vdo)

add_library(extblkdev STATIC ExtBlkDevPlugin.cc)

if(LINUX)
  find_package(cap REQUIRED)
  target_link_libraries(extblkdev cap)
endif()

add_custom_target(extblkdev_plugins DEPENDS
    ceph_ebd_vdo)
