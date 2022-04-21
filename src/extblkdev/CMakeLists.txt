## extended block device plugins

set(extblkdev_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/extblkdev)

add_subdirectory(vdo)

set(common_ebd_srcs
  ExtBlkDevPlugin.cc)

add_library(common_ebd_objs OBJECT
  ${common_ebd_srcs})

add_custom_target(extblkdev_plugins DEPENDS
    ebd_vdo)
