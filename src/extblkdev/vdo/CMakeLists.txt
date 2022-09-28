# vdo plugin

set(vdo_srcs
  ExtBlkDevPluginVdo.cc
  ExtBlkDevVdo.cc
)

add_library(ceph_ebd_vdo SHARED ${vdo_srcs})
install(TARGETS ceph_ebd_vdo DESTINATION ${extblkdev_plugin_dir})
