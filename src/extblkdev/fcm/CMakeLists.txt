# fcm plugin

set(fcm_srcs
  ExtBlkDevPluginFcm.cc
)

add_library(ceph_ebd_fcm SHARED ${fcm_srcs})
install(TARGETS ceph_ebd_fcm DESTINATION ${extblkdev_plugin_dir})
