# thin_nvme plugin

set(thin_nvme_srcs
  ExtBlkDevPluginThinNVMe.cc
  ExtBlkDevThinNVMe.cc
)

add_library(ceph_ebd_thin_nvme SHARED ${thin_nvme_srcs})
install(TARGETS ceph_ebd_thin_nvme DESTINATION ${extblkdev_plugin_dir})
