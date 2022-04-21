# vdo plugin

set(vdo_srcs
  ExtBlkDevPluginVdo.cc
  ExtBlkDevVdo.cc
)

add_library(ebd_vdo SHARED ${vdo_srcs})
target_link_libraries(ebd_vdo common ${EXTRALIBS})
install(TARGETS ebd_vdo DESTINATION ${extblkdev_plugin_dir})
