## extended block device plugins

set(extblkdev_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/extblkdev)

add_subdirectory(vdo)
if(HAVE_BLKID)
  add_subdirectory(fcm)
endif()

add_library(extblkdev STATIC ExtBlkDevPlugin.cc)

if(LINUX)
  find_package(cap REQUIRED)
  target_link_libraries(extblkdev cap)
endif()

if(HAVE_BLKID)
  add_custom_target(extblkdev_plugins DEPENDS
      ceph_ebd_vdo
      ceph_ebd_fcm)
else()
  add_custom_target(extblkdev_plugins DEPENDS
      ceph_ebd_vdo)
endif()
