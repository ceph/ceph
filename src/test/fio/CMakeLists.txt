# ObjectStore
add_library(fio_ceph_objectstore SHARED fio_ceph_objectstore.cc)
target_link_libraries(fio_ceph_objectstore fio)

# Messenger
add_library(fio_ceph_messenger SHARED fio_ceph_messenger.cc)
target_link_libraries(fio_ceph_messenger fio)

# librgw
add_library(fio_librgw SHARED fio_librgw.cc)
target_link_libraries(fio_librgw rgw fio)

target_link_libraries(fio_ceph_objectstore os global)
install(TARGETS fio_ceph_objectstore DESTINATION lib)

target_link_libraries(fio_ceph_messenger os global)
install(TARGETS fio_ceph_messenger DESTINATION lib)

target_link_libraries(fio_librgw os global rgw)
install(TARGETS fio_librgw DESTINATION lib)

