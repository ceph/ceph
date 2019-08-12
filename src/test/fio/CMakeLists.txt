# ObjectStore
add_library(fio_ceph_objectstore SHARED fio_ceph_objectstore.cc)
target_include_directories(fio_ceph_objectstore SYSTEM PUBLIC ${FIO_INCLUDE_DIR})

# Messenger
add_library(fio_ceph_messenger SHARED fio_ceph_messenger.cc)
target_include_directories(fio_ceph_messenger SYSTEM PUBLIC ${FIO_INCLUDE_DIR})

# prevent fio from adding a 'typedef int bool'
set(FIO_CFLAGS "-DCONFIG_HAVE_BOOL")

# fio headers use typeof(), which requires c++11 extensions
set_target_properties(fio_ceph_objectstore PROPERTIES
  CXX_EXTENSIONS ON
  COMPILE_FLAGS "${FIO_CFLAGS}")
set_target_properties(fio_ceph_messenger PROPERTIES
  CXX_EXTENSIONS ON
  COMPILE_FLAGS "${FIO_CFLAGS}")

if(WITH_FIO)
  add_dependencies(fio_ceph_objectstore fio_ext)
  add_dependencies(fio_ceph_messenger fio_ext)
endif()

target_link_libraries(fio_ceph_objectstore os global)
install(TARGETS fio_ceph_objectstore DESTINATION lib)

target_link_libraries(fio_ceph_messenger os global)
install(TARGETS fio_ceph_messenger DESTINATION lib)
