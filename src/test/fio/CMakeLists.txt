add_library(fio_ceph_objectstore SHARED fio_ceph_objectstore.cc)
add_library(rbd_trace SHARED rbd_trace.cc)
target_include_directories(fio_ceph_objectstore PUBLIC ${FIO_INCLUDE_DIR})
target_include_directories(rbd_trace PUBLIC ${FIO_INCLUDE_DIR})

# prevent fio from adding a 'typedef int bool'
set(FIO_CFLAGS "-DCONFIG_HAVE_BOOL")

# fio headers use typeof(), which requires c++11 extensions
if(CMAKE_VERSION VERSION_LESS "3.1")
  set_target_properties(fio_ceph_objectstore PROPERTIES
    COMPILE_FLAGS "-std=gnu++11 ${FIO_CFLAGS}")
  set_target_properties(rbd_trace PROPERTIES
    COMPILE_FLAGS "-std=gnu++11 ${FIO_CFLAGS}")
else()
  set_target_properties(fio_ceph_objectstore PROPERTIES
    CXX_EXTENSIONS ON
    COMPILE_FLAGS "${FIO_CFLAGS}")
  set_target_properties(rbd_trace PROPERTIES
    CXX_EXTENSIONS ON
    COMPILE_FLAGS "${FIO_CFLAGS}")
endif()

target_link_libraries(fio_ceph_objectstore os global)
target_link_libraries(rbd_trace os global common)
install(TARGETS fio_ceph_objectstore DESTINATION lib)
install(TARGETS rbd_trace DESTINATION lib)
