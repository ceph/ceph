add_library(fio_ceph_objectstore SHARED fio_ceph_objectstore.cc)
target_include_directories(fio_ceph_objectstore SYSTEM PUBLIC ${FIO_INCLUDE_DIR})

# prevent fio from adding a 'typedef int bool'
set(FIO_CFLAGS "-DCONFIG_HAVE_BOOL")

# fio headers use typeof(), which requires c++11 extensions
if(CMAKE_VERSION VERSION_LESS "3.1")
  set_target_properties(fio_ceph_objectstore PROPERTIES
    COMPILE_FLAGS "-std=gnu++11 ${FIO_CFLAGS}")
else()
  set_target_properties(fio_ceph_objectstore PROPERTIES
    CXX_EXTENSIONS ON
    COMPILE_FLAGS "${FIO_CFLAGS}")
endif()

if(WITH_FIO)
  add_dependencies(fio_ceph_objectstore fio_ext)
endif()
target_link_libraries(fio_ceph_objectstore os global)
install(TARGETS fio_ceph_objectstore DESTINATION lib)
