set(ceph_dokan_srcs
  ceph_dokan.cc
  dbg.cc
  utils.cc
  options.cc
  ../common/win32/code_page.rc)
add_executable(ceph-dokan ${ceph_dokan_srcs})
target_link_libraries(ceph-dokan ${DOKAN_LIBRARIES}
  ${GSSAPI_LIBRARIES}
  cephfs ceph-common global ${EXTRALIBS})
set_target_properties(ceph-dokan PROPERTIES
  COMPILE_FLAGS "-I${DOKAN_INCLUDE_DIRS}")
install(TARGETS ceph-dokan DESTINATION bin)
