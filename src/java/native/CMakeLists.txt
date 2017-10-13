add_library(cephfs_jni SHARED
  libcephfs_jni.cc
  ScopedLocalRef.h
  JniConstants.cpp
  JniConstants.h)
set_target_properties(cephfs_jni PROPERTIES
  VERSION 1.0.0
  SOVERSION 1)
add_dependencies(cephfs_jni jni-header)
include_directories(${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(cephfs_jni PRIVATE cephfs ceph-common
  ${EXTRALIBS} ${JNI_LIBRARIES})
install(TARGETS cephfs_jni
  DESTINATION ${CMAKE_INSTALL_LIBDIR})
