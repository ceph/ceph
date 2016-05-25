add_library(cephfs_jni SHARED
  libcephfs_jni.cc
  ScopedLocalRef.h
  JniConstants.cpp
  JniConstants.h)
add_dependencies(cephfs_jni jni-header)
include_directories(${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(cephfs_jni LINK_PRIVATE cephfs common ${EXTRALIBS} ${JNI_LIBRARIES})
install(TARGETS cephfs_jni
  DESTINATION lib)
