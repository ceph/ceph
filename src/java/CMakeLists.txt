find_package(Java COMPONENTS Development REQUIRED)
find_package(JNI REQUIRED)
include(UseJava)

set(java_srcs
  java/com/ceph/crush/Bucket.java
  java/com/ceph/fs/CephAlreadyMountedException.java
  java/com/ceph/fs/CephFileAlreadyExistsException.java
  java/com/ceph/fs/CephFileExtent.java
  java/com/ceph/fs/CephMount.java
  java/com/ceph/fs/CephNativeLoader.java
  java/com/ceph/fs/CephNotDirectoryException.java
  java/com/ceph/fs/CephNotMountedException.java
  java/com/ceph/fs/CephPoolException.java
  java/com/ceph/fs/CephStat.java
  java/com/ceph/fs/CephStatVFS.java)

# note: for the -source 1.7 builds, we add
#   -Xlint:-options
# to get rid of the warning
#   warning: [options] bootstrap class path not set in conjunction with -source 1.7
# as per
#   https://blogs.oracle.com/darcy/entry/bootclasspath_older_source
set(CMAKE_JAVA_COMPILE_FLAGS "-source" "1.8" "-target" "1.8" "-Xlint:-options")
set(jni_header_dir "${CMAKE_CURRENT_BINARY_DIR}/native")
if(CMAKE_VERSION VERSION_LESS 3.11)
  set(CMAKE_JAVA_COMPILE_FLAGS ${CMAKE_JAVA_COMPILE_FLAGS} "-h" ${jni_header_dir})
  add_jar(libcephfs ${java_srcs})
  add_custom_target(
    jni-header
    DEPENDS libcephfs)
  add_dependencies(jni-header libcephfs)
else()
  add_jar(libcephfs ${java_srcs}
    GENERATE_NATIVE_HEADERS jni-header
    DESTINATION ${jni_header_dir})
endif()
get_property(libcephfs_jar TARGET libcephfs PROPERTY JAR_FILE)
install_jar(libcephfs share/java)

find_jar(JUNIT_JAR
  NAMES junit4 junit
  PATHS "/usr/share/java")
if(JUNIT_JAR)
  set(CMAKE_JAVA_INCLUDE_PATH ${JUNIT_JAR} ${libcephfs_jar})
  set(java_test_srcs
    test/com/ceph/fs/CephAllTests.java
    test/com/ceph/fs/CephDoubleMountTest.java
    test/com/ceph/fs/CephMountCreateTest.java
    test/com/ceph/fs/CephMountTest.java
    test/com/ceph/fs/CephUnmountedTest.java)
  add_jar(libcephfs-test ${java_test_srcs})
  add_dependencies(libcephfs-test libcephfs)
  install_jar(libcephfs-test share/java)
endif(JUNIT_JAR)

add_subdirectory(native)

add_custom_target(java DEPENDS
  libcephfs.jar
  libcephfs_jni)
