# automake technique adapted from OpenMPI Java

JAVA_SRC = \
	java/com/ceph/fs/CephMount.java \
	java/com/ceph/fs/CephStat.java \
	java/com/ceph/fs/CephStatVFS.java \
	java/com/ceph/fs/CephNativeLoader.java \
	java/com/ceph/fs/CephNotMountedException.java \
	java/com/ceph/fs/CephFileAlreadyExistsException.java \
	java/com/ceph/fs/CephAlreadyMountedException.java \
	java/com/ceph/fs/CephNotDirectoryException.java \
	java/com/ceph/fs/CephPoolException.java \
	java/com/ceph/fs/CephFileExtent.java \
	java/com/ceph/crush/Bucket.java

JAVA_TEST_SRC = \
  test/com/ceph/fs/CephDoubleMountTest.java \
  test/com/ceph/fs/CephMountCreateTest.java \
  test/com/ceph/fs/CephMountTest.java \
  test/com/ceph/fs/CephUnmountedTest.java \
  test/com/ceph/fs/CephAllTests.java

EXTRA_DIST = $(JAVA_SRC) $(JAVA_TEST_SRC)

if ENABLE_CEPHFS_JAVA

JAVA_CLASSES = $(JAVA_SRC:java/%.java=%.class)
# This is dumb -- It might be better to split some work
# between Make and Ant or Maven
#ESCAPED_JAVA_CLASSES = \
#	com/ceph/fs/CephMount\$$State.class

JAVA_H = native/com_ceph_fs_CephMount.h

# target to make automake happy
CEPH_PROXY=java/com/ceph/fs/CephMount.class

$(CEPH_PROXY): $(JAVA_SRC)
	export CLASSPATH=java/ ; \
	$(JAVAC) -source 1.5 -target 1.5 java/com/ceph/fs/*.java

$(JAVA_H): $(CEPH_PROXY)
	export CLASSPATH=java/ ; \
	$(JAVAH) -jni -o $@ com.ceph.fs.CephMount

libcephfs.jar: $(CEPH_PROXY)
	$(JAR) cf $@ $(JAVA_CLASSES:%=-C java %) 

javadir = $(datadir)/java
java_DATA = libcephfs.jar

CLEANFILES = -rf java/com/ceph/fs/*.class $(JAVA_H) libcephfs.jar 

BUILT_SOURCES = $(JAVA_H)

# build the tests if *both* --enable-cephfs-java and --with-debug were specifed
if HAVE_JUNIT4

JAVA_TEST_CLASSES = $(JAVA_TEST_SRC:test/%.java=%.class)
ESCAPED_JAVA_TEST_CLASSES = com/ceph/fs/CephAllTests\$$1.class

CEPH_TEST_PROXY=test/com/ceph/fs/CephMountTest.class

$(CEPH_TEST_PROXY): $(JAVA_TEST_SRC) $(CEPH_PROXY)
	export CLASSPATH=$(CLASSPATH):$(EXTRA_CLASSPATH_JAR):java/:test/ ; \
	$(JAVAC) -source 1.5 -target 1.5 test/com/ceph/fs/*.java

libcephfs-test.jar: $(CEPH_TEST_PROXY)
	$(JAR) cf $@ $(JAVA_TEST_CLASSES:%=-C test %) $(ESCAPED_JAVA_TEST_CLASSES:%=-C test %)

java_DATA += libcephfs-test.jar

CLEANFILES += test/com/ceph/fs/*.class libcephfs-test.jar
endif # HAVE_JUNIT4 (includes WITH_DEBUG)
endif #ENABLE_CEPHFS_JAVA
