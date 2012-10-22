# automake technique adapted from OpenMPI Java

JAVA_SRC = \
	java/com/ceph/fs/CephMount.java \
	java/com/ceph/fs/CephStat.java \
	java/com/ceph/fs/CephStatVFS.java \
	java/com/ceph/fs/CephNativeLoader.java \
	java/com/ceph/fs/CephNotMountedException.java

EXTRA_DIST = $(JAVA_SRC) test

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
	export CLASSPATH=java/ ;
	$(JAVAC) -source 1.5 -target 1.5 java/com/ceph/fs/*.java

$(JAVA_H): $(CEPH_PROXY)
	export CLASSPATH=java/ ; \
	$(JAVAH) -jni -o $@ com.ceph.fs.CephMount

libcephfs.jar: $(CEPH_PROXY)
	$(JAR) cf $@ $(JAVA_CLASSES:%=-C java %) # $(ESCAPED_JAVA_CLASSES:%=-C java %)

javadir = $(libdir)
java_DATA = libcephfs.jar

BUILT_SOURCES = $(JAVA_H)

CLEANFILES = -rf java/com/ceph/fs/*.class $(JAVA_H) libcephfs.jar

endif
