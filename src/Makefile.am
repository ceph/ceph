include Makefile-env.am

SUBDIRS += ocf java
DIST_SUBDIRS += gtest ocf libs3 java


# subdirs

include arch/Makefile.am
include auth/Makefile.am
include brag/Makefile.am
include crush/Makefile.am
include mon/Makefile.am
include mds/Makefile.am
include os/Makefile.am
include osd/Makefile.am
include erasure-code/Makefile.am
include osdc/Makefile.am
include client/Makefile.am
include global/Makefile.am
include json_spirit/Makefile.am
include log/Makefile.am
include perfglue/Makefile.am
include common/Makefile.am
include msg/Makefile.am
include messages/Makefile.am
include include/Makefile.am
include librados/Makefile.am
include librbd/Makefile.am
include rgw/Makefile.am
include cls/Makefile.am
include key_value_store/Makefile.am
include test/Makefile.am
include tools/Makefile.am


# core daemons

ceph_mon_SOURCES = ceph_mon.cc
ceph_mon_LDADD = $(LIBMON) $(LIBOS) $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-mon

ceph_osd_SOURCES = ceph_osd.cc
ceph_osd_LDADD = $(LIBOSD) $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-osd

ceph_mds_SOURCES = ceph_mds.cc
ceph_mds_LDADD = $(LIBMDS) $(LIBOSDC) $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-mds


# admin tools


# user tools

mount_ceph_SOURCES = mount/mount.ceph.c common/secret.c
mount_ceph_LDADD = $(LIBCOMMON) $(KEYUTILS_LIB)
if LINUX
su_sbin_PROGRAMS += mount.ceph
endif # LINUX
su_sbin_SCRIPTS += mount.fuse.ceph

cephfs_SOURCES = cephfs.cc
cephfs_LDADD = $(LIBCOMMON)
bin_PROGRAMS += cephfs

librados_config_SOURCES = librados-config.cc
librados_config_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_PROGRAMS += librados-config

ceph_syn_SOURCES = ceph_syn.cc
ceph_syn_SOURCES += client/SyntheticClient.cc # uses g_conf.. needs cleanup
ceph_syn_LDADD = $(LIBCLIENT) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-syn

rbd_SOURCES = rbd.cc common/secret.c
rbd_LDADD = $(LIBRBD) $(LIBRADOS) $(CEPH_GLOBAL) -lblkid $(KEYUTILS_LIB)
if LINUX
bin_PROGRAMS += rbd
endif #LINUX



# Fuse targets

if WITH_FUSE
ceph_fuse_SOURCES = ceph_fuse.cc
ceph_fuse_LDADD = $(LIBCLIENT_FUSE) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-fuse

rbd_fuse_SOURCES = rbd_fuse/rbd-fuse.c
rbd_fuse_LDADD = -lfuse $(LIBRBD) $(LIBRADOS) $(CEPH_GLOBAL)
bin_PROGRAMS += rbd-fuse
endif # WITH_FUSE


# libcephfs (this should go somewhere else in the future)

libcephfs_la_SOURCES = libcephfs.cc
libcephfs_la_LIBADD = $(LIBCLIENT) $(LIBCOMMON) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
libcephfs_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '^ceph_.*'
lib_LTLIBRARIES += libcephfs.la

# jni library (java source is in src/java)

if ENABLE_CEPHFS_JAVA
libcephfs_jni_la_SOURCES = \
	java/native/libcephfs_jni.cc \
	java/native/ScopedLocalRef.h \
	java/native/JniConstants.cpp \
	java/native/JniConstants.h
libcephfs_jni_la_LIBADD = $(LIBCEPHFS) $(EXTRALIBS)
libcephfs_jni_la_CPPFLAGS = $(JDK_CPPFLAGS) $(AM_CPPFLAGS)
libcephfs_jni_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
lib_LTLIBRARIES += libcephfs_jni.la
endif


# shell scripts

editpaths = sed \
	-e 's|@bindir[@]|$(bindir)|g' \
	-e 's|@sbindir[@]|$(sbindir)|g' \
	-e 's|@libdir[@]|$(libdir)|g' \
	-e 's|@sysconfdir[@]|$(sysconfdir)|g' \
	-e 's|@datadir[@]|$(pkgdatadir)|g' \
	-e 's|@prefix[@]|$(prefix)|g' \
	-e 's|@@GCOV_PREFIX_STRIP[@][@]|$(GCOV_PREFIX_STRIP)|g'
shell_scripts = ceph-debugpack ceph-post-file ceph-crush-location
$(shell_scripts): Makefile
$(shell_scripts): %: %.in
	rm -f $@ $@.tmp
	$(editpaths) '$(srcdir)/$@.in' >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

EXTRA_DIST += $(srcdir)/$(shell_scripts:%=%.in)
CLEANFILES += $(shell_scripts)


# extra bits

EXTRA_DIST += \
	$(srcdir)/verify-mds-journal.sh \
	$(srcdir)/vstart.sh \
	$(srcdir)/stop.sh \
	ceph-run \
	$(srcdir)/ceph_common.sh \
	$(srcdir)/init-radosgw \
	$(srcdir)/init-radosgw.sysv \
	$(srcdir)/init-rbdmap \
	$(srcdir)/ceph-clsinfo \
	$(srcdir)/make_version \
	$(srcdir)/check_version \
	$(srcdir)/.git_version \
	$(srcdir)/ceph-rbdnamer \
	$(srcdir)/test/encoding/readable.sh \
	$(srcdir)/test/encoding/check-generated.sh \
	$(srcdir)/upstart/ceph-all.conf \
	$(srcdir)/upstart/ceph-mon.conf \
	$(srcdir)/upstart/ceph-mon-all.conf \
	$(srcdir)/upstart/ceph-mon-all-starter.conf \
	$(srcdir)/upstart/ceph-create-keys.conf \
	$(srcdir)/upstart/ceph-osd.conf \
	$(srcdir)/upstart/ceph-osd-all.conf \
	$(srcdir)/upstart/ceph-osd-all-starter.conf \
	$(srcdir)/upstart/ceph-mds.conf \
	$(srcdir)/upstart/ceph-mds-all.conf \
	$(srcdir)/upstart/ceph-mds-all-starter.conf \
	$(srcdir)/upstart/radosgw.conf \
	$(srcdir)/upstart/radosgw-all.conf \
	$(srcdir)/upstart/radosgw-all-starter.conf \
	$(srcdir)/upstart/rbdmap.conf \
	ceph.in \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys \
	ceph-rest-api \
	ceph-crush-location \
	mount.fuse.ceph \
	rbdmap \
	unittest_bufferlist.sh \
	yasm-wrapper

EXTRA_DIST += \
	libs3/COPYING \
	libs3/ChangeLog \
	libs3/GNUmakefile \
	libs3/GNUmakefile.mingw \
	libs3/GNUmakefile.osx \
	libs3/INSTALL \
	libs3/LICENSE \
	libs3/README \
	libs3/TODO \
	libs3/archlinux \
	libs3/debian \
	libs3/doxyfile \
	libs3/inc \
	libs3/libs3.spec \
	libs3/mswin \
	libs3/src \
	libs3/test \
	unittest_bufferlist.sh

# work around old versions of automake that don't define $docdir
# NOTE: this won't work on suse, where docdir is /usr/share/doc/packages/$package.
docdir ?= ${datadir}/doc/ceph
doc_DATA = $(srcdir)/sample.ceph.conf sample.fetch_config


# various scripts

shell_commondir = $(libdir)/ceph
shell_common_SCRIPTS = ceph_common.sh

bash_completiondir = $(sysconfdir)/bash_completion.d
bash_completion_DATA = $(srcdir)/bash_completion/ceph \
               $(srcdir)/bash_completion/rados \
               $(srcdir)/bash_completion/rbd \
               $(srcdir)/bash_completion/radosgw-admin

ceph_sbin_SCRIPTS = \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys

bin_SCRIPTS += \
	ceph \
	ceph-run \
	ceph-rest-api \
	ceph-clsinfo \
	ceph-debugpack \
	ceph-rbdnamer \
	ceph-post-file \
	ceph-crush-location

BUILT_SOURCES += init-ceph
su_sbin_SCRIPTS += mkcephfs

shell_scripts += init-ceph mkcephfs





# tests to actually run on "make check"; if you need extra, non-test,
# executables built, you need to replace this with manual assignments
# target by target

TESTS = \
	$(check_PROGRAMS) \
	$(check_SCRIPTS)

check-local:
	$(srcdir)/test/encoding/readable.sh ../ceph-object-corpus


# base targets

core-daemons: ceph-mon ceph-osd ceph-mds radosgw
admin-tools: monmaptool osdmaptool crushtool ceph-authtool
base: core-daemons admin-tools \
	cephfs ceph-syn ceph-conf \
	rados librados-config \
	init-ceph mkcephfs ceph_mon_store_converter ceph-post-file


# version stuff

FORCE:
.git_version: FORCE
	$(srcdir)/check_version $(srcdir)/.git_version

# if NO_VERSION is set, only generate a new ceph_ver.h if there currently 
# is none, and call "make_version -n" to fill it with a fixed string.
# Otherwise, set it from the contents of .git_version.

ceph_ver.h: .git_version
	if [ -n "$$NO_VERSION" ] ; then \
	    if [ ! -f ./ceph_ver.h ] ; then \
	        $(srcdir)/make_version -n ./ceph_ver.h ; \
	    fi; \
        else \
	    $(srcdir)/make_version $(srcdir)/.git_version ./ceph_ver.h ; \
	fi

ceph_ver.c: ./ceph_ver.h
common/version.cc: ./ceph_ver.h
test/encoding/ceph_dencoder.cc: ./ceph_ver.h

sample.fetch_config: fetch_config
	cp -f $(srcdir)/fetch_config ./sample.fetch_config

dist-hook:
	$(srcdir)/check_version $(srcdir)/.git_version

CLEANFILES += ceph_ver.h sample.fetch_config


# assemble Python script with global version variables
# NB: depends on format of ceph_ver.h

ceph: ceph.in ./ceph_ver.h Makefile
	rm -f $@ $@.tmp
	echo "#!/usr/bin/env python" >$@.tmp
	grep "#define CEPH_GIT_NICE_VER" ./ceph_ver.h | \
		sed -e 's/#define \(.*VER\) /\1=/' >>$@.tmp
	grep "#define CEPH_GIT_VER" ./ceph_ver.h | \
	  sed -e 's/#define \(.*VER\) /\1=/' -e 's/=\(.*\)$$/="\1"/' >>$@.tmp
	cat $(srcdir)/$@.in >>$@.tmp
	chmod a+x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

# cleaning

clean-local:
	rm -f *.so 
	find . -name '*.gcno' -o -name '*.gcda' -o -name '*.lcov' | xargs rm -f
	rm -f ceph java/java/com/ceph/crush/Bucket.class


# pybind

python_PYTHON = pybind/rados.py \
		pybind/rbd.py \
		pybind/cephfs.py \
		pybind/ceph_argparse.py \
		pybind/ceph_rest_api.py


# everything else we want to include in a 'make dist'

noinst_HEADERS += \
	cls_acl.cc\
	cls_crypto.cc\
	fetch_config\
	logrotate.conf\
	sample.ceph.conf\
	bash_completion/ceph \
	bash_completion/rados \
	bash_completion/rbd \
	bash_completion/radosgw-admin \
	mount/canonicalize.c \
	mount/mtab.c \
	objclass/objclass.h


# coverage

shell_scripts += ceph-coverage
bin_SCRIPTS += ceph-coverage


if ENABLE_COVERAGE
COV_DIR = $(DESTDIR)$(libdir)/ceph/coverage
COV_FILES = $(srcdir)/*.gcno
COV_LIB_FILES = $(srcdir)/.libs/*.gcno
endif

install-coverage:
if ENABLE_COVERAGE
	-mkdir -p $(COV_DIR)/.libs
	-$(INSTALL_DATA) $(COV_FILES) $(COV_DIR)
	-$(INSTALL_DATA) $(COV_LIB_FILES) $(COV_DIR)/.libs
endif

uninstall-coverage:
if ENABLE_COVERAGE
	-rm $(COV_DIR)/*.gcno
	-rm $(COV_DIR)/.libs/*.gcno
	-rmdir -p $(COV_DIR)/.libs
	-rmdir -p $(COV_DIR)
endif

check-coverage:
if ENABLE_COVERAGE
	-test/coverage.sh -d $(srcdir) -o check-coverage make check
endif

install-data-local: install-coverage
	-mkdir -p $(DESTDIR)$(sysconfdir)/ceph
	-mkdir -p $(DESTDIR)$(localstatedir)/log/ceph
	-mkdir -p $(DESTDIR)$(localstatedir)/lib/ceph/tmp

uninstall-local: uninstall-coverage
	-rmdir -p $(DESTDIR)$(sysconfdir)/ceph/
	-rmdir -p $(DESTDIR)$(localstatedir)/log/ceph
	-rmdir -p $(DESTDIR)$(localstatedir)/lib/ceph/tmp

#
# coverity rules expect:
#  - cov-build to be in the path
#  - password in ~/coverity.build.pass.txt
#  - ability to scp into the ceph.com directory
#
project.tgz: clean
	rm -rf cov-int
	cov-build --dir cov-int make
	echo Sage Weil sage@newdream.net ceph >> README
	tar czvf project.tgz README cov-int
	rm -f README

coverity-submit:
	scp project.tgz ceph.com:/home/ceph_site/ceph.com/coverity/`git describe`.tgz
	curl --data "project=ceph&password=`cat ~/coverity.build.pass.txt`&email=sage@newdream.net&url=http://ceph.com/coverity/`git describe`.tgz" http://scan5.coverity.com/cgi-bin/submit_build.py
