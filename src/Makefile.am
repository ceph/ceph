include Makefile-env.am

SUBDIRS += ocf java tracing
DIST_SUBDIRS += gmock ocf libs3 java tracing


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
include libradosstriper/Makefile.am
include librbd/Makefile.am
include rgw/Makefile.am
include cls/Makefile.am
include key_value_store/Makefile.am
include rbd_replay/Makefile.am
include test/Makefile.am
include tools/Makefile.am
include Makefile-rocksdb.am


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
	$(srcdir)/ceph-osd-prestart.sh \
	$(srcdir)/ceph_common.sh \
	$(srcdir)/init-radosgw \
	$(srcdir)/init-rbdmap \
	$(srcdir)/ceph-clsinfo \
	$(srcdir)/make_version \
	$(srcdir)/check_version \
	$(srcdir)/.git_version \
	$(srcdir)/ceph-rbdnamer \
	$(srcdir)/test/encoding/readable.sh \
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
	rbd-replay-many \
	rbdmap \
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

ceph_libexecdir = $(libexecdir)/ceph
ceph_libexec_SCRIPTS = ceph-osd-prestart.sh


# tests to actually run on "make check"; if you need extra, non-test,
# executables built, you need to replace this with manual assignments
# target by target

TESTS = \
	$(check_PROGRAMS) \
	$(check_SCRIPTS)

check-local:
	$(top_srcdir)/qa/workunits/erasure-code/encode-decode-non-regression.sh 
	$(srcdir)/test/encoding/readable.sh ../ceph-object-corpus

if WITH_LTTNG
# TODO: If we're running the parallel test harness (the preferred harness), this should be AM_TESTS_ENVIRONMENT instead.
# See: https://www.gnu.org/software/automake/manual/html_node/Scripts_002dbased-Testsuites.html
# I don't see the serial-tests Automake option anywhere, but my AM_TESTS_ENVIRONMENT was being ignored.
TESTS_ENVIRONMENT = LD_PRELOAD=liblttng-ust-fork.so; export LD_PRELOAD; echo "LD_PRELOAD=$${LD_PRELOAD}";
endif

# base targets

core-daemons: ceph-mon ceph-osd ceph-mds radosgw
admin-tools: monmaptool osdmaptool crushtool ceph-authtool
base: core-daemons admin-tools \
	cephfs ceph-syn ceph-conf \
	rados radosgw-admin librados-config \
	init-ceph ceph-post-file \
	ceph


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


# cleaning

clean-local:
	rm -f *.so 
	find . -name '*.gcno' -o -name '*.gcda' -o -name '*.lcov' -o -name "*.o" -o -name "*.lo" | xargs rm -f
	rm -f ceph java/java/com/ceph/crush/Bucket.class


# pybind

python_PYTHON = 

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

if ENABLE_CLIENT
include Makefile-client.am
endif

if ENABLE_SERVER
include Makefile-server.am
endif

