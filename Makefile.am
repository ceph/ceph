AUTOMAKE_OPTIONS = gnu
ACLOCAL_AMFLAGS = -I m4
EXTRA_DIST = autogen.sh ceph.spec.in ceph.spec
# the "." here makes sure check-local builds gtest before it is used
SUBDIRS = . src man

EXTRA_DIST += \
	src/test/run-cli-tests \
	src/test/run-cli-tests-maybe-unset-ccache \
	src/test/cli \
	src/test/downloads \
	udev/50-rbd.rules \
	udev/60-ceph-partuuid-workaround.rules \
	udev/95-ceph-osd.rules \
	udev/95-ceph-osd-alt.rules \
	share/known_hosts_drop.ceph.com \
	share/id_dsa_drop.ceph.com \
	share/id_dsa_drop.ceph.com.pub

# why is it so hard to make autotools to this?
install-data-local:
	-mkdir -p $(DESTDIR)$(datadir)/ceph
	-install -m 644 share/known_hosts_drop.ceph.com $(DESTDIR)$(datadir)/ceph/known_hosts_drop.ceph.com
	-install -m 644 share/id_dsa_drop.ceph.com $(DESTDIR)$(datadir)/ceph/id_dsa_drop.ceph.com
	-install -m 644 share/id_dsa_drop.ceph.com.pub $(DESTDIR)$(datadir)/ceph/id_dsa_drop.ceph.com.pub

all-local:
if WITH_DEBUG
#	We need gtest to build the rados-api tests. We only build those in
#	a debug build, though.
	@cd src/gtest && $(MAKE) $(AM_MAKEFLAGS) lib/libgtest.a lib/libgtest_main.a
endif

check-local:
#	We build gtest this way, instead of using SUBDIRS, because with that,
#	gtest's own tests would be run and that would slow us down.
	@cd src/gtest && $(MAKE) $(AM_MAKEFLAGS) lib/libgtest.a lib/libgtest_main.a
#	exercise cli tools
	$(srcdir)/src/test/run-cli-tests '$(top_builddir)/src/test'

# "make distclean" both runs this and recurses into src/gtest, if
# gtest is in DIST_SUBDIRS. Take extra care to not fail when
# effectively cleaned twice.
clean-local:
	@if test -e src/gtest/Makefile; then \
	  echo "Making clean in src/gtest"; \
	  cd src/gtest && $(MAKE) $(AM_MAKEFLAGS) clean; \
	fi

	@rm -rf src/test/virtualenv
