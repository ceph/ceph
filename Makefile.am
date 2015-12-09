AUTOMAKE_OPTIONS = gnu
ACLOCAL_AMFLAGS = -I m4
EXTRA_DIST = autogen.sh ceph.spec.in ceph.spec install-deps.sh
# the "." here makes sure check-local builds gtest and gmock before they are used
SUBDIRS = . src man doc systemd selinux

EXTRA_DIST += \
	src/test/run-cli-tests \
	src/test/run-cli-tests-maybe-unset-ccache \
	src/test/cli \
	src/test/downloads \
	systemd/ceph.tmpfiles.d \
	etc/default/ceph \
	etc/sysconfig/ceph \
	etc/sysconfig/SuSEfirewall2.d/services/ceph-mon \
	etc/sysconfig/SuSEfirewall2.d/services/ceph-osd-mds \
	udev/50-rbd.rules \
	udev/60-ceph-partuuid-workaround.rules \
	udev/95-ceph-osd.rules \
	udev/95-ceph-osd-alt.rules \
	share/known_hosts_drop.ceph.com \
	share/id_dsa_drop.ceph.com \
	share/id_dsa_drop.ceph.com.pub

NPROC = nproc
if FREEBSD
	NPROC = sysctl -n hw.ncpu
endif

# why is it so hard to make autotools to this?
install-data-local::
	-mkdir -p $(DESTDIR)$(datadir)/ceph
	-install -m 600 share/known_hosts_drop.ceph.com $(DESTDIR)$(datadir)/ceph/known_hosts_drop.ceph.com
	-install -m 600 share/id_dsa_drop.ceph.com $(DESTDIR)$(datadir)/ceph/id_dsa_drop.ceph.com
	-install -m 600 share/id_dsa_drop.ceph.com.pub $(DESTDIR)$(datadir)/ceph/id_dsa_drop.ceph.com.pub

all-local::
if WITH_DEBUG
#	We need gtest to build the rados-api tests. We only build those in
#	a debug build, though.
	@cd src/gmock/gtest && $(MAKE) $(AM_MAKEFLAGS) lib/libgtest.la lib/libgtest_main.la
	@cd src/gmock && $(MAKE) $(AM_MAKEFLAGS) lib/libgmock.la lib/libgmock_main.la
endif

CHECK_ULIMIT := true

check-local:: all
#	We build gtest this way, instead of using SUBDIRS, because with that,
#	gtest's own tests would be run and that would slow us down.
	@cd src/gmock/gtest && $(MAKE) $(AM_MAKEFLAGS) lib/libgtest.la lib/libgtest_main.la
	@cd src/gmock && $(MAKE) $(AM_MAKEFLAGS) lib/libgmock.la lib/libgmock_main.la
#	exercise cli tools
	u=`ulimit -u` ; \
	p=`expr $(shell ${NPROC}) / 2` ; \
	n=`expr $$p \* 1024` ; \
	if ${CHECK_ULIMIT} && echo ${MAKEFLAGS} | grep --quiet -e -j && test $$u -lt $$n ; then \
		echo "ulimit -u is $$u which is lower than $$n = $$p / 2 * 1024" ; \
		echo "If running make -j$$p check you will likely exceed this limit" ; \
		echo "and the tests will fail in mysterious ways." ; \
		echo "Update /etc/security/limits.conf to increase the limit" ; \
		echo "or run make CHECK_ULIMIT=false -j4 check to override this safeguard." ; \
		exit 1 ; \
	fi

# display the output of failed check_SCRIPTS after a failed make check
export VERBOSE = true

TESTS = $(check_SCRIPTS)

check_SCRIPTS = \
	src/test/run-cli-tests

# "make distclean" both runs this and recurses into src/gtest, if
# gtest is in DIST_SUBDIRS. Take extra care to not fail when
# effectively cleaned twice.
clean-local:
	@if test -e src/gmock/Makefile; then \
	  echo "Making clean in src/gmock"; \
	  cd src/gmock && $(MAKE) $(AM_MAKEFLAGS) clean; \
	fi

	@rm -rf src/test/virtualenv
	@rm -rf install-deps-*


# NOTE: This only works when enough dependencies are installed for
# autoconf to be happy.  These commands should be run manually to
# bootstrap.
install-deps:
	./install-deps.sh

dist-hook:
#	Generates the full list of contributors
	if test -d $(srcdir)/.git; then \
	  cd $(srcdir); \
	  git log --format='%aN <%aE>' | sort -u >$(distdir)/AUTHORS; \
	fi
#	Generates ChangeLog from git
	if test -d $(srcdir)/.git; then \
	  cd $(srcdir); \
	  git log --oneline --decorate --no-merges > $(distdir)/ChangeLog; \
	fi
