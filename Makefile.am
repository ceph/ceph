AUTOMAKE_OPTIONS = gnu
EXTRA_DIST = autogen.sh ceph.spec.in
# the "." here makes sure check-local builds gtest before it is used
SUBDIRS = . src qa man

EXTRA_DIST += \
	src/test/run-cli-tests \
	src/test/cli \
	src/test/downloads
check-local:
#	Build gtest before we build our own tests. Doing this instead
#	of SUBDIRS because with that, gtest's own tests would be run
#	and that would slow us down.
	@echo "Making lib/libgtest.a lib/libgtest_main.a in src/gtest"
	@cd src/gtest && $(MAKE) $(AM_MAKEFLAGS) lib/libgtest.la lib/libgtest_main.la

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
