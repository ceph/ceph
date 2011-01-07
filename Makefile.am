AUTOMAKE_OPTIONS = gnu
EXTRA_DIST = autogen.sh ceph.spec.in
# the "." here makes sure check-local builds gtest before it is used
SUBDIRS = . src man


# Build gtest before we build our own tests. Doing this instead of
# SUBDIRS gtest's own tests being run and slowing us down.
check-local:
	@echo "Making lib/libgtest.a lib/libgtest_main.a in src/gtest"
	@cd src/gtest && $(MAKE) $(AM_MAKEFLAGS) lib/libgtest.la lib/libgtest_main.la

# "make distclean" both runs this and recurses into src/gtest, if
# gtest is in DIST_SUBDIRS. Take extra care to not fail when
# effectively cleaned twice.
clean-local:
	@if test -e src/gtest/Makefile; then \
	  echo "Making clean in src/gtest"; \
	  cd src/gtest && $(MAKE) $(AM_MAKEFLAGS) clean; \
	fi
