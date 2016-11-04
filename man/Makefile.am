AUTOMAKE_OPTIONS = gnu

EXTRA_DIST = conf.py ceph_selinux.8 CMakeLists.txt

dist_man_MANS = 

if WITH_MAN_PAGES
if ENABLE_CLIENT
include Makefile-client.am
endif

if ENABLE_SERVER
include Makefile-server.am
endif

# prevent `make` from running in parallel, sphinx runs better in batch mode.
.PHONY: sphinx-build.stamp

$(dist_man_MANS): sphinx-build.stamp

# in a tree populated from dist tarball, the $(top_srcdir)/doc is not included
sphinx-build.stamp:
	if [ -d $(top_srcdir)/doc/man ] ; then \
		${SPHINX_BUILD} -b man -d doctrees -c $(top_srcdir)/man $(top_srcdir)/doc/man $(top_builddir)/man; \
	fi

clean-local:
	@rm -rf doctrees

MAINTAINERCLEANFILES = $(dist_man_MANS)
endif
