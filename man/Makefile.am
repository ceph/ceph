AUTOMAKE_OPTIONS = gnu

dist_man_MANS = 

if ENABLE_CLIENT
include Makefile-client.am
endif

if ENABLE_SERVER
include Makefile-server.am
endif

check-local:
	sphinx-build -b man -d doctrees . $(top_builddir)/man

# prevent `make` from running in parallel, sphinx runs better in batch mode.
.PHONY: sphinx-build.stamp

$(dist_man_MANS): sphinx-build.stamp

sphinx-build.stamp:
	sphinx-build -Q -b man -d doctrees -c . . $(top_builddir)/man

clean-local:
	@rm -f *.8
	@rm -rf doctrees
