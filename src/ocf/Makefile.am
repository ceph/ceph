EXTRA_DIST = ceph.in Makefile.in

if WITH_OCF
# The root of the OCF resource agent hierarchy
# Per the OCF standard, it's always "lib",
# not "lib64" (even on 64-bit platforms).
ocfdir = $(prefix)/lib/ocf

# The ceph provider directory
radir = $(ocfdir)/resource.d/$(PACKAGE_NAME)

ra_SCRIPTS = ceph rbd

install-data-hook:
	$(LN_S) ceph $(DESTDIR)$(radir)/osd
	$(LN_S) ceph $(DESTDIR)$(radir)/mds
	$(LN_S) ceph $(DESTDIR)$(radir)/mon

uninstall-hook:
	rm -f $(DESTDIR)$(radir)/osd
	rm -f $(DESTDIR)$(radir)/mds
	rm -f $(DESTDIR)$(radir)/mon
endif
