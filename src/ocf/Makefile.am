EXTRA_DIST = Makefile.in

if WITH_OCF
# The root of the OCF resource agent hierarchy
# Per the OCF standard, it's always "lib",
# not "lib64" (even on 64-bit platforms).
ocfdir = $(prefix)/lib/ocf

# The ceph provider directory
radir = $(ocfdir)/resource.d/$(PACKAGE_NAME)

ra_SCRIPTS = rbd
endif
