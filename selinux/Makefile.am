EXTRA_DIST = \
	ceph.te \
	ceph.fc \
	ceph.if

SELINUXROOT = $(DESTDIR)$(datadir)/selinux

ceph.pp: ceph.te ceph.fc ceph.if
	$(MAKE) -j1 -f $(datadir)/selinux/devel/Makefile ceph.pp

if ENABLE_SERVER
if WITH_SELINUX
all-local: ceph.pp

install-exec-local:
	$(INSTALL) -d $(SELINUXROOT)/packages
	$(INSTALL) -m 644 ceph.pp $(SELINUXROOT)/packages/
	$(INSTALL) -d $(SELINUXROOT)/devel/include/contrib
	$(INSTALL) -m 644 ceph.if $(SELINUXROOT)/devel/include/contrib/

endif
endif
