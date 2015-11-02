unitfiles = \
	ceph.target \
	ceph-mds@.service \
	ceph-mon@.service \
	ceph-create-keys@.service \
	ceph-osd@.service \
	ceph-radosgw@.service \
	ceph-disk@.service \
	rbdmap.service

unitdir = $(systemd_unit_dir)

unit_DATA = $(unitfiles)

EXTRA_DIST = \
	$(unitfiles) \
	ceph \
	ceph.tmpfiles.d \
	ceph-radosgw-prestart.sh
