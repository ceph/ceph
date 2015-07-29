unitfiles = \
	ceph.target \
	ceph-mds@.service \
	ceph-mon@.service \
	ceph-create-keys@.service \
	ceph-osd@.service \
	ceph-radosgw@.service

unitdir = $(systemd_unit_dir)

unit_DATA = $(unitfiles)

EXTRA_DIST = \
	$(unitfiles) \
	ceph \
	ceph.tmpfiles.d \
	ceph-rgw.tmpfiles.d.in \
	ceph-radosgw-prestart.sh
