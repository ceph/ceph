unitfiles = \
	ceph.target \
	ceph-mds@.service \
	ceph-mon@.service \
	ceph-create-keys@.service \
	ceph-osd@.service \
	ceph-radosgw@.service \
	ceph-disk-activate-journal@.service \
	ceph-disk-activate@.service \
	ceph-disk-dmcrypt-activate@.service

unitdir = $(systemd_unit_dir)

unit_DATA = $(unitfiles)

EXTRA_DIST = \
	$(unitfiles) \
	ceph \
	ceph.tmpfiles.d \
	ceph-rgw.tmpfiles.d.in \
	ceph-radosgw-prestart.sh
