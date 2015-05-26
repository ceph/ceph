unitfiles = \
	ceph.target \
	ceph-mds@.service \
	ceph-mon@.service \
	ceph-osd@.service

unitdir = $(systemd_unit_dir)

unit_DATA = $(unitfiles)

EXTRA_DIST = $(unitfiles)
