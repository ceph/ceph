unitfiles = \
	ceph.target \
        ceph-osd.target \
        ceph-mon.target \
        ceph-mds.target \
        ceph-radosgw.target \
	ceph-rbd-mirror.target \
	ceph-mds@.service \
	ceph-mon@.service \
	ceph-osd@.service \
	ceph-radosgw@.service \
	ceph-rbd-mirror@.service \
	ceph-disk@.service \
	rbdmap.service

unitdir = $(systemd_unit_dir)

unit_DATA = $(unitfiles)

EXTRA_DIST = \
	$(unitfiles) \
	ceph \
	ceph.tmpfiles.d \
	50-ceph.preset
