#
# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
check_SCRIPTS += ceph-disk/run-tox.sh

EXTRA_DIST += \
	ceph-disk/AUTHORS.rst \
	ceph-disk/ceph_disk/__init__.py \
	ceph-disk/ceph_disk/main.py \
	ceph-disk/Makefile.am \
	ceph-disk/MANIFEST.in \
	ceph-disk/README.rst \
	ceph-disk/requirements.txt \
	ceph-disk/run-tox.sh \
	ceph-disk/setup.py \
	ceph-disk/test-requirements.txt \
	ceph-disk/tests/test_main.py \
	ceph-disk/tox.ini

ceph-disk-all: ceph-disk/virtualenv

CEPH_DISK_VIRTUALENV := .

ceph-disk/virtualenv:
	cd $(srcdir)/ceph-disk ; ../tools/setup-virtualenv.sh ${CEPH_DISK_VIRTUALENV} ; test -d wheelhouse && export NO_INDEX=--no-index ; ${CEPH_DISK_VIRTUALENV}/virtualenv/bin/pip install $$NO_INDEX --use-wheel --find-links=file://$$(pwd)/wheelhouse -e .

ceph-disk-clean:
	cd $(srcdir)/ceph-disk ; python setup.py clean ; rm -fr wheelhouse .tox build ${CEPH_DISK_VIRTUALENV}/virtualenv .coverage *.egg-info

ceph-disk-install-data:
	cd $(srcdir)/ceph-disk ; \
	if test "$(DESTDIR)" ; then \
		if lsb_release -si | grep --quiet 'Ubuntu\|Debian\|Devuan' ; then \
			options=--install-layout=deb ; \
		else \
			options=--prefix=/usr ; \
		fi ; \
		root="--root=$(DESTDIR) --install-script=/usr/sbin" ; \
	fi ; \
	python setup.py install $$root $$options

LOCAL_ALL += ceph-disk-all
LOCAL_CLEAN += ceph-disk-clean
LOCAL_INSTALLDATA += ceph-disk-install-data
