#
# Copyright (C) 2015 SUSE LINUX GmbH
# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Owen Synge <osynge@suse.com>
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see `<http://www.gnu.org/licenses/>`.
#
check_SCRIPTS += ceph-detect-init/run-tox.sh

EXTRA_DIST += \
	ceph-detect-init/AUTHORS.rst \
	ceph-detect-init/ceph_detect_init/centos/__init__.py \
	ceph-detect-init/ceph_detect_init/exc.py \
	ceph-detect-init/ceph_detect_init/main.py \
	ceph-detect-init/ceph_detect_init/__init__.py \
	ceph-detect-init/ceph_detect_init/rhel/__init__.py \
	ceph-detect-init/ceph_detect_init/fedora/__init__.py \
	ceph-detect-init/ceph_detect_init/debian/__init__.py \
	ceph-detect-init/ceph_detect_init/suse/__init__.py \
	ceph-detect-init/integration/centos-6.dockerfile \
	ceph-detect-init/integration/debian-wheezy.dockerfile \
	ceph-detect-init/integration/debian-sid.dockerfile \
	ceph-detect-init/integration/debian-jessie.dockerfile \
	ceph-detect-init/integration/opensuse-13.1.dockerfile \
	ceph-detect-init/integration/fedora-21.dockerfile \
	ceph-detect-init/integration/ubuntu-14.04.dockerfile \
	ceph-detect-init/integration/test_main.py \
	ceph-detect-init/integration/opensuse-13.2.dockerfile \
	ceph-detect-init/integration/ubuntu-12.04.dockerfile \
	ceph-detect-init/integration/centos-7.dockerfile \
	ceph-detect-init/integration/ubuntu-15.04.dockerfile \
	ceph-detect-init/integration/debian-squeeze.dockerfile \
	ceph-detect-init/Makefile.am \
	ceph-detect-init/MANIFEST.in \
	ceph-detect-init/README.rst \
	ceph-detect-init/requirements.txt \
	ceph-detect-init/run-tox.sh \
	ceph-detect-init/setup.py \
	ceph-detect-init/test-requirements.txt \
	ceph-detect-init/tests/test_all.py \
	ceph-detect-init/tox.ini

export CEPH_DETECT_INIT_VIRTUALENV = ${CEPH_BUILD_VIRTUALENV}/ceph-detect-init-virtualenv

ceph-detect-init-all: ${CEPH_DETECT_INIT_VIRTUALENV}

${CEPH_DETECT_INIT_VIRTUALENV}:
	cd $(srcdir)/ceph-detect-init ; ../tools/setup-virtualenv.sh ${CEPH_DETECT_INIT_VIRTUALENV} ; test -d wheelhouse && export NO_INDEX=--no-index ; ${CEPH_DETECT_INIT_VIRTUALENV}/bin/pip install $$NO_INDEX --use-wheel --find-links=file://$$(pwd)/wheelhouse -e .

ceph-detect-init-clean:
	cd $(srcdir)/ceph-detect-init ; python setup.py clean ; rm -fr wheelhouse .tox build ${CEPH_DETECT_INIT_VIRTUALENV} .coverage *.egg-info

ceph-detect-init-install-data:
	cd $(srcdir)/ceph-detect-init ; \
	if test "$(DESTDIR)" ; then \
		if lsb_release -si | grep --quiet 'Ubuntu\|Debian\|Devuan' ; then \
			options=--install-layout=deb ; \
		else \
			options=--prefix=$(prefix) ; \
		fi ; \
		root="--root=$(DESTDIR)" ; \
	fi ; \
	python setup.py install $$root $$options

LOCAL_ALL += ceph-detect-init-all
LOCAL_CLEAN += ceph-detect-init-clean
LOCAL_INSTALLDATA += ceph-detect-init-install-data
