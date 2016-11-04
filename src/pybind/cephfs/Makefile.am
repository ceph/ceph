EXTRA_DIST += \
	$(srcdir)/pybind/cephfs/CMakeLists.txt \
	$(srcdir)/pybind/cephfs/setup.py \
	$(srcdir)/pybind/cephfs/cephfs.pyx

cephfs-pybind-all: libcephfs.la ${srcdir}/ceph_ver.h
	cd $(srcdir)/pybind/cephfs; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

cephfs-pybind-clean: ${srcdir}/ceph_ver.h
	cd $(srcdir)/pybind/cephfs; $(PY_DISTUTILS) clean \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

cephfs-pybind-install-exec: ${srcdir}/ceph_ver.h
	if test "$(DESTDIR)" ; then \
		if lsb_release -si | grep --quiet 'Ubuntu\|Debian\|Devuan' ; then \
			options=--install-layout=deb ; \
		else \
			options=--prefix=/usr ; \
		fi ; \
		root="--root=$(DESTDIR)" ; \
	else \
		options=--prefix=$(prefix) ; \
	fi ; \
	cd $(srcdir)/pybind/cephfs; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	install \
	$$options $$root \
	--single-version-externally-managed \
	--record /dev/null \
	--verbose

LOCAL_ALL += cephfs-pybind-all
LOCAL_CLEAN += cephfs-pybind-clean
LOCAL_INSTALLEXEC += cephfs-pybind-install-exec
