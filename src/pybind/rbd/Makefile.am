EXTRA_DIST += \
	$(srcdir)/pybind/rbd/CMakeLists.txt \
	$(srcdir)/pybind/rbd/setup.py \
	$(srcdir)/pybind/rbd/rbd.pyx

rbd-pybind-all: librbd.la ${srcdir}/ceph_ver.h
	cd $(srcdir)/pybind/rbd; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

rbd-pybind-clean: ${srcdir}/ceph_ver.h
	cd $(srcdir)/pybind/rbd; $(PY_DISTUTILS) clean \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

rbd-pybind-install-exec: ${srcdir}/ceph_ver.h
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
	cd $(srcdir)/pybind/rbd; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	install \
	$$options $$root \
	--single-version-externally-managed \
	--record /dev/null \
	--verbose

LOCAL_ALL += rbd-pybind-all
LOCAL_CLEAN += rbd-pybind-clean
LOCAL_INSTALLEXEC += rbd-pybind-install-exec
