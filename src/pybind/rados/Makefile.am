EXTRA_DIST += $(srcdir)/pybind/rados/setup.py $(srcdir)/pybind/rados/rados.pyx $(srcdir)/pybind/rados/rados.pxd

rados-pybind-all: librados.la ./ceph_ver.h
	cd $(srcdir)/pybind/rados; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

rados-pybind-clean: ./ceph_ver.h
	cd $(srcdir)/pybind/rados; $(PY_DISTUTILS) clean \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

rados-pybind-install-exec: ./ceph_ver.h
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
	cd $(srcdir)/pybind/rados; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	install \
	$$options $$root \
	--single-version-externally-managed \
	--record /dev/null \
	--verbose

LOCAL_ALL += rados-pybind-all
LOCAL_CLEAN += rados-pybind-clean
LOCAL_INSTALLEXEC += rados-pybind-install-exec

