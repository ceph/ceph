EXTRA_DIST += $(srcdir)/pybind/setup.py $(srcdir)/pybind/rbd.pyx

if ENABLE_CLIENT
if WITH_RADOS
if WITH_RBD
if WITH_CYTHON

PY_DISTUTILS = \
	CPPFLAGS="-iquote \${abs_srcdir}/include ${AM_CPPFLAGS} ${CPPFLAGS}" \
	CFLAGS="-iquote \${abs_srcdir}/include ${AM_CFLAGS} ${CFLAGS}" \
	LDFLAGS="-L\${abs_builddir}/.libs $(subst -pie,,${AM_LDFLAGS}) ${LDFLAGS}" \
	CYTHON_BUILD_DIR="$(shell readlink -f $(builddir))/build" \
	${PYTHON} ./setup.py

pybind-all: librbd.la ${srcdir}/ceph_ver.h
	cd $(srcdir)/pybind; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

pybind-clean: ${srcdir}/ceph_ver.h
	cd $(srcdir)/pybind; $(PY_DISTUTILS) clean \
	--build-base $(shell readlink -f $(builddir))/build \
	--verbose

pybind-install-exec: ${srcdir}/ceph_ver.h
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
	cd $(srcdir)/pybind; $(PY_DISTUTILS) build \
	--build-base $(shell readlink -f $(builddir))/build \
	install \
	$$options $$root \
	--single-version-externally-managed \
	--record /dev/null \
	--verbose

LOCAL_ALL += pybind-all
LOCAL_CLEAN += pybind-clean
LOCAL_INSTALLEXEC += pybind-install-exec

endif
endif
endif
endif
