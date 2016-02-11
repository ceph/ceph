
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

include pybind/rbd/Makefile.am

endif
endif
endif
endif
