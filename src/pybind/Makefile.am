
if ENABLE_CLIENT
if WITH_CYTHON

CYTHON_BUILD_DIR="$(shell readlink -f $(builddir))/build"

PY_DISTUTILS = \
	mkdir -p $(CYTHON_BUILD_DIR); \
	CC="${CC}" \
	CXX="${CXX}" \
	LDSHARED="${CC} -shared" \
	CPPFLAGS="-iquote \${abs_srcdir}/include ${AM_CPPFLAGS} ${CPPFLAGS}" \
	CFLAGS="-iquote \${abs_srcdir}/include ${AM_CFLAGS} ${PYTHON_CFLAGS}" \
	LDFLAGS="-L\${abs_builddir}/.libs $(subst -pie,,${AM_LDFLAGS}) ${PYTHON_LDFLAGS}" \
	CYTHON_BUILD_DIR=$(CYTHON_BUILD_DIR) \
	${PYTHON} ./setup.py

if WITH_RADOS
include pybind/rados/Makefile.am
if WITH_RBD
include pybind/rbd/Makefile.am
endif # WITH_RBD
if WITH_CEPHFS
include pybind/cephfs/Makefile.am
endif # WITH_CEPHFS
endif # WITH_RADOS

endif # WITH_CYTHON
endif # ENABLE_CLIENT

EXTRA_DIST += pybind/CMakeLists.txt
