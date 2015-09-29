# fio_ceph_objectstore
libfio_ceph_objectstore_la_SOURCES = test/fio/fio_ceph_objectstore.cc
libfio_ceph_objectstore_la_LIBADD = $(LIBOS) $(LIBGLOBAL) $(CRYPTO_LIBS) $(EXTRALIBS)
libfio_ceph_objectstore_la_LDFLAGS = -rdynamic -shared -fPIC
libfio_ceph_objectstore_la_CXXFLAGS = ${AM_CXXFLAGS} -I$(FIO_INC_DIR) -std=gnu++11
lib_LTLIBRARIES += libfio_ceph_objectstore.la
