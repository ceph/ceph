## Rados object classes

if ENABLE_SERVER
libcls_hello_la_SOURCES = cls/hello/cls_hello.cc
libcls_hello_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_hello_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_hello.la

libcls_rbd_la_SOURCES = cls/rbd/cls_rbd.cc
libcls_rbd_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_rbd_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_rbd.la

libcls_lock_la_SOURCES = cls/lock/cls_lock.cc
libcls_lock_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_lock_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_lock.la

libcls_refcount_la_SOURCES = \
	cls/refcount/cls_refcount.cc \
	cls/refcount/cls_refcount_ops.cc \
	common/ceph_json.cc 
libcls_refcount_la_LIBADD = libjson_spirit.la $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_refcount_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_refcount.la

libcls_version_la_SOURCES = cls/version/cls_version.cc
libcls_version_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_version_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_version.la

libcls_log_la_SOURCES = cls/log/cls_log.cc
libcls_log_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_log_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_log.la

libcls_statelog_la_SOURCES = cls/statelog/cls_statelog.cc
libcls_statelog_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_statelog_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_statelog.la

libcls_replica_log_la_SOURCES = cls/replica_log/cls_replica_log.cc
libcls_replica_log_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_replica_log_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_replica_log.la

libcls_user_la_SOURCES = cls/user/cls_user.cc
libcls_user_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_user_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_user.la

libcls_rgw_la_SOURCES = \
	cls/rgw/cls_rgw.cc \
	cls/rgw/cls_rgw_ops.cc \
	cls/rgw/cls_rgw_types.cc \
	common/ceph_json.cc
libcls_rgw_la_LIBADD = libjson_spirit.la $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_rgw_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_rgw.la
endif # ENABLE_SERVER

## Rados object client classes

libcls_lock_client_la_SOURCES = \
	cls/lock/cls_lock_client.cc \
	cls/lock/cls_lock_types.cc \
	cls/lock/cls_lock_ops.cc
noinst_LTLIBRARIES += libcls_lock_client.la
DENCODER_DEPS += libcls_lock_client.la

libcls_refcount_client_la_SOURCES = \
	cls/refcount/cls_refcount_client.cc \
	cls/refcount/cls_refcount_ops.cc
noinst_LTLIBRARIES += libcls_refcount_client.la
DENCODER_DEPS += libcls_refcount_client.la

libcls_version_client_a_SOURCES =  \
	cls/version/cls_version_client.cc \
	cls/version/cls_version_types.cc
noinst_LIBRARIES += libcls_version_client.a

libcls_log_client_a_SOURCES = cls/log/cls_log_client.cc
noinst_LIBRARIES += libcls_log_client.a

libcls_statelog_client_a_SOURCES = cls/statelog/cls_statelog_client.cc
noinst_LIBRARIES += libcls_statelog_client.a

libcls_replica_log_client_a_SOURCES = \
	cls/replica_log/cls_replica_log_types.cc \
	cls/replica_log/cls_replica_log_ops.cc \
	cls/replica_log/cls_replica_log_client.cc
noinst_LIBRARIES += libcls_replica_log_client.a
DENCODER_DEPS += libcls_replica_log_client.a

libcls_rgw_client_la_SOURCES = \
	cls/rgw/cls_rgw_client.cc \
	cls/rgw/cls_rgw_types.cc \
	cls/rgw/cls_rgw_ops.cc
noinst_LTLIBRARIES += libcls_rgw_client.la
DENCODER_DEPS += libcls_rgw_client.la

libcls_rbd_client_la_SOURCES = cls/rbd/cls_rbd_client.cc
noinst_LTLIBRARIES += libcls_rbd_client.la

libcls_user_client_a_SOURCES = cls/user/cls_user_client.cc \
	cls/user/cls_user_types.cc \
	cls/user/cls_user_ops.cc
DENCODER_DEPS += libcls_user_client.a

noinst_LIBRARIES += libcls_user_client.a

noinst_HEADERS += \
	cls/lock/cls_lock_types.h \
	cls/lock/cls_lock_ops.h \
	cls/lock/cls_lock_client.h \
	cls/rbd/cls_rbd.h \
	cls/rbd/cls_rbd_client.h \
	cls/refcount/cls_refcount_ops.h \
	cls/refcount/cls_refcount_client.h \
	cls/version/cls_version_types.h \
	cls/version/cls_version_ops.h \
	cls/version/cls_version_client.h \
	cls/log/cls_log_types.h \
	cls/log/cls_log_ops.h \
	cls/log/cls_log_client.h \
	cls/statelog/cls_statelog_types.h \
	cls/statelog/cls_statelog_ops.h \
	cls/statelog/cls_statelog_client.h \
	cls/replica_log/cls_replica_log_types.h \
	cls/replica_log/cls_replica_log_ops.h \
	cls/replica_log/cls_replica_log_client.h \
	cls/rgw/cls_rgw_client.h \
	cls/rgw/cls_rgw_ops.h \
	cls/rgw/cls_rgw_types.h \
	cls/user/cls_user_client.h \
	cls/user/cls_user_ops.h \
	cls/user/cls_user_types.h

