if ENABLE_CLIENT

# inject rgw stuff in the decoder testcase
DENCODER_SOURCES += \
	rgw/rgw_dencoder.cc \
	rgw/rgw_acl.cc \
	rgw/rgw_basic_types.cc \
	rgw/rgw_common.cc \
	rgw/rgw_env.cc \
	rgw/rgw_json_enc.cc \
	rgw/rgw_keystone.cc

DENCODER_DEPS += -lcurl -lexpat \
	libcls_version_client.la \
	libcls_log_client.la \
	libcls_refcount_client.la \
	libcls_user_client.la \
	libcls_timeindex_client.la \
	libcls_statelog_client.la

if WITH_RADOS
if WITH_RADOSGW

CIVETWEB_INCLUDE = -I$(builddir)/include
CIVETWEB_DEFINES = -DNO_SSL_DL -DUSE_IPV6

librgw_la_SOURCES = \
	rgw/rgw_acl.cc \
	rgw/rgw_acl_s3.cc \
	rgw/rgw_acl_swift.cc \
	rgw/rgw_coroutine.cc \
	rgw/rgw_cr_rados.cc \
	rgw/rgw_tools.cc \
	rgw/rgw_basic_types.cc \
	rgw/rgw_bucket.cc \
	rgw/rgw_cache.cc \
	rgw/rgw_client_io.cc \
	rgw/rgw_common.cc \
	rgw/rgw_cors.cc \
	rgw/rgw_cors_s3.cc \
	rgw/rgw_dencoder.cc \
	rgw/rgw_env.cc \
	rgw/rgw_fcgi.cc \
	rgw/rgw_formats.cc \
	rgw/rgw_frontend.cc \
	rgw/rgw_gc.cc \
	rgw/rgw_http_client.cc \
	rgw/rgw_json_enc.cc \
	rgw/rgw_keystone.cc \
	rgw/rgw_loadgen.cc \
	rgw/rgw_log.cc \
	rgw/rgw_metadata.cc \
	rgw/rgw_multi.cc \
	rgw/rgw_multi_del.cc \
	rgw/rgw_auth_s3.cc \
	rgw/rgw_period_history.cc \
	rgw/rgw_period_puller.cc \
	rgw/rgw_period_pusher.cc \
	rgw/rgw_realm_reloader.cc \
	rgw/rgw_realm_watcher.cc \
	rgw/rgw_sync.cc \
	rgw/rgw_data_sync.cc \
	rgw/rgw_object_expirer_core.cc \
	rgw/rgw_op.cc \
	rgw/rgw_os_lib.cc \
	rgw/rgw_policy_s3.cc \
	rgw/rgw_process.cc \
	rgw/rgw_quota.cc \
	rgw/rgw_rados.cc \
	rgw/rgw_replica_log.cc \
	rgw/rgw_request.cc \
	rgw/rgw_resolve.cc \
	rgw/rgw_rest_bucket.cc \
	rgw/rgw_rest.cc \
	rgw/rgw_rest_client.cc \
	rgw/rgw_rest_config.cc \
	rgw/rgw_rest_conn.cc \
	rgw/rgw_rest_log.cc \
	rgw/rgw_rest_metadata.cc \
	rgw/rgw_rest_opstate.cc \
	rgw/rgw_rest_realm.cc \
	rgw/rgw_rest_replica_log.cc \
	rgw/rgw_rest_objstore.cc \
	rgw/rgw_rest_s3.cc \
	rgw/rgw_rest_swift.cc \
	rgw/rgw_rest_usage.cc \
	rgw/rgw_rest_user.cc \
	rgw/rgw_swift_auth.cc \
	rgw/rgw_swift.cc \
	rgw/rgw_usage.cc \
	rgw/rgw_user.cc \
	rgw/rgw_file.cc \
	rgw/librgw.cc \
	rgw/rgw_xml.cc \
	rgw/rgw_xml_enc.cc \
	rgw/rgw_website.cc

librgw_la_CXXFLAGS = -Woverloaded-virtual -fPIC -I$(srcdir)/xxHash \
	${AM_CXXFLAGS} ${CIVETWEB_INCLUDE}
# noinst_LTLIBRARIES += librgw.la

LIBRGW_DEPS += \
	$(LIBRADOS) \
	libcls_rgw_client.la \
	libcls_log_client.la \
	libcls_statelog_client.la \
	libcls_timeindex_client.la \
	libcls_user_client.la \
	libcls_replica_log_client.la \
	libcls_lock_client.la \
	libcls_refcount_client.la \
	libcls_version_client.la \
	-lcurl \
	-lexpat \
	-lm \
	-lfcgi \
	-ldl

librgw_la_LIBADD = $(LIBRGW_DEPS) \
	$(PTHREAD_LIBS) $(RESOLV_LIBS) libglobal.la \
	$(EXTRALIBS)

librgw_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0

lib_LTLIBRARIES += librgw.la

BUILT_SOURCES += include/civetweb
include/civetweb:
	mkdir -p $(builddir)/include
	ln -sf "`readlink -f $(srcdir)/civetweb/include`" $(builddir)/include/civetweb

libcivetweb_la_SOURCES =  \
	rgw/rgw_civetweb.cc \
	rgw/rgw_civetweb_log.cc \
	civetweb/src/civetweb.c

libcivetweb_la_CXXFLAGS = ${CIVETWEB_INCLUDE} -fPIC -Woverloaded-virtual \
	${AM_CXXFLAGS}
libcivetweb_la_CFLAGS = ${CIVETWEB_INCLUDE} ${CIVETWEB_DEFINES} -I$(builddir)/include/civetweb -fPIC
LIBCIVETWEB_DEPS += -lssl -lcrypto

noinst_LTLIBRARIES += libcivetweb.la

radosgw_SOURCES = \
    rgw/rgw_fcgi_process.cc \
	rgw/rgw_loadgen_process.cc \
	rgw/rgw_civetweb.cc \
	rgw/rgw_civetweb_frontend.cc \
	rgw/rgw_civetweb_log.cc \
	civetweb/src/civetweb.c \
	rgw/rgw_main.cc

radosgw_CXXFLAGS = -fPIC -I$(srcdir)/xxHash ${CIVETWEB_INCLUDE}
radosgw_CFLAGS = -fPIC -I$(srcdir)/xxHash ${CIVETWEB_INCLUDE}  ${CIVETWEB_DEFINES} -I$(builddir)/include/civetweb
radosgw_LDADD = $(LIBRGW) $(LIBCIVETWEB) $(LIBCIVETWEB_DEPS) $(LIBRGW_DEPS) \
	$(RESOLV_LIBS) $(CEPH_GLOBAL)
bin_PROGRAMS += radosgw

radosgw_admin_SOURCES = rgw/rgw_admin.cc rgw/rgw_orphan.cc
radosgw_admin_LDADD = $(LIBRGW) $(LIBRGW_DEPS) $(CEPH_GLOBAL)
bin_PROGRAMS += radosgw-admin

radosgw_object_expirer_SOURCES = rgw/rgw_object_expirer.cc
radosgw_object_expirer_LDADD = $(LIBRGW) $(LIBRGW_DEPS) $(CEPH_GLOBAL)
bin_PROGRAMS += radosgw-object-expirer

ceph_rgw_multiparser_SOURCES = rgw/rgw_multiparser.cc
ceph_rgw_multiparser_LDADD = $(LIBRGW) $(LIBRGW_DEPS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_rgw_multiparser

ceph_rgw_jsonparser_SOURCES = \
	rgw/rgw_jsonparser.cc \
	rgw/rgw_common.cc \
	rgw/rgw_env.cc \
	rgw/rgw_json_enc.cc
ceph_rgw_jsonparser_LDADD = $(LIBRGW) $(LIBRGW_DEPS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_rgw_jsonparser

rgw_sts_SOURCES = \
	sts/sts_main.cc \
	sts/rgw_rest_sts.cc \
	sts/rgw_dummy.cc
rgw_sts_CXXFLAGS = -fPIC -I$(srcdir)/xxHash -I$(srcdir)/rgw ${CIVETWEB_INCLUDE}
rgw_sts_LDFLAGS = -Wl,-z,muldefs
rgw_sts_LDADD = $(LIBRGW) $(LIBCIVETWEB) $(LIBCIVETWEB_DEPS) $(LIBRGW_DEPS) \
	$(RESOLV_LIBS) $(CEPH_GLOBAL)
bin_PROGRAMS += rgw-sts

noinst_HEADERS += \
	rgw/rgw_acl.h \
	rgw/rgw_acl_s3.h \
	rgw/rgw_acl_swift.h \
	rgw/rgw_client_io.h \
	rgw/rgw_coroutine.h \
	rgw/rgw_cr_rados.h \
	rgw/rgw_cr_rest.h \
	rgw/rgw_fcgi.h \
	rgw/rgw_xml.h \
	rgw/rgw_basic_types.h \
	rgw/rgw_cache.h \
	rgw/rgw_common.h \
	rgw/rgw_cors.h \
	rgw/rgw_cors_s3.h \
	rgw/rgw_cors_swift.h \
	rgw/rgw_string.h \
	rgw/rgw_file.h \
	rgw/rgw_formats.h \
	rgw/rgw_http_errors.h \
	rgw/rgw_lib.h \
	rgw/rgw_lib_frontend.h \
	rgw/rgw_log.h \
	rgw/rgw_loadgen.h \
	rgw/rgw_process.h \
	rgw/rgw_request.h \
	rgw/rgw_frontend.h \
	rgw/rgw_multi.h \
	rgw/rgw_policy_s3.h \
	rgw/rgw_gc.h \
	rgw/rgw_metadata.h \
	rgw/rgw_meta_sync_status.h \
	rgw/rgw_multi_del.h \
	rgw/rgw_object_expirer_core.h \
	rgw/rgw_op.h \
	rgw/rgw_os_lib.h \
	rgw/rgw_orphan.h \
	rgw/rgw_http_client.h \
	rgw/rgw_swift.h \
	rgw/rgw_swift_auth.h \
	rgw/rgw_quota.h \
	rgw/rgw_rados.h \
	rgw/rgw_replica_log.h \
	rgw/rgw_resolve.h \
	rgw/rgw_rest.h \
	rgw/rgw_rest_swift.h \
	rgw/rgw_rest_s3.h \
	rgw/rgw_auth_s3.h \
	rgw/rgw_rest_admin.h \
	rgw/rgw_rest_usage.h \
	rgw/rgw_rest_user.h \
	rgw/rgw_rest_bucket.h \
	rgw/rgw_rest_client.h \
	rgw/rgw_rest_conn.h \
	rgw/rgw_tools.h \
	rgw/rgw_rest_metadata.h \
	rgw/rgw_rest_log.h \
	rgw/rgw_rest_opstate.h \
	rgw/rgw_rest_replica_log.h \
	rgw/rgw_rest_config.h \
	rgw/rgw_rest_realm.h \
	rgw/rgw_sync.h \
	rgw/rgw_data_sync.h \
	rgw/rgw_usage.h \
	rgw/rgw_user.h \
	rgw/rgw_bucket.h \
	rgw/rgw_keystone.h \
	rgw/rgw_period_history.h \
	rgw/rgw_period_pusher.h \
	rgw/rgw_period_puller.h \
	rgw/rgw_realm_reloader.h \
	rgw/rgw_realm_watcher.h \
	rgw/rgw_civetweb.h \
	rgw/rgw_boost_asio_coroutine.h \
	rgw/rgw_boost_asio_yield.h \
	rgw/rgw_civetweb_log.h \
	rgw/rgw_website.h \
	rgw/rgw_rest_s3website.h \
	civetweb/include/civetweb.h

endif # WITH_RADOSGW
endif # WITH_RADOS
endif # ENABLE_CLIENT
