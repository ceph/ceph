if WITH_RADOSGW
librgw_la_SOURCES =  \
	rgw/librgw.cc \
	rgw/rgw_acl.cc \
	rgw/rgw_acl_s3.cc \
	rgw/rgw_acl_swift.cc \
	rgw/rgw_client_io.cc \
	rgw/rgw_fcgi.cc \
	rgw/rgw_xml.cc \
	rgw/rgw_usage.cc \
	rgw/rgw_json_enc.cc \
	rgw/rgw_user.cc \
	rgw/rgw_bucket.cc\
	rgw/rgw_tools.cc \
	rgw/rgw_rados.cc \
	rgw/rgw_http_client.cc \
	rgw/rgw_rest_client.cc \
	rgw/rgw_rest_conn.cc \
	rgw/rgw_op.cc \
	rgw/rgw_common.cc \
	rgw/rgw_cache.cc \
	rgw/rgw_formats.cc \
	rgw/rgw_log.cc \
	rgw/rgw_multi.cc \
	rgw/rgw_policy_s3.cc \
	rgw/rgw_gc.cc \
	rgw/rgw_multi_del.cc \
	rgw/rgw_env.cc \
	rgw/rgw_cors.cc \
	rgw/rgw_cors_s3.cc \
	rgw/rgw_auth_s3.cc \
	rgw/rgw_metadata.cc \
	rgw/rgw_replica_log.cc \
	rgw/rgw_keystone.cc \
	rgw/rgw_quota.cc
librgw_la_CXXFLAGS = -Woverloaded-virtual ${AM_CXXFLAGS}
noinst_LTLIBRARIES += librgw.la

LIBRGW_DEPS += \
	$(LIBRADOS) \
	libcls_rgw_client.la \
	libcls_log_client.a \
	libcls_statelog_client.a \
	libcls_replica_log_client.a \
	libcls_lock_client.la \
	libcls_refcount_client.la \
	libcls_version_client.a \
	-lcurl \
	-lexpat \
	-lm \
	-lfcgi

radosgw_SOURCES = \
	rgw/rgw_resolve.cc \
	rgw/rgw_rest.cc \
	rgw/rgw_rest_swift.cc \
	rgw/rgw_rest_s3.cc \
	rgw/rgw_rest_usage.cc \
	rgw/rgw_rest_user.cc \
	rgw/rgw_rest_bucket.cc \
	rgw/rgw_rest_metadata.cc \
	rgw/rgw_replica_log.cc \
	rgw/rgw_rest_log.cc \
	rgw/rgw_rest_opstate.cc \
	rgw/rgw_rest_replica_log.cc \
	rgw/rgw_rest_config.cc \
	rgw/rgw_http_client.cc \
	rgw/rgw_swift.cc \
	rgw/rgw_swift_auth.cc \
	rgw/rgw_main.cc
radosgw_LDADD = $(LIBRGW) $(LIBRGW_DEPS) $(RESOLV_LIBS) $(CEPH_GLOBAL)
bin_PROGRAMS += radosgw

radosgw_admin_SOURCES = rgw/rgw_admin.cc
radosgw_admin_LDADD = $(LIBRGW) $(LIBRGW_DEPS) $(CEPH_GLOBAL)
bin_PROGRAMS += radosgw-admin

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

# inject rgw stuff in the decoder testcase
DENCODER_SOURCES += \
	rgw/rgw_dencoder.cc \
	rgw/rgw_acl.cc \
	rgw/rgw_common.cc \
	rgw/rgw_env.cc \
	rgw/rgw_json_enc.cc


endif # WITH_RADOSGW


noinst_HEADERS += \
	rgw/logrotate.conf \
	rgw/rgw_acl.h \
	rgw/rgw_acl_s3.h \
	rgw/rgw_acl_swift.h \
	rgw/rgw_client_io.h \
	rgw/rgw_fcgi.h \
	rgw/rgw_xml.h \
	rgw/rgw_cache.h \
	rgw/rgw_common.h \
	rgw/rgw_cors.h \
	rgw/rgw_cors_s3.h \
	rgw/rgw_cors_swift.h \
	rgw/rgw_string.h \
	rgw/rgw_formats.h \
	rgw/rgw_http_errors.h \
	rgw/rgw_log.h \
	rgw/rgw_multi.h \
	rgw/rgw_policy_s3.h \
	rgw/rgw_gc.h \
	rgw/rgw_metadata.h \
	rgw/rgw_multi_del.h \
	rgw/rgw_op.h \
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
	rgw/rgw_usage.h \
	rgw/rgw_user.h \
	rgw/rgw_bucket.h \
	rgw/rgw_keystone.h

