if ENABLE_SERVER
if WITH_OSD

if LINUX
libcls_kvs_la_SOURCES = key_value_store/cls_kvs.cc
libcls_kvs_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_kvs_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared -export-symbols-regex '.*__cls_.*'
radoslib_LTLIBRARIES += libcls_kvs.la
endif

noinst_HEADERS += \
	key_value_store/key_value_structure.h \
	key_value_store/kv_flat_btree_async.h \
	key_value_store/kvs_arg_types.h

endif # WITH_OSD
endif # ENABLE_SERVER
