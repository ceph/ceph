if ENABLE_CLIENT

if WITH_RADOS

rados_includedir = $(includedir)/rados
rados_include_DATA = \
	$(srcdir)/include/rados/librados.h \
	$(srcdir)/include/rados/rados_types.h \
	$(srcdir)/include/rados/rados_types.hpp \
	$(srcdir)/include/rados/librados.hpp \
	$(srcdir)/include/buffer.h \
	$(srcdir)/include/page.h \
	$(srcdir)/include/crc32c.h \
	$(srcdir)/include/memory.h


if WITH_RBD
librbd_includedir = $(includedir)/rbd
librbd_include_DATA = \
	$(srcdir)/include/rbd/features.h \
	$(srcdir)/include/rbd/librbd.h \
	$(srcdir)/include/rbd/librbd.hpp
endif # WITH_RBD


if WITH_RADOSSTRIPER
radosstriper_includedir = $(includedir)/radosstriper
radosstriper_include_DATA = \
	$(srcdir)/include/radosstriper/libradosstriper.h \
	$(srcdir)/include/radosstriper/libradosstriper.hpp
endif # WITH_RADOSSTRIPER


if WITH_CEPHFS
libcephfs_includedir = $(includedir)/cephfs
libcephfs_include_DATA = $(srcdir)/include/cephfs/libcephfs.h
endif # WITH_CEPHFS


endif # WITH_RADOS

endif # ENABLE_CLIENT

noinst_HEADERS += \
	include/Context.h \
	include/CompatSet.h \
	include/Distribution.h \
	include/Spinlock.h \
	include/addr_parsing.h \
	include/assert.h \
	include/atomic.h \
	include/bitmapper.h \
	include/blobhash.h \
	include/buffer.h \
	include/byteorder.h \
	include/cephfs/libcephfs.h \
	include/ceph_features.h \
	include/ceph_frag.h \
	include/ceph_fs.h \
	include/ceph_hash.h \
	include/cmp.h \
	include/color.h \
	include/compat.h \
	include/sock_compat.h \
	include/crc32c.h \
	include/encoding.h \
	include/err.h \
	include/error.h \
	include/filepath.h \
	include/frag.h \
	include/hash.h \
	include/inline_memory.h \
	include/intarith.h \
	include/interval_set.h \
	include/int_types.h \
	include/ipaddr.h \
	include/krbd.h \
	include/linux_fiemap.h \
	include/lru.h \
	include/msgr.h \
	include/object.h \
	include/page.h \
	include/rangeset.h \
	include/rados.h \
	include/rbd_types.h \
	include/statlite.h \
	include/str_list.h \
	include/str_map.h \
	include/stringify.h \
	include/types.h \
	include/utime.h \
	include/elist.h \
	include/uuid.h \
	include/xlist.h \
	include/compact_map.h \
	include/compact_set.h \
	include/rados/librados.h \
	include/rados/rados_types.h \
	include/rados/rados_types.hpp \
	include/rados/librados.hpp \
	include/rados/librgw.h \
	include/rados/page.h \
	include/rados/crc32c.h \
	include/rados/buffer.h \
	include/radosstriper/libradosstriper.h \
	include/radosstriper/libradosstriper.hpp \
	include/rbd/features.h \
	include/rbd/librbd.h \
	include/rbd/librbd.hpp\
	include/rbd/object_map_types.h \
	include/util.h\
	include/stat.h \
	include/on_exit.h \
	include/memory.h \
	include/rados/memory.h \
	include/unordered_set.h \
	include/unordered_map.h \
	include/timegm.h \
	include/event_type.h
