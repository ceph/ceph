
#ifndef __LIBCEPHFSD_PROXY_REQUESTS_H__
#define __LIBCEPHFSD_PROXY_REQUESTS_H__

#include "proxy.h"
#include "proxy_link.h"

/* Macros to add and get data from communication buffers. */

#define CEPH_BUFF_ADD(_data, _ptr, _size)                             \
	do {                                                          \
		_data##_iov[_data##_count].iov_base = (void *)(_ptr); \
		_data##_iov[_data##_count].iov_len = (_size);         \
		_data##_count++;                                      \
	} while (0)

#define CEPH_DATA_ADD(_data, _field, _ptr, _size)           \
	do {                                                \
		(_data)._field = (_size);                   \
		CEPH_BUFF_ADD(_data, _ptr, (_data)._field); \
	} while (0)

#define CEPH_STR_ADD(_data, _field, _str)                                     \
	do {                                                                  \
		if ((_str) != NULL) {                                         \
			CEPH_DATA_ADD(_data, _field, _str, strlen(_str) + 1); \
		} else {                                                      \
			(_data)._field = 0;                                   \
		}                                                             \
	} while (0)

#define CEPH_STR_GET(_data, _field, _ptr)   \
	({                                  \
		const void *__ptr = (_ptr); \
		if ((_data)._field == 0) {  \
			__ptr = NULL;       \
		}                           \
		__ptr;                      \
	})

#define PROTO_VERSION_SIZE_EXPAND(_data, _ver) \
	field_size(__typeof__(_data), v##_ver)

#define PROTO_VERSION_SIZE(_data, _ver) PROTO_VERSION_SIZE_EXPAND(_data, _ver)

#define PROTO_VERSION(_neg, _data, _ver) \
	do { \
		if ((_neg)->v2.protocol >= (_ver)) { \
			_data##_iov[0].iov_len = PROTO_VERSION_SIZE(_data, \
								    _ver); \
		} \
	} while (false)

#define CEPH_DATA(_name, _data, _data_count)         \
	proxy_##_name##_##_data##_t _data;           \
	struct iovec _data##_iov[(_data_count) + 1]; \
	int32_t _data##_count = 0;                   \
	CEPH_BUFF_ADD(_data, &_data,                 \
		      PROTO_VERSION_SIZE(_data, PROXY_PROTOCOL_V0))

#define CEPH_REQ(_name, _req, _req_count, _ans, _ans_count) \
	CEPH_DATA(_name, _req, _req_count);                 \
	CEPH_DATA(_name, _ans, _ans_count)

#define CEPH_CBK(_name, _cbk, _cbk_count) \
	CEPH_DATA(_name, _cbk, _cbk_count)

#define CEPH_CALL(_sd, _op, _req, _ans)                                      \
	proxy_link_request((_sd), _op, _req##_iov, _req##_count, _ans##_iov, \
			   _ans##_count)

#define CEPH_CALL_CBK(_sd, _op, _cbk) \
	proxy_link_req_send((_sd), _op, _cbk##_iov, _cbk##_count)

#define CEPH_RET(_sd, _res, _ans) \
	proxy_link_ans_send((_sd), (_res), _ans##_iov, _ans##_count)

enum {
	LIBCEPHFSD_OP_NULL = 0,

	LIBCEPHFSD_OP_VERSION,
	LIBCEPHFSD_OP_USERPERM_NEW,
	LIBCEPHFSD_OP_USERPERM_DESTROY,
	LIBCEPHFSD_OP_CREATE,
	LIBCEPHFSD_OP_RELEASE,
	LIBCEPHFSD_OP_CONF_READ_FILE,
	LIBCEPHFSD_OP_CONF_GET,
	LIBCEPHFSD_OP_CONF_SET,
	LIBCEPHFSD_OP_INIT,
	LIBCEPHFSD_OP_SELECT_FILESYSTEM,
	LIBCEPHFSD_OP_MOUNT,
	LIBCEPHFSD_OP_UNMOUNT,
	LIBCEPHFSD_OP_LL_STATFS,
	LIBCEPHFSD_OP_LL_LOOKUP,
	LIBCEPHFSD_OP_LL_LOOKUP_INODE,
	LIBCEPHFSD_OP_LL_LOOKUP_ROOT,
	LIBCEPHFSD_OP_LL_PUT,
	LIBCEPHFSD_OP_LL_WALK,
	LIBCEPHFSD_OP_CHDIR,
	LIBCEPHFSD_OP_GETCWD,
	LIBCEPHFSD_OP_READDIR,
	LIBCEPHFSD_OP_REWINDDIR,
	LIBCEPHFSD_OP_LL_OPEN,
	LIBCEPHFSD_OP_LL_CREATE,
	LIBCEPHFSD_OP_LL_MKNOD,
	LIBCEPHFSD_OP_LL_CLOSE,
	LIBCEPHFSD_OP_LL_RENAME,
	LIBCEPHFSD_OP_LL_LSEEK,
	LIBCEPHFSD_OP_LL_READ,
	LIBCEPHFSD_OP_LL_WRITE,
	LIBCEPHFSD_OP_LL_LINK,
	LIBCEPHFSD_OP_LL_UNLINK,
	LIBCEPHFSD_OP_LL_GETATTR,
	LIBCEPHFSD_OP_LL_SETATTR,
	LIBCEPHFSD_OP_LL_FALLOCATE,
	LIBCEPHFSD_OP_LL_FSYNC,
	LIBCEPHFSD_OP_LL_LISTXATTR,
	LIBCEPHFSD_OP_LL_GETXATTR,
	LIBCEPHFSD_OP_LL_SETXATTR,
	LIBCEPHFSD_OP_LL_REMOVEXATTR,
	LIBCEPHFSD_OP_LL_READLINK,
	LIBCEPHFSD_OP_LL_SYMLINK,
	LIBCEPHFSD_OP_LL_OPENDIR,
	LIBCEPHFSD_OP_LL_MKDIR,
	LIBCEPHFSD_OP_LL_RMDIR,
	LIBCEPHFSD_OP_LL_RELEASEDIR,
	LIBCEPHFSD_OP_MOUNT_PERMS,
	LIBCEPHFSD_OP_LL_NONBLOCKING_RW,

	/* Add more operations above this comment. */

	LIBCEPHFSD_OP_TOTAL_OPS
};

enum {
	LIBCEPHFSD_CBK_NULL = 0,
	LIBCEPHFSD_CBK_LL_NONBLOCKING_RW,

	/* Add more callbacks above this comment. */

	LIBCEPHFSD_CBK_TOTAL_OPS
};

typedef union {
	uint64_t ptr;
	struct {
		uint32_t uid;
		uint32_t gid;
	};
} embedded_perms_t;

#define PROTO_REQ(_fields...) _fields
#define PROTO_ANS(_fields...) _fields
#define PROTO_CBK(_fields...) _fields

#define PROTO_VER(_name, _fields) _fields, _name

#define PROTO_STRUCT(_name, _fields) \
	struct { \
		_fields \
	} _name;

#define PROTO_VOID(_a...)

#define PROTO_STRUCT_COND_SELECT(_a, _b, _c, _d...) _c

/* This macro outputs a version structure only if the "name" is present.
 * Otherwise it returns empty.
 *
 * Following this macro call, it's expected to find the normal arguments for
 * a PROTO_STRUCT() macro.
 *
 * Example: PROTO_STRUCT_COND(name)(name, fields) */
#define PROTO_STRUCT_COND(_name...) \
	PROTO_STRUCT_COND_SELECT(, ## _name, PROTO_STRUCT, PROTO_VOID)

/* These macros are prepared to create up to 4 version structures. If more are
 * needed, just create a PROTO_BUILD_5 similar to PROTO_BUILD_4 and call it
 * from PROTO_TYPE.
 *
 * The last version of the structure is also declared as an anonymous
 * substructure to make access to the fields safer and cleaner.
 *
 * This approach is needed to be able to define full structures for each
 * protocol version (i.e. v1 structure also contains all fields from v0), but
 * without having to explicitly write them in the protocol definitions. This
 * prevents a lot of copy&paste, it's less error prone, and makes the protocol
 * extensions cleaner. */
#define PROTO_BUILD_1(_fields, _name) \
	PROTO_STRUCT_COND(_name)(_name, _fields) \
	PROTO_STRUCT(,_fields)

#define PROTO_BUILD_2(_fields, _name, _more...) \
	PROTO_STRUCT_COND(_name)(_name, _fields) \
	PROTO_BUILD_1(_fields _more,)

#define PROTO_BUILD_3(_fields, _name, _more...) \
	PROTO_STRUCT_COND(_name)(_name, _fields) \
	PROTO_BUILD_2(_fields _more,)

#define PROTO_BUILD_4(_fields, _name, _more...) \
	PROTO_STRUCT_COND(_name)(_name, _fields) \
	PROTO_BUILD_3(_fields _more,)

#define PROTO_TYPE(_name, _fields...) \
	typedef union { PROTO_BUILD_4(_fields) } _name

#define PROTO_CALL(_name, _req, _ans) \
	PROTO_TYPE(proxy_##_name##_req_t, proxy_link_req_t header; _req); \
	PROTO_TYPE(proxy_##_name##_ans_t, proxy_link_ans_t header; _ans)

#define PROTO_NOTIFY(_name, _cbk) \
	PROTO_TYPE(proxy_##_name##_cbk_t, proxy_link_req_t header; _cbk)

/* Declaration of types used to transfer requests and answers. */

PROTO_CALL(ceph_version,
	PROTO_REQ(
		PROTO_VER(v0,
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			int32_t major;
			int32_t minor;
			int32_t patch;
			int16_t text;
		)
	)
);

PROTO_CALL(ceph_userperm_new,
	PROTO_REQ(
		PROTO_VER(v0,
			uint32_t uid;
			uint32_t gid;
			uint32_t groups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t userperm;
		)
	)
);

PROTO_CALL(ceph_userperm_destroy,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t userperm;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_create,
	PROTO_REQ(
		PROTO_VER(v0,
			int16_t id;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	)
);

PROTO_CALL(ceph_release,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_conf_read_file,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint16_t path;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_conf_get,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint32_t size;
			uint16_t option;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint16_t value;
		)
	)
);

PROTO_CALL(ceph_conf_set,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint16_t option;
			uint16_t value;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_init,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_select_filesystem,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint16_t fs;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_mount,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint16_t root;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_unmount,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_statfs,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t inode;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_lookup,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			uint32_t want;
			uint32_t flags;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_ll_lookup_inode,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			struct inodeno_t ino;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_ll_lookup_root,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_ll_put,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t inode;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_walk,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint32_t want;
			uint32_t flags;
			uint16_t path;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_chdir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint16_t path;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_getcwd,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint16_t path;
		)
	)
);

PROTO_CALL(ceph_readdir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t dir;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			bool eod;
		)
	)
);

PROTO_CALL(ceph_rewinddir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t dir;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_open,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			int32_t flags;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t fh;
		)
	)
);

PROTO_CALL(ceph_ll_create,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			mode_t mode;
			int32_t oflags;
			uint32_t want;
			uint32_t flags;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
			uint64_t fh;
		)
	)
);

PROTO_CALL(ceph_ll_mknod,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			mode_t mode;
			dev_t rdev;
			uint32_t want;
			uint32_t flags;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_ll_close,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t fh;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_rename,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t old_parent;
			uint64_t new_parent;
			uint16_t old_name;
			uint16_t new_name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_lseek,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t fh;
			off_t offset;
			int32_t whence;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			off_t offset;
		)
	)
);

PROTO_CALL(ceph_ll_read,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t fh;
			int64_t offset;
			uint64_t len;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_write,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t fh;
			int64_t offset;
			uint64_t len;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_link,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			uint64_t parent;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_unlink,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_getattr,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			uint32_t want;
			uint32_t flags;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_setattr,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			int32_t mask;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_fallocate,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t fh;
			int64_t offset;
			int64_t length;
			int32_t mode;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_fsync,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t fh;
			int32_t dataonly;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_listxattr,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			size_t size;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			size_t size;
		)
	)
);

PROTO_CALL(ceph_ll_getxattr,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			size_t size;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_setxattr,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			size_t size;
			int32_t flags;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_removexattr,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_readlink,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
			size_t size;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_symlink,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			uint32_t want;
			uint32_t flags;
			uint16_t name;
			uint16_t target;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_ll_opendir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t inode;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t dir;
		)
	)
);

PROTO_CALL(ceph_ll_mkdir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			mode_t mode;
			uint32_t want;
			uint32_t flags;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t inode;
		)
	)
);

PROTO_CALL(ceph_ll_rmdir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			embedded_perms_t userperm;
			uint64_t parent;
			uint16_t name;
		),
		PROTO_VER(v1,
			uint32_t ngroups;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_ll_releasedir,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t dir;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
		)
	)
);

PROTO_CALL(ceph_mount_perms,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			uint64_t userperm;
		)
	)
);

PROTO_CALL(ceph_ll_nonblocking_readv_writev,
	PROTO_REQ(
		PROTO_VER(v0,
			uint64_t cmount;
			uint64_t info;
			uint64_t fh;
			int64_t off;
			uint64_t size;
			bool write;
			bool fsync;
			bool syncdataonly;
		)
	),
	PROTO_ANS(
		PROTO_VER(v0,
			int64_t res;
		)
	)
);

typedef union _proxy_req {
	proxy_link_req_t header;

	proxy_ceph_version_req_t version;
	proxy_ceph_userperm_new_req_t userperm_new;
	proxy_ceph_userperm_destroy_req_t userperm_destroy;
	proxy_ceph_create_req_t create;
	proxy_ceph_release_req_t release;
	proxy_ceph_conf_read_file_req_t conf_read_file;
	proxy_ceph_conf_get_req_t conf_get;
	proxy_ceph_conf_set_req_t conf_set;
	proxy_ceph_init_req_t init;
	proxy_ceph_select_filesystem_req_t select_filesystem;
	proxy_ceph_mount_req_t mount;
	proxy_ceph_unmount_req_t unmount;
	proxy_ceph_ll_statfs_req_t ll_statfs;
	proxy_ceph_ll_lookup_req_t ll_lookup;
	proxy_ceph_ll_lookup_inode_req_t ll_lookup_inode;
	proxy_ceph_ll_lookup_root_req_t ll_lookup_root;
	proxy_ceph_ll_put_req_t ll_put;
	proxy_ceph_ll_walk_req_t ll_walk;
	proxy_ceph_chdir_req_t chdir;
	proxy_ceph_getcwd_req_t getcwd;
	proxy_ceph_readdir_req_t readdir;
	proxy_ceph_rewinddir_req_t rewinddir;
	proxy_ceph_ll_open_req_t ll_open;
	proxy_ceph_ll_create_req_t ll_create;
	proxy_ceph_ll_mknod_req_t ll_mknod;
	proxy_ceph_ll_close_req_t ll_close;
	proxy_ceph_ll_rename_req_t ll_rename;
	proxy_ceph_ll_lseek_req_t ll_lseek;
	proxy_ceph_ll_read_req_t ll_read;
	proxy_ceph_ll_write_req_t ll_write;
	proxy_ceph_ll_link_req_t ll_link;
	proxy_ceph_ll_unlink_req_t ll_unlink;
	proxy_ceph_ll_getattr_req_t ll_getattr;
	proxy_ceph_ll_setattr_req_t ll_setattr;
	proxy_ceph_ll_fallocate_req_t ll_fallocate;
	proxy_ceph_ll_fsync_req_t ll_fsync;
	proxy_ceph_ll_listxattr_req_t ll_listxattr;
	proxy_ceph_ll_getxattr_req_t ll_getxattr;
	proxy_ceph_ll_setxattr_req_t ll_setxattr;
	proxy_ceph_ll_removexattr_req_t ll_removexattr;
	proxy_ceph_ll_readlink_req_t ll_readlink;
	proxy_ceph_ll_symlink_req_t ll_symlink;
	proxy_ceph_ll_opendir_req_t ll_opendir;
	proxy_ceph_ll_mkdir_req_t ll_mkdir;
	proxy_ceph_ll_rmdir_req_t ll_rmdir;
	proxy_ceph_ll_releasedir_req_t ll_releasedir;
	proxy_ceph_mount_perms_req_t mount_perms;
	proxy_ceph_ll_nonblocking_readv_writev_req_t ll_nonblocking_rw;
} proxy_req_t;

PROTO_NOTIFY(ceph_ll_nonblocking_readv_writev,
	PROTO_CBK(
		PROTO_VER(v0,
			uint64_t info;
			int64_t res;
		)
	)
);

typedef union _proxy_cbk {
	proxy_link_req_t header;
	proxy_ceph_ll_nonblocking_readv_writev_cbk_t ll_nonblocking_rw;
} proxy_cbk_t;

#endif
