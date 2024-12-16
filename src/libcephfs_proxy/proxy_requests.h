
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

#define CEPH_DATA(_name, _data, _data_count)       \
	proxy_##_name##_##_data##_t _data;         \
	struct iovec _data##_iov[_data_count + 1]; \
	int32_t _data##_count = 0;                 \
	CEPH_BUFF_ADD(_data, &_data, sizeof(_data))

#define CEPH_REQ(_name, _req, _req_count, _ans, _ans_count) \
	CEPH_DATA(_name, _req, _req_count);                 \
	CEPH_DATA(_name, _ans, _ans_count)

#define CEPH_CALL(_sd, _op, _req, _ans)                                      \
	proxy_link_request((_sd), _op, _req##_iov, _req##_count, _ans##_iov, \
			   _ans##_count)

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

	LIBCEPHFSD_OP_TOTAL_OPS
};

#define CEPH_TYPE_REQ(_name, _fields...)                           \
	struct _proxy_##_name##_req;                               \
	typedef struct _proxy_##_name##_req proxy_##_name##_req_t; \
	struct _proxy_##_name##_req {                              \
		_fields                                            \
	}

#define CEPH_TYPE_ANS(_name, _fields...)                           \
	struct _proxy_##_name##_ans;                               \
	typedef struct _proxy_##_name##_ans proxy_##_name##_ans_t; \
	struct _proxy_##_name##_ans {                              \
		_fields                                            \
	}

#define FIELDS(_fields...) _fields
#define REQ(_fields...) FIELDS(proxy_link_req_t header; _fields)
#define REQ_CMOUNT(_fields...) REQ(uint64_t cmount; _fields)
#define ANS(_fields...) FIELDS(proxy_link_ans_t header; _fields)
#define ANS_CMOUNT(_fields...) ANS(uint64_t cmount; _fields)

#define CEPH_TYPE(_name, _req, _ans) \
	CEPH_TYPE_REQ(_name, _req);  \
	CEPH_TYPE_ANS(_name, _ans)

/* Declaration of types used to transder requests and answers. */

CEPH_TYPE(hello, FIELDS(uint32_t id;), FIELDS(int16_t major; int16_t minor;));

CEPH_TYPE(ceph_version, REQ(),
	  ANS(int32_t major; int32_t minor; int32_t patch; int16_t text;));

CEPH_TYPE(ceph_userperm_new, REQ(uint32_t uid; uint32_t gid; uint32_t groups;),
	  ANS(uint64_t userperm;));

CEPH_TYPE(ceph_userperm_destroy, REQ(uint64_t userperm;), ANS());

CEPH_TYPE(ceph_create, REQ(int16_t id;), ANS_CMOUNT());

CEPH_TYPE(ceph_release, REQ_CMOUNT(), ANS());

CEPH_TYPE(ceph_conf_read_file, REQ_CMOUNT(uint16_t path;), ANS());

CEPH_TYPE(ceph_conf_get, REQ_CMOUNT(uint32_t size; uint16_t option;),
	  ANS(uint16_t value;));

CEPH_TYPE(ceph_conf_set, REQ_CMOUNT(uint16_t option; uint16_t value;), ANS());

CEPH_TYPE(ceph_init, REQ_CMOUNT(), ANS());

CEPH_TYPE(ceph_select_filesystem, REQ_CMOUNT(uint16_t fs;), ANS());

CEPH_TYPE(ceph_mount, REQ_CMOUNT(uint16_t root;), ANS());

CEPH_TYPE(ceph_unmount, REQ_CMOUNT(), ANS());

CEPH_TYPE(ceph_ll_statfs, REQ_CMOUNT(uint64_t inode;), ANS());

CEPH_TYPE(ceph_ll_lookup,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; uint32_t want;
		     uint32_t flags; uint16_t name;),
	  ANS(uint64_t inode;));

CEPH_TYPE(ceph_ll_lookup_inode, REQ_CMOUNT(struct inodeno_t ino;),
	  ANS(uint64_t inode;));

CEPH_TYPE(ceph_ll_lookup_root, REQ_CMOUNT(), ANS(uint64_t inode;));

CEPH_TYPE(ceph_ll_put, REQ_CMOUNT(uint64_t inode;), ANS());

CEPH_TYPE(ceph_ll_walk,
	  REQ_CMOUNT(uint64_t userperm; uint32_t want; uint32_t flags;
		     uint16_t path;),
	  ANS(uint64_t inode;));

CEPH_TYPE(ceph_chdir, REQ_CMOUNT(uint16_t path;), ANS());

CEPH_TYPE(ceph_getcwd, REQ_CMOUNT(), ANS(uint16_t path;));

CEPH_TYPE(ceph_readdir, REQ_CMOUNT(uint64_t dir;), ANS(bool eod;));

CEPH_TYPE(ceph_rewinddir, REQ_CMOUNT(uint64_t dir;), ANS());

CEPH_TYPE(ceph_ll_open,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; int32_t flags;),
	  ANS(uint64_t fh;));

CEPH_TYPE(ceph_ll_create,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; mode_t mode;
		     int32_t oflags; uint32_t want; uint32_t flags;
		     uint16_t name;),
	  ANS(uint64_t inode; uint64_t fh;));

CEPH_TYPE(ceph_ll_mknod,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; mode_t mode;
		     dev_t rdev; uint32_t want; uint32_t flags; uint16_t name;),
	  ANS(uint64_t inode;));

CEPH_TYPE(ceph_ll_close, REQ_CMOUNT(uint64_t fh;), ANS());

CEPH_TYPE(ceph_ll_rename,
	  REQ_CMOUNT(uint64_t userperm; uint64_t old_parent;
		     uint64_t new_parent; uint16_t old_name;
		     uint16_t new_name;),
	  ANS());

CEPH_TYPE(ceph_ll_lseek, REQ_CMOUNT(uint64_t fh; off_t offset; int32_t whence;),
	  ANS(off_t offset;));

CEPH_TYPE(ceph_ll_read, REQ_CMOUNT(uint64_t fh; int64_t offset; uint64_t len;),
	  ANS());

CEPH_TYPE(ceph_ll_write, REQ_CMOUNT(uint64_t fh; int64_t offset; uint64_t len;),
	  ANS());

CEPH_TYPE(ceph_ll_link,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; uint64_t parent;
		     uint16_t name;),
	  ANS());

CEPH_TYPE(ceph_ll_unlink,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; uint16_t name;),
	  ANS());

CEPH_TYPE(ceph_ll_getattr,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; uint32_t want;
		     uint32_t flags;),
	  ANS());

CEPH_TYPE(ceph_ll_setattr,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; int32_t mask;), ANS());

CEPH_TYPE(ceph_ll_fallocate,
	  REQ_CMOUNT(uint64_t fh; int64_t offset; int64_t length;
		     int32_t mode;),
	  ANS());

CEPH_TYPE(ceph_ll_fsync, REQ_CMOUNT(uint64_t fh; int32_t dataonly;), ANS());

CEPH_TYPE(ceph_ll_listxattr,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; size_t size;),
	  ANS(size_t size;));

CEPH_TYPE(ceph_ll_getxattr,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; size_t size;
		     uint16_t name;),
	  ANS());

CEPH_TYPE(ceph_ll_setxattr,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; size_t size;
		     int32_t flags; uint16_t name;),
	  ANS());

CEPH_TYPE(ceph_ll_removexattr,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; uint16_t name;), ANS());

CEPH_TYPE(ceph_ll_readlink,
	  REQ_CMOUNT(uint64_t userperm; uint64_t inode; size_t size;), ANS());

CEPH_TYPE(ceph_ll_symlink,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; uint32_t want;
		     uint32_t flags; uint16_t name; uint16_t target;),
	  ANS(uint64_t inode;));

CEPH_TYPE(ceph_ll_opendir, REQ_CMOUNT(uint64_t userperm; uint64_t inode;),
	  ANS(uint64_t dir;));

CEPH_TYPE(ceph_ll_mkdir,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; mode_t mode;
		     uint32_t want; uint32_t flags; uint16_t name;),
	  ANS(uint64_t inode;));

CEPH_TYPE(ceph_ll_rmdir,
	  REQ_CMOUNT(uint64_t userperm; uint64_t parent; uint16_t name;),
	  ANS());

CEPH_TYPE(ceph_ll_releasedir, REQ_CMOUNT(uint64_t dir;), ANS());

CEPH_TYPE(ceph_mount_perms, REQ_CMOUNT(), ANS(uint64_t userperm;));

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
} proxy_req_t;

#endif
