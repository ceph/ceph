#ifndef CEPH_LIBRADOS_H
#define CEPH_LIBRADOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#if defined(__linux__)
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include "include/inttypes.h"
#endif
#include <string.h>

#ifndef CEPH_OSD_TMAP_SET
/* These are also defined in rados.h and objclass.h. Keep them in sync! */
#define CEPH_OSD_TMAP_HDR 'h'
#define CEPH_OSD_TMAP_SET 's'
#define CEPH_OSD_TMAP_CREATE 'c'
#define CEPH_OSD_TMAP_RM  'r'
#endif

#define LIBRADOS_VER_MAJOR 0
#define LIBRADOS_VER_MINOR 30
#define LIBRADOS_VER_EXTRA 0

#define LIBRADOS_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRADOS_VERSION_CODE LIBRADOS_VERSION(LIBRADOS_VER_MAJOR, LIBRADOS_VER_MINOR, LIBRADOS_VER_EXTRA)

#define LIBRADOS_SUPPORTS_WATCH 1

/* xattr comparison */
enum {
	LIBRADOS_CMPXATTR_OP_NOP = 0,
	LIBRADOS_CMPXATTR_OP_EQ  = 1,
	LIBRADOS_CMPXATTR_OP_NE  = 2,
	LIBRADOS_CMPXATTR_OP_GT  = 3,
	LIBRADOS_CMPXATTR_OP_GTE = 4,
	LIBRADOS_CMPXATTR_OP_LT  = 5,
	LIBRADOS_CMPXATTR_OP_LTE = 6
};

struct CephContext;

typedef void *rados_t;
typedef void *rados_ioctx_t;
typedef void *rados_list_ctx_t;
typedef uint64_t rados_snap_t;
typedef void *rados_xattrs_iter_t;

struct rados_pool_stat_t {
  uint64_t num_bytes;    // in bytes
  uint64_t num_kb;       // in KB
  uint64_t num_objects;
  uint64_t num_object_clones;
  uint64_t num_object_copies;  // num_objects * num_replicas
  uint64_t num_objects_missing_on_primary;
  uint64_t num_objects_unfound;
  uint64_t num_objects_degraded;
  uint64_t num_rd, num_rd_kb,num_wr, num_wr_kb;
};

struct rados_cluster_stat_t {
  uint64_t kb, kb_used, kb_avail;
  uint64_t num_objects;
};

void rados_version(int *major, int *minor, int *extra);

/* initialization */
int rados_create(rados_t *cluster, const char * const id);

/* initialize rados with an existing configuration. */
int rados_create_with_context(rados_t *cluster, struct CephContext *cct_);

/* Connect to the cluster */
int rados_connect(rados_t cluster);

/* destroy the cluster instance */
void rados_shutdown(rados_t cluster);

/* Config
 *
 * Functions for manipulating the Ceph configuration at runtime.
 */
int rados_conf_read_file(rados_t cluster, const char *path);

/* Parse argv */
int rados_conf_parse_argv(rados_t cluster, int argc, const char **argv);

int rados_conf_parse_env(rados_t cluster, const char *var);

/* Sets a configuration value from a string.
 * Returns 0 on success, error code otherwise. */
int rados_conf_set(rados_t cluster, const char *option, const char *value);

/* Returns a configuration value as a string.
 * len is the maximum number of bytes we'll write into the buffer.
 * Returns 0 on success, error code otherwise. Returns ENAMETOOLONG if the
 * buffer is too short. */
int rados_conf_get(rados_t cluster, const char *option, char *buf, size_t len);

/* cluster info */
int rados_cluster_stat(rados_t cluster, struct rados_cluster_stat_t *result);


/* pools */

/**
 * List objects in a pool.
 *
 * Gets a list of pool names as NULL-terminated strings.  The pool
 * names will be placed in the supplied @buf buffer one after another.
 * After the last pool name, there will be two 0 bytes in a row.
 *
 * If @len is too short to fit all the pool name entries we need, we will fill
 * as much as we can.
 *
 * @param cluster cluster handle
 * @param buf output buffer
 * @param len output buffer length
 * @return length of the buffer we would need to list all pools
 */
int rados_pool_list(rados_t cluster, char *buf, size_t len);

int rados_ioctx_create(rados_t cluster, const char *pool_name, rados_ioctx_t *ioctx);
void rados_ioctx_destroy(rados_ioctx_t io);

int rados_ioctx_pool_stat(rados_ioctx_t io, struct rados_pool_stat_t *stats);

int64_t rados_pool_lookup(rados_t cluster, const char *pool_name);
int rados_pool_create(rados_t cluster, const char *pool_name);
int rados_pool_create_with_auid(rados_t cluster, const char *pool_name, uint64_t auid);
int rados_pool_create_with_crush_rule(rados_t cluster, const char *pool_name,
				      __u8 crush_rule);
int rados_pool_create_with_all(rados_t cluster, const char *pool_name, uint64_t auid,
				     __u8 crush_rule);
int rados_pool_delete(rados_t cluster, const char *pool_name);
int rados_ioctx_pool_set_auid(rados_ioctx_t io, uint64_t auid);
int rados_ioctx_pool_get_auid(rados_ioctx_t io, uint64_t *auid);

void rados_ioctx_locator_set_key(rados_ioctx_t io, const char *key);
int rados_ioctx_get_id(rados_ioctx_t io);

/* objects */
int rados_objects_list_open(rados_ioctx_t io, rados_list_ctx_t *ctx);
int rados_objects_list_next(rados_list_ctx_t ctx, const char **entry);
void rados_objects_list_close(rados_list_ctx_t ctx);

/* snapshots */
int rados_ioctx_snap_create(rados_ioctx_t io, const char *snapname);
int rados_ioctx_snap_remove(rados_ioctx_t io, const char *snapname);
int rados_rollback(rados_ioctx_t io, const char *oid,
		   const char *snapname);
void rados_ioctx_snap_set_read(rados_ioctx_t io, rados_snap_t snap);
int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io, rados_snap_t *snapid);
int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io, rados_snap_t snapid);
int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io, const char *oid, rados_snap_t snapid);
int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io, rados_snap_t seq, rados_snap_t *snaps, int num_snaps);

int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t *snaps, int maxlen);
int rados_ioctx_snap_lookup(rados_ioctx_t io, const char *name, rados_snap_t *id);
int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id, char *name, int maxlen);
int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t *t);

/* sync io */

/**
 * Return the version of the last object read or written to.
 *
 * This exposes the internal version number of the last object read or
 * written via this rados_ioctx_t.
 *
 * @param io ioctx
 * @return object version
 */
uint64_t rados_get_last_version(rados_ioctx_t io);

int rados_write(rados_ioctx_t io, const char *oid, const char *buf, size_t len, uint64_t off);
int rados_write_full(rados_ioctx_t io, const char *oid, const char *buf, size_t len);
int rados_clone_range(rados_ioctx_t io, const char *dst, uint64_t dst_off,
                      const char *src, uint64_t src_off, size_t len);
int rados_append(rados_ioctx_t io, const char *oid, const char *buf, size_t len);
int rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off);
int rados_remove(rados_ioctx_t io, const char *oid);
int rados_trunc(rados_ioctx_t io, const char *oid, uint64_t size);

/* attrs */
int rados_getxattr(rados_ioctx_t io, const char *o, const char *name, char *buf, size_t len);
int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len);
int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name);

int rados_getxattrs(rados_ioctx_t io, const char *oid, rados_xattrs_iter_t *iter);
int rados_getxattrs_next(rados_xattrs_iter_t iter, const char **name,
			 const char **val, size_t *len);
void rados_getxattrs_end(rados_xattrs_iter_t iter);

/* misc */
/**
 * Get object stats (size/mtime)
 *
 * @param io ioctx
 * @param o object name
 * @param psize location to store object size
 * @param pmtime location to store modification time
 * @return 0 for success or negative error code
 */
int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime);

/**
 * Update tmap (trivial map)
 *
 * Do compound update to a tmap object, inserting or deleting some
 * number of records.  The @cmdbuf is a series of operation byte
 * codes, following by command payload.  Each command is a single-byte
 * command code, whose value is one of CEPH_OSD_TMAP_*.
 *
 *  - update tmap 'header'
 *    1 byte  = CEPH_OSD_TMAP_HDR
 *    4 bytes = data length (little endian)
 *    N bytes = data
 *
 *  - insert/update one key/value pair
 *    1 byte  = CEPH_OSD_TMAP_SET
 *    4 bytes = key name length (little endian)
 *    N bytes = key name
 *    4 bytes = data length (little endian)
 *    M bytes = data
 *
 *  - insert one key/value pair; return -EEXIST if it already exists.
 *    1 byte  = CEPH_OSD_TMAP_CREATE
 *    4 bytes = key name length (little endian)
 *    N bytes = key name
 *    4 bytes = data length (little endian)
 *    M bytes = data
 *
 *  - remove one key/value pair
 *    1 byte  = CEPH_OSD_TMAP_RM
 *    4 bytes = key name length (little endian)
 *    N bytes = key name
 *
 * Restrictions:
 *  - The HDR update must preceed any key/value updates.
 *  - All key/value updates must be in lexicographically sorted order
 *    in the @cmdbuf.
 *  - You can read/write to a tmap object via the regular APIs, but
 *    you should be careful not to corrupt it.  Also be aware that the
 *    object format may change without notice.
 *
 * @param io ioctx
 * @param o object name
 * @param cmdbuf command buffer
 * @param cmdbuflen command buffer length
 * @return 0 for success or negative error code
 */
int rados_tmap_update(rados_ioctx_t io, const char *o, const char *cmdbuf, size_t cmdbuflen);

/**
 * Store complete tmap (trivial map) object
 *
 * Put a full tmap object into the store, replacing what was there.
 *
 * The format of the @buf buffer is:
 *    4 bytes - length of header (little endian)
 *    N bytes - header data
 *    4 bytes - number of keys (little endian)
 * and for each key,
 *    4 bytes - key name length (little endian)
 *    N bytes - key name
 *    4 bytes - value length (little endian)
 *    M bytes - value data
 *
 * @param io ioctx
 * @param o object name
 * @param buf buffer
 * @param buflen buffer length
 * @return 0 for success or negative error code
 */
int rados_tmap_put(rados_ioctx_t io, const char *o, const char *buf, size_t buflen);

/**
 * Fetch complete tmap (trivial map) object
 *
 * Read a full tmap object.  See rados_tmap_put() for the format the
 * data is returned in.  If the supplied buffer isn't big enough,
 * returns -ERANGE.
 *
 * @param io ioctx
 * @param o object name
 * @param buf buffer
 * @param buflen buffer length
 * @return 0 for success or negative error code
 */
int rados_tmap_get(rados_ioctx_t io, const char *o, char *buf, size_t buflen);

int rados_exec(rados_ioctx_t io, const char *oid, const char *cls, const char *method,
	       const char *in_buf, size_t in_len, char *buf, size_t out_len);

/* async io */
typedef void *rados_completion_t;
typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);

int rados_aio_create_completion(void *cb_arg, rados_callback_t cb_complete, rados_callback_t cb_safe,
				rados_completion_t *pc);
int rados_aio_wait_for_complete(rados_completion_t c);
int rados_aio_wait_for_safe(rados_completion_t c);
int rados_aio_is_complete(rados_completion_t c);
int rados_aio_is_safe(rados_completion_t c);
int rados_aio_get_return_value(rados_completion_t c);
void rados_aio_release(rados_completion_t c);
int rados_aio_write(rados_ioctx_t io, const char *oid,
		    rados_completion_t completion,
		    const char *buf, size_t len, uint64_t off);
int rados_aio_append(rados_ioctx_t io, const char *oid,
		     rados_completion_t completion,
		     const char *buf, size_t len);
int rados_aio_write_full(rados_ioctx_t io, const char *oid,
			 rados_completion_t completion,
			 const char *buf, size_t len);
int rados_aio_read(rados_ioctx_t io, const char *oid,
		   rados_completion_t completion,
		   char *buf, size_t len, uint64_t off);

int rados_aio_flush(rados_ioctx_t io);

/* watch/notify */
typedef void (*rados_watchcb_t)(uint8_t opcode, uint64_t ver, void *arg);
int rados_watch(rados_ioctx_t io, const char *o, uint64_t ver, uint64_t *handle,
                rados_watchcb_t watchcb, void *arg);
int rados_unwatch(rados_ioctx_t io, const char *o, uint64_t handle);
int rados_notify(rados_ioctx_t io, const char *o, uint64_t ver, const char *buf, int buf_len);

#ifdef __cplusplus
}
#endif

#endif
