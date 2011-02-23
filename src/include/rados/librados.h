#ifndef CEPH_LIBRADOS_H
#define CEPH_LIBRADOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#include <linux/types.h>
#include <string.h>

#ifndef CEPH_OSD_TMAP_SET
#define CEPH_OSD_TMAP_HDR 'h'
#define CEPH_OSD_TMAP_SET 's'
#define CEPH_OSD_TMAP_RM  'r'
#endif

#define LIBRADOS_VER_MAJOR 0
#define LIBRADOS_VER_MINOR 25
#define LIBRADOS_VER_EXTRA 0

#define LIBRADOS_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)

#define LIBRADOS_VERSION_CODE LIBRADOS_VERSION(LIBRADOS_VER_MAJOR, LIBRADOS_VER_MINOR, LIBRADOS_VER_EXTRA)

#define LIBRADOS_SUPPORTS_WATCH 1

typedef void *rados_t;
typedef void *rados_pool_t;
typedef void *rados_list_ctx_t;
typedef uint64_t rados_snap_t;

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

struct rados_statfs_t {
  uint64_t kb, kb_used, kb_avail;
  uint64_t num_objects;
};


void rados_version(int *major, int *minor, int *extra);

/* initialization */
int rados_create(rados_t *cluster, const char * const id);

/* Connect to the cluster */
int rados_connect(rados_t cluster);

/* destroy the cluster instance */
void rados_destroy(rados_t cluster);

/* Config
 *
 * Functions for manipulating the Ceph configuration at runtime.
 * After changing the Ceph configuration, you should call rados_conf_apply to
 * ensure that the changes have been applied.
 */
int rados_conf_read_file(rados_t cluster, const char *path);

/* Sets a configuration value from a string.
 * Returns 0 on success, error code otherwise. */
int rados_conf_set(rados_t cluster, const char *option, const char *value);

/* Reopens the log file.
 * You must do this after changing the logging configuration.
 * It is also good practice to call this from your SIGHUP signal handler, so that users can send you
 * a SIGHUP to reopen the log.
 */
int rados_reopen_log(void);

/* Returns a configuration value as a string.
 * If len is positive, that is the maximum number of bytes we'll write into the
 * buffer. If len == -1, we'll call malloc() and set *buf.
 * Returns 0 on success, error code otherwise. Returns ENAMETOOLONG if the
 * buffer is too short. */
int rados_conf_get(rados_t cluster, const char *option, char **buf, int len);

/* pools */
int rados_pool_open(rados_t cluster, const char *name, rados_pool_t *pool);
int rados_pool_close(rados_pool_t pool);
int rados_pool_lookup(rados_t cluster, const char *name);

int rados_pool_stat(rados_pool_t pool, struct rados_pool_stat_t *stats);

void rados_snap_set_read(rados_pool_t pool, rados_snap_t snap);
int rados_snap_set_write_context(rados_pool_t pool, rados_snap_t seq, rados_snap_t *snaps, int num_snaps);

int rados_pool_create(rados_t cluster, const char *name);
int rados_pool_create_with_auid(rados_t cluster, const char *name, uint64_t auid);
int rados_pool_create_with_crush_rule(rados_t cluster, const char *name, __u8 crush_rule);
int rados_pool_create_with_all(rados_t cluster, const char *name, uint64_t auid,
			       __u8 crush_rule);
int rados_pool_delete(rados_pool_t pool);
int rados_pool_set_auid(rados_pool_t pool, uint64_t auid);

/* objects */
int rados_objects_list_open(rados_pool_t pool, rados_list_ctx_t *ctx);
int rados_object_list_next(rados_list_ctx_t ctx, const char **entry);
void rados_objects_list_close(rados_list_ctx_t ctx);


/* snapshots */
int rados_pool_snap_create(rados_pool_t pool, const char *snapname);
int rados_pool_snap_remove(rados_pool_t pool, const char *snapname);
int rados_pool_snap_rollback_object(rados_pool_t pool, const char *oid,
			  const char *snapname);
int rados_pool_selfmanaged_snap_create(rados_pool_t pool, uint64_t *snapid);
int rados_pool_selfmanaged_snap_remove(rados_pool_t pool, uint64_t snapid);
int rados_pool_snap_list(rados_pool_t pool, rados_snap_t *snaps, int maxlen);
int rados_pool_snap_lookup(rados_pool_t pool, const char *name, rados_snap_t *id);
int rados_pool_snap_get_name(rados_pool_t pool, rados_snap_t id, char *name, int maxlen);

/* sync io */
uint64_t rados_get_last_version(rados_pool_t pool);

int rados_write(rados_pool_t pool, const char *oid, off_t off, const char *buf, size_t len);
int rados_write_full(rados_pool_t pool, const char *oid, off_t off, const char *buf, size_t len);
int rados_read(rados_pool_t pool, const char *oid, off_t off, char *buf, size_t len);
int rados_remove(rados_pool_t pool, const char *oid);
int rados_trunc(rados_pool_t pool, const char *oid, size_t size);

/* attrs */
int rados_getxattr(rados_pool_t pool, const char *o, const char *name, char *buf, size_t len);
int rados_setxattr(rados_pool_t pool, const char *o, const char *name, const char *buf, size_t len);
int rados_rmxattr(rados_pool_t pool, const char *o, const char *name);

/* misc */
int rados_stat(rados_pool_t pool, const char *o, uint64_t *psize, time_t *pmtime);
int rados_tmap_update(rados_pool_t pool, const char *o, const char *cmdbuf, size_t cmdbuflen);
int rados_exec(rados_pool_t pool, const char *oid, const char *cls, const char *method,
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
uint64_t rados_aio_get_obj_ver(rados_completion_t c);
void rados_aio_release(rados_completion_t c);
int rados_aio_write(rados_pool_t pool, const char *oid,
		    off_t off, const char *buf, size_t len,
		    rados_completion_t completion);
int rados_aio_write_full(rados_pool_t pool, const char *oid,
			 off_t off, const char *buf, size_t len,
			 rados_completion_t completion);
int rados_aio_read(rados_pool_t pool, const char *oid,
		   off_t off, char *buf, size_t len,
		   rados_completion_t completion);

/* watch/notify */
typedef void (*rados_watchcb_t)(uint8_t opcode, uint64_t ver, void *arg);
int rados_watch(rados_pool_t pool, const char *o, uint64_t ver, uint64_t *handle,
                rados_watchcb_t watchcb, void *arg);
int rados_unwatch(rados_pool_t pool, const char *o, uint64_t handle);
int rados_notify(rados_pool_t pool, const char *o, uint64_t ver);

#ifdef __cplusplus
}
#endif

#endif
