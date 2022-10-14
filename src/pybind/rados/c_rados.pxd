# cython: embedsignature=True

from libc.stdint cimport *
from ctime cimport time_t, timeval
from rados cimport rados_t, rados_config_t, rados_ioctx_t

cdef extern from "err.h" nogil:
    cdef int _MAX_ERRNO "MAX_ERRNO"


cdef extern from "rados/rados_types.h" nogil:
    cdef char* _LIBRADOS_ALL_NSPACES "LIBRADOS_ALL_NSPACES"
    cdef struct notify_ack_t:
        unsigned long notifier_id
        unsigned long cookie
        char *payload
        unsigned long payload_len

    cdef struct notify_timeout_t:
        unsigned long notifier_id
        unsigned long cookie

cdef extern from "rados/librados.h" nogil:
    enum:
        _LIBRADOS_OP_FLAG_EXCL "LIBRADOS_OP_FLAG_EXCL"
        _LIBRADOS_OP_FLAG_FAILOK "LIBRADOS_OP_FLAG_FAILOK"
        _LIBRADOS_OP_FLAG_FADVISE_RANDOM "LIBRADOS_OP_FLAG_FADVISE_RANDOM"
        _LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL "LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL"
        _LIBRADOS_OP_FLAG_FADVISE_WILLNEED "LIBRADOS_OP_FLAG_FADVISE_WILLNEED"
        _LIBRADOS_OP_FLAG_FADVISE_DONTNEED "LIBRADOS_OP_FLAG_FADVISE_DONTNEED"
        _LIBRADOS_OP_FLAG_FADVISE_NOCACHE "LIBRADOS_OP_FLAG_FADVISE_NOCACHE"


    enum:
        _LIBRADOS_CMPXATTR_OP_EQ "LIBRADOS_CMPXATTR_OP_EQ"
        _LIBRADOS_CMPXATTR_OP_NE "LIBRADOS_CMPXATTR_OP_NE"
        _LIBRADOS_CMPXATTR_OP_GT "LIBRADOS_CMPXATTR_OP_GT"
        _LIBRADOS_CMPXATTR_OP_GTE "LIBRADOS_CMPXATTR_OP_GTE"
        _LIBRADOS_CMPXATTR_OP_LT "LIBRADOS_CMPXATTR_OP_LT"
        _LIBRADOS_CMPXATTR_OP_LTE "LIBRADOS_CMPXATTR_OP_LTE"

    enum:
        _LIBRADOS_OPERATION_NOFLAG "LIBRADOS_OPERATION_NOFLAG"
        _LIBRADOS_OPERATION_BALANCE_READS "LIBRADOS_OPERATION_BALANCE_READS"
        _LIBRADOS_OPERATION_LOCALIZE_READS "LIBRADOS_OPERATION_LOCALIZE_READS"
        _LIBRADOS_OPERATION_ORDER_READS_WRITES "LIBRADOS_OPERATION_ORDER_READS_WRITES"
        _LIBRADOS_OPERATION_IGNORE_CACHE "LIBRADOS_OPERATION_IGNORE_CACHE"
        _LIBRADOS_OPERATION_SKIPRWLOCKS "LIBRADOS_OPERATION_SKIPRWLOCKS"
        _LIBRADOS_OPERATION_IGNORE_OVERLAY "LIBRADOS_OPERATION_IGNORE_OVERLAY"
        _LIBRADOS_CREATE_EXCLUSIVE "LIBRADOS_CREATE_EXCLUSIVE"
        _LIBRADOS_CREATE_IDEMPOTENT "LIBRADOS_CREATE_IDEMPOTENT"

    cdef uint64_t _LIBRADOS_SNAP_HEAD "LIBRADOS_SNAP_HEAD"

    ctypedef void* rados_xattrs_iter_t
    ctypedef void* rados_omap_iter_t
    ctypedef void* rados_list_ctx_t
    ctypedef uint64_t rados_snap_t
    ctypedef void *rados_write_op_t
    ctypedef void *rados_read_op_t
    ctypedef void *rados_completion_t
    ctypedef void (*rados_callback_t)(rados_completion_t cb, void *arg)
    ctypedef void (*rados_log_callback_t)(void *arg, const char *line, const char *who,
                                          uint64_t sec, uint64_t nsec, uint64_t seq, const char *level, const char *msg)
    ctypedef void (*rados_log_callback2_t)(void *arg, const char *line, const char *channel, const char *who, const char *name,
                                          uint64_t sec, uint64_t nsec, uint64_t seq, const char *level, const char *msg)
    ctypedef void (*rados_watchcb2_t)(void *arg, int64_t notify_id,
                                      uint64_t handle, uint64_t notifier_id,
                                      void *data, size_t data_len)
    ctypedef void (*rados_watcherrcb_t)(void *pre, uint64_t cookie, int err)


    cdef struct rados_cluster_stat_t:
        uint64_t kb
        uint64_t kb_used
        uint64_t kb_avail
        uint64_t num_objects

    cdef struct rados_pool_stat_t:
        uint64_t num_bytes
        uint64_t num_kb
        uint64_t num_objects
        uint64_t num_object_clones
        uint64_t num_object_copies
        uint64_t num_objects_missing_on_primary
        uint64_t num_objects_unfound
        uint64_t num_objects_degraded
        uint64_t num_rd
        uint64_t num_rd_kb
        uint64_t num_wr
        uint64_t num_wr_kb

    void rados_buffer_free(char *buf)

    void rados_version(int *major, int *minor, int *extra)
    int rados_create2(rados_t *pcluster, const char *const clustername,
                      const char * const name, uint64_t flags)
    int rados_create_with_context(rados_t *cluster, rados_config_t cct)
    int rados_connect(rados_t cluster)
    void rados_shutdown(rados_t cluster)
    uint64_t rados_get_instance_id(rados_t cluster)
    int rados_conf_read_file(rados_t cluster, const char *path)
    int rados_conf_parse_argv_remainder(rados_t cluster, int argc, const char **argv, const char **remargv)
    int rados_conf_parse_env(rados_t cluster, const char *var)
    int rados_conf_set(rados_t cluster, char *option, const char *value)
    int rados_conf_get(rados_t cluster, char *option, char *buf, size_t len)

    rados_t rados_ioctx_get_cluster(rados_ioctx_t io)
    int rados_ioctx_pool_stat(rados_ioctx_t io, rados_pool_stat_t *stats)
    int64_t rados_pool_lookup(rados_t cluster, const char *pool_name)
    int rados_pool_reverse_lookup(rados_t cluster, int64_t id, char *buf, size_t maxlen)
    int rados_pool_create(rados_t cluster, const char *pool_name)
    int rados_pool_create_with_crush_rule(rados_t cluster, const char *pool_name, uint8_t crush_rule_num)
    int rados_pool_create_with_auid(rados_t cluster, const char *pool_name, uint64_t auid)
    int rados_pool_create_with_all(rados_t cluster, const char *pool_name, uint64_t auid, uint8_t crush_rule_num)
    int rados_pool_get_base_tier(rados_t cluster, int64_t pool, int64_t *base_tier)
    int rados_pool_list(rados_t cluster, char *buf, size_t len)
    int rados_pool_delete(rados_t cluster, const char *pool_name)
    int rados_inconsistent_pg_list(rados_t cluster, int64_t pool, char *buf, size_t len)

    int rados_cluster_stat(rados_t cluster, rados_cluster_stat_t *result)
    int rados_cluster_fsid(rados_t cluster, char *buf, size_t len)
    int rados_blocklist_add(rados_t cluster, char *client_address, uint32_t expire_seconds)
    int rados_getaddrs(rados_t cluster, char** addrs)
    int rados_application_enable(rados_ioctx_t io, const char *app_name,
                                 int force)
    void rados_set_pool_full_try(rados_ioctx_t io)
    void rados_unset_pool_full_try(rados_ioctx_t io)
    int rados_application_list(rados_ioctx_t io, char *values,
                             size_t *values_len)
    int rados_application_metadata_get(rados_ioctx_t io, const char *app_name,
                                       const char *key, char *value,
                                       size_t *value_len)
    int rados_application_metadata_set(rados_ioctx_t io, const char *app_name,
                                       const char *key, const char *value)
    int rados_application_metadata_remove(rados_ioctx_t io,
                                          const char *app_name, const char *key)
    int rados_application_metadata_list(rados_ioctx_t io,
                                        const char *app_name, char *keys,
                                        size_t *key_len, char *values,
                                        size_t *value_len)
    int rados_ping_monitor(rados_t cluster, const char *mon_id, char **outstr, size_t *outstrlen)
    int rados_mon_command(rados_t cluster, const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_mgr_command(rados_t cluster, const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_mgr_command_target(rados_t cluster,
                          const char *name,
			  const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_mon_command_target(rados_t cluster, const char *name, const char **cmd, size_t cmdlen,
                                 const char *inbuf, size_t inbuflen,
                                 char **outbuf, size_t *outbuflen,
                                 char **outs, size_t *outslen)
    int rados_osd_command(rados_t cluster, int osdid, const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_pg_command(rados_t cluster, const char *pgstr, const char **cmd, size_t cmdlen,
                         const char *inbuf, size_t inbuflen,
                         char **outbuf, size_t *outbuflen,
                         char **outs, size_t *outslen)
    int rados_monitor_log(rados_t cluster, const char *level, rados_log_callback_t cb, void *arg)
    int rados_monitor_log2(rados_t cluster, const char *level, rados_log_callback2_t cb, void *arg)

    int rados_wait_for_latest_osdmap(rados_t cluster)

    int rados_service_register(rados_t cluster, const char *service, const char *daemon, const char *metadata_dict)
    int rados_service_update_status(rados_t cluster, const char *status_dict)

    int rados_ioctx_create(rados_t cluster, const char *pool_name, rados_ioctx_t *ioctx)
    int rados_ioctx_create2(rados_t cluster, int64_t pool_id, rados_ioctx_t *ioctx)
    void rados_ioctx_destroy(rados_ioctx_t io)
    void rados_ioctx_locator_set_key(rados_ioctx_t io, const char *key)
    void rados_ioctx_set_namespace(rados_ioctx_t io, const char * nspace)

    uint64_t rados_get_last_version(rados_ioctx_t io)
    int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime)
    int rados_write(rados_ioctx_t io, const char *oid, const char *buf, size_t len, uint64_t off)
    int rados_write_full(rados_ioctx_t io, const char *oid, const char *buf, size_t len)
    int rados_writesame(rados_ioctx_t io, const char *oid, const char *buf, size_t data_len, size_t write_len, uint64_t off)
    int rados_append(rados_ioctx_t io, const char *oid, const char *buf, size_t len)
    int rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off)
    int rados_remove(rados_ioctx_t io, const char *oid)
    int rados_trunc(rados_ioctx_t io, const char *oid, uint64_t size)
    int rados_cmpext(rados_ioctx_t io, const char *o, const char *cmp_buf, size_t cmp_len, uint64_t off)
    int rados_getxattr(rados_ioctx_t io, const char *o, const char *name, char *buf, size_t len)
    int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len)
    int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name)
    int rados_getxattrs(rados_ioctx_t io, const char *oid, rados_xattrs_iter_t *iter)
    int rados_getxattrs_next(rados_xattrs_iter_t iter, const char **name, const char **val, size_t *len)
    void rados_getxattrs_end(rados_xattrs_iter_t iter)

    int rados_nobjects_list_open(rados_ioctx_t io, rados_list_ctx_t *ctx)
    int rados_nobjects_list_next(rados_list_ctx_t ctx, const char **entry, const char **key, const char **nspace)
    void rados_nobjects_list_close(rados_list_ctx_t ctx)

    int rados_ioctx_pool_requires_alignment2(rados_ioctx_t io, int * requires)
    int rados_ioctx_pool_required_alignment2(rados_ioctx_t io, uint64_t * alignment)

    int rados_ioctx_snap_rollback(rados_ioctx_t io, const char * oid, const char * snapname)
    int rados_ioctx_snap_create(rados_ioctx_t io, const char * snapname)
    int rados_ioctx_snap_remove(rados_ioctx_t io, const char * snapname)
    int rados_ioctx_snap_lookup(rados_ioctx_t io, const char * name, rados_snap_t * id)
    int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id, char * name, int maxlen)
    void rados_ioctx_snap_set_read(rados_ioctx_t io, rados_snap_t snap)
    int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t * snaps, int maxlen)
    int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t * t)
    int64_t rados_ioctx_get_id(rados_ioctx_t io)
    int rados_ioctx_get_pool_name(rados_ioctx_t io, char *buf, unsigned maxlen)

    int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io,
                                            rados_snap_t *snapid)
    int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io,
                                            rados_snap_t snapid)
    int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io,
                                                   rados_snap_t snap_seq,
                                                   rados_snap_t *snap,
                                                   int num_snaps)
    int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io, const char *oid,
                                              rados_snap_t snapid)

    int rados_lock_exclusive(rados_ioctx_t io, const char * oid, const char * name,
                             const char * cookie, const char * desc,
                             timeval * duration, uint8_t flags)
    int rados_lock_shared(rados_ioctx_t io, const char * o, const char * name,
                          const char * cookie, const char * tag, const char * desc,
                          timeval * duration, uint8_t flags)
    int rados_unlock(rados_ioctx_t io, const char * o, const char * name, const char * cookie)

    rados_write_op_t rados_create_write_op()
    void rados_release_write_op(rados_write_op_t write_op)

    rados_read_op_t rados_create_read_op()
    void rados_release_read_op(rados_read_op_t read_op)

    int rados_aio_create_completion2(void * cb_arg, rados_callback_t cb_complete, rados_completion_t * pc)
    void rados_aio_release(rados_completion_t c)
    int rados_aio_stat(rados_ioctx_t io, const char *oid, rados_completion_t completion, uint64_t *psize, time_t *pmtime)
    int rados_aio_write(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, size_t len, uint64_t off)
    int rados_aio_append(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, size_t len)
    int rados_aio_write_full(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, size_t len)
    int rados_aio_writesame(rados_ioctx_t io, const char *oid, rados_completion_t completion, const char *buf, size_t data_len, size_t write_len, uint64_t off)
    int rados_aio_remove(rados_ioctx_t io, const char * oid, rados_completion_t completion)
    int rados_aio_read(rados_ioctx_t io, const char * oid, rados_completion_t completion, char * buf, size_t len, uint64_t off)
    int rados_aio_flush(rados_ioctx_t io)
    int rados_aio_cmpext(rados_ioctx_t io, const char *o, rados_completion_t completion,  const char *cmp_buf, size_t cmp_len, uint64_t off)
    int rados_aio_rmxattr(rados_ioctx_t io, const char *o, rados_completion_t completion, const char *name)

    int rados_aio_get_return_value(rados_completion_t c)
    int rados_aio_wait_for_complete_and_cb(rados_completion_t c)
    int rados_aio_wait_for_complete(rados_completion_t c)
    int rados_aio_is_complete(rados_completion_t c)

    int rados_exec(rados_ioctx_t io, const char * oid, const char * cls, const char * method,
                   const char * in_buf, size_t in_len, char * buf, size_t out_len)
    int rados_aio_exec(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * cls, const char * method,
                       const char * in_buf, size_t in_len, char * buf, size_t out_len)
    int rados_aio_setxattr(rados_ioctx_t io, const char *o, rados_completion_t completion, const char *name, const char *buf, size_t len)

    int rados_write_op_operate(rados_write_op_t write_op, rados_ioctx_t io, const char * oid, time_t * mtime, int flags)
    int rados_aio_write_op_operate(rados_write_op_t write_op, rados_ioctx_t io, rados_completion_t completion, const char *oid, time_t *mtime, int flags)
    void rados_write_op_cmpext(rados_write_op_t write_op, const char *cmp_buf, size_t cmp_len, uint64_t off, int *prval)
    void rados_write_op_omap_cmp(rados_write_op_t write_op, const char *key, uint8_t comparison_operator, const char *val, size_t val_len, int *prval)
    void rados_write_op_omap_set(rados_write_op_t write_op, const char * const* keys, const char * const* vals, const size_t * lens, size_t num)
    void rados_write_op_omap_rm_keys(rados_write_op_t write_op, const char * const* keys, size_t keys_len)
    void rados_write_op_omap_clear(rados_write_op_t write_op)
    void rados_write_op_omap_rm_range2(rados_write_op_t write_op, const char *key_begin, size_t key_begin_len, const char *key_end, size_t key_end_len)
    void rados_write_op_set_flags(rados_write_op_t write_op, int flags)
    void rados_write_op_setxattr(rados_write_op_t write_op, const char *name, const char *value, size_t value_len)
    void rados_write_op_rmxattr(rados_write_op_t write_op, const char *name)

    void rados_write_op_create(rados_write_op_t write_op, int exclusive, const char *category)
    void rados_write_op_append(rados_write_op_t write_op, const char *buffer, size_t len)
    void rados_write_op_write_full(rados_write_op_t write_op, const char *buffer, size_t len)
    void rados_write_op_assert_version(rados_write_op_t write_op, uint64_t ver)
    void rados_write_op_write(rados_write_op_t write_op, const char *buffer, size_t len, uint64_t offset)
    void rados_write_op_remove(rados_write_op_t write_op)
    void rados_write_op_truncate(rados_write_op_t write_op, uint64_t offset)
    void rados_write_op_zero(rados_write_op_t write_op, uint64_t offset, uint64_t len)
    void rados_write_op_exec(rados_write_op_t write_op, const char *cls, const char *method, const char *in_buf, size_t in_len, int *prval)
    void rados_write_op_writesame(rados_write_op_t write_op, const char *buffer, size_t data_len, size_t write_len, uint64_t offset)
    void rados_read_op_cmpext(rados_read_op_t read_op, const char *cmp_buf, size_t cmp_len, uint64_t off, int *prval)
    void rados_read_op_omap_get_vals2(rados_read_op_t read_op, const char * start_after, const char * filter_prefix, uint64_t max_return, rados_omap_iter_t * iter, unsigned char *pmore, int * prval)
    void rados_read_op_omap_get_keys2(rados_read_op_t read_op, const char * start_after, uint64_t max_return, rados_omap_iter_t * iter, unsigned char *pmore, int * prval)
    void rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op, const char * const* keys, size_t keys_len, rados_omap_iter_t * iter, int * prval)
    int rados_read_op_operate(rados_read_op_t read_op, rados_ioctx_t io, const char * oid, int flags)
    int rados_aio_read_op_operate(rados_read_op_t read_op, rados_ioctx_t io, rados_completion_t completion, const char *oid, int flags)
    void rados_read_op_set_flags(rados_read_op_t read_op, int flags)
    int rados_omap_get_next(rados_omap_iter_t iter, const char * const* key, const char * const* val, size_t * len)
    void rados_omap_get_end(rados_omap_iter_t iter)
    int rados_notify2(rados_ioctx_t io, const char * o, const char *buf, int buf_len, uint64_t timeout_ms, char **reply_buffer, size_t *reply_buffer_len)
    int rados_aio_notify(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, int len, uint64_t timeout_ms, char **reply_buffer, size_t *reply_buffer_len)
    int rados_decode_notify_response(char *reply_buffer, size_t reply_buffer_len, notify_ack_t **acks, size_t *nr_acks, notify_timeout_t **timeouts, size_t *nr_timeouts)
    void rados_free_notify_response(notify_ack_t *acks, size_t nr_acks, notify_timeout_t *timeouts)
    int rados_notify_ack(rados_ioctx_t io, const char *o, uint64_t notify_id, uint64_t cookie, const char *buf, int buf_len)
    int rados_watch3(rados_ioctx_t io, const char *o, uint64_t *cookie, rados_watchcb2_t watchcb, rados_watcherrcb_t watcherrcb, uint32_t timeout, void *arg)
    int rados_watch_check(rados_ioctx_t io, uint64_t cookie)
    int rados_unwatch2(rados_ioctx_t io, uint64_t cookie)
    int rados_watch_flush(rados_t cluster)
