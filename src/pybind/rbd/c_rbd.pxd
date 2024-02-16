# cython: embedsignature=True

from libc.stdint cimport *
from ctime cimport time_t, timespec
cimport libcpp

cdef extern from "rados/librados.h":
    enum:
        _LIBRADOS_SNAP_HEAD "LIBRADOS_SNAP_HEAD"

cdef extern from "rbd/librbd.h":
    ctypedef int (*librbd_progress_fn_t)(uint64_t offset, uint64_t total, void* ptr)

cdef extern from "rbd/librbd.h" nogil:
    enum:
        _RBD_FEATURE_LAYERING "RBD_FEATURE_LAYERING"
        _RBD_FEATURE_STRIPINGV2 "RBD_FEATURE_STRIPINGV2"
        _RBD_FEATURE_EXCLUSIVE_LOCK "RBD_FEATURE_EXCLUSIVE_LOCK"
        _RBD_FEATURE_OBJECT_MAP "RBD_FEATURE_OBJECT_MAP"
        _RBD_FEATURE_FAST_DIFF "RBD_FEATURE_FAST_DIFF"
        _RBD_FEATURE_DEEP_FLATTEN "RBD_FEATURE_DEEP_FLATTEN"
        _RBD_FEATURE_JOURNALING "RBD_FEATURE_JOURNALING"
        _RBD_FEATURE_DATA_POOL "RBD_FEATURE_DATA_POOL"
        _RBD_FEATURE_OPERATIONS "RBD_FEATURE_OPERATIONS"
        _RBD_FEATURE_MIGRATING "RBD_FEATURE_MIGRATING"
        _RBD_FEATURE_NON_PRIMARY "RBD_FEATURE_NON_PRIMARY"

        _RBD_FEATURES_INCOMPATIBLE "RBD_FEATURES_INCOMPATIBLE"
        _RBD_FEATURES_RW_INCOMPATIBLE "RBD_FEATURES_RW_INCOMPATIBLE"
        _RBD_FEATURES_MUTABLE "RBD_FEATURES_MUTABLE"
        _RBD_FEATURES_SINGLE_CLIENT "RBD_FEATURES_SINGLE_CLIENT"
        _RBD_FEATURES_ALL "RBD_FEATURES_ALL"

        _RBD_OPERATION_FEATURE_CLONE_PARENT "RBD_OPERATION_FEATURE_CLONE_PARENT"
        _RBD_OPERATION_FEATURE_CLONE_CHILD "RBD_OPERATION_FEATURE_CLONE_CHILD"
        _RBD_OPERATION_FEATURE_GROUP "RBD_OPERATION_FEATURE_GROUP"
        _RBD_OPERATION_FEATURE_SNAP_TRASH "RBD_OPERATION_FEATURE_SNAP_TRASH"

        _RBD_FLAG_OBJECT_MAP_INVALID "RBD_FLAG_OBJECT_MAP_INVALID"
        _RBD_FLAG_FAST_DIFF_INVALID "RBD_FLAG_FAST_DIFF_INVALID"

        _RBD_IMAGE_OPTION_FORMAT "RBD_IMAGE_OPTION_FORMAT"
        _RBD_IMAGE_OPTION_FEATURES "RBD_IMAGE_OPTION_FEATURES"
        _RBD_IMAGE_OPTION_ORDER "RBD_IMAGE_OPTION_ORDER"
        _RBD_IMAGE_OPTION_STRIPE_UNIT "RBD_IMAGE_OPTION_STRIPE_UNIT"
        _RBD_IMAGE_OPTION_STRIPE_COUNT "RBD_IMAGE_OPTION_STRIPE_COUNT"
        _RBD_IMAGE_OPTION_DATA_POOL "RBD_IMAGE_OPTION_DATA_POOL"

        RBD_MAX_BLOCK_NAME_SIZE
        RBD_MAX_IMAGE_NAME_SIZE

        _RBD_SNAP_CREATE_SKIP_QUIESCE "RBD_SNAP_CREATE_SKIP_QUIESCE"
        _RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR "RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR"

        _RBD_SNAP_REMOVE_UNPROTECT "RBD_SNAP_REMOVE_UNPROTECT"
        _RBD_SNAP_REMOVE_FLATTEN "RBD_SNAP_REMOVE_FLATTEN"
        _RBD_SNAP_REMOVE_FORCE "RBD_SNAP_REMOVE_FORCE"

        _RBD_WRITE_ZEROES_FLAG_THICK_PROVISION "RBD_WRITE_ZEROES_FLAG_THICK_PROVISION"

    ctypedef void* rados_t
    ctypedef void* rados_ioctx_t
    ctypedef void* rbd_image_t
    ctypedef void* rbd_image_options_t
    ctypedef void* rbd_pool_stats_t
    ctypedef void *rbd_completion_t

    ctypedef struct rbd_image_info_t:
        uint64_t size
        uint64_t obj_size
        uint64_t num_objs
        int order
        char block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE]
        uint64_t parent_pool
        char parent_name[RBD_MAX_IMAGE_NAME_SIZE]

    ctypedef struct rbd_snap_info_t:
        uint64_t id
        uint64_t size
        char *name

    ctypedef struct rbd_snap_group_namespace_t:
        int64_t group_pool
        char *group_name
        char *group_snap_name

    ctypedef enum rbd_snap_mirror_state_t:
        _RBD_SNAP_MIRROR_STATE_PRIMARY "RBD_SNAP_MIRROR_STATE_PRIMARY"
        _RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED "RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED"
        _RBD_SNAP_MIRROR_STATE_NON_PRIMARY "RBD_SNAP_MIRROR_STATE_NON_PRIMARY"
        _RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED "RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED"

    ctypedef struct rbd_snap_mirror_namespace_t:
        rbd_snap_mirror_state_t state
        size_t mirror_peer_uuids_count
        char *mirror_peer_uuids
        bint complete
        char *primary_mirror_uuid
        uint64_t primary_snap_id
        uint64_t last_copied_object_number

    ctypedef struct rbd_group_info_t:
        char *name
        int64_t pool

    ctypedef struct rbd_image_spec_t:
        char *id
        char *name

    ctypedef struct rbd_linked_image_spec_t:
        int64_t pool_id
        char *pool_name
        char *pool_namespace
        char *image_id
        char *image_name
        bint trash

    ctypedef enum rbd_snap_namespace_type_t:
        _RBD_SNAP_NAMESPACE_TYPE_USER "RBD_SNAP_NAMESPACE_TYPE_USER"
        _RBD_SNAP_NAMESPACE_TYPE_GROUP "RBD_SNAP_NAMESPACE_TYPE_GROUP"
        _RBD_SNAP_NAMESPACE_TYPE_TRASH "RBD_SNAP_NAMESPACE_TYPE_TRASH"
        _RBD_SNAP_NAMESPACE_TYPE_MIRROR "RBD_SNAP_NAMESPACE_TYPE_MIRROR"

    ctypedef struct rbd_snap_spec_t:
        uint64_t id
        rbd_snap_namespace_type_t namespace_type
        char *name

    ctypedef enum rbd_mirror_mode_t:
        _RBD_MIRROR_MODE_DISABLED "RBD_MIRROR_MODE_DISABLED"
        _RBD_MIRROR_MODE_IMAGE "RBD_MIRROR_MODE_IMAGE"
        _RBD_MIRROR_MODE_POOL "RBD_MIRROR_MODE_POOL"

    ctypedef enum rbd_mirror_peer_direction_t:
        _RBD_MIRROR_PEER_DIRECTION_RX "RBD_MIRROR_PEER_DIRECTION_RX"
        _RBD_MIRROR_PEER_DIRECTION_TX "RBD_MIRROR_PEER_DIRECTION_TX"
        _RBD_MIRROR_PEER_DIRECTION_RX_TX "RBD_MIRROR_PEER_DIRECTION_RX_TX"

    ctypedef struct rbd_mirror_peer_site_t:
        char *uuid
        rbd_mirror_peer_direction_t direction
        char *site_name
        char *mirror_uuid
        char *client_name
        time_t last_seen

    cdef char* _RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST "RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST"
    cdef char* _RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY "RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY"

    ctypedef enum rbd_mirror_image_mode_t:
        _RBD_MIRROR_IMAGE_MODE_JOURNAL "RBD_MIRROR_IMAGE_MODE_JOURNAL"
        _RBD_MIRROR_IMAGE_MODE_SNAPSHOT "RBD_MIRROR_IMAGE_MODE_SNAPSHOT"

    ctypedef enum rbd_mirror_image_state_t:
        _RBD_MIRROR_IMAGE_DISABLING "RBD_MIRROR_IMAGE_DISABLING"
        _RBD_MIRROR_IMAGE_ENABLED "RBD_MIRROR_IMAGE_ENABLED"
        _RBD_MIRROR_IMAGE_DISABLED "RBD_MIRROR_IMAGE_DISABLED"

    ctypedef struct rbd_mirror_image_info_t:
        char *global_id
        rbd_mirror_image_state_t state
        bint primary

    ctypedef enum rbd_mirror_image_status_state_t:
        _MIRROR_IMAGE_STATUS_STATE_UNKNOWN "MIRROR_IMAGE_STATUS_STATE_UNKNOWN"
        _MIRROR_IMAGE_STATUS_STATE_ERROR "MIRROR_IMAGE_STATUS_STATE_ERROR"
        _MIRROR_IMAGE_STATUS_STATE_SYNCING "MIRROR_IMAGE_STATUS_STATE_SYNCING"
        _MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY "MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY"
        _MIRROR_IMAGE_STATUS_STATE_REPLAYING "MIRROR_IMAGE_STATUS_STATE_REPLAYING"
        _MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY "MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY"
        _MIRROR_IMAGE_STATUS_STATE_STOPPED "MIRROR_IMAGE_STATUS_STATE_STOPPED"

    ctypedef struct rbd_mirror_image_site_status_t:
        char *mirror_uuid
        rbd_mirror_image_status_state_t state
        char *description
        time_t last_update
        bint up

    ctypedef struct rbd_mirror_image_global_status_t:
        char *name
        rbd_mirror_image_info_t info
        uint32_t site_statuses_count
        rbd_mirror_image_site_status_t *site_statuses

    ctypedef enum rbd_lock_mode_t:
        _RBD_LOCK_MODE_EXCLUSIVE "RBD_LOCK_MODE_EXCLUSIVE"
        _RBD_LOCK_MODE_SHARED "RBD_LOCK_MODE_SHARED"

    ctypedef enum rbd_trash_image_source_t:
        _RBD_TRASH_IMAGE_SOURCE_USER "RBD_TRASH_IMAGE_SOURCE_USER",
        _RBD_TRASH_IMAGE_SOURCE_MIRRORING "RBD_TRASH_IMAGE_SOURCE_MIRRORING",
        _RBD_TRASH_IMAGE_SOURCE_MIGRATION "RBD_TRASH_IMAGE_SOURCE_MIGRATION"
        _RBD_TRASH_IMAGE_SOURCE_REMOVING "RBD_TRASH_IMAGE_SOURCE_REMOVING"

    ctypedef struct rbd_trash_image_info_t:
        char *id
        char *name
        rbd_trash_image_source_t source
        time_t deletion_time
        time_t deferment_end_time

    ctypedef struct rbd_image_watcher_t:
        char *addr
        int64_t id
        uint64_t cookie

    ctypedef enum rbd_group_image_state_t:
        _RBD_GROUP_IMAGE_STATE_ATTACHED "RBD_GROUP_IMAGE_STATE_ATTACHED"
        _RBD_GROUP_IMAGE_STATE_INCOMPLETE "RBD_GROUP_IMAGE_STATE_INCOMPLETE"

    ctypedef struct rbd_group_image_info_t:
        char *name
        int64_t pool
        rbd_group_image_state_t state

    ctypedef enum rbd_group_snap_state_t:
        _RBD_GROUP_SNAP_STATE_INCOMPLETE "RBD_GROUP_SNAP_STATE_INCOMPLETE"
        _RBD_GROUP_SNAP_STATE_COMPLETE "RBD_GROUP_SNAP_STATE_COMPLETE"

    ctypedef struct rbd_group_snap_info_t:
        char *name
        rbd_group_snap_state_t state

    ctypedef enum rbd_image_migration_state_t:
        _RBD_IMAGE_MIGRATION_STATE_UNKNOWN "RBD_IMAGE_MIGRATION_STATE_UNKNOWN"
        _RBD_IMAGE_MIGRATION_STATE_ERROR "RBD_IMAGE_MIGRATION_STATE_ERROR"
        _RBD_IMAGE_MIGRATION_STATE_PREPARING "RBD_IMAGE_MIGRATION_STATE_PREPARING"
        _RBD_IMAGE_MIGRATION_STATE_PREPARED "RBD_IMAGE_MIGRATION_STATE_PREPARED"
        _RBD_IMAGE_MIGRATION_STATE_EXECUTING "RBD_IMAGE_MIGRATION_STATE_EXECUTING"
        _RBD_IMAGE_MIGRATION_STATE_EXECUTED "RBD_IMAGE_MIGRATION_STATE_EXECUTED"
        _RBD_IMAGE_MIGRATION_STATE_ABORTING "RBD_IMAGE_MIGRATION_STATE_ABORTING"

    ctypedef struct rbd_image_migration_status_t:
        int64_t source_pool_id
        char *source_pool_namespace
        char *source_image_name
        char *source_image_id
        int64_t dest_pool_id
        char *dest_pool_namespace
        char *dest_image_name
        char *dest_image_id
        rbd_image_migration_state_t state
        char *state_description

    ctypedef enum rbd_config_source_t:
        _RBD_CONFIG_SOURCE_CONFIG "RBD_CONFIG_SOURCE_CONFIG"
        _RBD_CONFIG_SOURCE_POOL "RBD_CONFIG_SOURCE_POOL"
        _RBD_CONFIG_SOURCE_IMAGE "RBD_CONFIG_SOURCE_IMAGE"

    ctypedef struct rbd_config_option_t:
        char *name
        char *value
        rbd_config_source_t source

    ctypedef enum rbd_pool_stat_option_t:
        _RBD_POOL_STAT_OPTION_IMAGES "RBD_POOL_STAT_OPTION_IMAGES"
        _RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES "RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES"
        _RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES "RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES"
        _RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS "RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS"
        _RBD_POOL_STAT_OPTION_TRASH_IMAGES "RBD_POOL_STAT_OPTION_TRASH_IMAGES"
        _RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES "RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES"
        _RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES "RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES"
        _RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS "RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS"

    ctypedef enum rbd_encryption_format_t:
        _RBD_ENCRYPTION_FORMAT_LUKS1 "RBD_ENCRYPTION_FORMAT_LUKS1"
        _RBD_ENCRYPTION_FORMAT_LUKS2 "RBD_ENCRYPTION_FORMAT_LUKS2"
        _RBD_ENCRYPTION_FORMAT_LUKS "RBD_ENCRYPTION_FORMAT_LUKS"

    ctypedef enum rbd_encryption_algorithm_t:
        _RBD_ENCRYPTION_ALGORITHM_AES128 "RBD_ENCRYPTION_ALGORITHM_AES128"
        _RBD_ENCRYPTION_ALGORITHM_AES256 "RBD_ENCRYPTION_ALGORITHM_AES256"

    ctypedef struct rbd_encryption_luks1_format_options_t:
        rbd_encryption_algorithm_t alg
        const char* passphrase
        size_t passphrase_size

    ctypedef struct rbd_encryption_luks2_format_options_t:
        rbd_encryption_algorithm_t alg
        const char* passphrase
        size_t passphrase_size

    ctypedef struct rbd_encryption_luks_format_options_t:
        const char* passphrase
        size_t passphrase_size

    ctypedef void* rbd_encryption_options_t

    ctypedef struct rbd_encryption_spec_t:
        rbd_encryption_format_t format
        rbd_encryption_options_t opts
        size_t opts_size

    ctypedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg)

    void rbd_version(int *major, int *minor, int *extra)

    void rbd_image_spec_list_cleanup(rbd_image_spec_t *image, size_t num_images)
    void rbd_linked_image_spec_cleanup(rbd_linked_image_spec_t *image)
    void rbd_linked_image_spec_list_cleanup(rbd_linked_image_spec_t *images,
                                            size_t num_images)
    void rbd_snap_spec_cleanup(rbd_snap_spec_t *snap)

    void rbd_image_options_create(rbd_image_options_t* opts)
    void rbd_image_options_destroy(rbd_image_options_t opts)
    int rbd_image_options_set_string(rbd_image_options_t opts, int optname,
                                     const char* optval)
    int rbd_image_options_set_uint64(rbd_image_options_t opts, int optname,
                                     uint64_t optval)
    int rbd_image_options_get_string(rbd_image_options_t opts, int optname,
                                     char* optval, size_t maxlen)
    int rbd_image_options_get_uint64(rbd_image_options_t opts, int optname,
                                     uint64_t* optval)
    int rbd_image_options_unset(rbd_image_options_t opts, int optname)
    void rbd_image_options_clear(rbd_image_options_t opts)
    int rbd_image_options_is_empty(rbd_image_options_t opts)

    int rbd_list(rados_ioctx_t io, char *names, size_t *size)
    int rbd_list2(rados_ioctx_t io, rbd_image_spec_t *images,
                  size_t *num_images)
    int rbd_create(rados_ioctx_t io, const char *name, uint64_t size,
                   int *order)
    int rbd_create4(rados_ioctx_t io, const char *name, uint64_t size,
                    rbd_image_options_t opts)
    int rbd_clone3(rados_ioctx_t p_ioctx, const char *p_name,
                   const char *p_snapname, rados_ioctx_t c_ioctx,
                   const char *c_name, rbd_image_options_t c_opts)
    int rbd_remove_with_progress(rados_ioctx_t io, const char *name,
                                 librbd_progress_fn_t cb, void *cbdata)
    int rbd_rename(rados_ioctx_t src_io_ctx, const char *srcname,
                   const char *destname)

    int rbd_trash_move(rados_ioctx_t io, const char *name, uint64_t delay)
    int rbd_trash_get(rados_ioctx_t io, const char *id,
                      rbd_trash_image_info_t *info)
    void rbd_trash_get_cleanup(rbd_trash_image_info_t *info)
    int rbd_trash_list(rados_ioctx_t io, rbd_trash_image_info_t *trash_entries,
                       size_t *num_entries)
    void rbd_trash_list_cleanup(rbd_trash_image_info_t *trash_entries,
                                size_t num_entries)
    int rbd_trash_purge(rados_ioctx_t io, time_t expire_ts, float threshold)
    int rbd_trash_remove_with_progress(rados_ioctx_t io, const char *id,
                                       int force, librbd_progress_fn_t cb,
                                       void *cbdata)
    int rbd_trash_restore(rados_ioctx_t io, const char *id, const char *name)

    int rbd_migration_prepare(rados_ioctx_t io_ctx, const char *image_name,
                              rados_ioctx_t dest_io_ctx,
                              const char *dest_image_name,
                              rbd_image_options_t opts)
    int rbd_migration_prepare_import(const char *source_spec,
                                    rados_ioctx_t dest_io_ctx,
                                    const char *dest_image_name,
                                    rbd_image_options_t opts)
    int rbd_migration_execute_with_progress(rados_ioctx_t io_ctx,
                                            const char *image_name,
                                            librbd_progress_fn_t cb,
                                            void *cbdata)
    int rbd_migration_commit_with_progress(rados_ioctx_t io_ctx,
                                           const char *image_name,
                                           librbd_progress_fn_t cb,
                                           void *cbdata)
    int rbd_migration_abort_with_progress(rados_ioctx_t io_ctx,
                                          const char *image_name,
                                          librbd_progress_fn_t cb, void *cbdata)
    int rbd_migration_status(rados_ioctx_t io_ctx, const char *image_name,
                             rbd_image_migration_status_t *status,
                             size_t status_size)
    void rbd_migration_status_cleanup(rbd_image_migration_status_t *status)

    int rbd_mirror_site_name_get(rados_t cluster, char *name, size_t *max_len)
    int rbd_mirror_site_name_set(rados_t cluster, const char *name)

    int rbd_mirror_mode_get(rados_ioctx_t io, rbd_mirror_mode_t *mirror_mode)
    int rbd_mirror_mode_set(rados_ioctx_t io, rbd_mirror_mode_t mirror_mode)

    int rbd_mirror_uuid_get(rados_ioctx_t io_ctx, char *mirror_uuid,
                            size_t *max_len)

    int rbd_mirror_peer_bootstrap_create(rados_ioctx_t io_ctx, char *token,
                                         size_t *max_len)
    int rbd_mirror_peer_bootstrap_import(
        rados_ioctx_t io_ctx, rbd_mirror_peer_direction_t direction,
        const char *token)

    int rbd_mirror_peer_site_add(
        rados_ioctx_t io, char *uuid, size_t uuid_max_length,
        rbd_mirror_peer_direction_t direction, const char *site_name,
        const char *client_name)
    int rbd_mirror_peer_site_remove(rados_ioctx_t io, const char *uuid)
    int rbd_mirror_peer_site_list(
        rados_ioctx_t io_ctx, rbd_mirror_peer_site_t *peers,int *max_peers)
    void rbd_mirror_peer_site_list_cleanup(
        rbd_mirror_peer_site_t *peers, int max_peers)

    int rbd_mirror_peer_site_set_name(
        rados_ioctx_t io_ctx, const char *uuid, const char *site_name)
    int rbd_mirror_peer_site_set_client_name(
        rados_ioctx_t io_ctx, const char *uuid, const char *client_name)

    int rbd_mirror_peer_site_get_attributes(
        rados_ioctx_t io_ctx, const char *uuid, char *keys, size_t *max_key_len,
        char *values, size_t *max_val_length, size_t *key_value_count)
    int rbd_mirror_peer_site_set_attributes(
        rados_ioctx_t io_ctx, const char *uuid, const char *keys,
        const char *values, size_t count)

    int rbd_mirror_image_global_status_list(
        rados_ioctx_t io, const char *start_id, size_t max, char **image_ids,
        rbd_mirror_image_global_status_t *images, size_t *len)
    void rbd_mirror_image_global_status_list_cleanup(
        char **image_ids, rbd_mirror_image_global_status_t *images, size_t len)
    int rbd_mirror_image_status_summary(rados_ioctx_t io,
                                        rbd_mirror_image_status_state_t *states,
                                        int *counts, size_t *maxlen)
    int rbd_mirror_image_instance_id_list(rados_ioctx_t io_ctx,
                                          const char *start_id,
                                          size_t max, char **image_ids,
                                          char **instance_ids,
                                          size_t *len)
    void rbd_mirror_image_instance_id_list_cleanup(char **image_ids,
                                                   char **instance_ids,
                                                   size_t len)
    int rbd_mirror_image_info_list(rados_ioctx_t io_ctx,
                                   rbd_mirror_image_mode_t *mode_filter,
                                   const char *start_id, size_t max,
                                   char **image_ids,
                                   rbd_mirror_image_mode_t *mode_entries,
                                   rbd_mirror_image_info_t *info_entries,
                                   size_t *num_entries)
    void rbd_mirror_image_info_list_cleanup(char **image_ids,
                                            rbd_mirror_image_info_t *info_entries,
                                            size_t num_entries)

    int rbd_pool_metadata_get(rados_ioctx_t io_ctx, const char *key,
                              char *value, size_t *val_len)
    int rbd_pool_metadata_set(rados_ioctx_t io_ctx, const char *key,
                              const char *value)
    int rbd_pool_metadata_remove(rados_ioctx_t io_ctx, const char *key)
    int rbd_pool_metadata_list(rados_ioctx_t io_ctx, const char *start,
                               uint64_t max, char *keys, size_t *key_len,
                               char *values, size_t *vals_len)

    int rbd_config_pool_list(rados_ioctx_t io_ctx, rbd_config_option_t *options,
                             int *max_options)
    void rbd_config_pool_list_cleanup(rbd_config_option_t *options,
                                      int max_options)

    int rbd_open(rados_ioctx_t io, const char *name,
                 rbd_image_t *image, const char *snap_name)
    int rbd_open_by_id(rados_ioctx_t io, const char *image_id,
                       rbd_image_t *image, const char *snap_name)
    int rbd_open_read_only(rados_ioctx_t io, const char *name,
                           rbd_image_t *image, const char *snap_name)
    int rbd_open_by_id_read_only(rados_ioctx_t io, const char *image_id,
                                 rbd_image_t *image, const char *snap_name)
    int rbd_aio_open(rados_ioctx_t io, const char *name, rbd_image_t *image,
                     const char *snap_name, rbd_completion_t c)
    int rbd_aio_open_by_id(rados_ioctx_t io, const char *id, rbd_image_t *image,
                           const char *snap_name, rbd_completion_t c)
    int rbd_aio_open_read_only(rados_ioctx_t io, const char *name,
                               rbd_image_t *image, const char *snap_name,
                               rbd_completion_t c)
    int rbd_aio_open_by_id_read_only(rados_ioctx_t io, const char *id,
                                     rbd_image_t *image, const char *snap_name,
                                     rbd_completion_t c)
    int rbd_features_to_string(uint64_t features, char *str_features, size_t *size)
    int rbd_features_from_string(const char *str_features, uint64_t *features)
    int rbd_close(rbd_image_t image)
    int rbd_aio_close(rbd_image_t image, rbd_completion_t c)
    int rbd_resize2(rbd_image_t image, uint64_t size, bint allow_shrink,
                    librbd_progress_fn_t cb, void *cbdata)
    int rbd_stat(rbd_image_t image, rbd_image_info_t *info, size_t infosize)
    int rbd_get_old_format(rbd_image_t image, uint8_t *old)
    int rbd_get_size(rbd_image_t image, uint64_t *size)
    int rbd_get_features(rbd_image_t image, uint64_t *features)
    int rbd_update_features(rbd_image_t image, uint64_t features,
                            uint8_t enabled)
    int rbd_get_op_features(rbd_image_t image, uint64_t *op_features)
    int rbd_get_stripe_unit(rbd_image_t image, uint64_t *stripe_unit)
    int rbd_get_stripe_count(rbd_image_t image, uint64_t *stripe_count)
    int rbd_get_create_timestamp(rbd_image_t image, timespec *timestamp)
    int rbd_get_access_timestamp(rbd_image_t image, timespec *timestamp)
    int rbd_get_modify_timestamp(rbd_image_t image, timespec *timestamp)
    int rbd_get_overlap(rbd_image_t image, uint64_t *overlap)
    int rbd_get_name(rbd_image_t image, char *name, size_t *name_len)
    int rbd_get_id(rbd_image_t image, char *id, size_t id_len)
    int rbd_get_block_name_prefix(rbd_image_t image, char *prefix,
                                  size_t prefix_len)
    int64_t rbd_get_data_pool_id(rbd_image_t image)
    int rbd_get_parent(rbd_image_t image,
                       rbd_linked_image_spec_t *parent_image,
                       rbd_snap_spec_t *parent_snap)
    int rbd_get_migration_source_spec(rbd_image_t image,
                                      char* source_spec, size_t* max_len)
    int rbd_get_flags(rbd_image_t image, uint64_t *flags)
    int rbd_get_group(rbd_image_t image, rbd_group_info_t *group_info,
                      size_t group_info_size)

    ssize_t rbd_read2(rbd_image_t image, uint64_t ofs, size_t len,
                      char *buf, int op_flags)
    ssize_t rbd_write2(rbd_image_t image, uint64_t ofs, size_t len,
                       const char *buf, int op_flags)
    int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
    int rbd_write_zeroes(rbd_image_t image, uint64_t ofs, uint64_t len,
                         int zero_flags, int op_flags)
    int rbd_copy3(rbd_image_t src, rados_ioctx_t dest_io_ctx,
                  const char *destname, rbd_image_options_t dest_opts)
    int rbd_deep_copy(rbd_image_t src, rados_ioctx_t dest_io_ctx,
                      const char *destname, rbd_image_options_t dest_opts)
    int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps,
                      int *max_snaps)
    void rbd_snap_list_end(rbd_snap_info_t *snaps)
    int rbd_snap_create2(rbd_image_t image, const char *snapname, uint32_t flags,
			 librbd_progress_fn_t cb, void *cbdata)
    int rbd_snap_remove(rbd_image_t image, const char *snapname)
    int rbd_snap_remove2(rbd_image_t image, const char *snapname, uint32_t flags,
			 librbd_progress_fn_t cb, void *cbdata)
    int rbd_snap_remove_by_id(rbd_image_t image, uint64_t snap_id)
    int rbd_snap_rollback(rbd_image_t image, const char *snapname)
    int rbd_snap_rename(rbd_image_t image, const char *snapname,
                        const char* dstsnapsname)
    int rbd_snap_protect(rbd_image_t image, const char *snap_name)
    int rbd_snap_unprotect(rbd_image_t image, const char *snap_name)
    int rbd_snap_is_protected(rbd_image_t image, const char *snap_name,
                              int *is_protected)
    int rbd_snap_exists(rbd_image_t image, const char *snapname, libcpp.bool *exists)
    int rbd_snap_get_limit(rbd_image_t image, uint64_t *limit)
    int rbd_snap_set_limit(rbd_image_t image, uint64_t limit)
    int rbd_snap_get_timestamp(rbd_image_t image, uint64_t snap_id, timespec *timestamp)
    int rbd_snap_set(rbd_image_t image, const char *snapname)
    int rbd_snap_set_by_id(rbd_image_t image, uint64_t snap_id)
    int rbd_snap_get_name(rbd_image_t image, uint64_t snap_id,
                          char *snapname, size_t *name_len)
    int rbd_snap_get_id(rbd_image_t image, const char *snapname,
                                uint64_t *snap_id)
    int rbd_snap_get_namespace_type(rbd_image_t image,
                                    uint64_t snap_id,
                                    rbd_snap_namespace_type_t *namespace_type)
    int rbd_snap_get_group_namespace(rbd_image_t image, uint64_t snap_id,
                                     rbd_snap_group_namespace_t *group_info,
                                     size_t snap_group_namespace_size)
    void rbd_snap_group_namespace_cleanup(rbd_snap_group_namespace_t *group_spec,
                                          size_t snap_group_namespace_size)
    int rbd_snap_get_trash_namespace(rbd_image_t image, uint64_t snap_id,
                                     char *original_name, size_t max_length)
    int rbd_snap_get_mirror_namespace(
        rbd_image_t image, uint64_t snap_id,
        rbd_snap_mirror_namespace_t *mirror_ns,
        size_t snap_mirror_namespace_size)
    void rbd_snap_mirror_namespace_cleanup(
        rbd_snap_mirror_namespace_t *mirror_ns,
        size_t snap_mirror_namespace_size)

    int rbd_flatten_with_progress(rbd_image_t image, librbd_progress_fn_t cb,
                                  void *cbdata)
    int rbd_sparsify(rbd_image_t image, size_t sparse_size)
    int rbd_rebuild_object_map(rbd_image_t image, librbd_progress_fn_t cb,
                               void *cbdata)
    int rbd_list_children3(rbd_image_t image, rbd_linked_image_spec_t *children,
                           size_t *max_children)
    int rbd_list_descendants(rbd_image_t image,
                             rbd_linked_image_spec_t *descendants,
                             size_t *max_descendants)

    ssize_t rbd_list_lockers(rbd_image_t image, int *exclusive,
                             char *tag, size_t *tag_len,
                             char *clients, size_t *clients_len,
                             char *cookies, size_t *cookies_len,
                             char *addrs, size_t *addrs_len)
    int rbd_lock_exclusive(rbd_image_t image, const char *cookie)
    int rbd_lock_shared(rbd_image_t image, const char *cookie,
                        const char *tag)
    int rbd_unlock(rbd_image_t image, const char *cookie)
    int rbd_break_lock(rbd_image_t image, const char *client,
                       const char *cookie)

    int rbd_is_exclusive_lock_owner(rbd_image_t image, int *is_owner)
    int rbd_lock_acquire(rbd_image_t image, rbd_lock_mode_t lock_mode)
    int rbd_lock_release(rbd_image_t image)
    int rbd_lock_get_owners(rbd_image_t image, rbd_lock_mode_t *lock_mode,
                            char **lock_owners, size_t *max_lock_owners)
    void rbd_lock_get_owners_cleanup(char **lock_owners,
                                     size_t lock_owner_count)
    int rbd_lock_break(rbd_image_t image, rbd_lock_mode_t lock_mode,
                       char *lock_owner)

    # We use -9000 to propagate Python exceptions. We use except? to make sure
    # things still work as intended if -9000 happens to be a valid errno value
    # somewhere.
    int rbd_diff_iterate2(rbd_image_t image, const char *fromsnapname,
                         uint64_t ofs, uint64_t len,
                         uint8_t include_parent, uint8_t whole_object,
                         int (*cb)(uint64_t, size_t, int, void *)
                             nogil except? -9000,
                         void *arg) except? -9000

    int rbd_flush(rbd_image_t image)
    int rbd_invalidate_cache(rbd_image_t image)

    int rbd_mirror_image_enable2(rbd_image_t image,
                                 rbd_mirror_image_mode_t mode)
    int rbd_mirror_image_disable(rbd_image_t image, bint force)
    int rbd_mirror_image_promote(rbd_image_t image, bint force)
    int rbd_mirror_image_demote(rbd_image_t image)
    int rbd_mirror_image_resync(rbd_image_t image)
    int rbd_mirror_image_create_snapshot2(rbd_image_t image, uint32_t flags,
                                          uint64_t *snap_id)
    int rbd_aio_mirror_image_create_snapshot(rbd_image_t image, uint32_t flags,
                                             uint64_t *snap_id,
                                             rbd_completion_t c)
    int rbd_mirror_image_get_info(rbd_image_t image,
                                  rbd_mirror_image_info_t *mirror_image_info,
                                  size_t info_size)
    void rbd_mirror_image_get_info_cleanup(
        rbd_mirror_image_info_t *mirror_image_info)
    int rbd_aio_mirror_image_get_info(
        rbd_image_t image, rbd_mirror_image_info_t *mirror_image_info,
        size_t info_size, rbd_completion_t c)
    int rbd_mirror_image_get_mode(rbd_image_t image,
                                  rbd_mirror_image_mode_t *mode)
    int rbd_aio_mirror_image_get_mode(rbd_image_t image,
                                      rbd_mirror_image_mode_t *mode,
                                      rbd_completion_t c)
    int rbd_mirror_image_get_global_status(
        rbd_image_t image,
        rbd_mirror_image_global_status_t *mirror_image_global_status,
        size_t status_size)
    void rbd_mirror_image_global_status_cleanup(
        rbd_mirror_image_global_status_t *mirror_image_global_status)
    int rbd_mirror_image_get_instance_id(rbd_image_t image, char *instance_id,
                                         size_t *id_max_length)

    int rbd_aio_write2(rbd_image_t image, uint64_t off, size_t len,
                       const char *buf, rbd_completion_t c, int op_flags)
    int rbd_aio_read2(rbd_image_t image, uint64_t off, size_t len,
                      char *buf, rbd_completion_t c, int op_flags)
    int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
                        rbd_completion_t c)
    int rbd_aio_write_zeroes(rbd_image_t image, uint64_t off, uint64_t len,
                             rbd_completion_t c, int zero_flags, int op_flags)

    int rbd_aio_create_completion(void *cb_arg, rbd_callback_t complete_cb,
                                  rbd_completion_t *c)
    int rbd_aio_is_complete(rbd_completion_t c)
    int rbd_aio_wait_for_complete(rbd_completion_t c)
    ssize_t rbd_aio_get_return_value(rbd_completion_t c)
    void rbd_aio_release(rbd_completion_t c)
    int rbd_aio_flush(rbd_image_t image, rbd_completion_t c)

    int rbd_metadata_get(rbd_image_t image, const char *key, char *value,
                         size_t *val_len)
    int rbd_metadata_set(rbd_image_t image, const char *key, const char *value)
    int rbd_metadata_remove(rbd_image_t image, const char *key)
    int rbd_metadata_list(rbd_image_t image, const char *start, uint64_t max,
                          char *keys, size_t *key_len, char *values,
                          size_t *vals_len)
    int rbd_group_create(rados_ioctx_t p, const char *name)
    int rbd_group_remove(rados_ioctx_t p, const char *name)
    int rbd_group_list(rados_ioctx_t p, char *names, size_t *size)
    int rbd_group_rename(rados_ioctx_t p, const char *src, const char *dest)
    void rbd_group_info_cleanup(rbd_group_info_t *group_info,
                                size_t group_info_size)
    int rbd_group_image_add(rados_ioctx_t group_p, const char *group_name,
			    rados_ioctx_t image_p, const char *image_name)
    int rbd_group_image_remove(rados_ioctx_t group_p, const char *group_name,
                               rados_ioctx_t image_p, const char *image_name)

    int rbd_group_image_list(rados_ioctx_t group_p,
                             const char *group_name,
                             rbd_group_image_info_t *images,
                             size_t group_image_info_size,
                             size_t *image_size)
    void rbd_group_image_list_cleanup(rbd_group_image_info_t *images,
                                      size_t group_image_info_size, size_t len)

    int rbd_group_snap_create2(rados_ioctx_t group_p, const char *group_name,
                               const char *snap_name, uint32_t flags)

    int rbd_group_snap_remove(rados_ioctx_t group_p, const char *group_name,
                              const char *snap_name)

    int rbd_group_snap_rename(rados_ioctx_t group_p, const char *group_name,
                              const char *old_snap_name,
                              const char *new_snap_name)

    int rbd_group_snap_list(rados_ioctx_t group_p,
                            const char *group_name,
                            rbd_group_snap_info_t *snaps,
                            size_t group_snap_info_size,
                            size_t *snaps_size)

    void rbd_group_snap_list_cleanup(rbd_group_snap_info_t *snaps,
                                     size_t group_snap_info_size, size_t len)
    int rbd_group_snap_rollback(rados_ioctx_t group_p, const char *group_name,
                                const char *snap_name)

    int rbd_watchers_list(rbd_image_t image, rbd_image_watcher_t *watchers,
                          size_t *max_watchers)
    void rbd_watchers_list_cleanup(rbd_image_watcher_t *watchers,
                                   size_t num_watchers)

    int rbd_config_image_list(rbd_image_t image, rbd_config_option_t *options,
                              int *max_options)
    void rbd_config_image_list_cleanup(rbd_config_option_t *options,
                                       int max_options)

    int rbd_namespace_create(rados_ioctx_t io, const char *namespace_name)
    int rbd_namespace_remove(rados_ioctx_t io, const char *namespace_name)
    int rbd_namespace_list(rados_ioctx_t io, char *namespace_names,
                           size_t *size)
    int rbd_namespace_exists(rados_ioctx_t io, const char *namespace_name,
                             libcpp.bool *exists)

    int rbd_pool_init(rados_ioctx_t, bint force)

    void rbd_pool_stats_create(rbd_pool_stats_t *stats)
    void rbd_pool_stats_destroy(rbd_pool_stats_t stats)
    int rbd_pool_stats_option_add_uint64(rbd_pool_stats_t stats,
                                         int stat_option, uint64_t* stat_val)
    int rbd_pool_stats_get(rados_ioctx_t io, rbd_pool_stats_t stats)

    int rbd_encryption_format(rbd_image_t image,
                              rbd_encryption_format_t format,
                              rbd_encryption_options_t opts, size_t opts_size)
    int rbd_encryption_load(rbd_image_t image,
                            rbd_encryption_format_t format,
                            rbd_encryption_options_t opts, size_t opts_size)
    int rbd_encryption_load2(rbd_image_t image,
                             const rbd_encryption_spec_t *specs,
                             size_t spec_count)
