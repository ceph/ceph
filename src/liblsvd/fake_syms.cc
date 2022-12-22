/* various other stuff to prevent librbd.so from getting linked in */

#include "fake_rbd.h"

#define trace(x) CEPH_RBD_API long lttng_ust_tracepoint_librbd___ ## x[7] = {1}

trace(aio_close_image_enter);
trace(aio_close_image_exit);
trace(aio_compare_and_write_enter);
trace(aio_compare_and_write_exit);
trace(aio_complete_enter);
trace(aio_complete_exit);
trace(aio_discard_enter);
trace(aio_discard_exit);
trace(aio_flush_enter);
trace(aio_flush_exit);
trace(aio_get_return_value_enter);
trace(aio_get_return_value_exit);
trace(aio_is_complete_enter);
trace(aio_is_complete_exit);
trace(aio_open_image_by_id_enter);
trace(aio_open_image_by_id_exit);
trace(aio_open_image_enter);
trace(aio_open_image_exit);
trace(aio_read2_enter);
trace(aio_read_enter);
trace(aio_read_exit);
trace(aio_wait_for_complete_enter);
trace(aio_wait_for_complete_exit);
trace(aio_write2_enter);
trace(aio_write_enter);
trace(aio_write_exit);
trace(aio_writesame_enter);
trace(aio_writesame_exit);
trace(break_lock_enter);
trace(break_lock_exit);
trace(clone2_enter);
trace(clone2_exit);
trace(clone3_enter);
trace(clone3_exit);
trace(clone_enter);
trace(clone_exit);
trace(close_image_enter);
trace(close_image_exit);
trace(compare_and_write_enter);
trace(compare_and_write_exit);
trace(copy2_enter);
trace(copy2_exit);
trace(copy3_enter);
trace(copy3_exit);
trace(copy4_enter);
trace(copy4_exit);
trace(copy_enter);
trace(copy_exit);
trace(create2_enter);
trace(create2_exit);
trace(create3_enter);
trace(create3_exit);
trace(create4_enter);
trace(create4_exit);
trace(create_enter);
trace(create_exit);
trace(deep_copy_enter);
trace(deep_copy_exit);
trace(diff_iterate_enter);
trace(diff_iterate_exit);
trace(discard_enter);
trace(discard_exit);
trace(flatten_enter);
trace(flatten_exit);
trace(flush_enter);
trace(flush_exit);
trace(get_access_timestamp_enter);
trace(get_access_timestamp_exit);
trace(get_create_timestamp_enter);
trace(get_create_timestamp_exit);
trace(get_features_enter);
trace(get_features_exit);
trace(get_flags_enter);
trace(get_flags_exit);
trace(get_modify_timestamp_enter);
trace(get_modify_timestamp_exit);
trace(get_old_format_enter);
trace(get_old_format_exit);
trace(get_overlap_enter);
trace(get_overlap_exit);
trace(get_parent_info_enter);
trace(get_parent_info_exit);
trace(get_size_enter);
trace(get_size_exit);
trace(get_stripe_count_enter);
trace(get_stripe_count_exit);
trace(get_stripe_unit_enter);
trace(get_stripe_unit_exit);
trace(group_create_enter);
trace(group_create_exit);
trace(group_image_add_enter);
trace(group_image_add_exit);
trace(group_image_list_enter);
trace(group_image_list_exit);
trace(group_image_remove_by_id_enter);
trace(group_image_remove_by_id_exit);
trace(group_image_remove_enter);
trace(group_image_remove_exit);
trace(group_list_enter);
trace(group_list_entry);
trace(group_list_exit);
trace(group_remove_enter);
trace(group_remove_exit);
trace(group_rename_enter);
trace(group_rename_exit);
trace(group_snap_create_enter);
trace(group_snap_create_exit);
trace(group_snap_list_enter);
trace(group_snap_list_exit);
trace(group_snap_remove_enter);
trace(group_snap_remove_exit);
trace(group_snap_rename_enter);
trace(group_snap_rename_exit);
trace(group_snap_rollback_enter);
trace(group_snap_rollback_exit);
trace(image_get_group_enter);
trace(image_get_group_exit);
trace(invalidate_cache_enter);
trace(invalidate_cache_exit);
trace(is_exclusive_lock_owner_enter);
trace(is_exclusive_lock_owner_exit);
trace(list_children_enter);
trace(list_children_entry);
trace(list_children_exit);
trace(list_enter);
trace(list_entry);
trace(list_exit);
trace(list_lockers_enter);
trace(list_lockers_entry);
trace(list_lockers_exit);
trace(list_watchers_enter);
trace(list_watchers_entry);
trace(list_watchers_exit);
trace(lock_acquire_enter);
trace(lock_acquire_exit);
trace(lock_break_enter);
trace(lock_break_exit);
trace(lock_exclusive_enter);
trace(lock_exclusive_exit);
trace(lock_get_owners_enter);
trace(lock_get_owners_exit);
trace(lock_release_enter);
trace(lock_release_exit);
trace(lock_shared_enter);
trace(lock_shared_exit);
trace(metadata_get_enter);
trace(metadata_get_exit);
trace(metadata_list_enter);
trace(metadata_list_entry);
trace(metadata_list_exit);
trace(metadata_remove_enter);
trace(metadata_remove_exit);
trace(metadata_set_enter);
trace(metadata_set_exit);
trace(migration_abort_enter);
trace(migration_abort_exit);
trace(migration_commit_enter);
trace(migration_commit_exit);
trace(migration_execute_enter);
trace(migration_execute_exit);
trace(migration_prepare_enter);
trace(migration_prepare_exit);
trace(migration_status_enter);
trace(migration_status_exit);
trace(open_image_by_id_enter);
trace(open_image_by_id_exit);
trace(open_image_enter);
trace(open_image_exit);
trace(poll_io_events_enter);
trace(poll_io_events_exit);
trace(read2_enter);
trace(read_enter);
trace(read_exit);
trace(read_iterate2_enter);
trace(read_iterate2_exit);
trace(read_iterate_enter);
trace(read_iterate_exit);
trace(remove_enter);
trace(remove_exit);
trace(rename_enter);
trace(rename_exit);
trace(resize_enter);
trace(resize_exit);
trace(set_image_notification_enter);
trace(set_image_notification_exit);
trace(snap_create_enter);
trace(snap_create_exit);
trace(snap_exists_enter);
trace(snap_exists_exit);
trace(snap_get_group_namespace_enter);
trace(snap_get_group_namespace_exit);
trace(snap_get_limit_enter);
trace(snap_get_limit_exit);
trace(snap_get_namespace_type_enter);
trace(snap_get_namespace_type_exit);
trace(snap_get_timestamp_enter);
trace(snap_get_timestamp_exit);
trace(snap_is_protected_enter);
trace(snap_is_protected_exit);
trace(snap_list_end_enter);
trace(snap_list_end_exit);
trace(snap_list_enter);
trace(snap_list_entry);
trace(snap_list_exit);
trace(snap_protect_enter);
trace(snap_protect_exit);
trace(snap_remove2_enter);
trace(snap_remove2_exit);
trace(snap_remove_enter);
trace(snap_remove_exit);
trace(snap_rename_enter);
trace(snap_rename_exit);
trace(snap_rollback_enter);
trace(snap_rollback_exit);
trace(snap_set_enter);
trace(snap_set_exit);
trace(snap_set_limit_enter);
trace(snap_set_limit_exit);
trace(snap_unprotect_enter);
trace(snap_unprotect_exit);
trace(sparsify_enter);
trace(sparsify_exit);
trace(stat_enter);
trace(stat_exit);
trace(trash_list_enter);
trace(trash_list_entry);
trace(trash_list_exit);
trace(trash_move_enter);
trace(trash_move_exit);
trace(trash_purge_enter);
trace(trash_purge_exit);
trace(trash_remove_enter);
trace(trash_remove_exit);
trace(trash_undelete_enter);
trace(trash_undelete_exit);
trace(unlock_enter);
trace(unlock_exit);
trace(update_features_enter);
trace(update_features_exit);
trace(update_unwatch_enter);
trace(update_unwatch_exit);
trace(update_watch_enter);
trace(update_watch_exit);
trace(write2_enter);
trace(write_enter);
trace(write_exit);
trace(writesame_enter);
trace(writesame_exit);

typedef void *completion_t;
typedef void (*callback_t)(completion_t cb, void *arg);
namespace librbd {
    class CEPH_RBD_API ProgressContext
    {
    public:
	virtual ~ProgressContext();
	virtual int update_progress(uint64_t offset, uint64_t total) = 0;
    };
    class CEPH_RBD_API ImageOptions {
	ImageOptions() {}
	ImageOptions(rbd_image_options_t opts) {}
	ImageOptions(const ImageOptions &imgopts) {}
	~ImageOptions() {}
    };
    class CEPH_RBD_API RBD
    {
    public:
	RBD() {}
	~RBD() {}

	// This must be dynamically allocated with new, and
	// must be released with release().
	// Do not use delete.
	struct AioCompletion {
	    void *pc;
	    AioCompletion(void *cb_arg, callback_t complete_cb);
	    bool is_complete() {return false;}
	    int wait_for_complete() {return 0;}
	    ssize_t get_return_value() {return 0;}
	    void *get_arg() {return 0;}
	    void release() {}
	};
    };
};

class CEPH_RBD_API UpdateWatchCtx {
public:
  virtual ~UpdateWatchCtx() {}
  virtual void handle_notify() = 0;
};

class CEPH_RBD_API QuiesceWatchCtx {
public:
  virtual ~QuiesceWatchCtx() {}
  virtual void handle_quiesce() = 0;
  virtual void handle_unquiesce() = 0;
};

