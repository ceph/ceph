// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include "include/int_types.h"

#include <list>
#include <map>
#include <string>
#include <vector>

#include "common/event_socket.h"
#include "common/Mutex.h"
#include "common/Readahead.h"
#include "common/RWLock.h"
#include "common/snap_types.h"
#include "common/zipkin_trace.h"

#include "include/buffer_fwd.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "include/types.h"
#include "include/xlist.h"

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AsyncRequest.h"
#include "librbd/Types.h"

class CephContext;
class ContextWQ;
class Finisher;
class PerfCounters;
class ThreadPool;
class SafeTimer;

namespace librbd {

  template <typename> class ExclusiveLock;
  template <typename> class ImageState;
  template <typename> class ImageWatcher;
  template <typename> class Journal;
  class LibrbdAdminSocketHook;
  template <typename> class ObjectMap;
  template <typename> class Operations;

  namespace cache { struct ImageCache; }
  namespace exclusive_lock { struct Policy; }
  namespace io {
  class AioCompletion;
  class AsyncOperation;
  template <typename> class CopyupRequest;
  template <typename> class ImageRequestWQ;
  template <typename> class ObjectDispatcher;
  }
  namespace journal { struct Policy; }

  namespace operation {
  template <typename> class ResizeRequest;
  }

  struct ImageCtx {
    CephContext *cct;
    PerfCounters *perfcounter;
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    std::vector<librados::snap_t> snaps; // this mirrors snapc.snaps, but is in
                                        // a format librados can understand
    std::map<librados::snap_t, SnapInfo> snap_info;
    std::map<std::pair<cls::rbd::SnapshotNamespace, std::string>, librados::snap_t> snap_ids;
    uint64_t snap_id;
    bool snap_exists; // false if our snap_id was deleted
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;

    std::map<rados::cls::lock::locker_id_t,
	     rados::cls::lock::locker_info_t> lockers;
    bool exclusive_locked;
    std::string lock_tag;

    std::string name;
    cls::rbd::SnapshotNamespace snap_namespace;
    std::string snap_name;
    IoCtx data_ctx, md_ctx;
    ImageWatcher<ImageCtx> *image_watcher;
    Journal<ImageCtx> *journal;

    /**
     * Lock ordering:
     *
     * owner_lock, md_lock, snap_lock, parent_lock,
     * object_map_lock, async_op_lock
     */
    RWLock owner_lock; // protects exclusive lock leadership updates
    RWLock md_lock; // protects access to the mutable image metadata that
                   // isn't guarded by other locks below, and blocks writes
                   // when held exclusively, so snapshots can be consistent.
                   // Fields guarded include:
                   // total_bytes_read
                   // exclusive_locked
                   // lock_tag
                   // lockers
    RWLock snap_lock; // protects snapshot-related member variables,
                      // features (and associated helper classes), and flags
    RWLock parent_lock; // protects parent_md and parent
    RWLock object_map_lock; // protects object map updates and object_map itself
    Mutex async_ops_lock; // protects async_ops and async_requests
    Mutex copyup_list_lock; // protects copyup_waiting_list
    Mutex completed_reqs_lock; // protects completed_reqs

    unsigned extra_read_flags;

    bool old_format;
    uint8_t order;
    uint64_t size;
    uint64_t features;
    std::string object_prefix;
    char *format_string;
    std::string header_oid;
    std::string id; // only used for new-format images
    ParentInfo parent_md;
    ImageCtx *parent;
    ImageCtx *child = nullptr;
    cls::rbd::GroupSpec group_spec;
    uint64_t stripe_unit, stripe_count;
    uint64_t flags;
    uint64_t op_features = 0;
    bool operations_disabled = false;
    utime_t create_timestamp;

    file_layout_t layout;

    cache::ImageCache *image_cache = nullptr;

    Readahead readahead;
    uint64_t total_bytes_read;

    std::map<uint64_t, io::CopyupRequest<ImageCtx>*> copyup_list;

    xlist<io::AsyncOperation*> async_ops;
    xlist<AsyncRequest<>*> async_requests;
    std::list<Context*> async_requests_waiters;

    ImageState<ImageCtx> *state;
    Operations<ImageCtx> *operations;

    ExclusiveLock<ImageCtx> *exclusive_lock;
    ObjectMap<ImageCtx> *object_map;

    xlist<operation::ResizeRequest<ImageCtx>*> resize_reqs;

    io::ImageRequestWQ<ImageCtx> *io_work_queue;
    io::ObjectDispatcher<ImageCtx> *io_object_dispatcher = nullptr;

    xlist<io::AioCompletion*> completed_reqs;
    EventSocket event_socket;

    ContextWQ *op_work_queue;

    // Configuration
    static const string METADATA_CONF_PREFIX;
    bool non_blocking_aio;
    bool cache;
    bool cache_writethrough_until_flush;
    uint64_t cache_size;
    uint64_t cache_max_dirty;
    uint64_t cache_target_dirty;
    double cache_max_dirty_age;
    uint32_t cache_max_dirty_object;
    bool cache_block_writes_upfront;
    uint32_t concurrent_management_ops;
    bool balance_snap_reads;
    bool localize_snap_reads;
    bool balance_parent_reads;
    bool localize_parent_reads;
    uint64_t sparse_read_threshold_bytes;
    uint32_t readahead_trigger_requests;
    uint64_t readahead_max_bytes;
    uint64_t readahead_disable_after_bytes;
    bool clone_copy_on_read;
    bool blacklist_on_break_lock;
    uint32_t blacklist_expire_seconds;
    uint32_t request_timed_out_seconds;
    bool enable_alloc_hint;
    uint8_t journal_order;
    uint8_t journal_splay_width;
    double journal_commit_age;
    int journal_object_flush_interval;
    uint64_t journal_object_flush_bytes;
    double journal_object_flush_age;
    std::string journal_pool;
    uint32_t journal_max_payload_bytes;
    int journal_max_concurrent_object_sets;
    bool mirroring_resync_after_disconnect;
    uint64_t mirroring_delete_delay;
    int mirroring_replay_delay;
    bool skip_partial_discard;
    bool blkin_trace_all;
    uint64_t qos_iops_limit;

    LibrbdAdminSocketHook *asok_hook;

    exclusive_lock::Policy *exclusive_lock_policy = nullptr;
    journal::Policy *journal_policy = nullptr;

    ZTracer::Endpoint trace_endpoint;

    static bool _filter_metadata_confs(const string &prefix, std::map<string, bool> &configs,
                                       const map<string, bufferlist> &pairs, map<string, bufferlist> *res);

    // unit test mock helpers
    static ImageCtx* create(const std::string &image_name,
                            const std::string &image_id,
                            const char *snap, IoCtx& p, bool read_only) {
      return new ImageCtx(image_name, image_id, snap, p, read_only);
    }
    void destroy() {
      delete this;
    }

    /**
     * Either image_name or image_id must be set.
     * If id is not known, pass the empty std::string,
     * and init() will look it up.
     */
    ImageCtx(const std::string &image_name, const std::string &image_id,
	     const char *snap, IoCtx& p, bool read_only);
    ~ImageCtx();
    void init();
    void shutdown();
    void init_layout();
    void perf_start(std::string name);
    void perf_stop();
    void set_read_flag(unsigned flag);
    int get_read_flags(librados::snap_t snap_id);
    int snap_set(cls::rbd::SnapshotNamespace in_snap_namespace,
		 std::string in_snap_name);
    void snap_unset();
    librados::snap_t get_snap_id(const cls::rbd::SnapshotNamespace& in_snap_namespace,
                                 const std::string& in_snap_name) const;
    const SnapInfo* get_snap_info(librados::snap_t in_snap_id) const;
    int get_snap_name(librados::snap_t in_snap_id,
		      std::string *out_snap_name) const;
    int get_snap_namespace(librados::snap_t in_snap_id,
			   cls::rbd::SnapshotNamespace *out_snap_namespace) const;
    int get_parent_spec(librados::snap_t in_snap_id,
			ParentSpec *pspec) const;
    int is_snap_protected(librados::snap_t in_snap_id,
			  bool *is_protected) const;
    int is_snap_unprotected(librados::snap_t in_snap_id,
			    bool *is_unprotected) const;

    uint64_t get_current_size() const;
    uint64_t get_object_size() const;
    string get_object_name(uint64_t num) const;
    uint64_t get_stripe_unit() const;
    uint64_t get_stripe_count() const;
    uint64_t get_stripe_period() const;
    utime_t get_create_timestamp() const;

    void add_snap(cls::rbd::SnapshotNamespace in_snap_namespace,
		  std::string in_snap_name,
		  librados::snap_t id,
		  uint64_t in_size, const ParentInfo &parent,
		  uint8_t protection_status, uint64_t flags, utime_t timestamp);
    void rm_snap(cls::rbd::SnapshotNamespace in_snap_namespace,
		 std::string in_snap_name,
		 librados::snap_t id);
    uint64_t get_image_size(librados::snap_t in_snap_id) const;
    uint64_t get_object_count(librados::snap_t in_snap_id) const;
    bool test_features(uint64_t test_features) const;
    bool test_features(uint64_t test_features,
                       const RWLock &in_snap_lock) const;
    bool test_op_features(uint64_t op_features) const;
    bool test_op_features(uint64_t op_features,
                          const RWLock &in_snap_lock) const;
    int get_flags(librados::snap_t in_snap_id, uint64_t *flags) const;
    int test_flags(uint64_t test_flags, bool *flags_set) const;
    int test_flags(uint64_t test_flags, const RWLock &in_snap_lock,
                   bool *flags_set) const;
    int update_flags(librados::snap_t in_snap_id, uint64_t flag, bool enabled);

    const ParentInfo* get_parent_info(librados::snap_t in_snap_id) const;
    int64_t get_parent_pool_id(librados::snap_t in_snap_id) const;
    std::string get_parent_image_id(librados::snap_t in_snap_id) const;
    uint64_t get_parent_snap_id(librados::snap_t in_snap_id) const;
    int get_parent_overlap(librados::snap_t in_snap_id,
			   uint64_t *overlap) const;
    void register_watch(Context *on_finish);
    uint64_t prune_parent_extents(vector<pair<uint64_t,uint64_t> >& objectx,
				  uint64_t overlap);

    void flush_async_operations();
    void flush_async_operations(Context *on_finish);

    void cancel_async_requests();
    void cancel_async_requests(Context *on_finish);

    void apply_metadata(const std::map<std::string, bufferlist> &meta,
                        bool thread_safe);

    ExclusiveLock<ImageCtx> *create_exclusive_lock();
    ObjectMap<ImageCtx> *create_object_map(uint64_t snap_id);
    Journal<ImageCtx> *create_journal();

    void clear_pending_completions();

    void set_image_name(const std::string &name);

    void notify_update();
    void notify_update(Context *on_finish);

    exclusive_lock::Policy *get_exclusive_lock_policy() const;
    void set_exclusive_lock_policy(exclusive_lock::Policy *policy);

    journal::Policy *get_journal_policy() const;
    void set_journal_policy(journal::Policy *policy);

    static void get_thread_pool_instance(CephContext *cct,
                                         ThreadPool **thread_pool,
                                         ContextWQ **op_work_queue);
    static void get_timer_instance(CephContext *cct, SafeTimer **timer,
                                   Mutex **timer_lock);
  };
}

#endif
