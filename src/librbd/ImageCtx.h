// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include "include/int_types.h"

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/allocator.h"
#include "common/ceph_mutex.h"
#include "common/config_proxy.h"
#include "common/event_socket.h"
#include "common/Readahead.h"
#include "common/snap_types.h"
#include "common/zipkin_trace.h"

#include "include/common_fwd.h"
#include "include/buffer_fwd.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "include/types.h"
#include "include/xlist.h"

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AsyncRequest.h"
#include "librbd/Types.h"

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>

class SafeTimer;

namespace neorados {
class IOContext;
class RADOS;
} // namespace neorados

namespace librbd {

  struct AsioEngine;
  template <typename> class ExclusiveLock;
  template <typename> class ImageState;
  template <typename> class ImageWatcher;
  template <typename> class Journal;
  class LibrbdAdminSocketHook;
  template <typename> class ObjectMap;
  template <typename> class Operations;
  template <typename> class PluginRegistry;

  namespace asio { struct ContextWQ; }
  namespace cache { template <typename> class ImageCache; }
  namespace exclusive_lock { struct Policy; }
  namespace io {
  class AioCompletion;
  class AsyncOperation;
  template <typename> class CopyupRequest;
  struct ImageDispatcherInterface;
  struct ObjectDispatcherInterface;
  }
  namespace journal { struct Policy; }

  namespace operation {
  template <typename> class ResizeRequest;
  }

  struct ImageCtx {
    typedef std::pair<cls::rbd::SnapshotNamespace, std::string> SnapKey;
    struct SnapKeyComparator {
      inline bool operator()(const SnapKey& lhs, const SnapKey& rhs) const {
        // only compare by namespace type and name
        if (lhs.first.which() != rhs.first.which()) {
          return lhs.first.which() < rhs.first.which();
        }
        return lhs.second < rhs.second;
      }
    };

    static const string METADATA_CONF_PREFIX;

    CephContext *cct;
    ConfigProxy config;
    std::set<std::string> config_overrides;

    PerfCounters *perfcounter;
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    std::vector<librados::snap_t> snaps; // this mirrors snapc.snaps, but is in
                                        // a format librados can understand
    std::map<librados::snap_t, SnapInfo> snap_info;
    std::map<SnapKey, librados::snap_t, SnapKeyComparator> snap_ids;
    uint64_t open_snap_id = CEPH_NOSNAP;
    uint64_t snap_id;
    bool snap_exists; // false if our snap_id was deleted
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    uint32_t read_only_flags = 0U;
    uint32_t read_only_mask = ~0U;

    std::map<rados::cls::lock::locker_id_t,
	     rados::cls::lock::locker_info_t> lockers;
    bool exclusive_locked;
    std::string lock_tag;

    std::string name;
    cls::rbd::SnapshotNamespace snap_namespace;
    std::string snap_name;

    std::shared_ptr<AsioEngine> asio_engine;

    // New ASIO-style RADOS API
    neorados::RADOS& rados_api;

    // Legacy RADOS API
    librados::IoCtx data_ctx;
    librados::IoCtx md_ctx;

    ImageWatcher<ImageCtx> *image_watcher;
    Journal<ImageCtx> *journal;

    /**
     * Lock ordering:
     *
     * owner_lock, image_lock
     * async_op_lock, timestamp_lock
     */
    ceph::shared_mutex owner_lock; // protects exclusive lock leadership updates
    mutable ceph::shared_mutex image_lock; // protects snapshot-related member variables,
                       // features (and associated helper classes), and flags
                       // protects access to the mutable image metadata that
                       // isn't guarded by other locks below, and blocks writes
                       // when held exclusively, so snapshots can be consistent.
                       // Fields guarded include:
                       // total_bytes_read
                       // exclusive_locked
                       // lock_tag
                       // lockers
                       // object_map
                       // parent_md and parent

    ceph::shared_mutex timestamp_lock; // protects (create/access/modify)_timestamp
    ceph::mutex async_ops_lock; // protects async_ops and async_requests
    ceph::mutex copyup_list_lock; // protects copyup_waiting_list

    unsigned extra_read_flags;  // librados::OPERATION_*

    bool old_format;
    uint8_t order;
    uint64_t size;
    uint64_t features;
    std::string object_prefix;
    char *format_string;
    std::string header_oid;
    std::string id; // only used for new-format images
    ParentImageInfo parent_md;
    ImageCtx *parent;
    ImageCtx *child = nullptr;
    MigrationInfo migration_info;
    cls::rbd::GroupSpec group_spec;
    uint64_t stripe_unit, stripe_count;
    uint64_t flags;
    uint64_t op_features = 0;
    bool operations_disabled = false;
    utime_t create_timestamp;
    utime_t access_timestamp;
    utime_t modify_timestamp;

    file_layout_t layout;

    cache::ImageCache<ImageCtx> *image_cache = nullptr;

    Readahead readahead;
    std::atomic<uint64_t> total_bytes_read = {0};

    std::map<uint64_t, io::CopyupRequest<ImageCtx>*> copyup_list;

    xlist<io::AsyncOperation*> async_ops;
    xlist<AsyncRequest<>*> async_requests;
    std::list<Context*> async_requests_waiters;

    ImageState<ImageCtx> *state;
    Operations<ImageCtx> *operations;

    ExclusiveLock<ImageCtx> *exclusive_lock;
    ObjectMap<ImageCtx> *object_map;

    xlist<operation::ResizeRequest<ImageCtx>*> resize_reqs;

    io::ImageDispatcherInterface *io_image_dispatcher = nullptr;
    io::ObjectDispatcherInterface *io_object_dispatcher = nullptr;

    asio::ContextWQ *op_work_queue;

    PluginRegistry<ImageCtx>* plugin_registry;

    typedef boost::lockfree::queue<
      io::AioCompletion*,
      boost::lockfree::allocator<ceph::allocator<void>>> Completions;

    Completions event_socket_completions;
    EventSocket event_socket;

    bool ignore_migrating = false;
    bool disable_zero_copy = false;
    bool enable_sparse_copyup = false;

    /// Cached latency-sensitive configuration settings
    bool non_blocking_aio;
    bool cache;
    uint64_t sparse_read_threshold_bytes;
    uint64_t readahead_max_bytes;
    uint64_t readahead_disable_after_bytes;
    bool clone_copy_on_read;
    bool enable_alloc_hint;
    uint32_t alloc_hint_flags = 0U;
    uint32_t read_flags = 0U;  // librados::OPERATION_*
    uint32_t discard_granularity_bytes = 0;
    bool blkin_trace_all;
    uint64_t mirroring_replay_delay;
    uint64_t mtime_update_interval;
    uint64_t atime_update_interval;

    LibrbdAdminSocketHook *asok_hook;

    exclusive_lock::Policy *exclusive_lock_policy = nullptr;
    journal::Policy *journal_policy = nullptr;

    ZTracer::Endpoint trace_endpoint;

    // unit test mock helpers
    static ImageCtx* create(const std::string &image_name,
                            const std::string &image_id,
                            const char *snap, IoCtx& p, bool read_only) {
      return new ImageCtx(image_name, image_id, snap, p, read_only);
    }
    static ImageCtx* create(const std::string &image_name,
                            const std::string &image_id,
                            librados::snap_t snap_id, IoCtx& p,
                            bool read_only) {
      return new ImageCtx(image_name, image_id, snap_id, p, read_only);
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
    ImageCtx(const std::string &image_name, const std::string &image_id,
	     librados::snap_t snap_id, IoCtx& p, bool read_only);
    ~ImageCtx();
    void init();
    void shutdown();
    void init_layout(int64_t pool_id);
    void perf_start(std::string name);
    void perf_stop();
    void set_read_flag(unsigned flag);
    int get_read_flags(librados::snap_t snap_id);
    int snap_set(uint64_t snap_id);
    void snap_unset();
    librados::snap_t get_snap_id(const cls::rbd::SnapshotNamespace& in_snap_namespace,
                                 const std::string& in_snap_name) const;
    const SnapInfo* get_snap_info(librados::snap_t in_snap_id) const;
    int get_snap_name(librados::snap_t in_snap_id,
		      std::string *out_snap_name) const;
    int get_snap_namespace(librados::snap_t in_snap_id,
			   cls::rbd::SnapshotNamespace *out_snap_namespace) const;
    int get_parent_spec(librados::snap_t in_snap_id,
			cls::rbd::ParentImageSpec *pspec) const;
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
    utime_t get_access_timestamp() const;
    utime_t get_modify_timestamp() const;

    void set_access_timestamp(utime_t at);
    void set_modify_timestamp(utime_t at);

    void add_snap(cls::rbd::SnapshotNamespace in_snap_namespace,
		  std::string in_snap_name,
		  librados::snap_t id,
		  uint64_t in_size, const ParentImageInfo &parent,
		  uint8_t protection_status, uint64_t flags, utime_t timestamp);
    void rm_snap(cls::rbd::SnapshotNamespace in_snap_namespace,
		 std::string in_snap_name,
		 librados::snap_t id);
    uint64_t get_image_size(librados::snap_t in_snap_id) const;
    uint64_t get_object_count(librados::snap_t in_snap_id) const;
    bool test_features(uint64_t test_features) const;
    bool test_features(uint64_t test_features,
                       const ceph::shared_mutex &in_image_lock) const;
    bool test_op_features(uint64_t op_features) const;
    bool test_op_features(uint64_t op_features,
                          const ceph::shared_mutex &in_image_lock) const;
    int get_flags(librados::snap_t in_snap_id, uint64_t *flags) const;
    int test_flags(librados::snap_t in_snap_id,
                   uint64_t test_flags, bool *flags_set) const;
    int test_flags(librados::snap_t in_snap_id,
                   uint64_t test_flags, const ceph::shared_mutex &in_image_lock,
                   bool *flags_set) const;
    int update_flags(librados::snap_t in_snap_id, uint64_t flag, bool enabled);

    const ParentImageInfo* get_parent_info(librados::snap_t in_snap_id) const;
    int64_t get_parent_pool_id(librados::snap_t in_snap_id) const;
    std::string get_parent_image_id(librados::snap_t in_snap_id) const;
    uint64_t get_parent_snap_id(librados::snap_t in_snap_id) const;
    int get_parent_overlap(librados::snap_t in_snap_id,
			   uint64_t *overlap) const;
    void register_watch(Context *on_finish);
    uint64_t prune_parent_extents(vector<pair<uint64_t,uint64_t> >& objectx,
				  uint64_t overlap);

    void cancel_async_requests();
    void cancel_async_requests(Context *on_finish);

    void apply_metadata(const std::map<std::string, bufferlist> &meta,
                        bool thread_safe);

    ExclusiveLock<ImageCtx> *create_exclusive_lock();
    ObjectMap<ImageCtx> *create_object_map(uint64_t snap_id);
    Journal<ImageCtx> *create_journal();

    void set_image_name(const std::string &name);

    void notify_update();
    void notify_update(Context *on_finish);

    exclusive_lock::Policy *get_exclusive_lock_policy() const;
    void set_exclusive_lock_policy(exclusive_lock::Policy *policy);

    journal::Policy *get_journal_policy() const;
    void set_journal_policy(journal::Policy *policy);

    void rebuild_data_io_context();
    IOContext get_data_io_context() const;

    static void get_timer_instance(CephContext *cct, SafeTimer **timer,
                                   ceph::mutex **timer_lock);

  private:
    std::shared_ptr<neorados::IOContext> data_io_context;
  };
}

#endif
