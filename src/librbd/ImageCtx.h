// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include "include/int_types.h"

#include <map>
#include <set>
#include <string>
#include <vector>
#include <boost/optional.hpp>

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Readahead.h"
#include "common/RWLock.h"
#include "common/snap_types.h"
#include "include/atomic.h"
#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "include/types.h"
#include "include/xlist.h"
#include "osdc/ObjectCacher.h"

#include "cls/rbd/cls_rbd_client.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/ObjectMap.h"
#include "librbd/SnapInfo.h"
#include "librbd/parent_types.h"

class CephContext;
class ContextWQ;
class Finisher;
class PerfCounters;

namespace librbd {

  class AsyncOperation;
  class AsyncRequest;
  class AsyncResizeRequest;
  class CopyupRequest;
  class ImageWatcher;

  struct ImageCtx {
    CephContext *cct;
    PerfCounters *perfcounter;
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    std::vector<librados::snap_t> snaps; // this mirrors snapc.snaps, but is in
                                        // a format librados can understand
    std::map<librados::snap_t, SnapInfo> snap_info;
    std::map<std::string, librados::snap_t> snap_ids;
    uint64_t snap_id;
    bool snap_exists; // false if our snap_id was deleted
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    bool flush_encountered;

    std::map<rados::cls::lock::locker_id_t,
	     rados::cls::lock::locker_info_t> lockers;
    bool exclusive_locked;
    std::string lock_tag;

    std::string name;
    std::string snap_name;
    IoCtx data_ctx, md_ctx;
    ImageWatcher *image_watcher;
    int refresh_seq;    ///< sequence for refresh requests
    int last_refresh;   ///< last completed refresh

    /**
     * Lock ordering:
     *
     * owner_lock, md_lock, cache_lock, snap_lock, parent_lock,
     * refresh_lock, object_map_lock, async_op_lock
     */
    RWLock owner_lock; // protects exclusive lock leadership updates
    RWLock md_lock; // protects access to the mutable image metadata that
                   // isn't guarded by other locks below, and blocks writes
                   // when held exclusively, so snapshots can be consistent.
                   // Fields guarded include:
                   // flush_encountered
                   // total_bytes_read
                   // exclusive_locked
                   // lock_tag
                   // lockers
    Mutex cache_lock; // used as client_lock for the ObjectCacher
    RWLock snap_lock; // protects snapshot-related member variables, features, and flags
    RWLock parent_lock; // protects parent_md and parent
    Mutex refresh_lock; // protects refresh_seq and last_refresh
    RWLock object_map_lock; // protects object map updates and object_map itself
    Mutex async_ops_lock; // protects async_ops and async_requests
    Mutex copyup_list_lock; // protects copyup_waiting_list

    unsigned extra_read_flags;

    bool old_format;
    uint8_t order;
    uint64_t size;
    uint64_t features;
    std::string object_prefix;
    char *format_string;
    std::string header_oid;
    std::string id; // only used for new-format images
    parent_info parent_md;
    ImageCtx *parent;
    uint64_t stripe_unit, stripe_count;
    uint64_t flags;

    ceph_file_layout layout;

    ObjectCacher *object_cacher;
    LibrbdWriteback *writeback_handler;
    ObjectCacher::ObjectSet *object_set;

    Readahead readahead;
    uint64_t total_bytes_read;

    Finisher *copyup_finisher;
    std::map<uint64_t, CopyupRequest*> copyup_list;

    xlist<AsyncOperation*> async_ops;
    xlist<AsyncRequest*> async_requests;
    Cond async_requests_cond;

    ObjectMap object_map;

    atomic_t async_request_seq;

    xlist<AsyncResizeRequest*> async_resize_reqs;

    ContextWQ *aio_work_queue;

    /**
     * Either image_name or image_id must be set.
     * If id is not known, pass the empty std::string,
     * and init() will look it up.
     */
    ImageCtx(const std::string &image_name, const std::string &image_id,
	     const char *snap, IoCtx& p, bool read_only);
    ~ImageCtx();
    int init();
    void init_layout();
    void perf_start(std::string name);
    void perf_stop();
    void set_read_flag(unsigned flag);
    int get_read_flags(librados::snap_t snap_id);
    int snap_set(std::string in_snap_name);
    void snap_unset();
    librados::snap_t get_snap_id(std::string in_snap_name) const;
    const SnapInfo* get_snap_info(librados::snap_t in_snap_id) const;
    int get_snap_name(librados::snap_t in_snap_id,
		      std::string *out_snap_name) const;
    int get_parent_spec(librados::snap_t in_snap_id,
			parent_spec *pspec) const;
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

    void add_snap(std::string in_snap_name, librados::snap_t id,
		  uint64_t in_size, uint64_t features,
		  parent_info parent, uint8_t protection_status,
		  uint64_t flags);
    uint64_t get_image_size(librados::snap_t in_snap_id) const;
    int get_features(librados::snap_t in_snap_id,
		     uint64_t *out_features) const;
    bool test_features(uint64_t test_features) const;
    int get_flags(librados::snap_t in_snap_id, uint64_t *flags) const;
    bool test_flags(uint64_t test_flags) const;
    int update_flags(librados::snap_t in_snap_id, uint64_t flag, bool enabled);

    const parent_info* get_parent_info(librados::snap_t in_snap_id) const;
    int64_t get_parent_pool_id(librados::snap_t in_snap_id) const;
    std::string get_parent_image_id(librados::snap_t in_snap_id) const;
    uint64_t get_parent_snap_id(librados::snap_t in_snap_id) const;
    int get_parent_overlap(librados::snap_t in_snap_id,
			   uint64_t *overlap) const;
    void aio_read_from_cache(object_t o, uint64_t object_no, bufferlist *bl,
			     size_t len, uint64_t off, Context *onfinish,
			     int fadvise_flags);
    void write_to_cache(object_t o, const bufferlist& bl, size_t len,
			uint64_t off, Context *onfinish, int fadvise_flags);
    int read_from_cache(object_t o, uint64_t object_no, bufferlist *bl,
			size_t len, uint64_t off);
    void user_flushed();
    void flush_cache_aio(Context *onfinish);
    int flush_cache();
    void shutdown_cache();
    int invalidate_cache();
    void invalidate_cache(Context *on_finish);
    void invalidate_cache_completion(int r, Context *on_finish);
    void clear_nonexistence_cache();
    int register_watch();
    void unregister_watch();
    size_t parent_io_len(uint64_t offset, size_t length,
			 librados::snap_t in_snap_id);
    uint64_t prune_parent_extents(vector<pair<uint64_t,uint64_t> >& objectx,
				  uint64_t overlap);

    void flush_async_operations();
    void flush_async_operations(Context *on_finish);

    void cancel_async_requests();
  };
}

#endif
