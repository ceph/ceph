// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"

#include "librbd/AsyncOperation.h"
#include "librbd/AsyncRequest.h"
#include "librbd/AsyncResizeRequest.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"

#include <boost/bind.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageCtx: "

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;

namespace librbd {

namespace {

class ThreadPoolSingleton : public ThreadPool {
public:
  ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "librbd::thread_pool", cct->_conf->rbd_op_threads,
                 "rbd_op_threads") {
    start();
  }
  virtual ~ThreadPoolSingleton() {
    stop();
  }
};

} // anonymous namespace

  ImageCtx::ImageCtx(const string &image_name, const string &image_id,
		     const char *snap, IoCtx& p, bool ro)
    : cct((CephContext*)p.cct()),
      perfcounter(NULL),
      snap_id(CEPH_NOSNAP),
      snap_exists(true),
      read_only(ro),
      flush_encountered(false),
      exclusive_locked(false),
      name(image_name),
      image_watcher(NULL),
      refresh_seq(0),
      last_refresh(0),
      owner_lock(unique_lock_name("librbd::ImageCtx::owner_lock", this)),
      md_lock(unique_lock_name("librbd::ImageCtx::md_lock", this)),
      cache_lock(unique_lock_name("librbd::ImageCtx::cache_lock", this)),
      snap_lock(unique_lock_name("librbd::ImageCtx::snap_lock", this)),
      parent_lock(unique_lock_name("librbd::ImageCtx::parent_lock", this)),
      refresh_lock(unique_lock_name("librbd::ImageCtx::refresh_lock", this)),
      object_map_lock(unique_lock_name("librbd::ImageCtx::object_map_lock", this)),
      async_ops_lock(unique_lock_name("librbd::ImageCtx::async_ops_lock", this)),
      copyup_list_lock(unique_lock_name("librbd::ImageCtx::copyup_list_lock", this)),
      extra_read_flags(0),
      old_format(true),
      order(0), size(0), features(0),
      format_string(NULL),
      id(image_id), parent(NULL),
      stripe_unit(0), stripe_count(0), flags(0),
      object_cacher(NULL), writeback_handler(NULL), object_set(NULL),
      readahead(),
      total_bytes_read(0), copyup_finisher(NULL),
      object_map(*this), aio_work_queue(NULL), op_work_queue(NULL)
  {
    md_ctx.dup(p);
    data_ctx.dup(p);

    memset(&header, 0, sizeof(header));
    memset(&layout, 0, sizeof(layout));

    string pname = string("librbd-") + id + string("-") +
      data_ctx.get_pool_name() + string("/") + name;
    if (snap) {
      snap_name = snap;
      pname += "@";
      pname += snap_name;
    }
    perf_start(pname);

    if (cct->_conf->rbd_cache) {
      Mutex::Locker l(cache_lock);
      ldout(cct, 20) << "enabling caching..." << dendl;
      writeback_handler = new LibrbdWriteback(this, cache_lock);

      uint64_t init_max_dirty = cct->_conf->rbd_cache_max_dirty;
      if (cct->_conf->rbd_cache_writethrough_until_flush)
	init_max_dirty = 0;
      ldout(cct, 20) << "Initial cache settings:"
		     << " size=" << cct->_conf->rbd_cache_size
		     << " num_objects=" << 10
		     << " max_dirty=" << init_max_dirty
		     << " target_dirty=" << cct->_conf->rbd_cache_target_dirty
		     << " max_dirty_age="
		     << cct->_conf->rbd_cache_max_dirty_age << dendl;

      object_cacher = new ObjectCacher(cct, pname, *writeback_handler, cache_lock,
				       NULL, NULL,
				       cct->_conf->rbd_cache_size,
				       10,  /* reset this in init */
				       init_max_dirty,
				       cct->_conf->rbd_cache_target_dirty,
				       cct->_conf->rbd_cache_max_dirty_age,
				       cct->_conf->rbd_cache_block_writes_upfront);
      object_set = new ObjectCacher::ObjectSet(NULL, data_ctx.get_id(), 0);
      object_set->return_enoent = true;
      object_cacher->start();
    }

    if (cct->_conf->rbd_clone_copy_on_read) {
      copyup_finisher = new Finisher(cct);
      copyup_finisher->start();
    }

    ThreadPoolSingleton *thread_pool_singleton;
    cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
      thread_pool_singleton, "librbd::thread_pool");
    aio_work_queue = new ContextWQ("librbd::aio_work_queue",
                                   cct->_conf->rbd_op_thread_timeout,
                                   thread_pool_singleton);
    op_work_queue = new ContextWQ("librbd::op_work_queue",
                                  cct->_conf->rbd_op_thread_timeout,
                                  thread_pool_singleton);
  }

  ImageCtx::~ImageCtx() {
    perf_stop();
    if (object_cacher) {
      delete object_cacher;
      object_cacher = NULL;
    }
    if (writeback_handler) {
      delete writeback_handler;
      writeback_handler = NULL;
    }
    if (object_set) {
      delete object_set;
      object_set = NULL;
    }
    if (copyup_finisher != NULL) {
      delete copyup_finisher;
      copyup_finisher = NULL;
    }
    delete[] format_string;

    delete op_work_queue;
    delete aio_work_queue;
  }

  int ImageCtx::init() {
    int r;
    if (id.length()) {
      old_format = false;
    } else {
      r = detect_format(md_ctx, name, &old_format, NULL);
      if (r < 0) {
	lderr(cct) << "error finding header: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    if (!old_format) {
      if (!id.length()) {
	r = cls_client::get_id(&md_ctx, id_obj_name(name), &id);
	if (r < 0) {
	  lderr(cct) << "error reading image id: " << cpp_strerror(r)
		     << dendl;
	  return r;
	}
      }

      header_oid = header_name(id);
      r = cls_client::get_immutable_metadata(&md_ctx, header_oid,
					     &object_prefix, &order);
      if (r < 0) {
	lderr(cct) << "error reading immutable metadata: "
		   << cpp_strerror(r) << dendl;
	return r;
      }

      r = cls_client::get_stripe_unit_count(&md_ctx, header_oid,
					    &stripe_unit, &stripe_count);
      if (r < 0 && r != -ENOEXEC && r != -EINVAL) {
	lderr(cct) << "error reading striping metadata: "
		   << cpp_strerror(r) << dendl;
	return r;
      }

      init_layout();
    } else {
      header_oid = old_header_name(name);
    }

    md_config_t *conf = cct->_conf;
    readahead.set_trigger_requests(conf->rbd_readahead_trigger_requests);
    readahead.set_max_readahead_size(conf->rbd_readahead_max_bytes);
    return 0;
  }

  void ImageCtx::init_layout()
  {
    if (stripe_unit == 0 || stripe_count == 0) {
      stripe_unit = 1ull << order;
      stripe_count = 1;
    }

    vector<uint64_t> alignments;
    alignments.push_back(stripe_count << order); // object set (in file striping terminology)
    alignments.push_back(stripe_unit * stripe_count); // stripe
    alignments.push_back(stripe_unit); // stripe unit
    readahead.set_alignments(alignments);

    memset(&layout, 0, sizeof(layout));
    layout.fl_stripe_unit = stripe_unit;
    layout.fl_stripe_count = stripe_count;
    layout.fl_object_size = 1ull << order;
    layout.fl_pg_pool = data_ctx.get_id();  // FIXME: pool id overflow?

    delete[] format_string;
    size_t len = object_prefix.length() + 16;
    format_string = new char[len];
    if (old_format) {
      snprintf(format_string, len, "%s.%%012llx", object_prefix.c_str());
    } else {
      snprintf(format_string, len, "%s.%%016llx", object_prefix.c_str());
    }

    // size object cache appropriately
    if (object_cacher) {
      uint64_t obj = cct->_conf->rbd_cache_max_dirty_object;
      if (!obj) {
        obj = MIN(2000, MAX(10, cct->_conf->rbd_cache_size / 100 / sizeof(ObjectCacher::Object)));
      }
      ldout(cct, 10) << " cache bytes " << cct->_conf->rbd_cache_size
		     << " -> about " << obj << " objects" << dendl;
      object_cacher->set_max_objects(obj);
    }

    ldout(cct, 10) << "init_layout stripe_unit " << stripe_unit
		   << " stripe_count " << stripe_count
		   << " object_size " << layout.fl_object_size
		   << " prefix " << object_prefix
		   << " format " << format_string
		   << dendl;
  }

  void ImageCtx::perf_start(string name) {
    PerfCountersBuilder plb(cct, name, l_librbd_first, l_librbd_last);

    plb.add_u64_counter(l_librbd_rd, "rd");
    plb.add_u64_counter(l_librbd_rd_bytes, "rd_bytes");
    plb.add_time_avg(l_librbd_rd_latency, "rd_latency");
    plb.add_u64_counter(l_librbd_wr, "wr");
    plb.add_u64_counter(l_librbd_wr_bytes, "wr_bytes");
    plb.add_time_avg(l_librbd_wr_latency, "wr_latency");
    plb.add_u64_counter(l_librbd_discard, "discard");
    plb.add_u64_counter(l_librbd_discard_bytes, "discard_bytes");
    plb.add_time_avg(l_librbd_discard_latency, "discard_latency");
    plb.add_u64_counter(l_librbd_flush, "flush");
    plb.add_u64_counter(l_librbd_aio_rd, "aio_rd");
    plb.add_u64_counter(l_librbd_aio_rd_bytes, "aio_rd_bytes");
    plb.add_time_avg(l_librbd_aio_rd_latency, "aio_rd_latency");
    plb.add_u64_counter(l_librbd_aio_wr, "aio_wr");
    plb.add_u64_counter(l_librbd_aio_wr_bytes, "aio_wr_bytes");
    plb.add_time_avg(l_librbd_aio_wr_latency, "aio_wr_latency");
    plb.add_u64_counter(l_librbd_aio_discard, "aio_discard");
    plb.add_u64_counter(l_librbd_aio_discard_bytes, "aio_discard_bytes");
    plb.add_time_avg(l_librbd_aio_discard_latency, "aio_discard_latency");
    plb.add_u64_counter(l_librbd_aio_flush, "aio_flush");
    plb.add_time_avg(l_librbd_aio_flush_latency, "aio_flush_latency");
    plb.add_u64_counter(l_librbd_snap_create, "snap_create");
    plb.add_u64_counter(l_librbd_snap_remove, "snap_remove");
    plb.add_u64_counter(l_librbd_snap_rollback, "snap_rollback");
    plb.add_u64_counter(l_librbd_notify, "notify");
    plb.add_u64_counter(l_librbd_resize, "resize");
    plb.add_u64_counter(l_librbd_readahead, "readahead");
    plb.add_u64_counter(l_librbd_readahead_bytes, "readahead_bytes");

    perfcounter = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perfcounter);
  }

  void ImageCtx::perf_stop() {
    assert(perfcounter);
    cct->get_perfcounters_collection()->remove(perfcounter);
    delete perfcounter;
  }

  void ImageCtx::set_read_flag(unsigned flag) {
    extra_read_flags |= flag;
  }

  int ImageCtx::get_read_flags(snap_t snap_id) {
    int flags = librados::OPERATION_NOFLAG | extra_read_flags;
    if (snap_id == LIBRADOS_SNAP_HEAD)
      return flags;

    if (cct->_conf->rbd_balance_snap_reads)
      flags |= librados::OPERATION_BALANCE_READS;
    else if (cct->_conf->rbd_localize_snap_reads)
      flags |= librados::OPERATION_LOCALIZE_READS;
    return flags;
  }

  int ImageCtx::snap_set(string in_snap_name)
  {
    assert(snap_lock.is_wlocked());
    snap_t in_snap_id = get_snap_id(in_snap_name);
    if (in_snap_id != CEPH_NOSNAP) {
      snap_id = in_snap_id;
      snap_name = in_snap_name;
      snap_exists = true;
      data_ctx.snap_set_read(snap_id);
      object_map.refresh(in_snap_id);
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::snap_unset()
  {
    assert(snap_lock.is_wlocked());
    snap_id = CEPH_NOSNAP;
    snap_name = "";
    snap_exists = true;
    data_ctx.snap_set_read(snap_id);
    object_map.refresh(CEPH_NOSNAP);
  }

  snap_t ImageCtx::get_snap_id(string in_snap_name) const
  {
    assert(snap_lock.is_locked());
    map<string, snap_t>::const_iterator it =
      snap_ids.find(in_snap_name);
    if (it != snap_ids.end())
      return it->second;
    return CEPH_NOSNAP;
  }

  const SnapInfo* ImageCtx::get_snap_info(snap_t in_snap_id) const
  {
    assert(snap_lock.is_locked());
    map<snap_t, SnapInfo>::const_iterator it =
      snap_info.find(in_snap_id);
    if (it != snap_info.end())
      return &it->second;
    return NULL;
  }

  int ImageCtx::get_snap_name(snap_t in_snap_id,
			      string *out_snap_name) const
  {
    assert(snap_lock.is_locked());
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *out_snap_name = info->name;
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::get_parent_spec(snap_t in_snap_id,
				parent_spec *out_pspec) const
  {
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *out_pspec = info->parent.spec;
      return 0;
    }
    return -ENOENT;
  }

  uint64_t ImageCtx::get_current_size() const
  {
    assert(snap_lock.is_locked());
    return size;
  }

  uint64_t ImageCtx::get_object_size() const
  {
    return 1ull << order;
  }

  string ImageCtx::get_object_name(uint64_t num) const {
    char buf[object_prefix.length() + 32];
    snprintf(buf, sizeof(buf), format_string, num);
    return string(buf);
  }

  uint64_t ImageCtx::get_stripe_unit() const
  {
    return stripe_unit;
  }

  uint64_t ImageCtx::get_stripe_count() const
  {
    return stripe_count;
  }

  uint64_t ImageCtx::get_stripe_period() const
  {
    return stripe_count * (1ull << order);
  }

  int ImageCtx::is_snap_protected(snap_t in_snap_id,
				  bool *is_protected) const
  {
    assert(snap_lock.is_locked());
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *is_protected =
	(info->protection_status == RBD_PROTECTION_STATUS_PROTECTED);
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::is_snap_unprotected(snap_t in_snap_id,
				    bool *is_unprotected) const
  {
    assert(snap_lock.is_locked());
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *is_unprotected =
	(info->protection_status == RBD_PROTECTION_STATUS_UNPROTECTED);
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::add_snap(string in_snap_name, snap_t id, uint64_t in_size,
			  uint64_t features, parent_info parent,
			  uint8_t protection_status, uint64_t flags)
  {
    assert(snap_lock.is_wlocked());
    snaps.push_back(id);
    SnapInfo info(in_snap_name, in_size, features, parent, protection_status,
		  flags);
    snap_info.insert(pair<snap_t, SnapInfo>(id, info));
    snap_ids.insert(pair<string, snap_t>(in_snap_name, id));
  }

  uint64_t ImageCtx::get_image_size(snap_t in_snap_id) const
  {
    assert(snap_lock.is_locked());
    if (in_snap_id == CEPH_NOSNAP) {
      if (!async_resize_reqs.empty() &&
          async_resize_reqs.front()->shrinking()) {
        return async_resize_reqs.front()->get_image_size();
      }
      return size;
    }

    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      return info->size;
    }
    return 0;
  }

  int ImageCtx::get_features(snap_t in_snap_id, uint64_t *out_features) const
  {
    assert(snap_lock.is_locked());
    if (in_snap_id == CEPH_NOSNAP) {
      *out_features = features;
      return 0;
    }
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *out_features = info->features;
      return 0;
    }
    return -ENOENT;
  }

  bool ImageCtx::test_features(uint64_t test_features) const
  {
    RWLock::RLocker l(snap_lock);
    uint64_t snap_features = 0;
    get_features(snap_id, &snap_features);
    return ((snap_features & test_features) == test_features);
  }

  int ImageCtx::get_flags(librados::snap_t _snap_id, uint64_t *_flags) const
  {
    assert(snap_lock.is_locked());
    if (_snap_id == CEPH_NOSNAP) {
      *_flags = flags;
      return 0;
    }
    const SnapInfo *info = get_snap_info(_snap_id);
    if (info) {
      *_flags = info->flags;
      return 0;
    }
    return -ENOENT;
  }

  bool ImageCtx::test_flags(uint64_t test_flags) const
  {
    RWLock::RLocker l(snap_lock);
    uint64_t snap_flags;
    get_flags(snap_id, &snap_flags);
    return ((snap_flags & test_flags) == test_flags);
  }

  int ImageCtx::update_flags(snap_t in_snap_id, uint64_t flag, bool enabled)
  {
    assert(snap_lock.is_wlocked());
    uint64_t *_flags;
    if (in_snap_id == CEPH_NOSNAP) {
      _flags = &flags;
    } else {
      map<snap_t, SnapInfo>::iterator it = snap_info.find(in_snap_id);
      if (it == snap_info.end()) {
        return -ENOENT;
      }
      _flags = &it->second.flags;
    }

    if (enabled) {
      (*_flags) |= flag;
    } else {
      (*_flags) &= ~flag;
    }
    return 0;
  }

  const parent_info* ImageCtx::get_parent_info(snap_t in_snap_id) const
  {
    assert(snap_lock.is_locked());
    assert(parent_lock.is_locked());
    if (in_snap_id == CEPH_NOSNAP)
      return &parent_md;
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info)
      return &info->parent;
    return NULL;
  }

  int64_t ImageCtx::get_parent_pool_id(snap_t in_snap_id) const
  {
    const parent_info *info = get_parent_info(in_snap_id);
    if (info)
      return info->spec.pool_id;
    return -1;
  }

  string ImageCtx::get_parent_image_id(snap_t in_snap_id) const
  {
    const parent_info *info = get_parent_info(in_snap_id);
    if (info)
      return info->spec.image_id;
    return "";
  }

  uint64_t ImageCtx::get_parent_snap_id(snap_t in_snap_id) const
  {
    const parent_info *info = get_parent_info(in_snap_id);
    if (info)
      return info->spec.snap_id;
    return CEPH_NOSNAP;
  }

  int ImageCtx::get_parent_overlap(snap_t in_snap_id, uint64_t *overlap) const
  {
    assert(snap_lock.is_locked());
    if (in_snap_id == CEPH_NOSNAP && !async_resize_reqs.empty() &&
        async_resize_reqs.front()->shrinking()) {
      *overlap = async_resize_reqs.front()->get_parent_overlap();
      return 0;
    }

    const parent_info *info = get_parent_info(in_snap_id);
    if (info) {
      *overlap = info->overlap;
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::aio_read_from_cache(object_t o, uint64_t object_no,
				     bufferlist *bl, size_t len,
				     uint64_t off, Context *onfinish,
				     int fadvise_flags) {
    snap_lock.get_read();
    ObjectCacher::OSDRead *rd = object_cacher->prepare_read(snap_id, bl, fadvise_flags);
    snap_lock.put_read();
    ObjectExtent extent(o, object_no, off, len, 0);
    extent.oloc.pool = data_ctx.get_id();
    extent.buffer_extents.push_back(make_pair(0, len));
    rd->extents.push_back(extent);
    cache_lock.Lock();
    int r = object_cacher->readx(rd, object_set, onfinish);
    cache_lock.Unlock();
    if (r != 0)
      onfinish->complete(r);
  }

  void ImageCtx::write_to_cache(object_t o, const bufferlist& bl, size_t len,
				uint64_t off, Context *onfinish,
				int fadvise_flags) {
    snap_lock.get_read();
    ObjectCacher::OSDWrite *wr = object_cacher->prepare_write(snapc, bl,
							      utime_t(), fadvise_flags);
    snap_lock.put_read();
    ObjectExtent extent(o, 0, off, len, 0);
    extent.oloc.pool = data_ctx.get_id();
    // XXX: nspace is always default, io_ctx_impl field private
    //extent.oloc.nspace = data_ctx.io_ctx_impl->oloc.nspace;
    extent.buffer_extents.push_back(make_pair(0, len));
    wr->extents.push_back(extent);
    {
      Mutex::Locker l(cache_lock);
      object_cacher->writex(wr, object_set, onfinish);
    }
  }

  void ImageCtx::user_flushed() {
    if (object_cacher && cct->_conf->rbd_cache_writethrough_until_flush) {
      md_lock.get_read();
      bool flushed_before = flush_encountered;
      md_lock.put_read();

      uint64_t max_dirty = cct->_conf->rbd_cache_max_dirty;
      if (!flushed_before && max_dirty > 0) {
	md_lock.get_write();
	flush_encountered = true;
	md_lock.put_write();

	ldout(cct, 10) << "saw first user flush, enabling writeback" << dendl;
	Mutex::Locker l(cache_lock);
	object_cacher->set_max_dirty(max_dirty);
      }
    }
  }

  void ImageCtx::flush_cache_aio(Context *onfinish) {
    assert(owner_lock.is_locked());
    cache_lock.Lock();
    object_cacher->flush_set(object_set, onfinish);
    cache_lock.Unlock();
  }

  int ImageCtx::flush_cache() {
    int r = 0;
    Mutex mylock("librbd::ImageCtx::flush_cache");
    Cond cond;
    bool done;
    Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
    flush_cache_aio(onfinish);
    mylock.Lock();
    while (!done) {
      ldout(cct, 20) << "waiting for cache to be flushed" << dendl;
      cond.Wait(mylock);
    }
    mylock.Unlock();
    ldout(cct, 20) << "finished flushing cache" << dendl;
    return r;
  }

  void ImageCtx::shutdown_cache() {
    flush_async_operations();

    RWLock::RLocker owner_locker(owner_lock);
    invalidate_cache(true);
    object_cacher->stop();
  }

  int ImageCtx::invalidate_cache(bool purge_on_error) {
    int result;
    C_SaferCond ctx;
    invalidate_cache(&ctx);
    result = ctx.wait();

    if (result && purge_on_error) {
      cache_lock.Lock();
      if (object_cacher != NULL) {
	lderr(cct) << "invalidate cache met error " << cpp_strerror(result) << " !Purging cache..." << dendl;
	object_cacher->purge_set(object_set);
      }
      cache_lock.Unlock();
    }

    return result;
  }

  void ImageCtx::invalidate_cache(Context *on_finish) {
    if (object_cacher == NULL) {
      op_work_queue->queue(on_finish, 0);
      return;
    }

    cache_lock.Lock();
    object_cacher->release_set(object_set);
    cache_lock.Unlock();

    flush_cache_aio(new FunctionContext(boost::bind(
      &ImageCtx::invalidate_cache_completion, this, _1, on_finish)));
  }

  void ImageCtx::invalidate_cache_completion(int r, Context *on_finish) {
    assert(cache_lock.is_locked());
    if (r == -EBLACKLISTED) {
      lderr(cct) << "Blacklisted during flush!  Purging cache..." << dendl;
      object_cacher->purge_set(object_set);
    } else if (r != 0) {
      lderr(cct) << "flush_cache returned " << r << dendl;
    }

    loff_t unclean = object_cacher->release_set(object_set);
    if (unclean == 0) {
      r = 0;
    } else {
      lderr(cct) << "could not release all objects from cache: "
                 << unclean << " bytes remain" << dendl;
      r = -EBUSY;
    }

    op_work_queue->queue(on_finish, r);
  }

  void ImageCtx::clear_nonexistence_cache() {
    assert(cache_lock.is_locked());
    if (!object_cacher)
      return;
    object_cacher->clear_nonexistence(object_set);
  }

  int ImageCtx::register_watch() {
    assert(image_watcher == NULL);
    image_watcher = new ImageWatcher(*this);
    return image_watcher->register_watch();
  }

  void ImageCtx::unregister_watch() {
    assert(image_watcher != NULL);
    image_watcher->unregister_watch();
    delete image_watcher;
    image_watcher = NULL;
  }

  size_t ImageCtx::parent_io_len(uint64_t offset, size_t length,
				 snap_t in_snap_id)
  {
    uint64_t overlap = 0;
    get_parent_overlap(in_snap_id, &overlap);

    size_t parent_len = 0;
    if (get_parent_pool_id(in_snap_id) != -1 && offset <= overlap)
      parent_len = min(overlap, offset + length) - offset;

    ldout(cct, 20) << __func__ << " off = " << offset << " len = " << length
		   << " overlap = " << overlap << " parent_io_len = "
		   << parent_len << dendl;
    return parent_len;
  }

  uint64_t ImageCtx::prune_parent_extents(vector<pair<uint64_t,uint64_t> >& objectx,
					  uint64_t overlap)
  {
    // drop extents completely beyond the overlap
    while (!objectx.empty() && objectx.back().first >= overlap)
      objectx.pop_back();

    // trim final overlapping extent
    if (!objectx.empty() && objectx.back().first + objectx.back().second > overlap)
      objectx.back().second = overlap - objectx.back().first;

    uint64_t len = 0;
    for (vector<pair<uint64_t,uint64_t> >::iterator p = objectx.begin();
	 p != objectx.end();
	 ++p)
      len += p->second;
    ldout(cct, 10) << "prune_parent_extents image overlap " << overlap
		   << ", object overlap " << len
		   << " from image extents " << objectx << dendl;
    return len;
  }

  void ImageCtx::flush_async_operations() {
    C_SaferCond ctx;
    flush_async_operations(&ctx);
    ctx.wait();
  }

  void ImageCtx::flush_async_operations(Context *on_finish) {
    Mutex::Locker l(async_ops_lock);
    if (async_ops.empty()) {
      on_finish->complete(0);
      return;
    }

    ldout(cct, 20) << "flush async operations: " << on_finish << " "
                   << "count=" << async_ops.size() << dendl;
    async_ops.front()->add_flush_context(on_finish);
  }

  void ImageCtx::cancel_async_requests() {
    Mutex::Locker l(async_ops_lock);
    ldout(cct, 10) << "canceling async requests: count="
                   << async_requests.size() << dendl;

    for (xlist<AsyncRequest*>::iterator it = async_requests.begin();
         !it.end(); ++it) {
      ldout(cct, 10) << "canceling async request: " << *it << dendl;
      (*it)->cancel();
    }

    while (!async_requests.empty()) {
      async_requests_cond.Wait(async_ops_lock);
    }
  }
}
