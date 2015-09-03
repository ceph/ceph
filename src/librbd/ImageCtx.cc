// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"

#include "librbd/internal.h"
#include "librbd/WatchCtx.h"

#include "librbd/ImageCtx.h"

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
      wctx(NULL),
      refresh_seq(0),
      last_refresh(0),
      md_lock("librbd::ImageCtx::md_lock"),
      cache_lock("librbd::ImageCtx::cache_lock"),
      snap_lock("librbd::ImageCtx::snap_lock"),
      parent_lock("librbd::ImageCtx::parent_lock"),
      refresh_lock("librbd::ImageCtx::refresh_lock"),
      aio_lock("librbd::ImageCtx::aio_lock"),
      extra_read_flags(0),
      old_format(true),
      order(0), size(0), features(0),
      format_string(NULL),
      id(image_id), parent(NULL),
      stripe_unit(0), stripe_count(0),
      object_cacher(NULL), writeback_handler(NULL), object_set(NULL),
      pending_aio(0), aio_work_queue(NULL)
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

    ThreadPoolSingleton *thread_pool_singleton;
    cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
      thread_pool_singleton, "librbd::thread_pool");
    aio_work_queue = new ContextWQ("librbd::aio_work_queue",
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
    delete[] format_string;

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
    return 0;
  }
  
  void ImageCtx::init_layout()
  {
    if (stripe_unit == 0 || stripe_count == 0) {
      stripe_unit = 1ull << order;
      stripe_count = 1;
    }

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
    map<string, SnapInfo>::iterator it = snaps_by_name.find(in_snap_name);
    if (it != snaps_by_name.end()) {
      snap_name = in_snap_name;
      snap_id = it->second.id;
      snap_exists = true;
      data_ctx.snap_set_read(snap_id);
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::snap_unset()
  {
    snap_id = CEPH_NOSNAP;
    snap_name = "";
    snap_exists = true;
    data_ctx.snap_set_read(snap_id);
  }

  snap_t ImageCtx::get_snap_id(string in_snap_name) const
  {
    map<string, SnapInfo>::const_iterator it = snaps_by_name.find(in_snap_name);
    if (it != snaps_by_name.end())
      return it->second.id;
    return CEPH_NOSNAP;
  }

  int ImageCtx::get_snap_name(snapid_t in_snap_id, string *out_snap_name) const
  {
    map<string, SnapInfo>::const_iterator it;

    for (it = snaps_by_name.begin(); it != snaps_by_name.end(); ++it) {
      if (it->second.id == in_snap_id) {
	*out_snap_name = it->first;
	return 0;
      }
    }
    return -ENOENT;
  }

  int ImageCtx::get_parent_spec(snapid_t in_snap_id, parent_spec *out_pspec)
  {
    map<string, SnapInfo>::iterator it;

    for (it = snaps_by_name.begin(); it != snaps_by_name.end(); ++it) {
      if (it->second.id == in_snap_id) {
	*out_pspec = it->second.parent.spec;
	return 0;
      }
    }
    return -ENOENT;
  }

  uint64_t ImageCtx::get_current_size() const
  {
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

  uint64_t ImageCtx::get_num_objects() const
  {
    uint64_t period = get_stripe_period();
    uint64_t num_periods = (size + period - 1) / period;
    return num_periods * stripe_count;
  }

  int ImageCtx::is_snap_protected(string in_snap_name, bool *is_protected) const
  {
    map<string, SnapInfo>::const_iterator it = snaps_by_name.find(in_snap_name);
    if (it != snaps_by_name.end()) {
      *is_protected =
	(it->second.protection_status == RBD_PROTECTION_STATUS_PROTECTED);
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::is_snap_unprotected(string in_snap_name,
				    bool *is_unprotected) const
  {
    map<string, SnapInfo>::const_iterator it = snaps_by_name.find(in_snap_name);
    if (it != snaps_by_name.end()) {
      *is_unprotected =
	(it->second.protection_status == RBD_PROTECTION_STATUS_UNPROTECTED);
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::add_snap(string in_snap_name, snap_t id, uint64_t in_size,
			  uint64_t features,
			  parent_info parent,
			  uint8_t protection_status)
  {
    snaps.push_back(id);
    SnapInfo info(id, in_size, features, parent, protection_status);
    snaps_by_name.insert(pair<string, SnapInfo>(in_snap_name, info));
  }

  uint64_t ImageCtx::get_image_size(snap_t in_snap_id) const
  {
    if (in_snap_id == CEPH_NOSNAP) {
      return size;
    }
    string in_snap_name;
    int r = get_snap_name(in_snap_id, &in_snap_name);
    if (r < 0)
      return 0;
    map<string, SnapInfo>::const_iterator p = snaps_by_name.find(in_snap_name);
    if (p == snaps_by_name.end())
      return 0;
    return p->second.size;
  }

  int ImageCtx::get_features(snap_t in_snap_id, uint64_t *out_features) const
  {
    if (in_snap_id == CEPH_NOSNAP) {
      *out_features = features;
      return 0;
    }
    string in_snap_name;
    int r = get_snap_name(in_snap_id, &in_snap_name);
    if (r < 0)
      return r;
    map<string, SnapInfo>::const_iterator p = snaps_by_name.find(in_snap_name);
    if (p == snaps_by_name.end())
      return -ENOENT;
    *out_features = p->second.features;
    return 0;
  }

  int64_t ImageCtx::get_parent_pool_id(snap_t in_snap_id) const
  {
    if (in_snap_id == CEPH_NOSNAP) {
      return parent_md.spec.pool_id;
    }
    string in_snap_name;
    int r = get_snap_name(in_snap_id, &in_snap_name);
    if (r < 0)
      return -1;
    map<string, SnapInfo>::const_iterator p = snaps_by_name.find(in_snap_name);
    if (p == snaps_by_name.end())
      return -1;
    return p->second.parent.spec.pool_id;
  }

  string ImageCtx::get_parent_image_id(snap_t in_snap_id) const
  {
    if (in_snap_id == CEPH_NOSNAP) {
      return parent_md.spec.image_id;
    }
    string in_snap_name;
    int r = get_snap_name(in_snap_id, &in_snap_name);
    if (r < 0)
      return "";
    map<string, SnapInfo>::const_iterator p = snaps_by_name.find(in_snap_name);
    if (p == snaps_by_name.end())
      return "";
    return p->second.parent.spec.image_id;
  }

  uint64_t ImageCtx::get_parent_snap_id(snap_t in_snap_id) const
  {
    if (in_snap_id == CEPH_NOSNAP) {
      return parent_md.spec.snap_id;
    }
    string in_snap_name;
    int r = get_snap_name(in_snap_id, &in_snap_name);
    if (r < 0)
      return CEPH_NOSNAP;
    map<string, SnapInfo>::const_iterator p = snaps_by_name.find(in_snap_name);
    if (p == snaps_by_name.end())
      return CEPH_NOSNAP;
    return p->second.parent.spec.snap_id;
  }

  int ImageCtx::get_parent_overlap(snap_t in_snap_id, uint64_t *overlap) const
  {
    if (in_snap_id == CEPH_NOSNAP) {
      *overlap = parent_md.overlap;
      return 0;
    }
    string in_snap_name;
    int r = get_snap_name(in_snap_id, &in_snap_name);
    if (r < 0)
      return r;
    map<string, SnapInfo>::const_iterator p = snaps_by_name.find(in_snap_name);
    if (p == snaps_by_name.end())
      return -ENOENT;
    *overlap = p->second.parent.overlap;
    return 0;
  }

  void ImageCtx::aio_read_from_cache(object_t o, bufferlist *bl, size_t len,
				     uint64_t off, Context *onfinish) {
    snap_lock.get_read();
    ObjectCacher::OSDRead *rd = object_cacher->prepare_read(snap_id, bl, 0);
    snap_lock.put_read();
    ObjectExtent extent(o, 0 /* a lie */, off, len, 0);
    extent.oloc.pool = data_ctx.get_id();
    extent.buffer_extents.push_back(make_pair(0, len));
    rd->extents.push_back(extent);
    cache_lock.Lock();
    int r = object_cacher->readx(rd, object_set, onfinish);
    cache_lock.Unlock();
    if (r != 0)
      onfinish->complete(r);
  }

  void ImageCtx::write_to_cache(object_t o, bufferlist& bl, size_t len,
				uint64_t off, Context *onfinish) {
    snap_lock.get_read();
    ObjectCacher::OSDWrite *wr = object_cacher->prepare_write(snapc, bl,
							      utime_t(), 0);
    snap_lock.put_read();
    ObjectExtent extent(o, 0, off, len, 0);
    extent.oloc.pool = data_ctx.get_id();
    // XXX: nspace is always default, io_ctx_impl field private
    //extent.oloc.nspace = data_ctx.io_ctx_impl->oloc.nspace;
    extent.buffer_extents.push_back(make_pair(0, len));
    wr->extents.push_back(extent);
    {
      Mutex::Locker l(cache_lock);
      object_cacher->writex(wr, object_set, cache_lock, onfinish);
    }
  }

  int ImageCtx::read_from_cache(object_t o, bufferlist *bl, size_t len,
				uint64_t off) {
    int r;
    Mutex mylock("librbd::ImageCtx::read_from_cache");
    Cond cond;
    bool done;
    Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
    aio_read_from_cache(o, bl, len, off, onfinish);
    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();
    return r;
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
    md_lock.get_write();
    invalidate_cache();
    md_lock.put_write();
    object_cacher->stop();
  }

  int ImageCtx::invalidate_cache() {
    if (!object_cacher)
      return 0;
    cache_lock.Lock();
    object_cacher->release_set(object_set);
    cache_lock.Unlock();
    int r = flush_cache();
    if (r)
      lderr(cct) << "flush_cache returned " << r << dendl;
    wait_for_pending_aio();
    cache_lock.Lock();
    bool unclean = object_cacher->release_set(object_set);
    cache_lock.Unlock();
    if (unclean) {
      lderr(cct) << "could not release all objects from cache: "
                 << unclean << " bytes remain" << dendl;
      return -EBUSY;
    }
    return r;
  }

  void ImageCtx::clear_nonexistence_cache() {
    if (!object_cacher)
      return;
    object_cacher->clear_nonexistence(object_set);
  }

  int ImageCtx::register_watch() {
    assert(!wctx);
    wctx = new WatchCtx(this);
    return md_ctx.watch(header_oid, 0, &(wctx->cookie), wctx);
  }

  void ImageCtx::unregister_watch() {
    assert(wctx);
    wctx->invalidate();
    md_ctx.unwatch(header_oid, wctx->cookie);
    delete wctx;
    wctx = NULL;
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

  void ImageCtx::wait_for_pending_aio() {
    Mutex::Locker l(aio_lock);
    while (pending_aio > 0) {
      pending_aio_cond.Wait(aio_lock);
    }
  }
}
