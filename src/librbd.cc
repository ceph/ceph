// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <errno.h>
#include <inttypes.h>

#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/snap_types.h"
#include "common/perf_counters.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "osdc/ObjectCacher.h"

#include "librbd/LibrbdWriteback.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

namespace librbd {

  enum {
    l_librbd_first = 26000,

    l_librbd_rd,               // read ops
    l_librbd_rd_bytes,         // bytes read
    l_librbd_wr,
    l_librbd_wr_bytes,
    l_librbd_discard,
    l_librbd_discard_bytes,
    l_librbd_flush,

    l_librbd_aio_rd,               // read ops
    l_librbd_aio_rd_bytes,         // bytes read
    l_librbd_aio_wr,
    l_librbd_aio_wr_bytes,
    l_librbd_aio_discard,
    l_librbd_aio_discard_bytes,

    l_librbd_snap_create,
    l_librbd_snap_remove,
    l_librbd_snap_rollback,

    l_librbd_notify,
    l_librbd_resize,

    l_librbd_last,
  };

  using ceph::bufferlist;
  using librados::snap_t;
  using librados::IoCtx;
  using librados::Rados;

  // raw callbacks
  void rados_cb(rados_completion_t cb, void *arg);
  void rados_aio_sparse_read_cb(rados_completion_t cb, void *arg);

  class WatchCtx;

  struct SnapInfo {
    snap_t id;
    uint64_t size;
    SnapInfo(snap_t _id, uint64_t _size) : id(_id), size(_size) {};
  };

  struct AioCompletion;

  struct AioBlockCompletion : Context {
    CephContext *cct;
    struct AioCompletion *completion;
    uint64_t ofs;
    size_t len;
    char *buf;
    map<uint64_t,uint64_t> m;
    bufferlist data_bl;
    librados::ObjectWriteOperation write_op;

    AioBlockCompletion(CephContext *cct_, AioCompletion *aio_completion,
		       uint64_t _ofs, size_t _len, char *_buf)
      : cct(cct_), completion(aio_completion),
	ofs(_ofs), len(_len), buf(_buf) {}
    virtual ~AioBlockCompletion() {}
    virtual void finish(int r);
  };

  struct ImageCtx {
    CephContext *cct;
    PerfCounters *perfcounter;
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    vector<snap_t> snaps;
    std::map<std::string, struct SnapInfo> snaps_by_name;
    uint64_t snapid;
    bool snap_exists; // false if our snapid was deleted
    std::string name;
    std::string snapname;
    IoCtx data_ctx, md_ctx;
    WatchCtx *wctx;
    bool needs_refresh;
    Mutex refresh_lock;
    Mutex lock; // protects access to snapshot and header information
    Mutex cache_lock; // used as client_lock for the ObjectCacher

    ObjectCacher *object_cacher;
    LibrbdWriteback *writeback_handler;
    ObjectCacher::ObjectSet *object_set;

    ImageCtx(std::string imgname, const char *snap, IoCtx& p)
      : cct((CephContext*)p.cct()),
	perfcounter(NULL),
	snapid(CEPH_NOSNAP),
	snap_exists(true),
	name(imgname),
	needs_refresh(true),
	refresh_lock("librbd::ImageCtx::refresh_lock"),
	lock("librbd::ImageCtx::lock"),
	cache_lock("librbd::ImageCtx::cache_lock"),
	object_cacher(NULL), writeback_handler(NULL), object_set(NULL)
    {
      md_ctx.dup(p);
      data_ctx.dup(p);

      string pname = string("librbd-") + data_ctx.get_pool_name() + string("/") + name;
      if (snap) {
	snapname = snap;
	pname += "@";
	pname += snapname;
      }
      perf_start(pname);

      if (cct->_conf->rbd_cache) {
	Mutex::Locker l(cache_lock);
	ldout(cct, 20) << "enabling writback caching..." << dendl;
	writeback_handler = new LibrbdWriteback(data_ctx, cache_lock);
	object_cacher = new ObjectCacher(cct, pname, *writeback_handler, cache_lock,
					 NULL, NULL);
	object_cacher->set_max_size(cct->_conf->rbd_cache_size);
	object_cacher->set_max_dirty(cct->_conf->rbd_cache_max_dirty);
	object_cacher->set_target_dirty(cct->_conf->rbd_cache_target_dirty);
	object_set = new ObjectCacher::ObjectSet(NULL, data_ctx.get_id(), 0);
	object_cacher->start();
      }
    }

    ~ImageCtx() {
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
    }

    void perf_start(string name) {
      PerfCountersBuilder plb(cct, name, l_librbd_first, l_librbd_last);

      plb.add_u64_counter(l_librbd_rd, "rd");
      plb.add_u64_counter(l_librbd_rd_bytes, "rd_bytes");
      plb.add_u64_counter(l_librbd_wr, "wr");
      plb.add_u64_counter(l_librbd_wr_bytes, "wr_bytes");
      plb.add_u64_counter(l_librbd_discard, "discard");
      plb.add_u64_counter(l_librbd_discard_bytes, "discard_bytes");
      plb.add_u64_counter(l_librbd_flush, "flush");
      plb.add_u64_counter(l_librbd_aio_rd, "aio_rd");
      plb.add_u64_counter(l_librbd_aio_rd_bytes, "aio_rd_bytes");
      plb.add_u64_counter(l_librbd_aio_wr, "aio_wr");
      plb.add_u64_counter(l_librbd_aio_wr_bytes, "aio_wr_bytes");
      plb.add_u64_counter(l_librbd_aio_discard, "aio_discard");
      plb.add_u64_counter(l_librbd_aio_discard_bytes, "aio_discard_bytes");
      plb.add_u64_counter(l_librbd_snap_create, "snap_create");
      plb.add_u64_counter(l_librbd_snap_remove, "snap_remove");
      plb.add_u64_counter(l_librbd_snap_rollback, "snap_rollback");
      plb.add_u64_counter(l_librbd_notify, "notify");
      plb.add_u64_counter(l_librbd_resize, "resize");

      perfcounter = plb.create_perf_counters();
      cct->get_perfcounters_collection()->add(perfcounter);
    }

    void perf_stop() {
      assert(perfcounter);
      cct->get_perfcounters_collection()->remove(perfcounter);
      delete perfcounter;
    }

    int snap_set(std::string snap_name)
    {
      std::map<std::string, struct SnapInfo>::iterator it = snaps_by_name.find(snap_name);
      if (it != snaps_by_name.end()) {
	snapname = snap_name;
	snapid = it->second.id;
	return 0;
      }
      return -ENOENT;
    }

    void snap_unset()
    {
      snapid = CEPH_NOSNAP;
      snapname = "";
    }

    snap_t get_snapid(std::string snap_name) const
    {
      std::map<std::string, struct SnapInfo>::const_iterator it = snaps_by_name.find(snap_name);
      if (it != snaps_by_name.end())
	return it->second.id;
      return CEPH_NOSNAP;
    }

    int get_snap_size(std::string snap_name, uint64_t *size) const
    {
      std::map<std::string, struct SnapInfo>::const_iterator it = snaps_by_name.find(snap_name);
      if (it != snaps_by_name.end()) {
	*size = it->second.size;
	return 0;
      }
      return -ENOENT;
    }

    void add_snap(std::string snap_name, snap_t id, uint64_t size)
    {
      snapc.snaps.push_back(id);
      snaps.push_back(id);
      struct SnapInfo info(id, size);
      snaps_by_name.insert(std::pair<std::string, struct SnapInfo>(snap_name, info));
    }

    const string md_oid() const
    {
      return name + RBD_SUFFIX;
    }

    uint64_t get_image_size() const
    {
      if (snapname.length() == 0) {
	return header.image_size;
      } else {
	map<std::string,SnapInfo>::const_iterator p = snaps_by_name.find(snapname);
	if (p == snaps_by_name.end())
	  return 0;
	return p->second.size;
      }
    }

    void aio_read_from_cache(object_t o, bufferlist *bl, size_t len,
			     uint64_t off, Context *onfinish) {
      lock.Lock();
      ObjectCacher::OSDRead *rd = object_cacher->prepare_read(snapid, bl, 0);
      lock.Unlock();
      ObjectExtent extent(o, off, len);
      extent.oloc.pool = data_ctx.get_id();
      extent.buffer_extents[0] = len;
      rd->extents.push_back(extent);
      cache_lock.Lock();
      int r = object_cacher->readx(rd, object_set, onfinish);
      cache_lock.Unlock();
      if (r > 0)
	onfinish->complete(r);
    }

    void write_to_cache(object_t o, bufferlist& bl, size_t len, uint64_t off) {
      lock.Lock();
      ObjectCacher::OSDWrite *wr = object_cacher->prepare_write(snapc, bl,
								utime_t(), 0);
      lock.Unlock();
      ObjectExtent extent(o, off, len);
      extent.oloc.pool = data_ctx.get_id();
      extent.buffer_extents[0] = len;
      wr->extents.push_back(extent);
      {
	Mutex::Locker l(cache_lock);
	object_cacher->writex(wr, object_set, cache_lock);
      }
    }

    int read_from_cache(object_t o, bufferlist *bl, size_t len, uint64_t off) {
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

    void flush_cache() {
      int r;
      Mutex mylock("librbd::ImageCtx::flush_cache");
      Cond cond;
      bool done;
      Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
      cache_lock.Lock();
      bool already_flushed = object_cacher->commit_set(object_set, onfinish);
      cache_lock.Unlock();
      if (!already_flushed) {
	mylock.Lock();
	while (!done) {
	  ldout(cct, 20) << "waiting for cache to be flushed" << dendl;
	  cond.Wait(mylock);
	}
	mylock.Unlock();
	ldout(cct, 20) << "finished flushing cache" << dendl;
      }
    }

    void shutdown_cache() {
      lock.Lock();
      invalidate_cache();
      lock.Unlock();
      object_cacher->stop();
    }

    void invalidate_cache() {
      assert(lock.is_locked());
      if (!object_cacher)
	return;
      cache_lock.Lock();
      object_cacher->release_set(object_set);
      cache_lock.Unlock();
      flush_cache();
      cache_lock.Lock();
      bool unclean = object_cacher->release_set(object_set);
      cache_lock.Unlock();
      assert(!unclean);
    }
  };

  class WatchCtx : public librados::WatchCtx {
    ImageCtx *ictx;
    bool valid;
    Mutex lock;
  public:
    uint64_t cookie;
    WatchCtx(ImageCtx *ctx) : ictx(ctx),
			      valid(true),
			      lock("librbd::WatchCtx") {}
    virtual ~WatchCtx() {}
    void invalidate();
    virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl);
  };

  struct AioCompletion {
    Mutex lock;
    Cond cond;
    bool done;
    ssize_t rval;
    callback_t complete_cb;
    void *complete_arg;
    rbd_completion_t rbd_comp;
    int pending_count;
    int ref;
    bool released;

    AioCompletion() : lock("AioCompletion::lock", true),
		      done(false), rval(0), complete_cb(NULL), complete_arg(NULL),
		      rbd_comp(NULL), pending_count(1), ref(1), released(false) {
    }
    ~AioCompletion() {
    }

    int wait_for_complete() {
      lock.Lock();
      while (!done)
	cond.Wait(lock);
      lock.Unlock();
      return 0;
    }

    void add_block_completion(AioBlockCompletion *aio_completion) {
      lock.Lock();
      pending_count++;
      lock.Unlock();
      get();
    }

    void finish_adding_completions() {
      lock.Lock();
      assert(pending_count);
      int count = --pending_count;
      if (!count) {
	complete();
      }
      lock.Unlock();
    }

    void complete() {
      assert(lock.is_locked());
      if (complete_cb) {
	complete_cb(rbd_comp, complete_arg);
      }
      done = true;
      cond.Signal();
    }

    void set_complete_cb(void *cb_arg, callback_t cb) {
      complete_cb = cb;
      complete_arg = cb_arg;
    }

    void complete_block(AioBlockCompletion *block_completion, ssize_t r);

    ssize_t get_return_value() {
      lock.Lock();
      ssize_t r = rval;
      lock.Unlock();
      return r;
    }

    void get() {
      lock.Lock();
      assert(ref > 0);
      ref++;
      lock.Unlock();
    }
    void release() {
      lock.Lock();
      assert(!released);
      released = true;
      put_unlock();
    }
    void put() {
      lock.Lock();
      put_unlock();
    }
    void put_unlock() {
      assert(ref > 0);
      int n = --ref;
      lock.Unlock();
      if (!n)
	delete this;
    }
  };

  int snap_set(ImageCtx *ictx, const char *snap_name);
  int list(IoCtx& io_ctx, std::vector<string>& names);
  int create(IoCtx& io_ctx, const char *imgname, uint64_t size, int *order);
  int rename(IoCtx& io_ctx, const char *srcname, const char *dstname);
  int info(ImageCtx *ictx, image_info_t& info, size_t image_size);
  int remove(IoCtx& io_ctx, const char *imgname, ProgressContext& prog_ctx);
  int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx);
  int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx);
  int snap_create(ImageCtx *ictx, const char *snap_name);
  int snap_list(ImageCtx *ictx, std::vector<snap_info_t>& snaps);
  int snap_rollback(ImageCtx *ictx, const char *snap_name, ProgressContext& prog_ctx);
  int snap_remove(ImageCtx *ictx, const char *snap_name);
  int add_snap(ImageCtx *ictx, const char *snap_name);
  int rm_snap(ImageCtx *ictx, const char *snap_name);
  int ictx_check(ImageCtx *ictx);
  int ictx_refresh(ImageCtx *ictx);
  int copy(ImageCtx& srci, IoCtx& dest_md_ctx, const char *destname);

  int open_image(ImageCtx *ictx);
  void close_image(ImageCtx *ictx);

  void trim_image(IoCtx& io_ctx, const rbd_obj_header_ondisk &header, uint64_t newsize,
		  ProgressContext& prog_ctx);
  int read_rbd_info(IoCtx& io_ctx, const string& info_oid, struct rbd_info *info);

  int touch_rbd_info(IoCtx& io_ctx, const string& info_oid);
  int rbd_assign_bid(IoCtx& io_ctx, const string& info_oid, uint64_t *id);
  int read_header_bl(IoCtx& io_ctx, const string& md_oid, bufferlist& header, uint64_t *ver);
  int notify_change(IoCtx& io_ctx, const string& oid, uint64_t *pver, ImageCtx *ictx);
  int read_header(IoCtx& io_ctx, const string& md_oid, struct rbd_obj_header_ondisk *header, uint64_t *ver);
  int write_header(IoCtx& io_ctx, const string& md_oid, bufferlist& header);
  int tmap_set(IoCtx& io_ctx, const string& imgname);
  int tmap_rm(IoCtx& io_ctx, const string& imgname);
  int rollback_image(ImageCtx *ictx, uint64_t snapid, ProgressContext& prog_ctx);
  void image_info(const ImageCtx& ictx, image_info_t& info, size_t info_size);
  string get_block_oid(const rbd_obj_header_ondisk &header, uint64_t num);
  uint64_t get_max_block(uint64_t size, int obj_order);
  uint64_t get_max_block(const rbd_obj_header_ondisk &header);
  uint64_t get_block_size(const rbd_obj_header_ondisk &header);
  uint64_t get_block_num(const rbd_obj_header_ondisk &header, uint64_t ofs);
  uint64_t get_block_ofs(const rbd_obj_header_ondisk &header, uint64_t ofs);
  int check_io(ImageCtx *ictx, uint64_t off, uint64_t len);
  int init_rbd_info(struct rbd_info *info);
  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			      uint64_t size, int *order, uint64_t bid);

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, size_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg);
  ssize_t read(ImageCtx *ictx, uint64_t off, size_t len, char *buf);
  ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf);
  int discard(ImageCtx *ictx, uint64_t off, uint64_t len);
  int aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
                AioCompletion *c);
  int aio_discard(ImageCtx *ictx, uint64_t off, size_t len, AioCompletion *c);
  int aio_read(ImageCtx *ictx, uint64_t off, size_t len,
               char *buf, AioCompletion *c);
  int flush(ImageCtx *ictx);
  int _flush(ImageCtx *ictx);

  ssize_t handle_sparse_read(CephContext *cct,
			     bufferlist data_bl,
			     uint64_t block_ofs,
			     const map<uint64_t, uint64_t> &data_map,
			     uint64_t buf_ofs,
			     size_t buf_len,
			     int (*cb)(uint64_t, size_t, const char *, void *),
			     void *arg);

  AioCompletion *aio_create_completion() {
    AioCompletion *c= new AioCompletion();
    return c;
  }
  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete) {
    AioCompletion *c = new AioCompletion();
    c->set_complete_cb(cb_arg, cb_complete);
    return c;
  }

void WatchCtx::invalidate()
{
  Mutex::Locker l(lock);
  valid = false;
}

void WatchCtx::notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
{
  Mutex::Locker l(lock);
  ldout(ictx->cct, 1) <<  " got notification opcode=" << (int)opcode << " ver=" << ver << " cookie=" << cookie << dendl;
  if (valid) {
    Mutex::Locker lictx(ictx->refresh_lock);
    ictx->needs_refresh = true;
    ictx->perfcounter->inc(l_librbd_notify);
  }
}

void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
					uint64_t size, int *order, uint64_t bid)
{
  uint32_t hi = bid >> 32;
  uint32_t lo = bid & 0xFFFFFFFF;
  memset(&ondisk, 0, sizeof(ondisk));

  memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
  memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE, sizeof(RBD_HEADER_SIGNATURE));
  memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

  snprintf(ondisk.block_name, sizeof(ondisk.block_name), "rb.%x.%x", hi, lo);

  if (!*order)
    *order = RBD_DEFAULT_OBJ_ORDER;

  ondisk.image_size = size;
  ondisk.options.order = *order;
  ondisk.options.crypt_type = RBD_CRYPT_NONE;
  ondisk.options.comp_type = RBD_COMP_NONE;
  ondisk.snap_seq = 0;
  ondisk.snap_count = 0;
  ondisk.reserved = 0;
  ondisk.snap_names_len = 0;
}

void image_info(const ImageCtx& ictx, image_info_t& info, size_t infosize)
{
  int obj_order = ictx.header.options.order;
  info.size = ictx.get_image_size();
  info.obj_size = 1 << obj_order;
  info.num_objs = ictx.get_image_size() >> obj_order;
  info.order = obj_order;
  memcpy(&info.block_name_prefix, &ictx.header.block_name, RBD_MAX_BLOCK_NAME_SIZE);
  info.parent_pool = -1;
  bzero(&info.parent_name, RBD_MAX_IMAGE_NAME_SIZE);
}

string get_block_oid(const rbd_obj_header_ondisk &header, uint64_t num)
{
  char o[RBD_MAX_BLOCK_NAME_SIZE];
  snprintf(o, RBD_MAX_BLOCK_NAME_SIZE,
       "%s.%012" PRIx64, header.block_name, num);
  return o;
}

uint64_t get_max_block(uint64_t size, int obj_order)
{
  uint64_t block_size = 1 << obj_order;
  uint64_t numseg = (size + block_size - 1) >> obj_order;
  return numseg;
}

uint64_t get_max_block(const rbd_obj_header_ondisk &header)
{
  return get_max_block(header.image_size, header.options.order);
}

uint64_t get_block_ofs(const rbd_obj_header_ondisk &header, uint64_t ofs)
{
  int obj_order = header.options.order;
  uint64_t block_size = 1 << obj_order;
  return ofs & (block_size - 1);
}

uint64_t get_block_size(const rbd_obj_header_ondisk &header)
{
  return 1 << header.options.order;
}

uint64_t get_block_num(const rbd_obj_header_ondisk &header, uint64_t ofs)
{
  int obj_order = header.options.order;
  uint64_t num = ofs >> obj_order;

  return num;
}

int init_rbd_info(struct rbd_info *info)
{
  memset(info, 0, sizeof(*info));
  return 0;
}

void trim_image(IoCtx& io_ctx, const rbd_obj_header_ondisk &header, uint64_t newsize,
		ProgressContext& prog_ctx)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  uint64_t bsize = get_block_size(header);
  uint64_t numseg = get_max_block(header);
  uint64_t start = get_block_num(header, newsize);

  uint64_t block_ofs = get_block_ofs(header, newsize);
  if (block_ofs) {
    ldout(cct, 2) << "trim_image object " << numseg << " truncate to " << block_ofs << dendl;
    string oid = get_block_oid(header, start);
    librados::ObjectWriteOperation write_op;
    write_op.truncate(block_ofs);
    io_ctx.operate(oid, &write_op);
    start++;
  }
  if (start < numseg) {
    ldout(cct, 2) << "trim_image objects " << start << " to " << (numseg-1) << dendl;
    for (uint64_t i=start; i<numseg; i++) {
      string oid = get_block_oid(header, i);
      io_ctx.remove(oid);
      prog_ctx.update_progress(i * bsize, (numseg - start) * bsize);
    }
  }
}

int read_rbd_info(IoCtx& io_ctx, const string& info_oid, struct rbd_info *info)
{
  int r;
  bufferlist bl;
  r = io_ctx.read(info_oid, bl, sizeof(*info), 0);
  if (r < 0)
    return r;
  if (r == 0) {
    return init_rbd_info(info);
  }

  if (r < (int)sizeof(*info))
    return -EIO;

  memcpy(info, bl.c_str(), r);
  return 0;
}

int touch_rbd_info(IoCtx& io_ctx, const string& info_oid)
{
  bufferlist bl;
  int r = io_ctx.write(info_oid, bl, 0, 0);
  if (r < 0)
    return r;
  return 0;
}

int rbd_assign_bid(IoCtx& io_ctx, const string& info_oid, uint64_t *id)
{
  bufferlist bl, out;
  *id = 0;

  int r = touch_rbd_info(io_ctx, info_oid);
  if (r < 0)
    return r;

  r = io_ctx.exec(info_oid, "rbd", "assign_bid", bl, out);
  if (r < 0)
    return r;

  bufferlist::iterator iter = out.begin();
  ::decode(*id, iter);

  return 0;
}


int read_header_bl(IoCtx& io_ctx, const string& md_oid, bufferlist& header, uint64_t *ver)
{
  int r;
  uint64_t off = 0;
#define READ_SIZE 4096
  do {
    bufferlist bl;
    r = io_ctx.read(md_oid, bl, READ_SIZE, off);
    if (r < 0)
      return r;
    header.claim_append(bl);
    off += r;
   } while (r == READ_SIZE);

  if (memcmp(RBD_HEADER_TEXT, header.c_str(), sizeof(RBD_HEADER_TEXT))) {
    CephContext *cct = (CephContext *)io_ctx.cct();
    lderr(cct) << "unrecognized header format" << dendl;
    return -ENXIO;
  }

  if (ver)
    *ver = io_ctx.get_last_version();

  return 0;
}

int notify_change(IoCtx& io_ctx, const string& oid, uint64_t *pver, ImageCtx *ictx)
{
  uint64_t ver;

  if (ictx) {
    assert(ictx->lock.is_locked());
    ictx->refresh_lock.Lock();
    ictx->needs_refresh = true;
    ictx->refresh_lock.Unlock();
  }

  if (pver)
    ver = *pver;
  else
    ver = io_ctx.get_last_version();
  bufferlist bl;
  io_ctx.notify(oid, ver, bl);
  return 0;
}

int read_header(IoCtx& io_ctx, const string& md_oid, struct rbd_obj_header_ondisk *header, uint64_t *ver)
{
  bufferlist header_bl;
  int r = read_header_bl(io_ctx, md_oid, header_bl, ver);
  if (r < 0)
    return r;
  if (header_bl.length() < (int)sizeof(*header))
    return -EIO;
  memcpy(header, header_bl.c_str(), sizeof(*header));

  return 0;
}

int write_header(IoCtx& io_ctx, const string& md_oid, bufferlist& header)
{
  bufferlist bl;
  int r = io_ctx.write(md_oid, header, header.length(), 0);

  notify_change(io_ctx, md_oid, NULL, NULL);

  return r;
}

int tmap_set(IoCtx& io_ctx, const string& imgname)
{
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  ::encode(emptybl, cmdbl);
  return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
}

int tmap_rm(IoCtx& io_ctx, const string& imgname)
{
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
}

int rollback_image(ImageCtx *ictx, uint64_t snapid, ProgressContext& prog_ctx)
{
  assert(ictx->lock.is_locked());
  uint64_t numseg = get_max_block(ictx->header);
  uint64_t bsize = get_block_size(ictx->header);

  for (uint64_t i = 0; i < numseg; i++) {
    int r;
    string oid = get_block_oid(ictx->header, i);
    r = ictx->data_ctx.selfmanaged_snap_rollback(oid, snapid);
    ldout(ictx->cct, 10) << "selfmanaged_snap_rollback on " << oid << " to " << snapid << " returned " << r << dendl;
    prog_ctx.update_progress(i * bsize, numseg * bsize);
    if (r < 0 && r != -ENOENT)
      return r;
  }
  return 0;
}

int list(IoCtx& io_ctx, std::vector<std::string>& names)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "list " << &io_ctx << dendl;

  bufferlist bl;
  int r = io_ctx.read(RBD_DIRECTORY, bl, 0, 0);
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  bufferlist header;
  map<string,bufferlist> m;
  ::decode(header, p);
  ::decode(m, p);
  for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++)
    names.push_back(q->first);
  return 0;
}

int snap_create(ImageCtx *ictx, const char *snap_name)
{
  ldout(ictx->cct, 20) << "snap_create " << ictx << " " << snap_name << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  Mutex::Locker l(ictx->lock);
  r = add_snap(ictx, snap_name);

  if (r < 0)
    return r;

  notify_change(ictx->md_ctx, ictx->md_oid(), NULL, ictx);

  ictx->perfcounter->inc(l_librbd_snap_create);
  return 0;
}

int snap_remove(ImageCtx *ictx, const char *snap_name)
{
  ldout(ictx->cct, 20) << "snap_remove " << ictx << " " << snap_name << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  Mutex::Locker l(ictx->lock);
  snap_t snapid = ictx->get_snapid(snap_name);
  if (snapid == CEPH_NOSNAP)
    return -ENOENT;

  r = rm_snap(ictx, snap_name);
  if (r < 0)
    return r;

  r = ictx->data_ctx.selfmanaged_snap_remove(snapid);

  if (r < 0)
    return r;

  notify_change(ictx->md_ctx, ictx->md_oid(), NULL, ictx);

  ictx->perfcounter->inc(l_librbd_snap_remove);
  return 0;
}

int create(IoCtx& io_ctx, const char *imgname, uint64_t size, int *order)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "create " << &io_ctx << " name = " << imgname << " size = " << size << dendl;

  string md_oid = imgname;
  md_oid += RBD_SUFFIX;

  // make sure it doesn't already exist
  int r = io_ctx.stat(md_oid, NULL, NULL);
  if (r == 0) {
    lderr(cct) << "rbd image header " << md_oid << " already exists" << dendl;
    return -EEXIST;
  }

  uint64_t bid;
  string dir_info = RBD_INFO;
  r = rbd_assign_bid(io_ctx, dir_info, &bid);
  if (r < 0) {
    lderr(cct) << "failed to assign a block name for image" << dendl;
    return r;
  }

  struct rbd_obj_header_ondisk header;
  init_rbd_header(header, size, order, bid);

  bufferlist bl;
  bl.append((const char *)&header, sizeof(header));

  ldout(cct, 2) << "adding rbd image to directory..." << dendl;
  r = tmap_set(io_ctx, imgname);
  if (r < 0) {
    lderr(cct) << "error adding img to directory: " << cpp_strerror(-r)<< dendl;
    return r;
  }

  ldout(cct, 2) << "creating rbd image..." << dendl;
  r = io_ctx.write(md_oid, bl, bl.length(), 0);
  if (r < 0) {
    lderr(cct) << "error writing header: " << cpp_strerror(-r) << dendl;
    return r;
  }

  ldout(cct, 2) << "done." << dendl;
  return 0;
}

int rename(IoCtx& io_ctx, const char *srcname, const char *dstname)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "rename " << &io_ctx << " " << srcname << " -> " << dstname << dendl;

  string md_oid = srcname;
  md_oid += RBD_SUFFIX;
  string dst_md_oid = dstname;
  dst_md_oid += RBD_SUFFIX;
  string dstname_str = dstname;
  string imgname_str = srcname;
  uint64_t ver;
  bufferlist header;
  int r = read_header_bl(io_ctx, md_oid, header, &ver);
  if (r < 0) {
    lderr(cct) << "error reading header: " << md_oid << ": " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = io_ctx.stat(dst_md_oid, NULL, NULL);
  if (r == 0) {
    lderr(cct) << "rbd image header " << dst_md_oid << " already exists" << dendl;
    return -EEXIST;
  }
  r = write_header(io_ctx, dst_md_oid, header);
  if (r < 0) {
    lderr(cct) << "error writing header: " << dst_md_oid << ": " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = tmap_set(io_ctx, dstname_str);
  if (r < 0) {
    io_ctx.remove(dst_md_oid);
    lderr(cct) << "can't add " << dst_md_oid << " to directory" << dendl;
    return r;
  }
  r = tmap_rm(io_ctx, imgname_str);
  if (r < 0)
    lderr(cct) << "warning: couldn't remove old entry from directory (" << imgname_str << ")" << dendl;

  r = io_ctx.remove(md_oid);
  if (r < 0 && r != -ENOENT)
    lderr(cct) << "warning: couldn't remove old metadata" << dendl;
  notify_change(io_ctx, md_oid, NULL, NULL);

  return 0;
}


int info(ImageCtx *ictx, image_info_t& info, size_t infosize)
{
  ldout(ictx->cct, 20) << "info " << ictx << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  Mutex::Locker l(ictx->lock);
  image_info(*ictx, info, infosize);
  return 0;
}

bool has_snaps(IoCtx& io_ctx, const std::string& md_oid)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "has_snaps " << &io_ctx << " " << md_oid << dendl;

  bufferlist bl, bl2;
  int r = io_ctx.exec(md_oid, "rbd", "snap_list", bl, bl2);
  if (r < 0) {
    lderr(cct) << "Error listing snapshots: " << cpp_strerror(-r) << dendl;
    return true;
  }
  uint32_t num_snaps;
  uint64_t snap_seq;
  bufferlist::iterator iter = bl2.begin();
  ::decode(snap_seq, iter);
  ::decode(num_snaps, iter);
  return num_snaps > 0;
}

int remove(IoCtx& io_ctx, const char *imgname, ProgressContext& prog_ctx)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "remove " << &io_ctx << " " << imgname << dendl;

  string md_oid = imgname;
  md_oid += RBD_SUFFIX;

  struct rbd_obj_header_ondisk header;
  int r = read_header(io_ctx, md_oid, &header, NULL);
  if (r < 0) {
    ldout(cct, 2) << "error reading header: " << cpp_strerror(-r) << dendl;
  }
  if (r >= 0) {
    if (has_snaps(io_ctx, md_oid)) {
      lderr(cct) << "image has snapshots - not removing" << dendl;
      return -ENOTEMPTY;
    }
    trim_image(io_ctx, header, 0, prog_ctx);
    ldout(cct, 2) << "removing header..." << dendl;
    r = io_ctx.remove(md_oid);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  ldout(cct, 2) << "removing rbd image from directory..." << dendl;
  r = tmap_rm(io_ctx, imgname);
  if (r < 0) {
    lderr(cct) << "error removing img from directory: " << cpp_strerror(-r) << dendl;
    return r;
  }

  ldout(cct, 2) << "done." << dendl;
  return 0;
}

int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
{
  CephContext *cct = ictx->cct;
  if (size == ictx->header.image_size) {
    ldout(cct, 2) << "no change in size (" << ictx->header.image_size << " -> " << size << ")" << dendl;
    return 0;
  }

  if (size > ictx->header.image_size) {
    ldout(cct, 2) << "expanding image " << ictx->header.image_size << " -> " << size << dendl;
    ictx->header.image_size = size;
  } else {
    ldout(cct, 2) << "shrinking image " << ictx->header.image_size << " -> " << size << dendl;
    trim_image(ictx->data_ctx, ictx->header, size, prog_ctx);
    ictx->header.image_size = size;
  }

  // rewrite header
  bufferlist bl;
  bl.append((const char *)&(ictx->header), sizeof(ictx->header));
  int r = ictx->md_ctx.write(ictx->md_oid(), bl, bl.length(), 0);

  if (r == -ERANGE)
    lderr(cct) << "operation might have conflicted with another client!" << dendl;
  if (r < 0) {
    lderr(cct) << "error writing header: " << cpp_strerror(-r) << dendl;
    return r;
  } else {
    notify_change(ictx->md_ctx, ictx->md_oid(), NULL, ictx);
  }

  return 0;
}

int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "resize " << ictx << " " << ictx->header.image_size << " -> " << size << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  Mutex::Locker l(ictx->lock);
  if (size < ictx->header.image_size && ictx->object_cacher) {
    // need to invalidate since we're deleting objects, and
    // ObjectCacher doesn't track non-existent objects
    ictx->invalidate_cache();
  }
  resize_helper(ictx, size, prog_ctx);

  ldout(cct, 2) << "done." << dendl;

  ictx->perfcounter->inc(l_librbd_resize);
  return 0;
}

int snap_list(ImageCtx *ictx, std::vector<snap_info_t>& snaps)
{
  ldout(ictx->cct, 20) << "snap_list " << ictx << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;
  bufferlist bl, bl2;

  Mutex::Locker l(ictx->lock);
  for (std::map<std::string, struct SnapInfo>::iterator it = ictx->snaps_by_name.begin();
       it != ictx->snaps_by_name.end(); ++it) {
    snap_info_t info;
    info.name = it->first;
    info.id = it->second.id;
    info.size = it->second.size;
    snaps.push_back(info);
  }

  return 0;
}

int add_snap(ImageCtx *ictx, const char *snap_name)
{
  assert(ictx->lock.is_locked());

  bufferlist bl, bl2;
  uint64_t snap_id;

  int r = ictx->md_ctx.selfmanaged_snap_create(&snap_id);
  if (r < 0) {
    lderr(ictx->cct) << "failed to create snap id: " << cpp_strerror(-r) << dendl;
    return r;
  }

  ::encode(snap_name, bl);
  ::encode(snap_id, bl);

  r = ictx->md_ctx.exec(ictx->md_oid(), "rbd", "snap_add", bl, bl2);
  if (r < 0) {
    lderr(ictx->cct) << "rbd.snap_add execution failed failed: " << cpp_strerror(-r) << dendl;
    return r;
  }
  notify_change(ictx->md_ctx, ictx->md_oid(), NULL, ictx);

  return 0;
}

int rm_snap(ImageCtx *ictx, const char *snap_name)
{
  assert(ictx->lock.is_locked());

  bufferlist bl, bl2;
  ::encode(snap_name, bl);

  int r = ictx->md_ctx.exec(ictx->md_oid(), "rbd", "snap_remove", bl, bl2);
  if (r < 0) {
    lderr(ictx->cct) << "rbd.snap_remove execution failed: " << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

int ictx_check(ImageCtx *ictx)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "ictx_check " << ictx << dendl;
  ictx->refresh_lock.Lock();
  bool needs_refresh = ictx->needs_refresh;
  ictx->refresh_lock.Unlock();

  if (needs_refresh) {
    Mutex::Locker l(ictx->lock);

    int r = ictx_refresh(ictx);
    if (r < 0) {
      lderr(cct) << "Error re-reading rbd header: " << cpp_strerror(-r) << dendl;
      return r;
    }
  }
  return 0;
}

int ictx_refresh(ImageCtx *ictx)
{
  CephContext *cct = ictx->cct;
  assert(ictx->lock.is_locked());
  bufferlist bl, bl2;

  ldout(cct, 20) << "ictx_refresh " << ictx << dendl;

  ictx->refresh_lock.Lock();
  ictx->needs_refresh = false;
  ictx->refresh_lock.Unlock();

  int r = read_header(ictx->md_ctx, ictx->md_oid(), &(ictx->header), NULL);
  if (r < 0) {
    lderr(cct) << "Error reading header: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = ictx->md_ctx.exec(ictx->md_oid(), "rbd", "snap_list", bl, bl2);
  if (r < 0) {
    lderr(cct) << "Error listing snapshots: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = 0;

  std::map<snap_t, std::string> old_snap_ids;
  for (std::map<std::string, struct SnapInfo>::iterator it =
	 ictx->snaps_by_name.begin(); it != ictx->snaps_by_name.end(); ++it) {
    old_snap_ids[it->second.id] = it->first;
  }
  bool new_snap = false;

  ictx->snaps.clear();
  ictx->snapc.snaps.clear();
  ictx->snaps_by_name.clear();

  uint32_t num_snaps;
  bufferlist::iterator iter = bl2.begin();
  ::decode(ictx->snapc.seq, iter);
  ::decode(num_snaps, iter);
  for (uint32_t i=0; i < num_snaps; i++) {
    uint64_t id, image_size;
    string s;
    ::decode(id, iter);
    ::decode(image_size, iter);
    ::decode(s, iter);
    ictx->add_snap(s, id, image_size);
    std::map<snap_t, std::string>::const_iterator it = old_snap_ids.find(id);
    if (it == old_snap_ids.end()) {
      new_snap = true;
      ldout(cct, 20) << "new snapshot " << s << " size " << image_size << dendl;
    }
  }

  if (new_snap) {
    _flush(ictx);
  }

  if (!ictx->snapc.is_valid()) {
    lderr(cct) << "image snap context is invalid!" << dendl;
    ictx->refresh_lock.Lock();
    ictx->needs_refresh = true;
    ictx->refresh_lock.Unlock();
    return -EIO;
  }

  if (ictx->snapid != CEPH_NOSNAP &&
      ictx->get_snapid(ictx->snapname) == CEPH_NOSNAP) {
    lderr(cct) << "tried to read from a snapshot that no longer exists: "
	       << ictx->snapname << dendl;
    ictx->snap_exists = false;
  }

  ictx->data_ctx.selfmanaged_snap_set_write_ctx(ictx->snapc.seq, ictx->snaps);

  return 0;
}

ProgressContext::~ProgressContext()
{
}

class NoOpProgressContext : public librbd::ProgressContext
{
public:
  NoOpProgressContext()
  {
  }
  int update_progress(uint64_t offset, uint64_t src_size)
  {
    return 0;
  }
};

class CProgressContext : public librbd::ProgressContext
{
public:
  CProgressContext(librbd_progress_fn_t fn, void *data)
    : m_fn(fn), m_data(data)
  {
  }
  int update_progress(uint64_t offset, uint64_t src_size)
  {
    return m_fn(offset, src_size, m_data);
  }
private:
  librbd_progress_fn_t m_fn;
  void *m_data;
};

int snap_rollback(ImageCtx *ictx, const char *snap_name, ProgressContext& prog_ctx)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "snap_rollback " << ictx << " snap = " << snap_name << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  if (!ictx->snap_exists)
    return -ENOENT;

  if (ictx->snapid != CEPH_NOSNAP)
    return -EROFS;

  Mutex::Locker l(ictx->lock);
  snap_t snapid = ictx->get_snapid(snap_name);
  if (snapid == CEPH_NOSNAP) {
    lderr(cct) << "No such snapshot found." << dendl;
    return -ENOENT;
  }

  // need to flush any pending writes before resizing and rolling back -
  // writes might create new snapshots. Rolling back will replace
  // the current version, so we have to invalidate that too.
  ictx->invalidate_cache();

  uint64_t new_size = ictx->get_image_size();
  ictx->get_snap_size(snap_name, &new_size);
  ldout(cct, 2) << "resizing to snapshot size..." << dendl;
  NoOpProgressContext no_op;
  r = resize_helper(ictx, new_size, no_op);
  if (r < 0) {
    lderr(cct) << "Error resizing to snapshot size: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = rollback_image(ictx, snapid, prog_ctx);
  if (r < 0) {
    lderr(cct) << "Error rolling back image: " << cpp_strerror(-r) << dendl;
    return r;
  }

  ictx_refresh(ictx);
  snap_t new_snapid = ictx->get_snapid(snap_name);
  ldout(cct, 20) << "snapid is " << ictx->snapid << " new snapid is " << new_snapid << dendl;

  notify_change(ictx->md_ctx, ictx->md_oid(), NULL, ictx);

  ictx->perfcounter->inc(l_librbd_snap_rollback);
  return r;
}

struct CopyProgressCtx {
  CopyProgressCtx(ProgressContext &p)
	: prog_ctx(p)
  {
  }
  ImageCtx *destictx;
  uint64_t src_size;
  ProgressContext &prog_ctx;
};

int do_copy_extent(uint64_t offset, size_t len, const char *buf, void *data)
{
  CopyProgressCtx *cp = reinterpret_cast<CopyProgressCtx*>(data);
  cp->prog_ctx.update_progress(offset, cp->src_size);
  int ret = 0;
  if (buf) {
    ret = write(cp->destictx, offset, len, buf);
  }
  return ret;
}

int copy(ImageCtx& ictx, IoCtx& dest_md_ctx, const char *destname,
	 ProgressContext &prog_ctx)
{
  CephContext *cct = (CephContext *)dest_md_ctx.cct();
  CopyProgressCtx cp(prog_ctx);
  uint64_t src_size = ictx.get_image_size();
  int64_t r;

  int order = ictx.header.options.order;
  r = create(dest_md_ctx, destname, src_size, &order);
  if (r < 0) {
    lderr(cct) << "header creation failed" << dendl;
    return r;
  }

  cp.destictx = new librbd::ImageCtx(destname, NULL, dest_md_ctx);
  cp.src_size = src_size;
  r = open_image(cp.destictx);
  if (r < 0) {
    lderr(cct) << "failed to read newly created header" << dendl;
    return r;
  }

  r = read_iterate(&ictx, 0, src_size, do_copy_extent, &cp);

  if (r >= 0) {
    // don't return total bytes read, which may not fit in an int
    r = 0;
    prog_ctx.update_progress(cp.src_size, cp.src_size);
  }
  close_image(cp.destictx);
  return r;
}

int snap_set(ImageCtx *ictx, const char *snap_name)
{
  ldout(ictx->cct, 20) << "snap_set " << ictx << " snap = "
		       << (snap_name ? snap_name : "NULL") << dendl;

  Mutex::Locker l(ictx->lock);
  if (snap_name) {
    int r = ictx->snap_set(snap_name);
    if (r < 0) {
      return r;
    }
  } else {
    ictx->snap_unset();
  }

  ictx->snap_exists = true;
  ictx->data_ctx.snap_set_read(ictx->snapid);

  return 0;
}

int open_image(ImageCtx *ictx)
{
  ldout(ictx->cct, 20) << "open_image: ictx =  " << ictx
		       << " name =  '" << ictx->name << "' snap_name = '"
		       << ictx->snapname << "'" << dendl;

  ictx->lock.Lock();
  int r = ictx_refresh(ictx);
  ictx->lock.Unlock();
  if (r < 0)
    return r;

  ictx->snap_set(ictx->snapname);
  ictx->data_ctx.snap_set_read(ictx->snapid);

  WatchCtx *wctx = new WatchCtx(ictx);
  if (!wctx)
    return -ENOMEM;
  ictx->wctx = wctx;

  r = ictx->md_ctx.watch(ictx->md_oid(), 0, &(wctx->cookie), wctx);
  return r;
}

void close_image(ImageCtx *ictx)
{
  ldout(ictx->cct, 20) << "close_image " << ictx << dendl;
  if (ictx->object_cacher)
    ictx->shutdown_cache(); // implicitly flushes
  else
    flush(ictx);
  ictx->lock.Lock();
  ictx->wctx->invalidate();
  ictx->md_ctx.unwatch(ictx->md_oid(), ictx->wctx->cookie);
  delete ictx->wctx;
  ictx->lock.Unlock();
  delete ictx;
}

int64_t read_iterate(ImageCtx *ictx, uint64_t off, size_t len,
		     int (*cb)(uint64_t, size_t, const char *, void *),
		     void *arg)
{
  ldout(ictx->cct, 20) << "read_iterate " << ictx << " off = " << off << " len = " << len << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  r = check_io(ictx, off, len);
  if (r < 0)
    return r;

  int64_t ret;
  int64_t total_read = 0;
  ictx->lock.Lock();
  uint64_t start_block = get_block_num(ictx->header, off);
  uint64_t end_block = get_block_num(ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(ictx->header);
  ictx->lock.Unlock();
  uint64_t left = len;

  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    ictx->lock.Lock();
    string oid = get_block_oid(ictx->header, i);
    uint64_t block_ofs = get_block_ofs(ictx->header, off + total_read);
    ictx->lock.Unlock();
    uint64_t read_len = min(block_size - block_ofs, left);
    uint64_t bytes_read;

    if (ictx->object_cacher) {
      r = ictx->read_from_cache(oid, &bl, read_len, block_ofs);
      if (r < 0 && r != -ENOENT)
	return r;

      if (r == -ENOENT)
	r = cb(total_read, read_len, NULL, arg);
      else
	r = cb(total_read, read_len, bl.c_str(), arg);

      bytes_read = read_len; // ObjectCacher pads with zeroes at end of object
    } else {
      map<uint64_t, uint64_t> m;
      r = ictx->data_ctx.sparse_read(oid, m, bl, read_len, block_ofs);
      if (r < 0 && r == -ENOENT)
	r = 0;
      if (r < 0) {
	return r;
      }

      r = handle_sparse_read(ictx->cct, bl, block_ofs, m, total_read, read_len, cb, arg);
      bytes_read = r;
    }
    if (r < 0) {
      return r;
    }

    total_read += bytes_read;
    left -= bytes_read;
  }
  ret = total_read;

  ictx->perfcounter->inc(l_librbd_rd);
  ictx->perfcounter->inc(l_librbd_rd_bytes, len);
  return ret;
}

static int simple_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg)
{
  char *dest_buf = (char *)arg;
  if (buf)
    memcpy(dest_buf + ofs, buf, len);
  else
    memset(dest_buf + ofs, 0, len);

  return 0;
}


ssize_t read(ImageCtx *ictx, uint64_t ofs, size_t len, char *buf)
{
  return read_iterate(ictx, ofs, len, simple_read_cb, buf);
}

ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf)
{
  ldout(ictx->cct, 20) << "write " << ictx << " off = " << off << " len = " << len << dendl;

  if (!len)
    return 0;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  r = check_io(ictx, off, len);
  if (r < 0)
    return r;

  size_t total_write = 0;
  ictx->lock.Lock();
  uint64_t start_block = get_block_num(ictx->header, off);
  uint64_t end_block = get_block_num(ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(ictx->header);
  snapid_t snap = ictx->snapid;
  ictx->lock.Unlock();
  uint64_t left = len;

  if (snap != CEPH_NOSNAP)
    return -EROFS;

  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    ictx->lock.Lock();
    string oid = get_block_oid(ictx->header, i);
    uint64_t block_ofs = get_block_ofs(ictx->header, off + total_write);
    ictx->lock.Unlock();
    uint64_t write_len = min(block_size - block_ofs, left);
    bl.append(buf + total_write, write_len);
    if (ictx->object_cacher) {
      ictx->write_to_cache(oid, bl, write_len, block_ofs);
    } else {
      r = ictx->data_ctx.write(oid, bl, write_len, block_ofs);
      if (r < 0)
	return r;
      if ((uint64_t)r != write_len)
	return -EIO;
    }
    total_write += write_len;
    left -= write_len;
  }

  ictx->perfcounter->inc(l_librbd_wr);
  ictx->perfcounter->inc(l_librbd_wr_bytes, total_write);
  return total_write;
}

int discard(ImageCtx *ictx, uint64_t off, uint64_t len)
{
  ldout(ictx->cct, 20) << "discard " << ictx << " off = " << off << " len = " << len << dendl;

  if (!len)
    return 0;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  r = check_io(ictx, off, len);
  if (r < 0)
    return r;

  size_t total_write = 0;
  ictx->lock.Lock();
  uint64_t start_block = get_block_num(ictx->header, off);
  uint64_t end_block = get_block_num(ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(ictx->header);
  ictx->lock.Unlock();
  uint64_t left = len;

  vector<ObjectExtent> v;
  if (ictx->object_cacher)
    v.reserve(end_block - start_block + 1);

  for (uint64_t i = start_block; i <= end_block; i++) {
    ictx->lock.Lock();
    string oid = get_block_oid(ictx->header, i);
    uint64_t block_ofs = get_block_ofs(ictx->header, off + total_write);
    ictx->lock.Unlock();
    uint64_t write_len = min(block_size - block_ofs, left);

    if (ictx->object_cacher) {
      v.push_back(ObjectExtent(oid, block_ofs, write_len));
      v.back().oloc.pool = ictx->data_ctx.get_id();
    }

    librados::ObjectWriteOperation write_op;
    if (block_ofs == 0 && write_len == block_size)
      write_op.remove();
    else if (write_len + block_ofs == block_size)
      write_op.truncate(block_ofs);
    else
      write_op.zero(block_ofs, write_len);
    r = ictx->data_ctx.operate(oid, &write_op);
    if (r < 0)
      return r;
    total_write += write_len;
    left -= write_len;
  }

  if (ictx->object_cacher)
    ictx->object_cacher->discard_set(ictx->object_set, v);

  ictx->perfcounter->inc(l_librbd_discard);
  ictx->perfcounter->inc(l_librbd_discard_bytes, total_write);
  return total_write;
}

ssize_t handle_sparse_read(CephContext *cct,
			   bufferlist data_bl,
			   uint64_t block_ofs,
			   const map<uint64_t, uint64_t> &data_map,
			   uint64_t buf_ofs,   // offset into buffer
			   size_t buf_len,     // length in buffer (not size of buffer!)
			   int (*cb)(uint64_t, size_t, const char *, void *),
			   void *arg)
{
  int r;
  uint64_t bl_ofs = 0;
  size_t buf_left = buf_len;

  for (map<uint64_t, uint64_t>::const_iterator iter = data_map.begin();
       iter != data_map.end();
       ++iter) {
    uint64_t extent_ofs = iter->first;
    size_t extent_len = iter->second;

    ldout(cct, 10) << "extent_ofs=" << extent_ofs
		   << " extent_len=" << extent_len << dendl;
    ldout(cct, 10) << "block_ofs=" << block_ofs << dendl;

    /* a hole? */
    if (extent_ofs > block_ofs) {
      uint64_t gap = extent_ofs - block_ofs;
      ldout(cct, 10) << "<1>zeroing " << buf_ofs << "~" << gap << dendl;
      r = cb(buf_ofs, gap, NULL, arg);
      if (r < 0) {
	return r;
      }
      buf_ofs += gap;
      buf_left -= gap;
      block_ofs = extent_ofs;
    } else if (extent_ofs < block_ofs) {
      assert(0 == "osd returned data prior to what we asked for");
      return -EIO;
    }

    if (bl_ofs + extent_len > (buf_ofs + buf_left)) {
      assert(0 == "osd returned more data than we asked for");
      return -EIO;
    }

    /* data */
    ldout(cct, 10) << "<2>copying " << buf_ofs << "~" << extent_len
		   << " from ofs=" << bl_ofs << dendl;
    r = cb(buf_ofs, extent_len, data_bl.c_str() + bl_ofs, arg);
    if (r < 0) {
      return r;
    }
    bl_ofs += extent_len;
    buf_ofs += extent_len;
    assert(buf_left >= extent_len);
    buf_left -= extent_len;
    block_ofs += extent_len;
  }

  /* last hole */
  if (buf_left > 0) {
    ldout(cct, 0) << "<3>zeroing " << buf_ofs << "~" << buf_left << dendl;
    r = cb(buf_ofs, buf_left, NULL, arg);
    if (r < 0) {
      return r;
    }
  }

  return buf_len;
}

void AioBlockCompletion::finish(int r)
{
  ldout(cct, 10) << "AioBlockCompletion::finish()" << dendl;
  if ((r >= 0 || r == -ENOENT) && buf) { // this was a sparse_read operation
    ldout(cct, 10) << "ofs=" << ofs << " len=" << len << dendl;
    r = handle_sparse_read(cct, data_bl, ofs, m, 0, len, simple_read_cb, buf);
  }
  completion->complete_block(this, r);
}

void AioCompletion::complete_block(AioBlockCompletion *block_completion, ssize_t r)
{
  CephContext *cct = block_completion->cct;
  ldout(cct, 20) << "AioCompletion::complete_block() this=" 
	         << (void *)this << " complete_cb=" << (void *)complete_cb << dendl;
  lock.Lock();
  if (rval >= 0) {
    if (r < 0 && r != -EEXIST)
      rval = r;
    else if (r > 0)
      rval += r;
  }
  assert(pending_count);
  int count = --pending_count;
  if (!count) {
    complete();
  }
  put_unlock();
}

void rados_cb(rados_completion_t c, void *arg)
{
  AioBlockCompletion *block_completion = (AioBlockCompletion *)arg;
  block_completion->finish(rados_aio_get_return_value(c));
  delete block_completion;
}

int check_io(ImageCtx *ictx, uint64_t off, uint64_t len)
{
  ictx->lock.Lock();
  uint64_t image_size = ictx->get_image_size();
  bool snap_exists = ictx->snap_exists;
  ictx->lock.Unlock();

  if (!snap_exists)
    return -ENOENT;

  if ((uint64_t)(off + len) > image_size)
    return -EINVAL;
  return 0;
}

int flush(ImageCtx *ictx)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "flush " << ictx << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  return _flush(ictx);
}

int _flush(ImageCtx *ictx)
{
  CephContext *cct = ictx->cct;
  int r;
  // flush any outstanding writes
  if (ictx->object_cacher) {
    ictx->flush_cache();
    r = 0;
  } else {
    r = ictx->data_ctx.aio_flush();
  }

  if (r)
    ldout(cct, 10) << "aio_flush " << ictx << " r = " << r << dendl;

  return r;
}

int aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
			         AioCompletion *c)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "aio_write " << ictx << " off = " << off << " len = " << len << dendl;

  if (!len)
    return 0;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  size_t total_write = 0;
  ictx->lock.Lock();
  uint64_t start_block = get_block_num(ictx->header, off);
  uint64_t end_block = get_block_num(ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(ictx->header);
  snapid_t snap = ictx->snapid;
  ictx->lock.Unlock();
  uint64_t left = len;

  r = check_io(ictx, off, len);
  if (r < 0)
    return r;

  if (snap != CEPH_NOSNAP)
    return -EROFS;

  c->get();
  for (uint64_t i = start_block; i <= end_block; i++) {
    ictx->lock.Lock();
    string oid = get_block_oid(ictx->header, i);
    uint64_t block_ofs = get_block_ofs(ictx->header, off + total_write);
    ictx->lock.Unlock();

    uint64_t write_len = min(block_size - block_ofs, left);
    bufferlist bl;
    bl.append(buf + total_write, write_len);
    if (ictx->object_cacher) {
      // may block
      ictx->write_to_cache(oid, bl, write_len, block_ofs);
    } else {
      AioBlockCompletion *block_completion = new AioBlockCompletion(cct, c, off, len, NULL);
      c->add_block_completion(block_completion);
      librados::AioCompletion *rados_completion =
	Rados::aio_create_completion(block_completion, NULL, rados_cb);
      r = ictx->data_ctx.aio_write(oid, rados_completion, bl, write_len, block_ofs);
      rados_completion->release();
      if (r < 0)
	goto done;
    }
    total_write += write_len;
    left -= write_len;
  }
done:
  c->finish_adding_completions();
  c->put();

  ictx->perfcounter->inc(l_librbd_aio_wr);
  ictx->perfcounter->inc(l_librbd_aio_wr_bytes, len);

  /* FIXME: cleanup all the allocated stuff */
  return r;
}

int aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "aio_discard " << ictx << " off = " << off << " len = " << len << dendl;

  if (!len)
    return 0;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  size_t total_write = 0;
  ictx->lock.Lock();
  uint64_t start_block = get_block_num(ictx->header, off);
  uint64_t end_block = get_block_num(ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(ictx->header);
  ictx->lock.Unlock();
  uint64_t left = len;

  r = check_io(ictx, off, len);
  if (r < 0)
    return r;

  vector<ObjectExtent> v;
  if (ictx->object_cacher)
    v.reserve(end_block - start_block + 1);

  c->get();
  for (uint64_t i = start_block; i <= end_block; i++) {
    ictx->lock.Lock();
    string oid = get_block_oid(ictx->header, i);
    uint64_t block_ofs = get_block_ofs(ictx->header, off + total_write);
    ictx->lock.Unlock();

    AioBlockCompletion *block_completion = new AioBlockCompletion(cct, c, off, len, NULL);

    uint64_t write_len = min(block_size - block_ofs, left);

    if (ictx->object_cacher) {
      v.push_back(ObjectExtent(oid, block_ofs, write_len));
      v.back().oloc.pool = ictx->data_ctx.get_id();
    }

    if (block_ofs == 0 && write_len == block_size)
      block_completion->write_op.remove();
    else if (block_ofs + write_len == block_size)
      block_completion->write_op.truncate(block_ofs);
    else
      block_completion->write_op.zero(block_ofs, write_len);

    c->add_block_completion(block_completion);
    librados::AioCompletion *rados_completion =
      Rados::aio_create_completion(block_completion, NULL, rados_cb);

    r = ictx->data_ctx.aio_operate(oid, rados_completion, &block_completion->write_op);
    rados_completion->release();
    if (r < 0)
      goto done;
    total_write += write_len;
    left -= write_len;
  }
  r = 0;
done:
  if (ictx->object_cacher)
    ictx->object_cacher->discard_set(ictx->object_set, v);

  c->finish_adding_completions();
  c->put();

  ictx->perfcounter->inc(l_librbd_aio_discard);
  ictx->perfcounter->inc(l_librbd_aio_discard_bytes, len);

  /* FIXME: cleanup all the allocated stuff */
  return r;
}

void rados_aio_sparse_read_cb(rados_completion_t c, void *arg)
{
  AioBlockCompletion *block_completion = (AioBlockCompletion *)arg;
  block_completion->finish(rados_aio_get_return_value(c));
  delete block_completion;
}

int aio_read(ImageCtx *ictx, uint64_t off, size_t len,
				char *buf,
                                AioCompletion *c)
{
  ldout(ictx->cct, 20) << "aio_read " << ictx << " off = " << off << " len = " << len << dendl;

  int r = ictx_check(ictx);
  if (r < 0)
    return r;

  r = check_io(ictx, off, len);
  if (r < 0)
    return r;

  int64_t ret;
  int total_read = 0;
  ictx->lock.Lock();
  uint64_t start_block = get_block_num(ictx->header, off);
  uint64_t end_block = get_block_num(ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(ictx->header);
  ictx->lock.Unlock();
  uint64_t left = len;

  c->get();
  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    ictx->lock.Lock();
    string oid = get_block_oid(ictx->header, i);
    uint64_t block_ofs = get_block_ofs(ictx->header, off + total_read);
    ictx->lock.Unlock();
    uint64_t read_len = min(block_size - block_ofs, left);

    map<uint64_t,uint64_t> m;
    map<uint64_t,uint64_t>::iterator iter;

    AioBlockCompletion *block_completion =
	new AioBlockCompletion(ictx->cct, c, block_ofs, read_len, buf + total_read);
    c->add_block_completion(block_completion);

    if (ictx->object_cacher) {
      block_completion->m[block_ofs] = read_len;
      ictx->aio_read_from_cache(oid, &block_completion->data_bl,
				read_len, block_ofs, block_completion);
    } else {
      librados::AioCompletion *rados_completion =
	Rados::aio_create_completion(block_completion, rados_aio_sparse_read_cb, NULL);
      r = ictx->data_ctx.aio_sparse_read(oid, rados_completion,
					 &block_completion->m, &block_completion->data_bl,
					 read_len, block_ofs);
      rados_completion->release();
      if (r < 0 && r == -ENOENT)
	r = 0;
      if (r < 0) {
	ret = r;
	goto done;
      }
    }

    total_read += read_len;
    left -= read_len;
  }
  ret = total_read;
done:
  c->finish_adding_completions();
  c->put();

  ictx->perfcounter->inc(l_librbd_aio_rd);
  ictx->perfcounter->inc(l_librbd_aio_rd_bytes, len);

  return ret;
}

/*
   RBD
*/
RBD::RBD()
{
}

RBD::~RBD()
{
}

void RBD::version(int *major, int *minor, int *extra)
{
  rbd_version(major, minor, extra);
}

int RBD::open(IoCtx& io_ctx, Image& image, const char *name)
{
  return open(io_ctx, image, name, NULL);
}

int RBD::open(IoCtx& io_ctx, Image& image, const char *name, const char *snapname)
{
  ImageCtx *ictx = new ImageCtx(name, snapname, io_ctx);
  if (!ictx)
    return -ENOMEM;

  int r = librbd::open_image(ictx);
  if (r < 0)
    return r;

  image.ctx = (image_ctx_t) ictx;
  return 0;
}

int RBD::create(IoCtx& io_ctx, const char *name, uint64_t size, int *order)
{
  int r = librbd::create(io_ctx, name, size, order);
  return r;
}

int RBD::remove(IoCtx& io_ctx, const char *name)
{
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::remove(io_ctx, name, prog_ctx);
  return r;
}

int RBD::remove_with_progress(IoCtx& io_ctx, const char *name, ProgressContext& pctx)
{
  int r = librbd::remove(io_ctx, name, pctx);
  return r;
}

int RBD::list(IoCtx& io_ctx, std::vector<std::string>& names)
{
  int r = librbd::list(io_ctx, names);
  return r;
}

int RBD::rename(IoCtx& src_io_ctx, const char *srcname, const char *destname)
{
  int r = librbd::rename(src_io_ctx, srcname, destname);
  return r;
}

RBD::AioCompletion::AioCompletion(void *cb_arg, callback_t complete_cb)
{
  librbd::AioCompletion *c = librbd::aio_create_completion(cb_arg, complete_cb);
  pc = (void *)c;
  c->rbd_comp = this;
}

int RBD::AioCompletion::wait_for_complete()
{
  librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
  return c->wait_for_complete();
}

ssize_t RBD::AioCompletion::get_return_value()
{
  librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
  return c->get_return_value();
}

void RBD::AioCompletion::release()
{
  librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
  c->release();
  delete this;
}

/*
  Image
*/

Image::Image() : ctx(NULL)
{
}

Image::~Image()
{
  if (ctx) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    close_image(ictx);
  }
}

int Image::resize(uint64_t size)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::resize(ictx, size, prog_ctx);
  return r;
}

int Image::resize_with_progress(uint64_t size, librbd::ProgressContext& pctx)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  int r = librbd::resize(ictx, size, pctx);
  return r;
}

int Image::stat(image_info_t& info, size_t infosize)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  int r = librbd::info(ictx, info, infosize);
  return r;
}

int Image::copy(IoCtx& dest_io_ctx, const char *destname)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(*ictx, dest_io_ctx, destname, prog_ctx);
  return r;
}

int Image::copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
			      librbd::ProgressContext &pctx)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  int r = librbd::copy(*ictx, dest_io_ctx, destname, pctx);
  return r;
}

int Image::snap_create(const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  int r = librbd::snap_create(ictx, snap_name);
  return r;
}

int Image::snap_remove(const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  int r = librbd::snap_remove(ictx, snap_name);
  return r;
}

int Image::snap_rollback(const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::snap_rollback(ictx, snap_name, prog_ctx);
  return r;
}

int Image::snap_rollback_with_progress(const char *snap_name, ProgressContext& prog_ctx)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  int r = librbd::snap_rollback(ictx, snap_name, prog_ctx);
  return r;
}

int Image::snap_list(std::vector<librbd::snap_info_t>& snaps)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  return librbd::snap_list(ictx, snaps);
}

int Image::snap_set(const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  return librbd::snap_set(ictx, snap_name);
}

ssize_t Image::read(uint64_t ofs, size_t len, bufferlist& bl)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  bufferptr ptr(len);
  bl.push_back(ptr);
  return librbd::read(ictx, ofs, len, bl.c_str());
}

int64_t Image::read_iterate(uint64_t ofs, size_t len,
			    int (*cb)(uint64_t, size_t, const char *, void *), void *arg)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  return librbd::read_iterate(ictx, ofs, len, cb, arg);
}

ssize_t Image::write(uint64_t ofs, size_t len, bufferlist& bl)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  if (bl.length() < len)
    return -EINVAL;
  return librbd::write(ictx, ofs, len, bl.c_str());
}

int Image::discard(uint64_t ofs, uint64_t len)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  return librbd::discard(ictx, ofs, len);
}

int Image::aio_write(uint64_t off, size_t len, bufferlist& bl, RBD::AioCompletion *c)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  if (bl.length() < len)
    return -EINVAL;
  return librbd::aio_write(ictx, off, len, bl.c_str(), (librbd::AioCompletion *)c->pc);
}

int Image::aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  return librbd::aio_discard(ictx, off, len, (librbd::AioCompletion *)c->pc);
}

int Image::aio_read(uint64_t off, size_t len, bufferlist& bl, RBD::AioCompletion *c)
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  bufferptr ptr(len);
  bl.push_back(ptr);
  ldout(ictx->cct, 10) << "Image::aio_read() buf=" << (void *)bl.c_str() << "~" << (void *)(bl.c_str() + len - 1) << dendl;
  return librbd::aio_read(ictx, off, len, bl.c_str(), (librbd::AioCompletion *)c->pc);
}

int Image::flush()
{
  ImageCtx *ictx = (ImageCtx *)ctx;
  return librbd::flush(ictx);
}

} // namespace librbd

extern "C" void rbd_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRBD_VER_MAJOR;
  if (minor)
    *minor = LIBRBD_VER_MINOR;
  if (extra)
    *extra = LIBRBD_VER_EXTRA;
}

/* images */
extern "C" int rbd_list(rados_ioctx_t p, char *names, size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  std::vector<std::string> cpp_names;
  int r = librbd::list(io_ctx, cpp_names);
  if (r == -ENOENT)
    return 0;

  if (r < 0)
    return r;

  size_t expected_size = 0;

  for (size_t i = 0; i < cpp_names.size(); i++) {
    expected_size += cpp_names[i].size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    return -ERANGE;
  }

  for (int i = 0; i < (int)cpp_names.size(); i++) {
    strcpy(names, cpp_names[i].c_str());
    names += strlen(names) + 1;
  }
  return (int)expected_size;
}

extern "C" int rbd_create(rados_ioctx_t p, const char *name, uint64_t size, int *order)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::create(io_ctx, name, size, order);
}

extern "C" int rbd_remove(rados_ioctx_t p, const char *name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::NoOpProgressContext prog_ctx;
  return librbd::remove(io_ctx, name, prog_ctx);
}

extern "C" int rbd_remove_with_progress(rados_ioctx_t p, const char *name,
					librbd_progress_fn_t cb, void *cbdata)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::remove(io_ctx, name, prog_ctx);
}

extern "C" int rbd_copy(rbd_image_t image, rados_ioctx_t dest_p, const char *destname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  librbd::NoOpProgressContext prog_ctx;
  return librbd::copy(*ictx, dest_io_ctx, destname, prog_ctx);
}

extern "C" int rbd_copy_with_progress(rbd_image_t image, rados_ioctx_t dest_p,
	      const char *destname, librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(*ictx, dest_io_ctx, destname, prog_ctx);
  return ret;
}

extern "C" int rbd_rename(rados_ioctx_t src_p, const char *srcname, const char *destname)
{
  librados::IoCtx src_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(src_p, src_io_ctx);
  return librbd::rename(src_io_ctx, srcname, destname);
}

extern "C" int rbd_open(rados_ioctx_t p, const char *name, rbd_image_t *image, const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, snap_name, io_ctx);
  if (!ictx)
    return -ENOMEM;
  int r = librbd::open_image(ictx);
  *image = (rbd_image_t)ictx;
  return r;
}

extern "C" int rbd_close(rbd_image_t image)
{
  librbd::ImageCtx *ctx = (librbd::ImageCtx *)image;
  librbd::close_image(ctx);
  return 0; 
}

extern "C" int rbd_resize(rbd_image_t image, uint64_t size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::NoOpProgressContext prog_ctx;
  return librbd::resize(ictx, size, prog_ctx);
}

extern "C" int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
					librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::resize(ictx, size, prog_ctx);
}

extern "C" int rbd_stat(rbd_image_t image, rbd_image_info_t *info, size_t infosize)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::info(ictx, *info, infosize);
}

/* snapshots */
extern "C" int rbd_snap_create(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::snap_create(ictx, snap_name);
}

extern "C" int rbd_snap_remove(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::snap_remove(ictx, snap_name);
}

extern "C" int rbd_snap_rollback(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::NoOpProgressContext prog_ctx;
  return librbd::snap_rollback(ictx, snap_name, prog_ctx);
}

extern "C" int rbd_snap_rollback_with_progress(rbd_image_t image, const char *snap_name,
					       librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::snap_rollback(ictx, snap_name, prog_ctx);
}

extern "C" int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps, int *max_snaps)
{
  std::vector<librbd::snap_info_t> cpp_snaps;
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  int r = librbd::snap_list(ictx, cpp_snaps);
  if (r == -ENOENT)
    return 0;
  if (r < 0)
    return r;
  if (!max_snaps)
    return -EINVAL;
  if (*max_snaps < (int)cpp_snaps.size() + 1) {
    *max_snaps = (int)cpp_snaps.size() + 1;
    return -ERANGE;
  }

  int i;

  for (i = 0; i < (int)cpp_snaps.size(); i++) {
    snaps[i].id = cpp_snaps[i].id;
    snaps[i].size = cpp_snaps[i].size;
    snaps[i].name = strdup(cpp_snaps[i].name.c_str());
    if (!snaps[i].name) {
      for (int j = 0; j < i; j++)
	free((void *)snaps[j].name);
      return -ENOMEM;
    }
  }
  snaps[i].id = 0;
  snaps[i].size = 0;
  snaps[i].name = NULL;

  return (int)cpp_snaps.size();
}

extern "C" void rbd_snap_list_end(rbd_snap_info_t *snaps)
{
  while (snaps->name) {
    free((void *)snaps->name);
    snaps++;
  }
}

extern "C" int rbd_snap_set(rbd_image_t image, const char *snapname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::snap_set(ictx, snapname);
}

/* I/O */
extern "C" ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len, char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::read(ictx, ofs, len, buf);
}

extern "C" int64_t rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
				    int (*cb)(uint64_t, size_t, const char *, void *), void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::read_iterate(ictx, ofs, len, cb, arg);
}

extern "C" ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len, const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::write(ictx, ofs, len, buf);
}

extern "C" int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::discard(ictx, ofs, len);
}

extern "C" int rbd_aio_create_completion(void *cb_arg, rbd_callback_t complete_cb, rbd_completion_t *c)
{
  librbd::RBD::AioCompletion *rbd_comp = new librbd::RBD::AioCompletion(cb_arg, complete_cb);
  *c = (rbd_completion_t) rbd_comp;
  return 0;
}

extern "C" int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len, const char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_write(ictx, off, len, buf, (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_discard(ictx, off, len, (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len, char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_read(ictx, off, len, buf, (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_flush(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::flush(ictx);
}

extern "C" int rbd_aio_wait_for_complete(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->wait_for_complete();
}

extern "C" ssize_t rbd_aio_get_return_value(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->get_return_value();
}

extern "C" void rbd_aio_release(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  comp->release();
}
