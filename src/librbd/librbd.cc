// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */
#include "include/int_types.h"

#include <errno.h>

#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/snap_types.h"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "osdc/ObjectCacher.h"

#include "librbd/AioCompletion.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/LibrbdWriteback.h"

#include <algorithm>
#include <string>
#include <vector>

#ifdef WITH_LTTNG
#include "tracing/librbd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

using std::string;
using std::vector;

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;

namespace {

class C_AioReadWQ : public Context {
public:
  C_AioReadWQ(librbd::ImageCtx *ictx, uint64_t off, size_t len,
              char *buf, bufferlist *pbl, librbd::AioCompletion *c,
              int op_flags)
    : m_ictx(ictx), m_off(off), m_len(len), m_buf(buf), m_pbl(pbl), m_comp(c),
      m_op_flags(op_flags) {
  }
protected:
  virtual void finish(int r) {
    librbd::aio_read(m_ictx, m_off, m_len, m_buf, m_pbl, m_comp, m_op_flags);
  }
private:
  librbd::ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  char *m_buf;
  bufferlist *m_pbl;
  librbd::AioCompletion *m_comp;
  int m_op_flags;
};

class C_AioWriteWQ : public Context {
public:
  C_AioWriteWQ(librbd::ImageCtx *ictx, uint64_t off, size_t len,
               const char *buf, librbd::AioCompletion *c, int op_flags)
    : m_ictx(ictx), m_off(off), m_len(len), m_buf(buf), m_comp(c),
      m_op_flags(op_flags) {
  }
protected:
  virtual void finish(int r) {
    librbd::aio_write(m_ictx, m_off, m_len, m_buf, m_comp, m_op_flags);
  }
private:
  librbd::ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  const char *m_buf;
  librbd::AioCompletion *m_comp;
  int m_op_flags;
};

class C_AioDiscardWQ : public Context {
public:
  C_AioDiscardWQ(librbd::ImageCtx *ictx, uint64_t off, uint64_t len,
                 librbd::AioCompletion *c)
    : m_ictx(ictx), m_off(off), m_len(len), m_comp(c) {
  }
protected:
  virtual void finish(int r) {
    librbd::aio_discard(m_ictx, m_off, m_len, m_comp);
  }
private:
  librbd::ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  librbd::AioCompletion *m_comp;
};

class C_AioFlushWQ : public Context {
public:
  C_AioFlushWQ(librbd::ImageCtx *ictx, librbd::AioCompletion *c)
    : m_ictx(ictx), m_comp(c) {
  }
protected:
  virtual void finish(int r) {
    librbd::aio_flush(m_ictx, m_comp);
  }
private:
  librbd::ImageCtx *m_ictx;
  librbd::AioCompletion *m_comp;
};

void submit_aio_read(librbd::ImageCtx *ictx, uint64_t off, size_t len,
                     char *buf, bufferlist *pbl, librbd::AioCompletion *c,
                     int op_flags) {
  if (ictx->cct->_conf->rbd_non_blocking_aio) {
    ictx->aio_work_queue->queue(new C_AioReadWQ(ictx, off, len, buf, pbl, c,
                                                op_flags));
  } else {
    librbd::aio_read(ictx, off, len, buf, pbl, c, op_flags);
  }
}

void submit_aio_write(librbd::ImageCtx *ictx, uint64_t off, size_t len,
                      const char *buf, librbd::AioCompletion *c, int op_flags) {
  if (ictx->cct->_conf->rbd_non_blocking_aio) {
    ictx->aio_work_queue->queue(new C_AioWriteWQ(ictx, off, len, buf, c,
                                                 op_flags));
  } else {
    librbd::aio_write(ictx, off, len, buf, c, op_flags);
  }
}

void submit_aio_discard(librbd::ImageCtx *ictx, uint64_t off, uint64_t len,
                        librbd::AioCompletion *c) {
  if (ictx->cct->_conf->rbd_non_blocking_aio) {
    ictx->aio_work_queue->queue(new C_AioDiscardWQ(ictx, off, len, c));
  } else {
    librbd::aio_discard(ictx, off, len, c);
  }
}

void submit_aio_flush(librbd::ImageCtx *ictx, librbd::AioCompletion *c) {
  if (ictx->cct->_conf->rbd_non_blocking_aio) {
    ictx->aio_work_queue->queue(new C_AioFlushWQ(ictx, c));
  } else {
    librbd::aio_flush(ictx, c);
  }
}

librbd::AioCompletion* get_aio_completion(librbd::RBD::AioCompletion *comp) {
  return reinterpret_cast<librbd::AioCompletion *>(comp->pc);
}

} // anonymous namespace

namespace librbd {
  ProgressContext::~ProgressContext()
  {
  }

  class CProgressContext : public ProgressContext
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

  int RBD::open(IoCtx& io_ctx, Image& image, const char *name,
		const char *snap_name)
  {
    ImageCtx *ictx = new ImageCtx(name, "", snap_name, io_ctx, false);
    tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

    int r = librbd::open_image(ictx);
    if (r < 0) {
      tracepoint(librbd, open_image_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_exit, 0);
    return 0;
  }

  int RBD::open_read_only(IoCtx& io_ctx, Image& image, const char *name,
			  const char *snap_name)
  {
    ImageCtx *ictx = new ImageCtx(name, "", snap_name, io_ctx, true);
    tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

    int r = librbd::open_image(ictx);
    if (r < 0) {
      tracepoint(librbd, open_image_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_exit, 0);
    return 0;
  }

  int RBD::create(IoCtx& io_ctx, const char *name, uint64_t size, int *order)
  {
    tracepoint(librbd, create_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, *order);
    int r = librbd::create(io_ctx, name, size, order);
    tracepoint(librbd, create_exit, r, *order);
    return r;
  }

  int RBD::create2(IoCtx& io_ctx, const char *name, uint64_t size,
		   uint64_t features, int *order)
  {
    tracepoint(librbd, create2_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order);
    int r = librbd::create(io_ctx, name, size, false, features, order, 0, 0);
    tracepoint(librbd, create2_exit, r, *order);
    return r;
  }

  int RBD::create3(IoCtx& io_ctx, const char *name, uint64_t size,
		   uint64_t features, int *order, uint64_t stripe_unit,
		   uint64_t stripe_count)
  {
    tracepoint(librbd, create3_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order, stripe_unit, stripe_count);
    int r = librbd::create(io_ctx, name, size, false, features, order,
			  stripe_unit, stripe_count);
    tracepoint(librbd, create3_exit, r, *order);
    return r;
  }

  int RBD::clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
		 IoCtx& c_ioctx, const char *c_name, uint64_t features,
		 int *c_order)
  {
    tracepoint(librbd, clone_enter, p_ioctx.get_pool_name().c_str(), p_ioctx.get_id(), p_name, p_snap_name, c_ioctx.get_pool_name().c_str(), c_ioctx.get_id(), c_name, features);
    int r = librbd::clone(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
			 features, c_order, 0, 0);
    tracepoint(librbd, clone_exit, r, *c_order);
    return r;
  }

  int RBD::clone2(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
		  IoCtx& c_ioctx, const char *c_name, uint64_t features,
		  int *c_order, uint64_t stripe_unit, int stripe_count)
  {
    tracepoint(librbd, clone2_enter, p_ioctx.get_pool_name().c_str(), p_ioctx.get_id(), p_name, p_snap_name, c_ioctx.get_pool_name().c_str(), c_ioctx.get_id(), c_name, features, stripe_unit, stripe_count);
    int r = librbd::clone(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
			 features, c_order, stripe_unit, stripe_count);
    tracepoint(librbd, clone_exit, r, *c_order);
    return r;
  }

  int RBD::remove(IoCtx& io_ctx, const char *name)
  {
    tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::remove(io_ctx, name, prog_ctx);
    tracepoint(librbd, remove_exit, r);
    return r;
  }

  int RBD::remove_with_progress(IoCtx& io_ctx, const char *name,
				ProgressContext& pctx)
  {
    tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
    int r = librbd::remove(io_ctx, name, pctx);
    tracepoint(librbd, remove_exit, r);
    return r;
  }

  int RBD::list(IoCtx& io_ctx, vector<string>& names)
  {
    tracepoint(librbd, list_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id());
    int r = librbd::list(io_ctx, names);
    if (r >= 0) {
      for (vector<string>::iterator itr = names.begin(), end = names.end(); itr != end; ++itr) {
	tracepoint(librbd, list_entry, itr->c_str());
      }
    }
    tracepoint(librbd, list_exit, r, r);
    return r;
  }

  int RBD::rename(IoCtx& src_io_ctx, const char *srcname, const char *destname)
  {
    tracepoint(librbd, rename_enter, src_io_ctx.get_pool_name().c_str(), src_io_ctx.get_id(), srcname, destname);
    int r = librbd::rename(src_io_ctx, srcname, destname);
    tracepoint(librbd, rename_exit, r);
    return r;
  }

  RBD::AioCompletion::AioCompletion(void *cb_arg, callback_t complete_cb)
  {
    librbd::AioCompletion *c = librbd::aio_create_completion(cb_arg,
							     complete_cb);
    pc = (void *)c;
    c->rbd_comp = this;
  }

  bool RBD::AioCompletion::is_complete()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    return c->is_complete();
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
      tracepoint(librbd, close_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
      close_image(ictx);
      tracepoint(librbd, close_image_exit);
    }
  }

  int Image::resize(uint64_t size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::resize(ictx, size, prog_ctx);
    tracepoint(librbd, resize_exit, r);
    return r;
  }

  int Image::resize_with_progress(uint64_t size, librbd::ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    int r = librbd::resize(ictx, size, pctx);
    tracepoint(librbd, resize_exit, r);
    return r;
  }

  int Image::stat(image_info_t& info, size_t infosize)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, stat_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::info(ictx, info, infosize);
    tracepoint(librbd, stat_exit, r, &info);
    return r;
  }

  int Image::old_format(uint8_t *old)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_old_format_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::get_old_format(ictx, old);
    tracepoint(librbd, get_old_format_exit, r, *old);
    return r;
  }

  int Image::size(uint64_t *size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_size_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::get_size(ictx, size);
    tracepoint(librbd, get_size_exit, r, *size);
    return r;
  }

  int Image::features(uint64_t *features)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_features_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::get_features(ictx, features);
    tracepoint(librbd, get_features_exit, r, *features);
    return r;
  }

  uint64_t Image::get_stripe_unit() const
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_stripe_unit_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    uint64_t stripe_unit = ictx->get_stripe_unit();
    tracepoint(librbd, get_stripe_unit_exit, 0, stripe_unit);
    return stripe_unit;
  }

  uint64_t Image::get_stripe_count() const
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_stripe_count_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    uint64_t stripe_count = ictx->get_stripe_count();
    tracepoint(librbd, get_stripe_count_exit, 0, stripe_count);
    return stripe_count;
  }

  int Image::overlap(uint64_t *overlap)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_overlap_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::get_overlap(ictx, overlap);
    tracepoint(librbd, get_overlap_exit, r, *overlap);
    return r;
  }

  int Image::parent_info(string *parent_pool_name, string *parent_name,
			 string *parent_snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_parent_info_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::get_parent_info(ictx, parent_pool_name, parent_name,
				   parent_snap_name);
    tracepoint(librbd, get_parent_info_exit, r, parent_pool_name ? parent_pool_name->c_str() : NULL, parent_name ? parent_name->c_str() : NULL, parent_snap_name ? parent_snap_name->c_str() : NULL);
    return r;
  }

  int Image::get_flags(uint64_t *flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_flags_enter, ictx);
    int r = librbd::get_flags(ictx, flags);
    tracepoint(librbd, get_flags_exit, ictx, r, *flags);
    return r;
  }

  int Image::is_exclusive_lock_owner(bool *is_owner)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, is_exclusive_lock_owner_enter, ictx);
    int r = librbd::is_exclusive_lock_owner(ictx, is_owner);
    tracepoint(librbd, is_exclusive_lock_owner_exit, ictx, r, *is_owner);
    return r;
  }

  int Image::copy(IoCtx& dest_io_ctx, const char *destname)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
    tracepoint(librbd, copy_exit, r);
    return r;
  }

  int Image::copy2(Image& dest)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    tracepoint(librbd, copy2_enter, srcctx, srcctx->name.c_str(), srcctx->snap_name.c_str(), srcctx->read_only, destctx, destctx->name.c_str(), destctx->snap_name.c_str(), destctx->read_only);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(srcctx, destctx, prog_ctx);
    tracepoint(librbd, copy2_exit, r);
    return r;
  }

  int Image::copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
				librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
    int r = librbd::copy(ictx, dest_io_ctx, destname, pctx);
    tracepoint(librbd, copy_exit, r);
    return r;
  }

  int Image::copy_with_progress2(Image& dest, librbd::ProgressContext &pctx)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    tracepoint(librbd, copy2_enter, srcctx, srcctx->name.c_str(), srcctx->snap_name.c_str(), srcctx->read_only, destctx, destctx->name.c_str(), destctx->snap_name.c_str(), destctx->read_only);
    int r = librbd::copy(srcctx, destctx, pctx);
    tracepoint(librbd, copy2_exit, r);
    return r;
  }

  int Image::flatten()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::flatten(ictx, prog_ctx);
    tracepoint(librbd, flatten_exit, r);
    return r;
  }

  int Image::flatten_with_progress(librbd::ProgressContext& prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
    int r = librbd::flatten(ictx, prog_ctx);
    tracepoint(librbd, flatten_exit, r);
    return r;
  }

  int Image::list_children(set<pair<string, string> > *children)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, list_children_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::list_children(ictx, *children);
    if (r >= 0) {
      for (set<pair<string, string> >::const_iterator it = children->begin();
	   it != children->end(); ++it) {
	tracepoint(librbd, list_children_entry, it->first.c_str(), it->second.c_str());
      }
    }
    tracepoint(librbd, list_children_exit, r);
    return r;
  }

  int Image::list_lockers(std::list<librbd::locker_t> *lockers,
			  bool *exclusive, string *tag)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, list_lockers_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::list_lockers(ictx, lockers, exclusive, tag);
    if (r >= 0) {
      for (std::list<librbd::locker_t>::const_iterator it = lockers->begin();
	   it != lockers->end(); ++it) {
	tracepoint(librbd, list_lockers_entry, it->client.c_str(), it->cookie.c_str(), it->address.c_str());
      }
    }
    tracepoint(librbd, list_lockers_exit, r);
    return r;
  }

  int Image::lock_exclusive(const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, lock_exclusive_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, cookie.c_str());
    int r = librbd::lock(ictx, true, cookie, "");
    tracepoint(librbd, lock_exclusive_exit, r);
    return r;
  }

  int Image::lock_shared(const string& cookie, const std::string& tag)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, lock_shared_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, cookie.c_str(), tag.c_str());
    int r = librbd::lock(ictx, false, cookie, tag);
    tracepoint(librbd, lock_shared_exit, r);
    return r;
  }

  int Image::unlock(const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, unlock_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, cookie.c_str());
    int r = librbd::unlock(ictx, cookie);
    tracepoint(librbd, unlock_exit, r);
    return r;
  }

  int Image::break_lock(const string& client, const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, break_lock_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, client.c_str(), cookie.c_str());
    int r = librbd::break_lock(ictx, client, cookie);
    tracepoint(librbd, break_lock_exit, r);
    return r;
  }

  int Image::snap_create(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_create_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_create(ictx, snap_name);
    tracepoint(librbd, snap_create_exit, r);
    return r;
  }

  int Image::snap_remove(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_remove_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_remove(ictx, snap_name);
    tracepoint(librbd, snap_remove_exit, r);
    return r;
  }

  int Image::snap_rollback(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::snap_rollback(ictx, snap_name, prog_ctx);
    tracepoint(librbd, snap_rollback_exit, r);
    return r;
  }

  int Image::snap_rollback_with_progress(const char *snap_name,
					 ProgressContext& prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_rollback(ictx, snap_name, prog_ctx);
    tracepoint(librbd, snap_rollback_exit, r);
    return r;
  }

  int Image::snap_protect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_protect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_protect(ictx, snap_name);
    tracepoint(librbd, snap_protect_exit, r);
    return r;
  }

  int Image::snap_unprotect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_unprotect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_unprotect(ictx, snap_name);
    tracepoint(librbd, snap_unprotect_exit, r);
    return r;
  }

  int Image::snap_is_protected(const char *snap_name, bool *is_protected)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_is_protected_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_is_protected(ictx, snap_name, is_protected);
    tracepoint(librbd, snap_is_protected_exit, r, *is_protected ? 1 : 0);
    return r;
  }

  int Image::snap_list(vector<librbd::snap_info_t>& snaps)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_list_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, &snaps);
    int r = librbd::snap_list(ictx, snaps);
    if (r >= 0) {
      for (int i = 0, n = snaps.size(); i < n; i++) {
	tracepoint(librbd, snap_list_entry, snaps[i].id, snaps[i].size, snaps[i].name.c_str());
      }
    }
    tracepoint(librbd, snap_list_exit, r, snaps.size());
    if (r >= 0) {
      // A little ugly, but the C++ API doesn't need a Image::snap_list_end,
      // and we want the tracepoints to mirror the C API
      tracepoint(librbd, snap_list_end_enter, &snaps);
      tracepoint(librbd, snap_list_end_exit);
    }
    return r;
  }

  bool Image::snap_exists(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_exists(ictx, snap_name);
  }

  int Image::snap_set(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_set_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_set(ictx, snap_name);
    tracepoint(librbd, snap_set_exit, r);
    return r;
  }

  ssize_t Image::read(uint64_t ofs, size_t len, bufferlist& bl)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    bufferptr ptr(len);
    bl.push_back(ptr);
    int r = librbd::read(ictx, ofs, len, bl.c_str(), 0);
    tracepoint(librbd, read_exit, r);
    return r;
  }

  ssize_t Image::read2(uint64_t ofs, size_t len, bufferlist& bl, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, read2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
		ictx->read_only, ofs, len, op_flags);
    bufferptr ptr(len);
    bl.push_back(ptr);
    int r = librbd::read(ictx, ofs, len, bl.c_str(), op_flags);
    tracepoint(librbd, read_exit, r);
    return r;
  }

  int64_t Image::read_iterate(uint64_t ofs, size_t len,
			      int (*cb)(uint64_t, size_t, const char *, void *),
			      void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, read_iterate_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
    tracepoint(librbd, read_iterate_exit, r);
    return r;
  }

  int Image::read_iterate2(uint64_t ofs, uint64_t len,
			      int (*cb)(uint64_t, size_t, const char *, void *),
			      void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, read_iterate2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
    if (r > 0)
      r = 0;
    tracepoint(librbd, read_iterate2_exit, r);
    return (int)r;
  }

  int Image::diff_iterate(const char *fromsnapname,
			  uint64_t ofs, uint64_t len,
			  int (*cb)(uint64_t, size_t, int, void *),
			  void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, diff_iterate_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, fromsnapname, ofs, len);
    int r = librbd::diff_iterate(ictx, fromsnapname, ofs, len, cb, arg);
    tracepoint(librbd, diff_iterate_exit, r);
    return r;
  }

  ssize_t Image::write(uint64_t ofs, size_t len, bufferlist& bl)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len, bl.length() < len ? NULL : bl.c_str());
    if (bl.length() < len) {
      tracepoint(librbd, write_exit, -EINVAL);
      return -EINVAL;
    }
    int r = librbd::write(ictx, ofs, len, bl.c_str(), 0);
    tracepoint(librbd, write_exit, r);
    return r;
  }

   ssize_t Image::write2(uint64_t ofs, size_t len, bufferlist& bl, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, write2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only,
		ofs, len, bl.length() < len ? NULL : bl.c_str(), op_flags);
    if (bl.length() < len) {
      tracepoint(librbd, write_exit, -EINVAL);
      return -EINVAL;
    }
    int r = librbd::write(ictx, ofs, len, bl.c_str(), op_flags);
    tracepoint(librbd, write_exit, r);
    return r;
  }

  int Image::discard(uint64_t ofs, uint64_t len)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    int r = librbd::discard(ictx, ofs, len);
    tracepoint(librbd, discard_exit, r);
    return r;
  }

  int Image::aio_write(uint64_t off, size_t len, bufferlist& bl,
		       RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, bl.length() < len ? NULL : bl.c_str(), c->pc);
    if (bl.length() < len) {
      tracepoint(librbd, aio_write_exit, -EINVAL);
      return -EINVAL;
    }
    submit_aio_write(ictx, off, len, bl.c_str(), get_aio_completion(c), 0);
    tracepoint(librbd, aio_write_exit, 0);
    return 0;
  }

  int Image::aio_write2(uint64_t off, size_t len, bufferlist& bl,
			  RBD::AioCompletion *c, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_write2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
		ictx->read_only, off, len, bl.length() < len ? NULL : bl.c_str(), c->pc, op_flags);
    if (bl.length() < len) {
      tracepoint(librbd, aio_write_exit, -EINVAL);
      return -EINVAL;
    }
    submit_aio_write(ictx, off, len, bl.c_str(), get_aio_completion(c),
                     op_flags);
    tracepoint(librbd, aio_write_exit, 0);
    return 0;
  }

  int Image::aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, c->pc);
    submit_aio_discard(ictx, off, len, get_aio_completion(c));
    tracepoint(librbd, aio_discard_exit, 0);
    return 0;
  }

  int Image::aio_read(uint64_t off, size_t len, bufferlist& bl,
		      RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, bl.c_str(), c->pc);
    ldout(ictx->cct, 10) << "Image::aio_read() buf=" << (void *)bl.c_str() << "~"
			 << (void *)(bl.c_str() + len - 1) << dendl;
    submit_aio_read(ictx, off, len, NULL, &bl, get_aio_completion(c), 0);
    tracepoint(librbd, aio_read_exit, 0);
    return 0;
  }

  int Image::aio_read2(uint64_t off, size_t len, bufferlist& bl,
			RBD::AioCompletion *c, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_read2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
		ictx->read_only, off, len, bl.c_str(), c->pc, op_flags);
    ldout(ictx->cct, 10) << "Image::aio_read() buf=" << (void *)bl.c_str() << "~"
			 << (void *)(bl.c_str() + len - 1) << dendl;
    submit_aio_read(ictx, off, len, NULL, &bl, get_aio_completion(c), op_flags);
    tracepoint(librbd, aio_read_exit, 0);
    return 0;
  }

  int Image::flush()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::flush(ictx);
    tracepoint(librbd, flush_exit, r);
    return r;
  }

  int Image::aio_flush(RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, c->pc);
    submit_aio_flush(ictx, get_aio_completion(c));
    tracepoint(librbd, aio_flush_exit, 0);
    return 0;
  }

  int Image::invalidate_cache()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, invalidate_cache_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::invalidate_cache(ictx);
    tracepoint(librbd, invalidate_cache_exit, r);
    return r;
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
  tracepoint(librbd, list_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id());
  vector<string> cpp_names;
  int r = librbd::list(io_ctx, cpp_names);
  if (r == -ENOENT) {
    tracepoint(librbd, list_exit, 0, *size);
    return 0;
  }

  if (r < 0) {
    tracepoint(librbd, list_exit, r, *size);
    return r;
  }

  size_t expected_size = 0;

  for (size_t i = 0; i < cpp_names.size(); i++) {
    expected_size += cpp_names[i].size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    tracepoint(librbd, list_exit, -ERANGE, *size);
    return -ERANGE;
  }

  if (!names) 
    return -EINVAL;

  for (int i = 0; i < (int)cpp_names.size(); i++) {
    const char* name = cpp_names[i].c_str();
    tracepoint(librbd, list_entry, name);
    strcpy(names, name);
    names += strlen(names) + 1;
  }
  tracepoint(librbd, list_exit, (int)expected_size, *size);
  return (int)expected_size;
}

extern "C" int rbd_create(rados_ioctx_t p, const char *name, uint64_t size, int *order)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  tracepoint(librbd, create_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, *order);
  int r = librbd::create(io_ctx, name, size, order);
  tracepoint(librbd, create_exit, r, *order);
  return r;
}

extern "C" int rbd_create2(rados_ioctx_t p, const char *name,
			   uint64_t size, uint64_t features,
			   int *order)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  tracepoint(librbd, create2_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order);
  int r = librbd::create(io_ctx, name, size, false, features, order, 0, 0);
  tracepoint(librbd, create2_exit, r, *order);
  return r;
}

extern "C" int rbd_create3(rados_ioctx_t p, const char *name,
			   uint64_t size, uint64_t features,
			   int *order,
			   uint64_t stripe_unit, uint64_t stripe_count)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  tracepoint(librbd, create3_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order, stripe_unit, stripe_count);
  int r = librbd::create(io_ctx, name, size, false, features, order,
			stripe_unit, stripe_count);
  tracepoint(librbd, create3_exit, r, *order);
  return r;
}

extern "C" int rbd_clone(rados_ioctx_t p_ioctx, const char *p_name,
			 const char *p_snap_name, rados_ioctx_t c_ioctx,
			 const char *c_name, uint64_t features, int *c_order)
{
  librados::IoCtx p_ioc, c_ioc;
  librados::IoCtx::from_rados_ioctx_t(p_ioctx, p_ioc);
  librados::IoCtx::from_rados_ioctx_t(c_ioctx, c_ioc);
  tracepoint(librbd, clone_enter, p_ioc.get_pool_name().c_str(), p_ioc.get_id(), p_name, p_snap_name, c_ioc.get_pool_name().c_str(), c_ioc.get_id(), c_name, features);
  int r = librbd::clone(p_ioc, p_name, p_snap_name, c_ioc, c_name,
		       features, c_order, 0, 0);
  tracepoint(librbd, clone_exit, r, *c_order);
  return r;
}

extern "C" int rbd_clone2(rados_ioctx_t p_ioctx, const char *p_name,
			  const char *p_snap_name, rados_ioctx_t c_ioctx,
			  const char *c_name, uint64_t features, int *c_order,
			  uint64_t stripe_unit, int stripe_count)
{
  librados::IoCtx p_ioc, c_ioc;
  librados::IoCtx::from_rados_ioctx_t(p_ioctx, p_ioc);
  librados::IoCtx::from_rados_ioctx_t(c_ioctx, c_ioc);
  tracepoint(librbd, clone2_enter, p_ioc.get_pool_name().c_str(), p_ioc.get_id(), p_name, p_snap_name, c_ioc.get_pool_name().c_str(), c_ioc.get_id(), c_name, features, stripe_unit, stripe_count);
  int r = librbd::clone(p_ioc, p_name, p_snap_name, c_ioc, c_name,
		       features, c_order, stripe_unit, stripe_count);
  tracepoint(librbd, clone2_exit, r, *c_order);
  return r;
}

extern "C" int rbd_remove(rados_ioctx_t p, const char *name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::remove(io_ctx, name, prog_ctx);
  tracepoint(librbd, remove_exit, r);
  return r;
}

extern "C" int rbd_remove_with_progress(rados_ioctx_t p, const char *name,
					librbd_progress_fn_t cb, void *cbdata)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::remove(io_ctx, name, prog_ctx);
  tracepoint(librbd, remove_exit, r);
  return r;
}

extern "C" int rbd_copy(rbd_image_t image, rados_ioctx_t dest_p,
			const char *destname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
  tracepoint(librbd, copy_exit, r);
  return r;
}

extern "C" int rbd_copy2(rbd_image_t srcp, rbd_image_t destp)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  tracepoint(librbd, copy2_enter, src, src->name.c_str(), src->snap_name.c_str(), src->read_only, dest, dest->name.c_str(), dest->snap_name.c_str(), dest->read_only);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(src, dest, prog_ctx);
  tracepoint(librbd, copy2_exit, r);
  return r;
}

extern "C" int rbd_copy_with_progress(rbd_image_t image, rados_ioctx_t dest_p,
				      const char *destname,
				      librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
  tracepoint(librbd, copy_exit, ret);
  return ret;
}

extern "C" int rbd_copy_with_progress2(rbd_image_t srcp, rbd_image_t destp,
				      librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  tracepoint(librbd, copy2_enter, src, src->name.c_str(), src->snap_name.c_str(), src->read_only, dest, dest->name.c_str(), dest->snap_name.c_str(), dest->read_only);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(src, dest, prog_ctx);
  tracepoint(librbd, copy2_exit, ret);
  return ret;
}

extern "C" int rbd_flatten(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::flatten(ictx, prog_ctx);
  tracepoint(librbd, flatten_exit, r);
  return r;
}

extern "C" int rbd_flatten_with_progress(rbd_image_t image,
					 librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::flatten(ictx, prog_ctx);
  tracepoint(librbd, flatten_exit, r);
  return r;
}

extern "C" int rbd_rename(rados_ioctx_t src_p, const char *srcname,
			  const char *destname)
{
  librados::IoCtx src_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(src_p, src_io_ctx);
  tracepoint(librbd, rename_enter, src_io_ctx.get_pool_name().c_str(), src_io_ctx.get_id(), srcname, destname);
  int r = librbd::rename(src_io_ctx, srcname, destname);
  tracepoint(librbd, rename_exit, r);
  return r;
}

extern "C" int rbd_open(rados_ioctx_t p, const char *name, rbd_image_t *image,
			const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						false);
  tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::open_image(ictx);
  if (r >= 0)
    *image = (rbd_image_t)ictx;
  tracepoint(librbd, open_image_exit, r);
  return r;
}

extern "C" int rbd_open_read_only(rados_ioctx_t p, const char *name,
				  rbd_image_t *image, const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						true);
  tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::open_image(ictx);
  if (r >= 0)
    *image = (rbd_image_t)ictx;
  tracepoint(librbd, open_image_exit, r);
  return r;
}

extern "C" int rbd_close(rbd_image_t image)
{
  librbd::ImageCtx *ctx = (librbd::ImageCtx *)image;
  tracepoint(librbd, close_image_enter, ctx, ctx->name.c_str(), ctx->id.c_str());
  librbd::close_image(ctx);
  tracepoint(librbd, close_image_exit);
  return 0;
}

extern "C" int rbd_resize(rbd_image_t image, uint64_t size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::resize(ictx, size, prog_ctx);
  tracepoint(librbd, resize_exit, r);
  return r;
}

extern "C" int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
					librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::resize(ictx, size, prog_ctx);
  tracepoint(librbd, resize_exit, r);
  return r;
}

extern "C" int rbd_stat(rbd_image_t image, rbd_image_info_t *info,
			size_t infosize)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, stat_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::info(ictx, *info, infosize);
  tracepoint(librbd, stat_exit, r, info);
  return r;
}

extern "C" int rbd_get_old_format(rbd_image_t image, uint8_t *old)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_old_format_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::get_old_format(ictx, old);
  tracepoint(librbd, get_old_format_exit, r, *old);
  return r;
}

extern "C" int rbd_get_size(rbd_image_t image, uint64_t *size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_size_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::get_size(ictx, size);
  tracepoint(librbd, get_size_exit, r, *size);
  return r;
}

extern "C" int rbd_get_features(rbd_image_t image, uint64_t *features)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_features_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::get_features(ictx, features);
  tracepoint(librbd, get_features_exit, r, *features);
  return r;
}

extern "C" int rbd_get_stripe_unit(rbd_image_t image, uint64_t *stripe_unit)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_stripe_unit_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  *stripe_unit = ictx->get_stripe_unit();
  tracepoint(librbd, get_stripe_unit_exit, 0, *stripe_unit);
  return 0;
}

extern "C" int rbd_get_stripe_count(rbd_image_t image, uint64_t *stripe_count)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_stripe_count_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  *stripe_count = ictx->get_stripe_count();
  tracepoint(librbd, get_stripe_count_exit, 0, *stripe_count);
  return 0;
}

extern "C" int rbd_get_overlap(rbd_image_t image, uint64_t *overlap)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_overlap_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::get_overlap(ictx, overlap);
  tracepoint(librbd, get_overlap_exit, r, *overlap);
  return r;
}

extern "C" int rbd_get_parent_info(rbd_image_t image,
  char *parent_pool_name, size_t ppool_namelen, char *parent_name,
  size_t pnamelen, char *parent_snap_name, size_t psnap_namelen)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_parent_info_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  string p_pool_name, p_name, p_snap_name;

  int r = librbd::get_parent_info(ictx, &p_pool_name, &p_name, &p_snap_name);
  if (r < 0) {
    tracepoint(librbd, get_parent_info_exit, r, NULL, NULL, NULL);
    return r;
  }

  if (parent_pool_name) {
    if (p_pool_name.length() + 1 > ppool_namelen) {
      tracepoint(librbd, get_parent_info_exit, -ERANGE, NULL, NULL, NULL);
      return -ERANGE;
    }

    strcpy(parent_pool_name, p_pool_name.c_str());
  }
  if (parent_name) {
    if (p_name.length() + 1 > pnamelen) {
      tracepoint(librbd, get_parent_info_exit, -ERANGE, NULL, NULL, NULL);
      return -ERANGE;
    }

    strcpy(parent_name, p_name.c_str());
  }
  if (parent_snap_name) {
    if (p_snap_name.length() + 1 > psnap_namelen) {
      tracepoint(librbd, get_parent_info_exit, -ERANGE, NULL, NULL, NULL);
      return -ERANGE;
    }

    strcpy(parent_snap_name, p_snap_name.c_str());
  }

  tracepoint(librbd, get_parent_info_exit, 0, parent_pool_name, parent_name, parent_snap_name);
  return 0;
}

extern "C" int rbd_get_flags(rbd_image_t image, uint64_t *flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_flags_enter, ictx);
  int r = librbd::get_flags(ictx, flags);
  tracepoint(librbd, get_flags_exit, ictx, r, *flags);
  return r;
}

extern "C" int rbd_is_exclusive_lock_owner(rbd_image_t image, int *is_owner)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, is_exclusive_lock_owner_enter, ictx);
  bool owner;
  int r = librbd::is_exclusive_lock_owner(ictx, &owner);
  *is_owner = owner ? 1 : 0;
  tracepoint(librbd, is_exclusive_lock_owner_exit, ictx, r, *is_owner);
  return r;
}

/* snapshots */
extern "C" int rbd_snap_create(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_create_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = librbd::snap_create(ictx, snap_name);
  tracepoint(librbd, snap_create_exit, r);
  return r;
}

extern "C" int rbd_snap_remove(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_remove_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = librbd::snap_remove(ictx, snap_name);
  tracepoint(librbd, snap_remove_exit, r);
  return r;
}

extern "C" int rbd_snap_rollback(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::snap_rollback(ictx, snap_name, prog_ctx);
  tracepoint(librbd, snap_rollback_exit, r);
  return r;
}

extern "C" int rbd_snap_rollback_with_progress(rbd_image_t image,
					       const char *snap_name,
					       librbd_progress_fn_t cb,
					       void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::snap_rollback(ictx, snap_name, prog_ctx);
  tracepoint(librbd, snap_rollback_exit, r);
  return r;
}

extern "C" int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps,
			     int *max_snaps)
{
  vector<librbd::snap_info_t> cpp_snaps;
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_list_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snaps);

  if (!max_snaps) {
    tracepoint(librbd, snap_list_exit, -EINVAL, 0);
    return -EINVAL;
  }

  int r = librbd::snap_list(ictx, cpp_snaps);
  if (r == -ENOENT) {
    tracepoint(librbd, snap_list_exit, 0, *max_snaps);
    return 0;
  }
  if (r < 0) {
    tracepoint(librbd, snap_list_exit, r, *max_snaps);
    return r;
  }
  if (*max_snaps < (int)cpp_snaps.size() + 1) {
    *max_snaps = (int)cpp_snaps.size() + 1;
    tracepoint(librbd, snap_list_exit, -ERANGE, *max_snaps);
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
      tracepoint(librbd, snap_list_exit, -ENOMEM, *max_snaps);
      return -ENOMEM;
    }
    tracepoint(librbd, snap_list_entry, snaps[i].id, snaps[i].size, snaps[i].name);
  }
  snaps[i].id = 0;
  snaps[i].size = 0;
  snaps[i].name = NULL;

  r = (int)cpp_snaps.size();
  tracepoint(librbd, snap_list_exit, r, *max_snaps);
  return r;
}

extern "C" void rbd_snap_list_end(rbd_snap_info_t *snaps)
{
  tracepoint(librbd, snap_list_end_enter, snaps);
  while (snaps->name) {
    free((void *)snaps->name);
    snaps++;
  }
  tracepoint(librbd, snap_list_end_exit);
}

extern "C" int rbd_snap_protect(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_protect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = librbd::snap_protect(ictx, snap_name);
  tracepoint(librbd, snap_protect_exit, r);
  return r;
}

extern "C" int rbd_snap_unprotect(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_unprotect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = librbd::snap_unprotect(ictx, snap_name);
  tracepoint(librbd, snap_unprotect_exit, r);
  return r;
}

extern "C" int rbd_snap_is_protected(rbd_image_t image, const char *snap_name,
				     int *is_protected)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_is_protected_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  bool protected_snap;
  int r = librbd::snap_is_protected(ictx, snap_name, &protected_snap);
  if (r < 0) {
    tracepoint(librbd, snap_is_protected_exit, r, *is_protected ? 1 : 0);
    return r;
  }
  *is_protected = protected_snap ? 1 : 0;
  tracepoint(librbd, snap_is_protected_exit, 0, *is_protected ? 1 : 0);
  return 0;
}

extern "C" int rbd_snap_set(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_set_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = librbd::snap_set(ictx, snap_name);
  tracepoint(librbd, snap_set_exit, r);
  return r;
}

extern "C" ssize_t rbd_list_children(rbd_image_t image, char *pools,
				     size_t *pools_len, char *images,
				     size_t *images_len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, list_children_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  set<pair<string, string> > image_set;

  int r = librbd::list_children(ictx, image_set);
  if (r < 0) {
    tracepoint(librbd, list_children_exit, r);
    return r;
  }

  size_t pools_total = 0;
  size_t images_total = 0;
  for (set<pair<string, string> >::const_iterator it = image_set.begin();
       it != image_set.end(); ++it) {
    pools_total += it->first.length() + 1;
    images_total += it->second.length() + 1;
  }

  bool too_short = false;
  if (pools_total > *pools_len)
    too_short = true;
  if (images_total > *images_len)
    too_short = true;
  *pools_len = pools_total;
  *images_len = images_total;
  if (too_short) {
    tracepoint(librbd, list_children_exit, -ERANGE);
    return -ERANGE;
  }

  char *pools_p = pools;
  char *images_p = images;
  for (set<pair<string, string> >::const_iterator it = image_set.begin();
       it != image_set.end(); ++it) {
    const char* pool = it->first.c_str();
    strcpy(pools_p, pool);
    pools_p += it->first.length() + 1;
    const char* image = it->second.c_str();
    strcpy(images_p, image);
    images_p += it->second.length() + 1;
    tracepoint(librbd, list_children_entry, pool, image);
  }

  ssize_t ret = image_set.size();
  tracepoint(librbd, list_children_exit, ret);
  return ret;
}

extern "C" ssize_t rbd_list_lockers(rbd_image_t image, int *exclusive,
				    char *tag, size_t *tag_len,
				    char *clients, size_t *clients_len,
				    char *cookies, size_t *cookies_len,
				    char *addrs, size_t *addrs_len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, list_lockers_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  std::list<librbd::locker_t> lockers;
  bool exclusive_bool;
  string tag_str;

  int r = list_lockers(ictx, &lockers, &exclusive_bool, &tag_str);
  if (r < 0) {
    tracepoint(librbd, list_lockers_exit, r);
    return r;
  }

  ldout(ictx->cct, 20) << "list_lockers r = " << r << " lockers.size() = " << lockers.size() << dendl;

  *exclusive = (int)exclusive_bool;
  size_t clients_total = 0;
  size_t cookies_total = 0;
  size_t addrs_total = 0;
  for (list<librbd::locker_t>::const_iterator it = lockers.begin();
       it != lockers.end(); ++it) {
    clients_total += it->client.length() + 1;
    cookies_total += it->cookie.length() + 1;
    addrs_total += it->address.length() + 1;
  }

  bool too_short = ((clients_total > *clients_len) ||
		    (cookies_total > *cookies_len) ||
		    (addrs_total > *addrs_len) ||
		    (tag_str.length() + 1 > *tag_len));
  *clients_len = clients_total;
  *cookies_len = cookies_total;
  *addrs_len = addrs_total;
  *tag_len = tag_str.length() + 1;
  if (too_short) {
    tracepoint(librbd, list_lockers_exit, -ERANGE);
    return -ERANGE;
  }

  strcpy(tag, tag_str.c_str());
  char *clients_p = clients;
  char *cookies_p = cookies;
  char *addrs_p = addrs;
  for (list<librbd::locker_t>::const_iterator it = lockers.begin();
       it != lockers.end(); ++it) {
    const char* client = it->client.c_str();
    strcpy(clients_p, client);
    clients_p += it->client.length() + 1;
    const char* cookie = it->cookie.c_str();
    strcpy(cookies_p, cookie);
    cookies_p += it->cookie.length() + 1;
    const char* address = it->address.c_str();
    strcpy(addrs_p, address);
    addrs_p += it->address.length() + 1;
    tracepoint(librbd, list_lockers_entry, client, cookie, address);
  }

  ssize_t ret = lockers.size();
  tracepoint(librbd, list_lockers_exit, ret);
  return ret;
}

extern "C" int rbd_lock_exclusive(rbd_image_t image, const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, lock_exclusive_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, cookie);
  int r = librbd::lock(ictx, true, cookie ? cookie : "", "");
  tracepoint(librbd, lock_exclusive_exit, r);
  return r;
}

extern "C" int rbd_lock_shared(rbd_image_t image, const char *cookie,
			       const char *tag)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, lock_shared_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, cookie, tag);
  int r = librbd::lock(ictx, false, cookie ? cookie : "", tag ? tag : "");
  tracepoint(librbd, lock_shared_exit, r);
  return r;
}

extern "C" int rbd_unlock(rbd_image_t image, const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, unlock_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, cookie);
  int r = librbd::unlock(ictx, cookie ? cookie : "");
  tracepoint(librbd, unlock_exit, r);
  return r;
}

extern "C" int rbd_break_lock(rbd_image_t image, const char *client,
			      const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, break_lock_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, client, cookie);
  int r = librbd::break_lock(ictx, client, cookie ? cookie : "");
  tracepoint(librbd, break_lock_exit, r);
  return r;
}

/* I/O */
extern "C" ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len,
			    char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
  int r = librbd::read(ictx, ofs, len, buf, 0);
  tracepoint(librbd, read_exit, r);
  return r;
}

extern "C" ssize_t rbd_read2(rbd_image_t image, uint64_t ofs, size_t len,
			      char *buf, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, read2_enter, ictx, ictx->name.c_str(),
	      ictx->snap_name.c_str(), ictx->read_only, ofs, len, op_flags);
  int r = librbd::read(ictx, ofs, len, buf, op_flags);
  tracepoint(librbd, read_exit, r);
  return r;
}


extern "C" int64_t rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
				    int (*cb)(uint64_t, size_t, const char *, void *),
				    void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, read_iterate_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
  int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
  tracepoint(librbd, read_iterate_exit, r);
  return r;
}

extern "C" int rbd_read_iterate2(rbd_image_t image, uint64_t ofs, uint64_t len,
				 int (*cb)(uint64_t, size_t, const char *, void *),
				 void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, read_iterate2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
  int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
  if (r > 0)
    r = 0;
  tracepoint(librbd, read_iterate2_exit, r);
  return (int)r;
}

extern "C" int rbd_diff_iterate(rbd_image_t image,
				const char *fromsnapname,
				uint64_t ofs, uint64_t len,
				int (*cb)(uint64_t, size_t, int, void *),
				void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, diff_iterate_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, fromsnapname, ofs, len);
  int r = librbd::diff_iterate(ictx, fromsnapname, ofs, len, cb, arg);
  tracepoint(librbd, diff_iterate_exit, r);
  return r;
}

extern "C" ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len,
			     const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len, buf);
  int r = librbd::write(ictx, ofs, len, buf, 0);
  tracepoint(librbd, write_exit, r);
  return r;
}

extern "C" ssize_t rbd_write2(rbd_image_t image, uint64_t ofs, size_t len,
			      const char *buf, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, write2_enter, ictx, ictx->name.c_str(),
	      ictx->snap_name.c_str(), ictx->read_only, ofs, len, buf, op_flags);
  int r = librbd::write(ictx, ofs, len, buf, op_flags);
  tracepoint(librbd, write_exit, r);
  return r;
}


extern "C" int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
  int r = librbd::discard(ictx, ofs, len);
  tracepoint(librbd, discard_exit, r);
  return r;
}

extern "C" int rbd_aio_create_completion(void *cb_arg,
					 rbd_callback_t complete_cb,
					 rbd_completion_t *c)
{
  librbd::RBD::AioCompletion *rbd_comp =
    new librbd::RBD::AioCompletion(cb_arg, complete_cb);
  *c = (rbd_completion_t) rbd_comp;
  return 0;
}

extern "C" int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len,
			     const char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, buf, comp->pc);
  submit_aio_write(ictx, off, len, buf, get_aio_completion(comp), 0);
  tracepoint(librbd, aio_write_exit, 0);
  return 0;
}

extern "C" int rbd_aio_write2(rbd_image_t image, uint64_t off, size_t len,
			      const char *buf, rbd_completion_t c, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_write2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
	      ictx->read_only, off, len, buf, comp->pc, op_flags);
  submit_aio_write(ictx, off, len, buf, get_aio_completion(comp), op_flags);
  tracepoint(librbd, aio_write_exit, 0);
  return 0;
}


extern "C" int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
			       rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, comp->pc);
  submit_aio_discard(ictx, off, len, get_aio_completion(comp));
  tracepoint(librbd, aio_discard_exit, 0);
  return 0;
}

extern "C" int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
			    char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, buf, comp->pc);
  submit_aio_read(ictx, off, len, buf, NULL, get_aio_completion(comp), 0);
  tracepoint(librbd, aio_read_exit, 0);
  return 0;
}

extern "C" int rbd_aio_read2(rbd_image_t image, uint64_t off, size_t len,
			      char *buf, rbd_completion_t c, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_read2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
	      ictx->read_only, off, len, buf, comp->pc, op_flags);
  submit_aio_read(ictx, off, len, buf, NULL, get_aio_completion(comp),
                  op_flags);
  tracepoint(librbd, aio_read_exit, 0);
  return 0;
}

extern "C" int rbd_flush(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::flush(ictx);
  tracepoint(librbd, flush_exit, r);
  return r;
}

extern "C" int rbd_aio_flush(rbd_image_t image, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, comp->pc);
  submit_aio_flush(ictx, get_aio_completion(comp));
  tracepoint(librbd, aio_flush_exit, 0);
  return 0;
}

extern "C" int rbd_invalidate_cache(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, invalidate_cache_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = librbd::invalidate_cache(ictx);
  tracepoint(librbd, invalidate_cache_exit, r);
  return r;
}

extern "C" int rbd_aio_is_complete(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->is_complete();
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
