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
              char *buf, bufferlist *pbl, librbd::AioCompletion *c)
    : m_ictx(ictx), m_off(off), m_len(len), m_buf(buf), m_pbl(pbl), m_comp(c) {
  }
protected:
  virtual void finish(int r) {
    librbd::aio_read(m_ictx, m_off, m_len, m_buf, m_pbl, m_comp);
  }
private:
  librbd::ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  char *m_buf;
  bufferlist *m_pbl;
  librbd::AioCompletion *m_comp;
};

class C_AioWriteWQ : public Context {
public:
  C_AioWriteWQ(librbd::ImageCtx *ictx, uint64_t off, size_t len,
               const char *buf, librbd::AioCompletion *c)
    : m_ictx(ictx), m_off(off), m_len(len), m_buf(buf), m_comp(c) {
  }
protected:
  virtual void finish(int r) {
    librbd::aio_write(m_ictx, m_off, m_len, m_buf, m_comp);
  }
private:
  librbd::ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  const char *m_buf;
  librbd::AioCompletion *m_comp;
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
                     char *buf, bufferlist *pbl, librbd::AioCompletion *c) {
  if (ictx->cct->_conf->rbd_non_blocking_aio) {
    ictx->aio_work_queue->queue(new C_AioReadWQ(ictx, off, len, buf, pbl, c));
  } else {
    librbd::aio_read(ictx, off, len, buf, pbl, c);
  }
}

void submit_aio_write(librbd::ImageCtx *ictx, uint64_t off, size_t len,
                      const char *buf, librbd::AioCompletion *c) {
  if (ictx->cct->_conf->rbd_non_blocking_aio) {
    ictx->aio_work_queue->queue(new C_AioWriteWQ(ictx, off, len, buf, c));
  } else {
    librbd::aio_write(ictx, off, len, buf, c);
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

    int r = librbd::open_image(ictx);
    if (r < 0)
      return r;

    image.ctx = (image_ctx_t) ictx;
    return 0;
  }

  int RBD::open_read_only(IoCtx& io_ctx, Image& image, const char *name,
			  const char *snap_name)
  {
    ImageCtx *ictx = new ImageCtx(name, "", snap_name, io_ctx, true);

    int r = librbd::open_image(ictx);
    if (r < 0)
      return r;

    image.ctx = (image_ctx_t) ictx;
    return 0;
  }

  int RBD::create(IoCtx& io_ctx, const char *name, uint64_t size, int *order)
  {
    return librbd::create(io_ctx, name, size, order);
  }

  int RBD::create2(IoCtx& io_ctx, const char *name, uint64_t size,
		   uint64_t features, int *order)
  {
    return librbd::create(io_ctx, name, size, false, features, order, 0, 0);
  }

  int RBD::create3(IoCtx& io_ctx, const char *name, uint64_t size,
		   uint64_t features, int *order, uint64_t stripe_unit,
		   uint64_t stripe_count)
  {
    return librbd::create(io_ctx, name, size, false, features, order,
			  stripe_unit, stripe_count);
  }

  int RBD::clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
		 IoCtx& c_ioctx, const char *c_name, uint64_t features,
		 int *c_order)
  {
    return librbd::clone(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
			 features, c_order, 0, 0);
  }

  int RBD::clone2(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
		  IoCtx& c_ioctx, const char *c_name, uint64_t features,
		  int *c_order, uint64_t stripe_unit, int stripe_count)
  {
    return librbd::clone(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
			 features, c_order, stripe_unit, stripe_count);
  }

  int RBD::remove(IoCtx& io_ctx, const char *name)
  {
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::remove(io_ctx, name, prog_ctx);
    return r;
  }

  int RBD::remove_with_progress(IoCtx& io_ctx, const char *name,
				ProgressContext& pctx)
  {
    int r = librbd::remove(io_ctx, name, pctx);
    return r;
  }

  int RBD::list(IoCtx& io_ctx, vector<string>& names)
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
      close_image(ictx);
    }
  }

  int Image::resize(uint64_t size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::resize(ictx, size, prog_ctx);
  }

  int Image::resize_with_progress(uint64_t size, librbd::ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::resize(ictx, size, pctx);
  }

  int Image::stat(image_info_t& info, size_t infosize)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::info(ictx, info, infosize);
  }

  int Image::old_format(uint8_t *old)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::get_old_format(ictx, old);
  }

  int Image::size(uint64_t *size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::get_size(ictx, size);
  }

  int Image::features(uint64_t *features)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::get_features(ictx, features);
  }

  uint64_t Image::get_stripe_unit() const
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return ictx->get_stripe_unit();
  }

  uint64_t Image::get_stripe_count() const
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return ictx->get_stripe_count();
  }

  int Image::overlap(uint64_t *overlap)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::get_overlap(ictx, overlap);
  }

  int Image::parent_info(string *parent_pool_name, string *parent_name,
			 string *parent_snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::get_parent_info(ictx, parent_pool_name, parent_name,
				   parent_snap_name);
  }

  int Image::copy(IoCtx& dest_io_ctx, const char *destname)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
  }

  int Image::copy2(Image& dest)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::copy(srcctx, destctx, prog_ctx);
  }

  int Image::copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
				librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::copy(ictx, dest_io_ctx, destname, pctx);
  }

  int Image::copy_with_progress2(Image& dest, librbd::ProgressContext &pctx)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    return librbd::copy(srcctx, destctx, pctx);
  }

  int Image::flatten()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::flatten(ictx, prog_ctx);
  }

  int Image::flatten_with_progress(librbd::ProgressContext& prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::flatten(ictx, prog_ctx);
  }

  int Image::list_children(set<pair<string, string> > *children)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::list_children(ictx, *children);
  }

  int Image::list_lockers(std::list<librbd::locker_t> *lockers,
			  bool *exclusive, string *tag)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::list_lockers(ictx, lockers, exclusive, tag);
  }

  int Image::lock_exclusive(const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::lock(ictx, true, cookie, "");
  }

  int Image::lock_shared(const string& cookie, const std::string& tag)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::lock(ictx, false, cookie, tag);
  }

  int Image::unlock(const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::unlock(ictx, cookie);
  }

  int Image::break_lock(const string& client, const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::break_lock(ictx, client, cookie);
  }

  int Image::snap_create(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_create(ictx, snap_name);
  }

  int Image::snap_remove(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_remove(ictx, snap_name);
  }

  int Image::snap_rollback(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::snap_rollback(ictx, snap_name, prog_ctx);
  }

  int Image::snap_rollback_with_progress(const char *snap_name,
					 ProgressContext& prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_rollback(ictx, snap_name, prog_ctx);
  }

  int Image::snap_protect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_protect(ictx, snap_name);
  }

  int Image::snap_unprotect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_unprotect(ictx, snap_name);
  }

  int Image::snap_is_protected(const char *snap_name, bool *is_protected)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_is_protected(ictx, snap_name, is_protected);
  }

  int Image::snap_list(vector<librbd::snap_info_t>& snaps)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_list(ictx, snaps);
  }

  bool Image::snap_exists(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::snap_exists(ictx, snap_name);
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
			      int (*cb)(uint64_t, size_t, const char *, void *),
			      void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::read_iterate(ictx, ofs, len, cb, arg);
  }

  int Image::read_iterate2(uint64_t ofs, uint64_t len,
			      int (*cb)(uint64_t, size_t, const char *, void *),
			      void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
    if (r > 0)
      r = 0;
    return (int)r;
  }

  int Image::diff_iterate(const char *fromsnapname,
			  uint64_t ofs, uint64_t len,
			  int (*cb)(uint64_t, size_t, int, void *),
			  void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::diff_iterate(ictx, fromsnapname, ofs, len, cb, arg);
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

  int Image::aio_write(uint64_t off, size_t len, bufferlist& bl,
		       RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    if (bl.length() < len)
      return -EINVAL;
    submit_aio_write(ictx, off, len, bl.c_str(), get_aio_completion(c));
    return 0;
  }

  int Image::aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    submit_aio_discard(ictx, off, len, get_aio_completion(c));
    return 0;
  }

  int Image::aio_read(uint64_t off, size_t len, bufferlist& bl,
		      RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    ldout(ictx->cct, 10) << "Image::aio_read() buf=" << (void *)bl.c_str() << "~"
			 << (void *)(bl.c_str() + len - 1) << dendl;
    submit_aio_read(ictx, off, len, NULL, &bl, get_aio_completion(c));
    return 0;
  }

  int Image::flush()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::flush(ictx);
  }

  int Image::aio_flush(RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    submit_aio_flush(ictx, get_aio_completion(c));
    return 0;
  }

  int Image::invalidate_cache()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::invalidate_cache(ictx);
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
  vector<string> cpp_names;
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

extern "C" int rbd_create2(rados_ioctx_t p, const char *name,
			   uint64_t size, uint64_t features,
			   int *order)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::create(io_ctx, name, size, false, features, order, 0, 0);
}

extern "C" int rbd_create3(rados_ioctx_t p, const char *name,
			   uint64_t size, uint64_t features,
			   int *order,
			   uint64_t stripe_unit, uint64_t stripe_count)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::create(io_ctx, name, size, false, features, order,
			stripe_unit, stripe_count);
}

extern "C" int rbd_clone(rados_ioctx_t p_ioctx, const char *p_name,
			 const char *p_snap_name, rados_ioctx_t c_ioctx,
			 const char *c_name, uint64_t features, int *c_order)
{
  librados::IoCtx p_ioc, c_ioc;
  librados::IoCtx::from_rados_ioctx_t(p_ioctx, p_ioc);
  librados::IoCtx::from_rados_ioctx_t(c_ioctx, c_ioc);
  return librbd::clone(p_ioc, p_name, p_snap_name, c_ioc, c_name,
		       features, c_order, 0, 0);
}

extern "C" int rbd_clone2(rados_ioctx_t p_ioctx, const char *p_name,
			  const char *p_snap_name, rados_ioctx_t c_ioctx,
			  const char *c_name, uint64_t features, int *c_order,
			  uint64_t stripe_unit, int stripe_count)
{
  librados::IoCtx p_ioc, c_ioc;
  librados::IoCtx::from_rados_ioctx_t(p_ioctx, p_ioc);
  librados::IoCtx::from_rados_ioctx_t(c_ioctx, c_ioc);
  return librbd::clone(p_ioc, p_name, p_snap_name, c_ioc, c_name,
		       features, c_order, stripe_unit, stripe_count);
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

extern "C" int rbd_copy(rbd_image_t image, rados_ioctx_t dest_p,
			const char *destname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  librbd::NoOpProgressContext prog_ctx;
  return librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
}

extern "C" int rbd_copy2(rbd_image_t srcp, rbd_image_t destp)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  librbd::NoOpProgressContext prog_ctx;
  return librbd::copy(src, dest, prog_ctx);
}

extern "C" int rbd_copy_with_progress(rbd_image_t image, rados_ioctx_t dest_p,
				      const char *destname,
				      librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
  return ret;
}

extern "C" int rbd_copy_with_progress2(rbd_image_t srcp, rbd_image_t destp,
				      librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(src, dest, prog_ctx);
  return ret;
}

extern "C" int rbd_flatten(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::NoOpProgressContext prog_ctx;
  return librbd::flatten(ictx, prog_ctx);
}

extern "C" int rbd_flatten_with_progress(rbd_image_t image,
					 librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::flatten(ictx, prog_ctx);
}

extern "C" int rbd_rename(rados_ioctx_t src_p, const char *srcname,
			  const char *destname)
{
  librados::IoCtx src_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(src_p, src_io_ctx);
  return librbd::rename(src_io_ctx, srcname, destname);
}

extern "C" int rbd_open(rados_ioctx_t p, const char *name, rbd_image_t *image,
			const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						false);
  int r = librbd::open_image(ictx);
  if (r >= 0)
    *image = (rbd_image_t)ictx;
  return r;
}

extern "C" int rbd_open_read_only(rados_ioctx_t p, const char *name,
				  rbd_image_t *image, const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						true);
  int r = librbd::open_image(ictx);
  if (r >= 0)
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

extern "C" int rbd_stat(rbd_image_t image, rbd_image_info_t *info,
			size_t infosize)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::info(ictx, *info, infosize);
}

extern "C" int rbd_get_old_format(rbd_image_t image, uint8_t *old)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::get_old_format(ictx, old);
}

extern "C" int rbd_get_size(rbd_image_t image, uint64_t *size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::get_size(ictx, size);
}

extern "C" int rbd_get_features(rbd_image_t image, uint64_t *features)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::get_features(ictx, features);
}

extern "C" int rbd_get_stripe_unit(rbd_image_t image, uint64_t *stripe_unit)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  *stripe_unit = ictx->get_stripe_unit();
  return 0;
}

extern "C" int rbd_get_stripe_count(rbd_image_t image, uint64_t *stripe_count)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  *stripe_count = ictx->get_stripe_count();
  return 0;
}

extern "C" int rbd_get_overlap(rbd_image_t image, uint64_t *overlap)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::get_overlap(ictx, overlap);
}

extern "C" int rbd_get_parent_info(rbd_image_t image,
  char *parent_pool_name, size_t ppool_namelen, char *parent_name,
  size_t pnamelen, char *parent_snap_name, size_t psnap_namelen)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  string p_pool_name, p_name, p_snap_name;

  int r = librbd::get_parent_info(ictx, &p_pool_name, &p_name, &p_snap_name);
  if (r < 0)
    return r;

  // compare against input bufferlen, leaving room for \0
  if (p_pool_name.length() + 1 > ppool_namelen ||
      p_name.length() + 1 > pnamelen ||
      p_snap_name.length() + 1 > psnap_namelen) {
    return -ERANGE;
  }

  strcpy(parent_pool_name, p_pool_name.c_str());
  strcpy(parent_name, p_name.c_str());
  strcpy(parent_snap_name, p_snap_name.c_str());
  return 0;
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

extern "C" int rbd_snap_rollback_with_progress(rbd_image_t image,
					       const char *snap_name,
					       librbd_progress_fn_t cb,
					       void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::snap_rollback(ictx, snap_name, prog_ctx);
}

extern "C" int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps,
			     int *max_snaps)
{
  vector<librbd::snap_info_t> cpp_snaps;
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

extern "C" int rbd_snap_protect(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::snap_protect(ictx, snap_name);
}

extern "C" int rbd_snap_unprotect(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::snap_unprotect(ictx, snap_name);
}

extern "C" int rbd_snap_is_protected(rbd_image_t image, const char *snap_name,
				     int *is_protected)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  bool protected_snap;
  int r = librbd::snap_is_protected(ictx, snap_name, &protected_snap);
  if (r < 0)
    return r;
  *is_protected = protected_snap ? 1 : 0;
  return 0;
}

extern "C" int rbd_snap_set(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::snap_set(ictx, snap_name);
}

extern "C" ssize_t rbd_list_children(rbd_image_t image, char *pools,
				     size_t *pools_len, char *images,
				     size_t *images_len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  set<pair<string, string> > image_set;

  int r = librbd::list_children(ictx, image_set);
  if (r < 0)
    return r;

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
  if (too_short)
    return -ERANGE;

  char *pools_p = pools;
  char *images_p = images;
  for (set<pair<string, string> >::const_iterator it = image_set.begin();
       it != image_set.end(); ++it) {
    strcpy(pools_p, it->first.c_str());
    pools_p += it->first.length() + 1;
    strcpy(images_p, it->second.c_str());
    images_p += it->second.length() + 1;
  }

  return image_set.size();
}

extern "C" ssize_t rbd_list_lockers(rbd_image_t image, int *exclusive,
				    char *tag, size_t *tag_len,
				    char *clients, size_t *clients_len,
				    char *cookies, size_t *cookies_len,
				    char *addrs, size_t *addrs_len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  std::list<librbd::locker_t> lockers;
  bool exclusive_bool;
  string tag_str;

  int r = list_lockers(ictx, &lockers, &exclusive_bool, &tag_str);
  if (r < 0)
    return r;

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
  if (too_short)
    return -ERANGE;

  strcpy(tag, tag_str.c_str());
  char *clients_p = clients;
  char *cookies_p = cookies;
  char *addrs_p = addrs;
  for (list<librbd::locker_t>::const_iterator it = lockers.begin();
       it != lockers.end(); ++it) {
    strcpy(clients_p, it->client.c_str());
    clients_p += it->client.length() + 1;
    strcpy(cookies_p, it->cookie.c_str());
    cookies_p += it->cookie.length() + 1;
    strcpy(addrs_p, it->address.c_str());
    addrs_p += it->address.length() + 1;
  }

  return lockers.size();
}

extern "C" int rbd_lock_exclusive(rbd_image_t image, const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::lock(ictx, true, cookie ? cookie : "", "");
}

extern "C" int rbd_lock_shared(rbd_image_t image, const char *cookie,
			       const char *tag)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::lock(ictx, false, cookie ? cookie : "", tag ? tag : "");
}

extern "C" int rbd_unlock(rbd_image_t image, const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::unlock(ictx, cookie ? cookie : "");
}

extern "C" int rbd_break_lock(rbd_image_t image, const char *client,
			      const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::break_lock(ictx, client, cookie ? cookie : "");
}

/* I/O */
extern "C" ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len,
			    char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::read(ictx, ofs, len, buf);
}

extern "C" int64_t rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
				    int (*cb)(uint64_t, size_t, const char *, void *),
				    void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::read_iterate(ictx, ofs, len, cb, arg);
}

extern "C" int rbd_read_iterate2(rbd_image_t image, uint64_t ofs, uint64_t len,
				 int (*cb)(uint64_t, size_t, const char *, void *),
				 void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
  if (r > 0)
    r = 0;
  return (int)r;
}

extern "C" int rbd_diff_iterate(rbd_image_t image,
				const char *fromsnapname,
				uint64_t ofs, uint64_t len,
				int (*cb)(uint64_t, size_t, int, void *),
				void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::diff_iterate(ictx, fromsnapname, ofs, len, cb, arg);
}

extern "C" ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len,
			     const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::write(ictx, ofs, len, buf);
}

extern "C" int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::discard(ictx, ofs, len);
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
  submit_aio_write(ictx, off, len, buf, get_aio_completion(comp));
  return 0;
}

extern "C" int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
			       rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  submit_aio_discard(ictx, off, len, get_aio_completion(comp));
  return 0;
}

extern "C" int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
			    char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  submit_aio_read(ictx, off, len, buf, NULL, get_aio_completion(comp));
  return 0;
}

extern "C" int rbd_flush(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::flush(ictx);
}

extern "C" int rbd_aio_flush(rbd_image_t image, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  submit_aio_flush(ictx, get_aio_completion(comp));
  return 0;
}

extern "C" int rbd_invalidate_cache(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::invalidate_cache(ictx);
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
