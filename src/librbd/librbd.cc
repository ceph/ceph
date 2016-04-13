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
#include "common/TracepointProvider.h"
#include "include/Context.h"
#include "osdc/ObjectCacher.h"

#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequestWQ.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"

#include <algorithm>
#include <string>
#include <vector>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/librbd.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
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

TracepointProvider::Traits tracepoint_traits("librbd_tp.so", "rbd_tracing");

CephContext* get_cct(IoCtx &io_ctx) {
  return reinterpret_cast<CephContext*>(io_ctx.cct());
}

librbd::AioCompletion* get_aio_completion(librbd::RBD::AioCompletion *comp) {
  return reinterpret_cast<librbd::AioCompletion *>(comp->pc);
}

struct C_OpenComplete : public Context {
  librbd::ImageCtx *ictx;
  librbd::AioCompletion* comp;
  void **ictxp;
  bool reopen;
  C_OpenComplete(librbd::ImageCtx *ictx, librbd::AioCompletion* comp,
		 void **ictxp, bool reopen = false)
    : ictx(ictx), comp(comp), ictxp(ictxp), reopen(reopen) {
    comp->init_time(ictx, librbd::AIO_TYPE_OPEN);
    comp->get();
  }
  virtual void finish(int r) {
    ldout(ictx->cct, 20) << "C_OpenComplete::finish: r=" << r << dendl;
    if (reopen) {
      delete reinterpret_cast<librbd::ImageCtx*>(*ictxp);
    }
    if (r < 0) {
      *ictxp = nullptr;
      comp->fail(ictx->cct, r);
    } else {
      *ictxp = ictx;
      comp->lock.Lock();
      comp->complete(ictx->cct);
      comp->put_unlock();
    }
  }
};

struct C_OpenAfterCloseComplete : public Context {
  librbd::ImageCtx *ictx;
  librbd::AioCompletion* comp;
  void **ictxp;
  C_OpenAfterCloseComplete(librbd::ImageCtx *ictx, librbd::AioCompletion* comp,
			   void **ictxp)
    : ictx(ictx), comp(comp), ictxp(ictxp) {
  }
  virtual void finish(int r) {
    ldout(ictx->cct, 20) << "C_OpenAfterCloseComplete::finish: r=" << r
			 << dendl;
    ictx->state->open(new C_OpenComplete(ictx, comp, ictxp, true));
  }
};

struct C_CloseComplete : public Context {
  CephContext *cct;
  librbd::AioCompletion* comp;
  C_CloseComplete(librbd::ImageCtx *ictx, librbd::AioCompletion* comp)
    : cct(ictx->cct), comp(comp) {
    comp->init_time(ictx, librbd::AIO_TYPE_CLOSE);
    comp->get();
  }
  virtual void finish(int r) {
    ldout(cct, 20) << "C_CloseComplete::finish: r=" << r << dendl;
    if (r < 0) {
      comp->fail(cct, r);
    } else {
      comp->lock.Lock();
      comp->complete(cct);
      comp->put_unlock();
    }
  }
};

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
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

    if (image.ctx != NULL) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close();
      image.ctx = NULL;
    }

    int r = ictx->state->open();
    if (r < 0) {
      delete ictx;
      tracepoint(librbd, open_image_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_exit, 0);
    return 0;
  }

  int RBD::aio_open(IoCtx& io_ctx, Image& image, const char *name,
		    const char *snap_name, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = new ImageCtx(name, "", snap_name, io_ctx, false);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, aio_open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only, c->pc);

    if (image.ctx != NULL) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close(
	new C_OpenAfterCloseComplete(ictx, get_aio_completion(c), &image.ctx));
    } else {
      ictx->state->open(new C_OpenComplete(ictx, get_aio_completion(c),
					   &image.ctx));
    }
    tracepoint(librbd, aio_open_image_exit, 0);
    return 0;
  }

  int RBD::open_read_only(IoCtx& io_ctx, Image& image, const char *name,
			  const char *snap_name)
  {
    ImageCtx *ictx = new ImageCtx(name, "", snap_name, io_ctx, true);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

    if (image.ctx != NULL) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close();
      image.ctx = NULL;
    }

    int r = ictx->state->open();
    if (r < 0) {
      delete ictx;
      tracepoint(librbd, open_image_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_exit, 0);
    return 0;
  }

  int RBD::aio_open_read_only(IoCtx& io_ctx, Image& image, const char *name,
			      const char *snap_name, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = new ImageCtx(name, "", snap_name, io_ctx, true);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, aio_open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only, c->pc);

    if (image.ctx != NULL) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close(
	new C_OpenAfterCloseComplete(ictx, get_aio_completion(c), &image.ctx));
    } else {
      ictx->state->open(new C_OpenComplete(ictx, get_aio_completion(c),
					   &image.ctx));
    }
    tracepoint(librbd, aio_open_image_exit, 0);
    return 0;
  }

  int RBD::create(IoCtx& io_ctx, const char *name, uint64_t size, int *order)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, create_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, *order);
    int r = librbd::create(io_ctx, name, size, order);
    tracepoint(librbd, create_exit, r, *order);
    return r;
  }

  int RBD::create_cg(IoCtx& io_ctx, const char *cg_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, create_cg_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), cg_name);
    ldout(get_cct(io_ctx), 10) << "Creating consistency group" << dendl;
    int r = librbd::create_cg(io_ctx, cg_name);

    tracepoint(librbd, create_cg_exit, r);
    return 0;
  }

  int RBD::create2(IoCtx& io_ctx, const char *name, uint64_t size,
		   uint64_t features, int *order)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, create2_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order);
    int r = librbd::create(io_ctx, name, size, false, features, order, 0, 0);
    tracepoint(librbd, create2_exit, r, *order);
    return r;
  }

  int RBD::create3(IoCtx& io_ctx, const char *name, uint64_t size,
		   uint64_t features, int *order, uint64_t stripe_unit,
		   uint64_t stripe_count)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, create3_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order, stripe_unit, stripe_count);
    int r = librbd::create(io_ctx, name, size, false, features, order,
			  stripe_unit, stripe_count);
    tracepoint(librbd, create3_exit, r, *order);
    return r;
  }

  int RBD::create4(IoCtx& io_ctx, const char *name, uint64_t size,
		   ImageOptions& opts)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, create4_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, opts.opts);
    int r = librbd::create(io_ctx, name, size, opts);
    tracepoint(librbd, create4_exit, r);
    return r;
  }

  int RBD::clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
		 IoCtx& c_ioctx, const char *c_name, uint64_t features,
		 int *c_order)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(p_ioctx));
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
    TracepointProvider::initialize<tracepoint_traits>(get_cct(p_ioctx));
    tracepoint(librbd, clone2_enter, p_ioctx.get_pool_name().c_str(), p_ioctx.get_id(), p_name, p_snap_name, c_ioctx.get_pool_name().c_str(), c_ioctx.get_id(), c_name, features, stripe_unit, stripe_count);
    int r = librbd::clone(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
			 features, c_order, stripe_unit, stripe_count);
    tracepoint(librbd, clone2_exit, r, *c_order);
    return r;
  }

  int RBD::clone3(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
		  IoCtx& c_ioctx, const char *c_name, ImageOptions& c_opts)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(p_ioctx));
    tracepoint(librbd, clone3_enter, p_ioctx.get_pool_name().c_str(), p_ioctx.get_id(), p_name, p_snap_name, c_ioctx.get_pool_name().c_str(), c_ioctx.get_id(), c_name, c_opts.opts);
    int r = librbd::clone(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
			  c_opts);
    tracepoint(librbd, clone3_exit, r);
    return r;
  }

  int RBD::remove(IoCtx& io_ctx, const char *name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::remove(io_ctx, name, prog_ctx);
    tracepoint(librbd, remove_exit, r);
    return r;
  }

  int RBD::remove_with_progress(IoCtx& io_ctx, const char *name,
				ProgressContext& pctx)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
    int r = librbd::remove(io_ctx, name, pctx);
    tracepoint(librbd, remove_exit, r);
    return r;
  }

  int RBD::list(IoCtx& io_ctx, vector<string>& names)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
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

  int RBD::list_cgs(IoCtx& io_ctx, vector<string>& names)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, list_cgs_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id());

    int r = librbd::list_cgs(io_ctx, names);
    if (r >= 0) {
      for (auto itr : names) {
	tracepoint(librbd, list_cgs_entry, itr.c_str());
      }
    }
    tracepoint(librbd, list_cgs_exit, r, r);
    return r;
  }

  int RBD::rename(IoCtx& src_io_ctx, const char *srcname, const char *destname)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(src_io_ctx));
    tracepoint(librbd, rename_enter, src_io_ctx.get_pool_name().c_str(), src_io_ctx.get_id(), srcname, destname);
    int r = librbd::rename(src_io_ctx, srcname, destname);
    tracepoint(librbd, rename_exit, r);
    return r;
  }

  int RBD::mirror_mode_get(IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode) {
    return librbd::mirror_mode_get(io_ctx, mirror_mode);
  }

  int RBD::mirror_mode_set(IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode) {
    return librbd::mirror_mode_set(io_ctx, mirror_mode);
  }

  int RBD::mirror_peer_add(IoCtx& io_ctx, std::string *uuid,
                           const std::string &cluster_name,
                           const std::string &client_name) {
    return librbd::mirror_peer_add(io_ctx, uuid, cluster_name, client_name);
  }

  int RBD::mirror_peer_remove(IoCtx& io_ctx, const std::string &uuid) {
    return librbd::mirror_peer_remove(io_ctx, uuid);
  }

  int RBD::mirror_peer_list(IoCtx& io_ctx, std::vector<mirror_peer_t> *peers) {
    return librbd::mirror_peer_list(io_ctx, peers);
  }

  int RBD::mirror_peer_set_client(IoCtx& io_ctx, const std::string &uuid,
                                  const std::string &client_name) {
    return librbd::mirror_peer_set_client(io_ctx, uuid, client_name);
  }

  int RBD::mirror_peer_set_cluster(IoCtx& io_ctx, const std::string &uuid,
                                   const std::string &cluster_name) {
    return librbd::mirror_peer_set_cluster(io_ctx, uuid, cluster_name);
  }

  RBD::AioCompletion::AioCompletion(void *cb_arg, callback_t complete_cb)
  {
    pc = reinterpret_cast<void*>(librbd::AioCompletion::create(
      cb_arg, complete_cb, this));
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

  void *RBD::AioCompletion::get_arg()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    return c->get_arg();
  }

  void RBD::AioCompletion::release()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    c->release();
    delete this;
  }

  /*
    ImageOptions
  */

  ImageOptions::ImageOptions()
  {
    librbd::image_options_create(&opts);
  }

  ImageOptions::ImageOptions(rbd_image_options_t opts_)
  {
    librbd::image_options_create_ref(&opts, opts_);
  }

  ImageOptions::~ImageOptions()
  {
    librbd::image_options_destroy(opts);
  }

  int ImageOptions::set(int optname, const std::string& optval)
  {
    return librbd::image_options_set(opts, optname, optval);
  }

  int ImageOptions::set(int optname, uint64_t optval)
  {
    return librbd::image_options_set(opts, optname, optval);
  }

  int ImageOptions::get(int optname, std::string* optval) const
  {
    return librbd::image_options_get(opts, optname, optval);
  }

  int ImageOptions::get(int optname, uint64_t* optval) const
  {
    return librbd::image_options_get(opts, optname, optval);
  }

  int ImageOptions::unset(int optname)
  {
    return librbd::image_options_unset(opts, optname);
  }

  void ImageOptions::clear()
  {
    librbd::image_options_clear(opts);
  }

  bool ImageOptions::empty() const
  {
    return librbd::image_options_is_empty(opts);
  }

  /*
    Image
  */

  Image::Image() : ctx(NULL)
  {
  }

  Image::~Image()
  {
    close();
  }

  int Image::close()
  {
    int r = 0;
    if (ctx) {
      ImageCtx *ictx = (ImageCtx *)ctx;
      tracepoint(librbd, close_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str());

      r = ictx->state->close();
      ctx = NULL;

      tracepoint(librbd, close_image_exit, r);
    }
    return r;
  }

  int Image::aio_close(RBD::AioCompletion *c)
  {
    if (!ctx) {
      return -EINVAL;
    }

    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_close_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), c->pc);

    ictx->state->close(new C_CloseComplete(ictx, get_aio_completion(c)));
    ctx = NULL;

    tracepoint(librbd, aio_close_image_exit, 0);
    return 0;
  }

  int Image::resize(uint64_t size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    librbd::NoOpProgressContext prog_ctx;
    int r = ictx->operations->resize(size, prog_ctx);
    tracepoint(librbd, resize_exit, r);
    return r;
  }

  int Image::resize_with_progress(uint64_t size, librbd::ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    int r = ictx->operations->resize(size, pctx);
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

  int Image::update_features(uint64_t features, bool enabled)
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx *>(ctx);
    tracepoint(librbd, update_features_enter, ictx, features, enabled);
    int r = librbd::update_features(ictx, features, enabled);
    tracepoint(librbd, update_features_exit, r);
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

  int Image::set_image_notification(int fd, int type)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, set_image_notification_enter, ictx, fd, type);
    int r = librbd::set_image_notification(ictx, fd, type);
    tracepoint(librbd, set_image_notification_exit, ictx, r);
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

  int Image::rebuild_object_map(ProgressContext &prog_ctx)
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx*>(ctx);
    return ictx->operations->rebuild_object_map(prog_ctx);
  }

  int Image::copy(IoCtx& dest_io_ctx, const char *destname)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
    ImageOptions opts;
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx);
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

  int Image::copy3(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy3_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, opts.opts);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx);
    tracepoint(librbd, copy3_exit, r);
    return r;
  }

  int Image::copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
				librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
    ImageOptions opts;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, pctx);
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

  int Image::copy_with_progress3(IoCtx& dest_io_ctx, const char *destname,
				 ImageOptions& opts,
				 librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy3_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, opts.opts);
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, pctx);
    tracepoint(librbd, copy3_exit, r);
    return r;
  }

  int Image::flatten()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
    librbd::NoOpProgressContext prog_ctx;
    int r = ictx->operations->flatten(prog_ctx);
    tracepoint(librbd, flatten_exit, r);
    return r;
  }

  int Image::flatten_with_progress(librbd::ProgressContext& prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
    int r = ictx->operations->flatten(prog_ctx);
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
    int r = ictx->operations->snap_create(snap_name);
    tracepoint(librbd, snap_create_exit, r);
    return r;
  }

  int Image::snap_remove(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_remove_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = ictx->operations->snap_remove(snap_name);
    tracepoint(librbd, snap_remove_exit, r);
    return r;
  }

  int Image::snap_rollback(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = ictx->operations->snap_rollback(snap_name, prog_ctx);
    tracepoint(librbd, snap_rollback_exit, r);
    return r;
  }

  int Image::snap_rename(const char *srcname, const char *dstname)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_rename_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, srcname, dstname);
    int r = ictx->operations->snap_rename(srcname, dstname);
    tracepoint(librbd, snap_rename_exit, r);
    return r;
  }

  int Image::snap_rollback_with_progress(const char *snap_name,
					 ProgressContext& prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = ictx->operations->snap_rollback(snap_name, prog_ctx);
    tracepoint(librbd, snap_rollback_exit, r);
    return r;
  }

  int Image::snap_protect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_protect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = ictx->operations->snap_protect(snap_name);
    tracepoint(librbd, snap_protect_exit, r);
    return r;
  }

  int Image::snap_unprotect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_unprotect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = ictx->operations->snap_unprotect(snap_name);
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
    tracepoint(librbd, snap_exists_enter, ictx, ictx->name.c_str(), 
      ictx->snap_name.c_str(), ictx->read_only, snap_name);
    bool exists; 
    int r = librbd::snap_exists(ictx, snap_name, &exists);
    tracepoint(librbd, snap_exists_exit, r, exists);
    if (r < 0) {
      // lie to caller since we don't know the real answer yet.
      return false;
    }
    return exists;
  }

  // A safer verion of snap_exists.
  int Image::snap_exists2(const char *snap_name, bool *exists)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_exists_enter, ictx, ictx->name.c_str(), 
      ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::snap_exists(ictx, snap_name, exists);
    tracepoint(librbd, snap_exists_exit, r, *exists);
    return r;
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
    bl.push_back(std::move(ptr));
    int r = ictx->aio_work_queue->read(ofs, len, bl.c_str(), 0);
    tracepoint(librbd, read_exit, r);
    return r;
  }

  ssize_t Image::read2(uint64_t ofs, size_t len, bufferlist& bl, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, read2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
		ictx->read_only, ofs, len, op_flags);
    bufferptr ptr(len);
    bl.push_back(std::move(ptr));
    int r = ictx->aio_work_queue->read(ofs, len, bl.c_str(), op_flags);
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
    tracepoint(librbd, diff_iterate_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(), ictx->read_only, fromsnapname, ofs, len,
               true, false);
    int r = librbd::diff_iterate(ictx, fromsnapname, ofs, len, true, false, cb,
                                 arg);
    tracepoint(librbd, diff_iterate_exit, r);
    return r;
  }

  int Image::diff_iterate2(const char *fromsnapname, uint64_t ofs, uint64_t len,
                           bool include_parent, bool whole_object,
                           int (*cb)(uint64_t, size_t, int, void *), void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, diff_iterate_enter, ictx, ictx->name.c_str(),
              ictx->snap_name.c_str(), ictx->read_only, fromsnapname, ofs, len,
              include_parent, whole_object);
    int r = librbd::diff_iterate(ictx, fromsnapname, ofs, len, include_parent,
                                whole_object, cb, arg);
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
    int r = ictx->aio_work_queue->write(ofs, len, bl.c_str(), 0);
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
    int r = ictx->aio_work_queue->write(ofs, len, bl.c_str(), op_flags);
    tracepoint(librbd, write_exit, r);
    return r;
  }

  int Image::discard(uint64_t ofs, uint64_t len)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    int r = ictx->aio_work_queue->discard(ofs, len);
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
    ictx->aio_work_queue->aio_write(get_aio_completion(c), off, len, bl.c_str(),
                                    0);
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
    ictx->aio_work_queue->aio_write(get_aio_completion(c), off, len, bl.c_str(),
                                    op_flags);
    tracepoint(librbd, aio_write_exit, 0);
    return 0;
  }

  int Image::aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, c->pc);
    ictx->aio_work_queue->aio_discard(get_aio_completion(c), off, len);
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
    ictx->aio_work_queue->aio_read(get_aio_completion(c), off, len, NULL, &bl,
                                   0);
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
    ictx->aio_work_queue->aio_read(get_aio_completion(c), off, len, NULL, &bl,
                                   op_flags);
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
    ictx->aio_work_queue->aio_flush(get_aio_completion(c));
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

  int Image::poll_io_events(RBD::AioCompletion **comps, int numcomp)
  {
    AioCompletion *cs[numcomp];
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, poll_io_events_enter, ictx, numcomp);
    int r = librbd::poll_io_events(ictx, cs, numcomp);
    tracepoint(librbd, poll_io_events_exit, r);
    if (r > 0) {
      for (int i = 0; i < numcomp; ++i)
        comps[i] = (RBD::AioCompletion *)cs[i]->rbd_comp;
    }
    return r;
  }

  int Image::metadata_get(const std::string &key, std::string *value)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, metadata_get_enter, ictx, key.c_str());
    int r = librbd::metadata_get(ictx, key, value);
    if (r < 0) {
      tracepoint(librbd, metadata_get_exit, r, key.c_str(), NULL);
    } else {
      tracepoint(librbd, metadata_get_exit, r, key.c_str(), value->c_str());
    }
    return r;
  }

  int Image::metadata_set(const std::string &key, const std::string &value)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, metadata_set_enter, ictx, key.c_str(), value.c_str());
    int r = librbd::metadata_set(ictx, key, value);
    tracepoint(librbd, metadata_set_exit, r);
    return r;
  }

  int Image::metadata_remove(const std::string &key)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, metadata_remove_enter, ictx, key.c_str());
    int r = librbd::metadata_remove(ictx, key);
    tracepoint(librbd, metadata_remove_exit, r);
    return r;
  }

  int Image::metadata_list(const std::string &start, uint64_t max, map<string, bufferlist> *pairs)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, metadata_list_enter, ictx);
    int r = librbd::metadata_list(ictx, start, max, pairs);
    if (r >= 0) {
      for (map<string, bufferlist>::iterator it = pairs->begin();
           it != pairs->end(); ++it) {
        tracepoint(librbd, metadata_list_entry, it->first.c_str(), it->second.c_str());
      }
    }
    tracepoint(librbd, metadata_list_exit, r);
    return r;
  }

  int Image::mirror_image_enable() {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::mirror_image_enable(ictx);
  }

  int Image::mirror_image_disable(bool force) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::mirror_image_disable(ictx, force);
  }

  int Image::mirror_image_promote(bool force) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::mirror_image_promote(ictx, force);
  }

  int Image::mirror_image_demote() {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::mirror_image_demote(ictx);
  }

  int Image::mirror_image_resync()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::mirror_image_resync(ictx);
  }

  int Image::mirror_image_get_info(mirror_image_info_t *mirror_image_info,
                                   size_t info_size) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::mirror_image_get_info(ictx, mirror_image_info, info_size);
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

extern "C" void rbd_image_options_create(rbd_image_options_t* opts)
{
  librbd::image_options_create(opts);
}

extern "C" void rbd_image_options_destroy(rbd_image_options_t opts)
{
  librbd::image_options_destroy(opts);
}

extern "C" int rbd_image_options_set_string(rbd_image_options_t opts, int optname,
					    const char* optval)
{
  return librbd::image_options_set(opts, optname, optval);
}

extern "C" int rbd_image_options_set_uint64(rbd_image_options_t opts, int optname,
					    uint64_t optval)
{
  return librbd::image_options_set(opts, optname, optval);
}

extern "C" int rbd_image_options_get_string(rbd_image_options_t opts, int optname,
					    char* optval, size_t maxlen)
{
  std::string optval_;

  int r = librbd::image_options_get(opts, optname, &optval_);

  if (r < 0) {
    return r;
  }

  if (optval_.size() >= maxlen) {
    return -E2BIG;
  }

  strncpy(optval, optval_.c_str(), maxlen);

  return 0;
}

extern "C" int rbd_image_options_get_uint64(rbd_image_options_t opts, int optname,
				 uint64_t* optval)
{
  return librbd::image_options_get(opts, optname, optval);
}

extern "C" int rbd_image_options_unset(rbd_image_options_t opts, int optname)
{
  return librbd::image_options_unset(opts, optname);
}

extern "C" void rbd_image_options_clear(rbd_image_options_t opts)
{
  librbd::image_options_clear(opts);
}

extern "C" int rbd_image_options_is_empty(rbd_image_options_t opts)
{
  return librbd::image_options_is_empty(opts);
}

/* pool mirroring */
extern "C" int rbd_mirror_mode_get(rados_ioctx_t p,
                                   rbd_mirror_mode_t *mirror_mode) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::mirror_mode_get(io_ctx, mirror_mode);
}

extern "C" int rbd_mirror_mode_set(rados_ioctx_t p,
                                   rbd_mirror_mode_t mirror_mode) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::mirror_mode_set(io_ctx, mirror_mode);
}

extern "C" int rbd_mirror_peer_add(rados_ioctx_t p, char *uuid,
                                   size_t uuid_max_length,
                                   const char *cluster_name,
                                   const char *client_name) {
  static const std::size_t UUID_LENGTH = 36;

  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  if (uuid_max_length < UUID_LENGTH + 1) {
    return -E2BIG;
  }

  std::string uuid_str;
  int r = librbd::mirror_peer_add(io_ctx, &uuid_str, cluster_name, client_name);
  if (r >= 0) {
    strncpy(uuid, uuid_str.c_str(), uuid_max_length);
    uuid[uuid_max_length - 1] = '\0';
  }
  return r;
}

extern "C" int rbd_mirror_peer_remove(rados_ioctx_t p, const char *uuid) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  int r = librbd::mirror_peer_remove(io_ctx, uuid);
  return r;
}

extern "C" int rbd_mirror_peer_list(rados_ioctx_t p,
                                    rbd_mirror_peer_t *peers, int *max_peers) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  std::vector<librbd::mirror_peer_t> peer_vector;
  int r = librbd::mirror_peer_list(io_ctx, &peer_vector);
  if (r < 0) {
    return r;
  }

  if (*max_peers < static_cast<int>(peer_vector.size())) {
    *max_peers = static_cast<int>(peer_vector.size());
    return -ERANGE;
  }

  for (int i = 0; i < static_cast<int>(peer_vector.size()); ++i) {
    peers[i].uuid = strdup(peer_vector[i].uuid.c_str());
    peers[i].cluster_name = strdup(peer_vector[i].cluster_name.c_str());
    peers[i].client_name = strdup(peer_vector[i].client_name.c_str());
  }
  *max_peers = static_cast<int>(peer_vector.size());
  return 0;
}

extern "C" void rbd_mirror_peer_list_cleanup(rbd_mirror_peer_t *peers,
                                             int max_peers) {
  for (int i = 0; i < max_peers; ++i) {
    free(peers[i].uuid);
    free(peers[i].cluster_name);
    free(peers[i].client_name);
  }
}

extern "C" int rbd_mirror_peer_set_client(rados_ioctx_t p, const char *uuid,
                                          const char *client_name) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::mirror_peer_set_client(io_ctx, uuid, client_name);
}

extern "C" int rbd_mirror_peer_set_cluster(rados_ioctx_t p, const char *uuid,
                                           const char *cluster_name) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::mirror_peer_set_cluster(io_ctx, uuid, cluster_name);
}

extern "C" int rbd_list_cgs(rados_ioctx_t p, char *names, size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, list_cgs_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id());

  vector<string> cpp_names;
  int r = librbd::list(io_ctx, cpp_names);

  if (r == -ENOENT) {
    tracepoint(librbd, list_cgs_exit, 0, *size);
    return 0;
  }

  if (r < 0) {
    tracepoint(librbd, list_cgs_exit, r, *size);
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

/* images */
extern "C" int rbd_list(rados_ioctx_t p, char *names, size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
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
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
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
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
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
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, create3_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, features, *order, stripe_unit, stripe_count);
  int r = librbd::create(io_ctx, name, size, false, features, order,
			stripe_unit, stripe_count);
  tracepoint(librbd, create3_exit, r, *order);
  return r;
}

extern "C" int rbd_create4(rados_ioctx_t p, const char *name,
			   uint64_t size, rbd_image_options_t opts)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, create4_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name, size, opts);
  librbd::ImageOptions opts_(opts);
  int r = librbd::create(io_ctx, name, size, opts_);
  tracepoint(librbd, create4_exit, r);
  return r;
}

extern "C" int rbd_clone(rados_ioctx_t p_ioctx, const char *p_name,
			 const char *p_snap_name, rados_ioctx_t c_ioctx,
			 const char *c_name, uint64_t features, int *c_order)
{
  librados::IoCtx p_ioc, c_ioc;
  librados::IoCtx::from_rados_ioctx_t(p_ioctx, p_ioc);
  librados::IoCtx::from_rados_ioctx_t(c_ioctx, c_ioc);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(p_ioc));
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
  TracepointProvider::initialize<tracepoint_traits>(get_cct(p_ioc));
  tracepoint(librbd, clone2_enter, p_ioc.get_pool_name().c_str(), p_ioc.get_id(), p_name, p_snap_name, c_ioc.get_pool_name().c_str(), c_ioc.get_id(), c_name, features, stripe_unit, stripe_count);
  int r = librbd::clone(p_ioc, p_name, p_snap_name, c_ioc, c_name,
		       features, c_order, stripe_unit, stripe_count);
  tracepoint(librbd, clone2_exit, r, *c_order);
  return r;
}

extern "C" int rbd_clone3(rados_ioctx_t p_ioctx, const char *p_name,
			  const char *p_snap_name, rados_ioctx_t c_ioctx,
			  const char *c_name, rbd_image_options_t c_opts)
{
  librados::IoCtx p_ioc, c_ioc;
  librados::IoCtx::from_rados_ioctx_t(p_ioctx, p_ioc);
  librados::IoCtx::from_rados_ioctx_t(c_ioctx, c_ioc);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(p_ioc));
  tracepoint(librbd, clone3_enter, p_ioc.get_pool_name().c_str(), p_ioc.get_id(), p_name, p_snap_name, c_ioc.get_pool_name().c_str(), c_ioc.get_id(), c_name, c_opts);
  librbd::ImageOptions c_opts_(c_opts);
  int r = librbd::clone(p_ioc, p_name, p_snap_name, c_ioc, c_name, c_opts_);
  tracepoint(librbd, clone3_exit, r);
  return r;
}

extern "C" int rbd_remove(rados_ioctx_t p, const char *name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
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
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
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
  librbd::ImageOptions opts;
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx);
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

extern "C" int rbd_copy3(rbd_image_t image, rados_ioctx_t dest_p,
			 const char *destname, rbd_image_options_t c_opts)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, copy3_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, c_opts);
  librbd::ImageOptions c_opts_(c_opts);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(ictx, dest_io_ctx, destname, c_opts_, prog_ctx);
  tracepoint(librbd, copy3_exit, r);
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
  librbd::ImageOptions opts;
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx);
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

extern "C" int rbd_copy_with_progress3(rbd_image_t image, rados_ioctx_t dest_p,
				       const char *destname,
				       rbd_image_options_t dest_opts,
				       librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
  librbd::ImageOptions dest_opts_(dest_opts);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, dest_opts_, prog_ctx);
  tracepoint(librbd, copy_exit, ret);
  return ret;
}

extern "C" int rbd_flatten(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
  librbd::NoOpProgressContext prog_ctx;
  int r = ictx->operations->flatten(prog_ctx);
  tracepoint(librbd, flatten_exit, r);
  return r;
}

extern "C" int rbd_flatten_with_progress(rbd_image_t image,
					 librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, flatten_enter, ictx, ictx->name.c_str(), ictx->id.c_str());
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = ictx->operations->flatten(prog_ctx);
  tracepoint(librbd, flatten_exit, r);
  return r;
}

extern "C" int rbd_rename(rados_ioctx_t src_p, const char *srcname,
			  const char *destname)
{
  librados::IoCtx src_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(src_p, src_io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(src_io_ctx));
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
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						false);
  tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

  int r = ictx->state->open();
  if (r < 0) {
    delete ictx;
  } else {
    *image = (rbd_image_t)ictx;
  }
  tracepoint(librbd, open_image_exit, r);
  return r;
}

extern "C" int rbd_aio_open(rados_ioctx_t p, const char *name,
			    rbd_image_t *image, const char *snap_name,
			    rbd_completion_t c)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						false);
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only, comp->pc);
  ictx->state->open(new C_OpenComplete(ictx, get_aio_completion(comp), image));
  tracepoint(librbd, aio_open_image_exit, 0);
  return 0;
}

extern "C" int rbd_open_read_only(rados_ioctx_t p, const char *name,
				  rbd_image_t *image, const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						true);
  tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

  int r = ictx->state->open();
  if (r < 0) {
    delete ictx;
  } else {
    *image = (rbd_image_t)ictx;
  }
  tracepoint(librbd, open_image_exit, r);
  return r;
}

extern "C" int rbd_aio_open_read_only(rados_ioctx_t p, const char *name,
				      rbd_image_t *image, const char *snap_name,
				      rbd_completion_t c)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, "", snap_name, io_ctx,
						true);
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_open_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only, comp->pc);
  ictx->state->open(new C_OpenComplete(ictx, get_aio_completion(comp), image));
  tracepoint(librbd, aio_open_image_exit, 0);
  return 0;
}

extern "C" int rbd_close(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, close_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str());

  int r = ictx->state->close();

  tracepoint(librbd, close_image_exit, r);
  return r;
}

extern "C" int rbd_aio_close(rbd_image_t image, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_close_image_enter, ictx, ictx->name.c_str(), ictx->id.c_str(), comp->pc);
  ictx->state->close(new C_CloseComplete(ictx, get_aio_completion(comp)));
  tracepoint(librbd, aio_close_image_exit, 0);
  return 0;
}

extern "C" int rbd_resize(rbd_image_t image, uint64_t size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::NoOpProgressContext prog_ctx;
  int r = ictx->operations->resize(size, prog_ctx);
  tracepoint(librbd, resize_exit, r);
  return r;
}

extern "C" int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
					librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = ictx->operations->resize(size, prog_ctx);
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

extern "C" int rbd_update_features(rbd_image_t image, uint64_t features,
                                  uint8_t enabled)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx *>(image);
  bool features_enabled = enabled != 0;
  tracepoint(librbd, update_features_enter, ictx, features, features_enabled);
  int r = librbd::update_features(ictx, features, features_enabled);
  tracepoint(librbd, update_features_exit, r);
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

extern "C" int rbd_set_image_notification(rbd_image_t image, int fd, int type)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, set_image_notification_enter, ictx, fd, type);
  int r = librbd::set_image_notification(ictx, fd, type);
  tracepoint(librbd, set_image_notification_exit, ictx, r);
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

extern "C" int rbd_rebuild_object_map(rbd_image_t image,
                                      librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return ictx->operations->rebuild_object_map(prog_ctx);
}

/* snapshots */
extern "C" int rbd_snap_create(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_create_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = ictx->operations->snap_create(snap_name);
  tracepoint(librbd, snap_create_exit, r);
  return r;
}

extern "C" int rbd_snap_rename(rbd_image_t image, const char *srcname, const char *dstname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_rename_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, srcname, dstname);
  int r = ictx->operations->snap_rename(srcname, dstname);
  tracepoint(librbd, snap_rename_exit, r);
  return r;
}

extern "C" int rbd_snap_remove(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_remove_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = ictx->operations->snap_remove(snap_name);
  tracepoint(librbd, snap_remove_exit, r);
  return r;
}

extern "C" int rbd_snap_rollback(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  librbd::NoOpProgressContext prog_ctx;
  int r = ictx->operations->snap_rollback(snap_name, prog_ctx);
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
  int r = ictx->operations->snap_rollback(snap_name, prog_ctx);
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
  int r = ictx->operations->snap_protect(snap_name);
  tracepoint(librbd, snap_protect_exit, r);
  return r;
}

extern "C" int rbd_snap_unprotect(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_unprotect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = ictx->operations->snap_unprotect(snap_name);
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
  int r = ictx->aio_work_queue->read(ofs, len, buf, 0);
  tracepoint(librbd, read_exit, r);
  return r;
}

extern "C" ssize_t rbd_read2(rbd_image_t image, uint64_t ofs, size_t len,
			      char *buf, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, read2_enter, ictx, ictx->name.c_str(),
	      ictx->snap_name.c_str(), ictx->read_only, ofs, len, op_flags);
  int r = ictx->aio_work_queue->read(ofs, len, buf, op_flags);
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
  tracepoint(librbd, diff_iterate_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only, fromsnapname, ofs, len,
             true, false);
  int r = librbd::diff_iterate(ictx, fromsnapname, ofs, len, true, false, cb,
                               arg);
  tracepoint(librbd, diff_iterate_exit, r);
  return r;
}

extern "C" int rbd_diff_iterate2(rbd_image_t image, const char *fromsnapname,
                                uint64_t ofs, uint64_t len,
                                uint8_t include_parent, uint8_t whole_object,
                                int (*cb)(uint64_t, size_t, int, void *),
                                void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, diff_iterate_enter, ictx, ictx->name.c_str(),
            ictx->snap_name.c_str(), ictx->read_only, fromsnapname, ofs, len,
            include_parent != 0, whole_object != 0);
  int r = librbd::diff_iterate(ictx, fromsnapname, ofs, len, include_parent,
                              whole_object, cb, arg);
  tracepoint(librbd, diff_iterate_exit, r);
  return r;
}

extern "C" ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len,
			     const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len, buf);
  int r = ictx->aio_work_queue->write(ofs, len, buf, 0);
  tracepoint(librbd, write_exit, r);
  return r;
}

extern "C" ssize_t rbd_write2(rbd_image_t image, uint64_t ofs, size_t len,
			      const char *buf, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, write2_enter, ictx, ictx->name.c_str(),
	      ictx->snap_name.c_str(), ictx->read_only, ofs, len, buf, op_flags);
  int r = ictx->aio_work_queue->write(ofs, len, buf, op_flags);
  tracepoint(librbd, write_exit, r);
  return r;
}


extern "C" int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
  int r = ictx->aio_work_queue->discard(ofs, len);
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
  ictx->aio_work_queue->aio_write(get_aio_completion(comp), off, len, buf, 0);
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
  ictx->aio_work_queue->aio_write(get_aio_completion(comp), off, len, buf,
                                  op_flags);
  tracepoint(librbd, aio_write_exit, 0);
  return 0;
}


extern "C" int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
			       rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, comp->pc);
  ictx->aio_work_queue->aio_discard(get_aio_completion(comp), off, len);
  tracepoint(librbd, aio_discard_exit, 0);
  return 0;
}

extern "C" int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
			    char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, buf, comp->pc);
  ictx->aio_work_queue->aio_read(get_aio_completion(comp), off, len, buf, NULL,
                                 0);
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
  ictx->aio_work_queue->aio_read(get_aio_completion(comp), off, len, buf, NULL,
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
  ictx->aio_work_queue->aio_flush(get_aio_completion(comp));
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

extern "C" int rbd_poll_io_events(rbd_image_t image, rbd_completion_t *comps, int numcomp)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::AioCompletion *cs[numcomp];
  tracepoint(librbd, poll_io_events_enter, ictx, numcomp);
  int r = librbd::poll_io_events(ictx, cs, numcomp);
  tracepoint(librbd, poll_io_events_exit, r);
  if (r > 0) {
    for (int i = 0; i < r; ++i)
      comps[i] = cs[i]->rbd_comp;
  }
  return r;
}

extern "C" int rbd_metadata_get(rbd_image_t image, const char *key, char *value, size_t *vallen)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  string val_s;
  tracepoint(librbd, metadata_get_enter, ictx, key);
  int r = librbd::metadata_get(ictx, key, &val_s);
  if (r < 0) {
    tracepoint(librbd, metadata_get_exit, r, key, NULL);
    return r;
  }
  if (*vallen < val_s.size()) {
    r = -ERANGE;
    *vallen = val_s.size();
    tracepoint(librbd, metadata_get_exit, r, key, NULL);
  } else {
    strncpy(value, val_s.c_str(), val_s.size());
    tracepoint(librbd, metadata_get_exit, r, key, value);
  }
  return r;
}

extern "C" int rbd_metadata_set(rbd_image_t image, const char *key, const char *value)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, metadata_set_enter, ictx, key, value);
  int r = librbd::metadata_set(ictx, key, value);
  tracepoint(librbd, metadata_set_exit, r);
  return r;
}

extern "C" int rbd_metadata_remove(rbd_image_t image, const char *key)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, metadata_remove_enter, ictx, key);
  int r = librbd::metadata_remove(ictx, key);
  tracepoint(librbd, metadata_remove_exit, r);
  return r;
}

extern "C" int rbd_metadata_list(rbd_image_t image, const char *start, uint64_t max,
                                 char *key, size_t *key_len, char *value, size_t *val_len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, metadata_list_enter, ictx);
  map<string, bufferlist> pairs;
  int r = librbd::metadata_list(ictx, start, max, &pairs);
  size_t key_total_len = 0, val_total_len = 0;
  bool too_short = false;
  for (map<string, bufferlist>::iterator it = pairs.begin();
       it != pairs.end(); ++it) {
    key_total_len += it->first.size() + 1;
    val_total_len += it->second.length() + 1;
  }
  if (*key_len < key_total_len || *val_len < key_total_len)
    too_short = true;
  *key_len = key_total_len;
  *val_len = val_total_len;
  if (too_short) {
    tracepoint(librbd, metadata_list_exit, -ERANGE);
    return -ERANGE;
  }

  char *key_p = key, *value_p = value;

  for (map<string, bufferlist>::iterator it = pairs.begin();
       it != pairs.end(); ++it) {
    strncpy(key_p, it->first.c_str(), it->first.size());
    key_p += it->first.size() + 1;
    strncpy(value_p, it->second.c_str(), it->second.length());
    value_p += it->second.length() + 1;
    tracepoint(librbd, metadata_list_entry, it->first.c_str(), it->second.c_str());
  }
  tracepoint(librbd, metadata_list_exit, r);
  return r;
}

extern "C" int rbd_mirror_image_enable(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::mirror_image_enable(ictx);
}

extern "C" int rbd_mirror_image_disable(rbd_image_t image, bool force)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::mirror_image_disable(ictx, force);
}

extern "C" int rbd_mirror_image_promote(rbd_image_t image, bool force)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::mirror_image_promote(ictx, force);
}

extern "C" int rbd_mirror_image_demote(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::mirror_image_demote(ictx);
}

extern "C" int rbd_mirror_image_resync(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::mirror_image_resync(ictx);
}

extern "C" int rbd_mirror_image_get_info(rbd_image_t image,
                                         rbd_mirror_image_info_t *mirror_image_info,
                                         size_t info_size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  librbd::mirror_image_info_t cpp_mirror_image;
  int r = librbd::mirror_image_get_info(ictx, &cpp_mirror_image,
                                        sizeof(cpp_mirror_image));
  if (r < 0) {
    return r;
  }

  mirror_image_info->global_id = strdup(cpp_mirror_image.global_id.c_str());
  mirror_image_info->state = cpp_mirror_image.state;
  mirror_image_info->primary = cpp_mirror_image.primary;
  return 0;
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

extern "C" void *rbd_aio_get_arg(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->get_arg();
}

extern "C" void rbd_aio_release(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  comp->release();
}
