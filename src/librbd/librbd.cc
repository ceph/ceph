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

#include "common/dout.h"
#include "common/errno.h"
#include "common/TracepointProvider.h"
#include "include/Context.h"

#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/api/Config.h"
#include "librbd/api/DiffIterate.h"
#include "librbd/api/Group.h"
#include "librbd/api/Image.h"
#include "librbd/api/Migration.h"
#include "librbd/api/Mirror.h"
#include "librbd/api/Namespace.h"
#include "librbd/api/Pool.h"
#include "librbd/api/PoolMetadata.h"
#include "librbd/api/Snapshot.h"
#include "librbd/api/Trash.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ReadResult.h"
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

static auto create_write_raw(librbd::ImageCtx *ictx, const char *buf,
                             size_t len) {
  // TODO: until librados can guarantee memory won't be referenced after
  // it ACKs a request, always make a copy of the user-provided memory
  return buffer::copy(buf, len);
}

CephContext* get_cct(IoCtx &io_ctx) {
  return reinterpret_cast<CephContext*>(io_ctx.cct());
}

librbd::io::AioCompletion* get_aio_completion(librbd::RBD::AioCompletion *comp) {
  return reinterpret_cast<librbd::io::AioCompletion *>(comp->pc);
}

struct C_AioCompletion : public Context {
  CephContext *cct;
  librbd::io::aio_type_t aio_type;
  librbd::io::AioCompletion* aio_comp;

  C_AioCompletion(librbd::ImageCtx *ictx, librbd::io::aio_type_t aio_type,
                  librbd::io::AioCompletion* aio_comp)
    : cct(ictx->cct), aio_type(aio_type), aio_comp(aio_comp) {
    aio_comp->init_time(ictx, aio_type);
    aio_comp->get();
  }

  void finish(int r) override {
    ldout(cct, 20) << "C_AioComplete::finish: r=" << r << dendl;
    if (r < 0) {
      aio_comp->fail(r);
    } else {
      aio_comp->lock.Lock();
      aio_comp->complete();
      aio_comp->put_unlock();
    }
  }
};

struct C_OpenComplete : public C_AioCompletion {
  librbd::ImageCtx *ictx;
  void **ictxp;
  C_OpenComplete(librbd::ImageCtx *ictx, librbd::io::AioCompletion* comp,
		 void **ictxp)
    : C_AioCompletion(ictx, librbd::io::AIO_TYPE_OPEN, comp),
      ictx(ictx), ictxp(ictxp) {
  }
  void finish(int r) override {
    ldout(ictx->cct, 20) << "C_OpenComplete::finish: r=" << r << dendl;
    if (r < 0) {
      *ictxp = nullptr;
    } else {
      *ictxp = ictx;
    }

    C_AioCompletion::finish(r);
  }
};

struct C_OpenAfterCloseComplete : public Context {
  librbd::ImageCtx *ictx;
  librbd::io::AioCompletion* comp;
  void **ictxp;
  C_OpenAfterCloseComplete(librbd::ImageCtx *ictx,
                           librbd::io::AioCompletion* comp,
			   void **ictxp)
    : ictx(ictx), comp(comp), ictxp(ictxp) {
  }
  void finish(int r) override {
    ldout(ictx->cct, 20) << "C_OpenAfterCloseComplete::finish: r=" << r
			 << dendl;
    delete reinterpret_cast<librbd::ImageCtx*>(*ictxp);
    *ictxp = nullptr;

    ictx->state->open(0, new C_OpenComplete(ictx, comp, ictxp));
  }
};

struct C_UpdateWatchCB : public librbd::UpdateWatchCtx {
  rbd_update_callback_t watch_cb;
  void *arg;
  uint64_t handle = 0;

  C_UpdateWatchCB(rbd_update_callback_t watch_cb, void *arg) :
    watch_cb(watch_cb), arg(arg) {
  }
  void handle_notify() override {
    watch_cb(arg);
  }
};

void group_image_status_cpp_to_c(const librbd::group_image_info_t &cpp_info,
				 rbd_group_image_info_t *c_info) {
  c_info->name = strdup(cpp_info.name.c_str());
  c_info->pool = cpp_info.pool;
  c_info->state = cpp_info.state;
}

void group_info_cpp_to_c(const librbd::group_info_t &cpp_info,
			 rbd_group_info_t *c_info) {
  c_info->name = strdup(cpp_info.name.c_str());
  c_info->pool = cpp_info.pool;
}

void group_snap_info_cpp_to_c(const librbd::group_snap_info_t &cpp_info,
			      rbd_group_snap_info_t *c_info) {
  c_info->name = strdup(cpp_info.name.c_str());
  c_info->state = cpp_info.state;
}

void mirror_image_info_cpp_to_c(const librbd::mirror_image_info_t &cpp_info,
				rbd_mirror_image_info_t *c_info) {
  c_info->global_id = strdup(cpp_info.global_id.c_str());
  c_info->state = cpp_info.state;
  c_info->primary = cpp_info.primary;
}

void mirror_image_status_cpp_to_c(const librbd::mirror_image_status_t &cpp_status,
				  rbd_mirror_image_status_t *c_status) {
  c_status->name = strdup(cpp_status.name.c_str());
  mirror_image_info_cpp_to_c(cpp_status.info, &c_status->info);
  c_status->state = cpp_status.state;
  c_status->description = strdup(cpp_status.description.c_str());
  c_status->last_update = cpp_status.last_update;
  c_status->up = cpp_status.up;
}

void trash_image_info_cpp_to_c(const librbd::trash_image_info_t &cpp_info,
                               rbd_trash_image_info_t *c_info) {
  c_info->id = strdup(cpp_info.id.c_str());
  c_info->name = strdup(cpp_info.name.c_str());
  c_info->source = cpp_info.source;
  c_info->deletion_time = cpp_info.deletion_time;
  c_info->deferment_end_time = cpp_info.deferment_end_time;
}

void config_option_cpp_to_c(const librbd::config_option_t &cpp_option,
                            rbd_config_option_t *c_option) {
  c_option->name = strdup(cpp_option.name.c_str());
  c_option->value = strdup(cpp_option.value.c_str());
  c_option->source = cpp_option.source;
}

void config_option_cleanup(rbd_config_option_t &option) {
    free(option.name);
    free(option.value);
}

struct C_MirrorImageGetInfo : public Context {
    rbd_mirror_image_info_t *mirror_image_info;
  Context *on_finish;

  librbd::mirror_image_info_t cpp_mirror_image_info;

  C_MirrorImageGetInfo(rbd_mirror_image_info_t *mirror_image_info,
                         Context *on_finish)
    : mirror_image_info(mirror_image_info), on_finish(on_finish) {
  }

  void finish(int r) override {
    if (r < 0) {
      on_finish->complete(r);
      return;
    }

    mirror_image_info_cpp_to_c(cpp_mirror_image_info, mirror_image_info);
    on_finish->complete(0);
  }
};

struct C_MirrorImageGetStatus : public Context {
  rbd_mirror_image_status_t *mirror_image_status;
  Context *on_finish;

  librbd::mirror_image_status_t cpp_mirror_image_status;

  C_MirrorImageGetStatus(rbd_mirror_image_status_t *mirror_image_status,
                         Context *on_finish)
    : mirror_image_status(mirror_image_status), on_finish(on_finish) {
  }

  void finish(int r) override {
    if (r < 0) {
      on_finish->complete(r);
      return;
    }

    mirror_image_status_cpp_to_c(cpp_mirror_image_status, mirror_image_status);
    on_finish->complete(0);
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
    int update_progress(uint64_t offset, uint64_t src_size) override
    {
      return m_fn(offset, src_size, m_data);
    }
  private:
    librbd_progress_fn_t m_fn;
    void *m_data;
  };

  /*
   * Pool stats
   */
  PoolStats::PoolStats() {
    rbd_pool_stats_create(&pool_stats);
  }

  PoolStats::~PoolStats() {
    rbd_pool_stats_destroy(pool_stats);
  }

  int PoolStats::add(rbd_pool_stat_option_t option, uint64_t* opt_val) {
    return rbd_pool_stats_option_add_uint64(pool_stats, option, opt_val);
  }

  /*
   *  RBD
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

  int RBD::open_by_id(IoCtx& io_ctx, Image& image, const char *id)
  {
    return open_by_id(io_ctx, image, id, nullptr);
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

    int r = ictx->state->open(0);
    if (r < 0) {
      tracepoint(librbd, open_image_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_exit, 0);
    return 0;
  }

  int RBD::open_by_id(IoCtx& io_ctx, Image& image, const char *id,
		      const char *snap_name)
  {
    ImageCtx *ictx = new ImageCtx("", id, snap_name, io_ctx, false);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, open_image_by_id_enter, ictx, ictx->id.c_str(),
               ictx->snap_name.c_str(), ictx->read_only);

    if (image.ctx != nullptr) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close();
      image.ctx = nullptr;
    }

    int r = ictx->state->open(0);
    if (r < 0) {
      tracepoint(librbd, open_image_by_id_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_by_id_exit, 0);
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
      ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(c),
                                              &image.ctx));
    }
    tracepoint(librbd, aio_open_image_exit, 0);
    return 0;
  }

  int RBD::aio_open_by_id(IoCtx& io_ctx, Image& image, const char *id,
		          const char *snap_name, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = new ImageCtx("", id, snap_name, io_ctx, false);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, aio_open_image_by_id_enter, ictx, ictx->id.c_str(),
               ictx->snap_name.c_str(), ictx->read_only, c->pc);

    if (image.ctx != nullptr) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close(
	new C_OpenAfterCloseComplete(ictx, get_aio_completion(c), &image.ctx));
    } else {
      ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(c),
                                              &image.ctx));
    }
    tracepoint(librbd, aio_open_image_by_id_exit, 0);
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

    int r = ictx->state->open(0);
    if (r < 0) {
      tracepoint(librbd, open_image_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_exit, 0);
    return 0;
  }

  int RBD::open_by_id_read_only(IoCtx& io_ctx, Image& image, const char *id,
			        const char *snap_name)
  {
    ImageCtx *ictx = new ImageCtx("", id, snap_name, io_ctx, true);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, open_image_by_id_enter, ictx, ictx->id.c_str(),
               ictx->snap_name.c_str(), ictx->read_only);

    if (image.ctx != nullptr) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close();
      image.ctx = nullptr;
    }

    int r = ictx->state->open(0);
    if (r < 0) {
      tracepoint(librbd, open_image_by_id_exit, r);
      return r;
    }

    image.ctx = (image_ctx_t) ictx;
    tracepoint(librbd, open_image_by_id_exit, 0);
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
      ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(c),
                                              &image.ctx));
    }
    tracepoint(librbd, aio_open_image_exit, 0);
    return 0;
  }

  int RBD::aio_open_by_id_read_only(IoCtx& io_ctx, Image& image, const char *id,
	                            const char *snap_name, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = new ImageCtx("", id, snap_name, io_ctx, true);
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, aio_open_image_by_id_enter, ictx, ictx->id.c_str(),
               ictx->snap_name.c_str(), ictx->read_only, c->pc);

    if (image.ctx != nullptr) {
      reinterpret_cast<ImageCtx*>(image.ctx)->state->close(
	new C_OpenAfterCloseComplete(ictx, get_aio_completion(c), &image.ctx));
    } else {
      ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(c),
                                              &image.ctx));
    }
    tracepoint(librbd, aio_open_image_by_id_exit, 0);
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
    int r = librbd::create(io_ctx, name, "", size, opts, "", "", false);
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
    int r = librbd::clone(p_ioctx, nullptr, p_name, p_snap_name, c_ioctx,
                          nullptr, c_name, c_opts, "", "");
    tracepoint(librbd, clone3_exit, r);
    return r;
  }

  int RBD::remove(IoCtx& io_ctx, const char *name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Image<>::remove(io_ctx, name, "", prog_ctx);
    tracepoint(librbd, remove_exit, r);
    return r;
  }

  int RBD::remove_with_progress(IoCtx& io_ctx, const char *name,
				ProgressContext& pctx)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, remove_enter, io_ctx.get_pool_name().c_str(), io_ctx.get_id(), name);
    int r = librbd::api::Image<>::remove(io_ctx, name, "", pctx);
    tracepoint(librbd, remove_exit, r);
    return r;
  }

  int RBD::trash_move(IoCtx &io_ctx, const char *name, uint64_t delay) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_move_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), name);
    int r = librbd::api::Trash<>::move(io_ctx, RBD_TRASH_IMAGE_SOURCE_USER,
                                       name, delay);
    tracepoint(librbd, trash_move_exit, r);
    return r;
  }

  int RBD::trash_get(IoCtx &io_ctx, const char *id, trash_image_info_t *info) {
    return librbd::api::Trash<>::get(io_ctx, id, info);
  }

  int RBD::trash_list(IoCtx &io_ctx, vector<trash_image_info_t> &entries) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_list_enter,
               io_ctx.get_pool_name().c_str(), io_ctx.get_id());
    int r = librbd::api::Trash<>::list(io_ctx, entries);
#ifdef WITH_LTTNG
    if (r >= 0) {
      for (const auto& entry : entries) {
	tracepoint(librbd, trash_list_entry, entry.id.c_str());
      }
    }
#endif
    tracepoint(librbd, trash_list_exit, r, r);
    return r;
  }

  int RBD::trash_remove(IoCtx &io_ctx, const char *image_id, bool force) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_remove_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_id, force);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Trash<>::remove(io_ctx, image_id, force, prog_ctx);
    tracepoint(librbd, trash_remove_exit, r);
    return r;
  }

  int RBD::trash_remove_with_progress(IoCtx &io_ctx, const char *image_id,
                                      bool force, ProgressContext &pctx) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_remove_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_id, force);
    int r = librbd::api::Trash<>::remove(io_ctx, image_id, force, pctx);
    tracepoint(librbd, trash_remove_exit, r);
    return r;
  }

  int RBD::trash_restore(IoCtx &io_ctx, const char *id, const char *name) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_undelete_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), id, name);
    int r = librbd::api::Trash<>::restore(io_ctx, RBD_TRASH_IMAGE_SOURCE_USER,
                                          id, name);
    tracepoint(librbd, trash_undelete_exit, r);
    return r;
  }

  int RBD::trash_purge(IoCtx &io_ctx, time_t expire_ts, float threshold) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_purge_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), expire_ts, threshold);
    NoOpProgressContext nop_pctx;
    int r = librbd::api::Trash<>::purge(io_ctx, expire_ts, threshold, nop_pctx);
    tracepoint(librbd, trash_purge_exit, r);
    return r;
  }

  int RBD::trash_purge_with_progress(IoCtx &io_ctx, time_t expire_ts,
                                     float threshold, ProgressContext &pctx) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, trash_purge_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), expire_ts, threshold);
    int r = librbd::api::Trash<>::purge(io_ctx, expire_ts, threshold, pctx);
    tracepoint(librbd, trash_purge_exit, r);
    return r;
  }

  int RBD::namespace_create(IoCtx& io_ctx, const char *namespace_name) {
    return librbd::api::Namespace<>::create(io_ctx, namespace_name);
  }

  int RBD::namespace_remove(IoCtx& io_ctx, const char *namespace_name) {
    return librbd::api::Namespace<>::remove(io_ctx, namespace_name);
  }

  int RBD::namespace_list(IoCtx& io_ctx,
                          std::vector<std::string>* namespace_names) {
    return librbd::api::Namespace<>::list(io_ctx, namespace_names);
  }

  int RBD::namespace_exists(IoCtx& io_ctx, const char *namespace_name,
                            bool *exists) {
    return librbd::api::Namespace<>::exists(io_ctx, namespace_name, exists);
  }

  int RBD::pool_init(IoCtx& io_ctx, bool force) {
    return librbd::api::Pool<>::init(io_ctx, force);
  }

  int RBD::pool_stats_get(IoCtx& io_ctx, PoolStats* stats) {
    auto pool_stat_options =
      reinterpret_cast<librbd::api::Pool<>::StatOptions*>(stats->pool_stats);
    return librbd::api::Pool<>::get_stats(io_ctx, pool_stat_options);
  }

  int RBD::list(IoCtx& io_ctx, vector<string>& names)
  {
    std::vector<image_spec_t> image_specs;
    int r = list2(io_ctx, &image_specs);
    if (r < 0) {
      return r;
    }

    names.clear();
    for (auto& it : image_specs) {
      names.push_back(it.name);
    }
    return 0;
  }

  int RBD::list2(IoCtx& io_ctx, std::vector<image_spec_t> *images)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, list_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id());

    int r = librbd::api::Image<>::list_images(io_ctx, images);
    if (r >= 0) {
      for (auto& it : *images) {
        tracepoint(librbd, list_entry, it.name.c_str());
      }
    }
    tracepoint(librbd, list_exit, r, r);
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

  int RBD::migration_prepare(IoCtx& io_ctx, const char *image_name,
                             IoCtx& dest_io_ctx, const char *dest_image_name,
                             ImageOptions& opts)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_prepare_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name, dest_io_ctx.get_pool_name().c_str(),
               dest_io_ctx.get_id(), dest_image_name, opts.opts);
    int r = librbd::api::Migration<>::prepare(io_ctx, image_name, dest_io_ctx,
                                              dest_image_name, opts);
    tracepoint(librbd, migration_prepare_exit, r);
    return r;
  }

  int RBD::migration_execute(IoCtx& io_ctx, const char *image_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_execute_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Migration<>::execute(io_ctx, image_name, prog_ctx);
    tracepoint(librbd, migration_execute_exit, r);
    return r;
  }

  int RBD::migration_execute_with_progress(IoCtx& io_ctx,
                                           const char *image_name,
                                           librbd::ProgressContext &prog_ctx)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_execute_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);
    int r = librbd::api::Migration<>::execute(io_ctx, image_name, prog_ctx);
    tracepoint(librbd, migration_execute_exit, r);
    return r;
  }

  int RBD::migration_abort(IoCtx& io_ctx, const char *image_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_abort_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Migration<>::abort(io_ctx, image_name, prog_ctx);
    tracepoint(librbd, migration_abort_exit, r);
    return r;
  }

  int RBD::migration_abort_with_progress(IoCtx& io_ctx, const char *image_name,
                                         librbd::ProgressContext &prog_ctx)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_abort_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);
    int r = librbd::api::Migration<>::abort(io_ctx, image_name, prog_ctx);
    tracepoint(librbd, migration_abort_exit, r);
    return r;
  }

  int RBD::migration_commit(IoCtx& io_ctx, const char *image_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_commit_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Migration<>::commit(io_ctx, image_name, prog_ctx);
    tracepoint(librbd, migration_commit_exit, r);
    return r;
  }

  int RBD::migration_commit_with_progress(IoCtx& io_ctx, const char *image_name,
                                          librbd::ProgressContext &prog_ctx)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_commit_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);
    int r = librbd::api::Migration<>::commit(io_ctx, image_name, prog_ctx);
    tracepoint(librbd, migration_commit_exit, r);
    return r;
  }

  int RBD::migration_status(IoCtx& io_ctx, const char *image_name,
                            image_migration_status_t *status,
                            size_t status_size)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, migration_status_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), image_name);

    if (status_size != sizeof(image_migration_status_t)) {
      tracepoint(librbd, migration_status_exit, -ERANGE);
      return -ERANGE;
    }

    int r = librbd::api::Migration<>::status(io_ctx, image_name, status);
    tracepoint(librbd, migration_status_exit, r);
    return r;
  }

  int RBD::mirror_mode_get(IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode) {
    return librbd::api::Mirror<>::mode_get(io_ctx, mirror_mode);
  }

  int RBD::mirror_mode_set(IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode) {
    return librbd::api::Mirror<>::mode_set(io_ctx, mirror_mode);
  }

  int RBD::mirror_peer_add(IoCtx& io_ctx, std::string *uuid,
                           const std::string &cluster_name,
                           const std::string &client_name) {
    return librbd::api::Mirror<>::peer_add(io_ctx, uuid, cluster_name,
                                           client_name);
  }

  int RBD::mirror_peer_remove(IoCtx& io_ctx, const std::string &uuid) {
    return librbd::api::Mirror<>::peer_remove(io_ctx, uuid);
  }

  int RBD::mirror_peer_list(IoCtx& io_ctx, std::vector<mirror_peer_t> *peers) {
    return librbd::api::Mirror<>::peer_list(io_ctx, peers);
  }

  int RBD::mirror_peer_set_client(IoCtx& io_ctx, const std::string &uuid,
                                  const std::string &client_name) {
    return librbd::api::Mirror<>::peer_set_client(io_ctx, uuid, client_name);
  }

  int RBD::mirror_peer_set_cluster(IoCtx& io_ctx, const std::string &uuid,
                                   const std::string &cluster_name) {
    return librbd::api::Mirror<>::peer_set_cluster(io_ctx, uuid, cluster_name);
  }

  int RBD::mirror_peer_get_attributes(
      IoCtx& io_ctx, const std::string &uuid,
      std::map<std::string, std::string> *key_vals) {
    return librbd::api::Mirror<>::peer_get_attributes(io_ctx, uuid, key_vals);
  }

  int RBD::mirror_peer_set_attributes(
      IoCtx& io_ctx, const std::string &uuid,
      const std::map<std::string, std::string>& key_vals) {
    return librbd::api::Mirror<>::peer_set_attributes(io_ctx, uuid, key_vals);
  }

  int RBD::mirror_image_status_list(IoCtx& io_ctx, const std::string &start_id,
      size_t max, std::map<std::string, mirror_image_status_t> *images) {
    return librbd::api::Mirror<>::image_status_list(io_ctx, start_id, max,
                                                    images);
  }

  int RBD::mirror_image_status_summary(IoCtx& io_ctx,
      std::map<mirror_image_status_state_t, int> *states) {
    return librbd::api::Mirror<>::image_status_summary(io_ctx, states);
  }

  int RBD::mirror_image_instance_id_list(IoCtx& io_ctx,
      const std::string &start_id, size_t max,
      std::map<std::string, std::string> *instance_ids) {
    return librbd::api::Mirror<>::image_instance_id_list(io_ctx, start_id, max,
                                                         instance_ids);
  }

  int RBD::group_create(IoCtx& io_ctx, const char *group_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, group_create_enter, io_ctx.get_pool_name().c_str(),
	       io_ctx.get_id(), group_name);
    int r = librbd::api::Group<>::create(io_ctx, group_name);
    tracepoint(librbd, group_create_exit, r);
    return r;
  }

  int RBD::group_remove(IoCtx& io_ctx, const char *group_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, group_remove_enter, io_ctx.get_pool_name().c_str(),
	       io_ctx.get_id(), group_name);
    int r = librbd::api::Group<>::remove(io_ctx, group_name);
    tracepoint(librbd, group_remove_exit, r);
    return r;
  }

  int RBD::group_list(IoCtx& io_ctx, vector<string> *names)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, group_list_enter, io_ctx.get_pool_name().c_str(),
	       io_ctx.get_id());

    int r = librbd::api::Group<>::list(io_ctx, names);
    if (r >= 0) {
      for (auto itr : *names) {
	tracepoint(librbd, group_list_entry, itr.c_str());
      }
    }
    tracepoint(librbd, group_list_exit, r);
    return r;
  }

  int RBD::group_rename(IoCtx& io_ctx, const char *src_name,
                        const char *dest_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
    tracepoint(librbd, group_rename_enter, io_ctx.get_pool_name().c_str(),
               io_ctx.get_id(), src_name, dest_name);
    int r = librbd::api::Group<>::rename(io_ctx, src_name, dest_name);
    tracepoint(librbd, group_rename_exit, r);
    return r;
  }

  int RBD::group_image_add(IoCtx& group_ioctx, const char *group_name,
                           IoCtx& image_ioctx, const char *image_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_image_add_enter,
               group_ioctx.get_pool_name().c_str(),
               group_ioctx.get_id(), group_name,
               image_ioctx.get_pool_name().c_str(),
               image_ioctx.get_id(), image_name);
    int r = librbd::api::Group<>::image_add(group_ioctx, group_name,
                                            image_ioctx, image_name);
    tracepoint(librbd, group_image_add_exit, r);
    return r;
  }

  int RBD::group_image_remove(IoCtx& group_ioctx, const char *group_name,
                              IoCtx& image_ioctx, const char *image_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_image_remove_enter,
               group_ioctx.get_pool_name().c_str(),
               group_ioctx.get_id(), group_name,
               image_ioctx.get_pool_name().c_str(),
               image_ioctx.get_id(), image_name);
    int r = librbd::api::Group<>::image_remove(group_ioctx, group_name,
                                               image_ioctx, image_name);
    tracepoint(librbd, group_image_remove_exit, r);
    return r;
  }

  int RBD::group_image_remove_by_id(IoCtx& group_ioctx, const char *group_name,
                                    IoCtx& image_ioctx, const char *image_id)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_image_remove_by_id_enter,
               group_ioctx.get_pool_name().c_str(),
               group_ioctx.get_id(), group_name,
               image_ioctx.get_pool_name().c_str(),
               image_ioctx.get_id(), image_id);
    int r = librbd::api::Group<>::image_remove_by_id(group_ioctx, group_name,
                                                     image_ioctx, image_id);
    tracepoint(librbd, group_image_remove_by_id_exit, r);
    return r;
  }

  int RBD::group_image_list(IoCtx& group_ioctx, const char *group_name,
                            std::vector<group_image_info_t> *images,
                            size_t group_image_info_size)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_image_list_enter,
               group_ioctx.get_pool_name().c_str(),
	       group_ioctx.get_id(), group_name);

    if (group_image_info_size != sizeof(group_image_info_t)) {
      tracepoint(librbd, group_image_list_exit, -ERANGE);
      return -ERANGE;
    }

    int r = librbd::api::Group<>::image_list(group_ioctx, group_name, images);
    tracepoint(librbd, group_image_list_exit, r);
    return r;
  }

  int RBD::group_snap_create(IoCtx& group_ioctx, const char *group_name,
			     const char *snap_name) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_snap_create_enter,
               group_ioctx.get_pool_name().c_str(),
	       group_ioctx.get_id(), group_name, snap_name);
    int r = librbd::api::Group<>::snap_create(group_ioctx, group_name,
                                              snap_name);
    tracepoint(librbd, group_snap_create_exit, r);
    return r;
  }

  int RBD::group_snap_remove(IoCtx& group_ioctx, const char *group_name,
			     const char *snap_name) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_snap_remove_enter,
               group_ioctx.get_pool_name().c_str(),
	       group_ioctx.get_id(), group_name, snap_name);
    int r = librbd::api::Group<>::snap_remove(group_ioctx, group_name,
                                              snap_name);
    tracepoint(librbd, group_snap_remove_exit, r);
    return r;
  }

  int RBD::group_snap_list(IoCtx& group_ioctx, const char *group_name,
			   std::vector<group_snap_info_t> *snaps,
                           size_t group_snap_info_size)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_snap_list_enter,
               group_ioctx.get_pool_name().c_str(),
	       group_ioctx.get_id(), group_name);

    if (group_snap_info_size != sizeof(group_snap_info_t)) {
      tracepoint(librbd, group_snap_list_exit, -ERANGE);
      return -ERANGE;
    }

    int r = librbd::api::Group<>::snap_list(group_ioctx, group_name, snaps);
    tracepoint(librbd, group_snap_list_exit, r);
    return r;
  }

  int RBD::group_snap_rename(IoCtx& group_ioctx, const char *group_name,
                             const char *old_snap_name,
                             const char *new_snap_name)
  {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_snap_rename_enter,
               group_ioctx.get_pool_name().c_str(), group_ioctx.get_id(),
               group_name, old_snap_name, new_snap_name);
    int r = librbd::api::Group<>::snap_rename(group_ioctx, group_name,
                                              old_snap_name, new_snap_name);
    tracepoint(librbd, group_snap_list_exit, r);
    return r;
  }

  int RBD::group_snap_rollback(IoCtx& group_ioctx, const char *group_name,
                               const char *snap_name) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_snap_rollback_enter,
               group_ioctx.get_pool_name().c_str(),
               group_ioctx.get_id(), group_name, snap_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Group<>::snap_rollback(group_ioctx, group_name,
                                                snap_name, prog_ctx);
    tracepoint(librbd, group_snap_rollback_exit, r);
    return r;
  }

  int RBD::group_snap_rollback_with_progress(IoCtx& group_ioctx,
                                             const char *group_name,
                                             const char *snap_name,
                                             ProgressContext& prog_ctx) {
    TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
    tracepoint(librbd, group_snap_rollback_enter,
               group_ioctx.get_pool_name().c_str(),
               group_ioctx.get_id(), group_name, snap_name);
    int r = librbd::api::Group<>::snap_rollback(group_ioctx, group_name,
                                                snap_name, prog_ctx);
    tracepoint(librbd, group_snap_rollback_exit, r);
    return r;
  }

  int RBD::pool_metadata_get(IoCtx& ioctx, const std::string &key,
                             std::string *value)
  {
    int r = librbd::api::PoolMetadata<>::get(ioctx, key, value);
    return r;
  }

  int RBD::pool_metadata_set(IoCtx& ioctx, const std::string &key,
                             const std::string &value)
  {
    int r = librbd::api::PoolMetadata<>::set(ioctx, key, value);
    return r;
  }

  int RBD::pool_metadata_remove(IoCtx& ioctx, const std::string &key)
  {
    int r = librbd::api::PoolMetadata<>::remove(ioctx, key);
    return r;
  }

  int RBD::pool_metadata_list(IoCtx& ioctx, const std::string &start,
                              uint64_t max, map<string, bufferlist> *pairs)
  {
    int r = librbd::api::PoolMetadata<>::list(ioctx, start, max, pairs);
    return r;
  }

  int RBD::config_list(IoCtx& io_ctx, std::vector<config_option_t> *options) {
    return librbd::api::Config<>::list(io_ctx, options);
  }

  RBD::AioCompletion::AioCompletion(void *cb_arg, callback_t complete_cb)
  {
    pc = reinterpret_cast<void*>(librbd::io::AioCompletion::create(
      cb_arg, complete_cb, this));
  }

  bool RBD::AioCompletion::is_complete()
  {
    librbd::io::AioCompletion *c = (librbd::io::AioCompletion *)pc;
    return c->is_complete();
  }

  int RBD::AioCompletion::wait_for_complete()
  {
    librbd::io::AioCompletion *c = (librbd::io::AioCompletion *)pc;
    return c->wait_for_complete();
  }

  ssize_t RBD::AioCompletion::get_return_value()
  {
    librbd::io::AioCompletion *c = (librbd::io::AioCompletion *)pc;
    return c->get_return_value();
  }

  void *RBD::AioCompletion::get_arg()
  {
    librbd::io::AioCompletion *c = (librbd::io::AioCompletion *)pc;
    return c->get_arg();
  }

  void RBD::AioCompletion::release()
  {
    librbd::io::AioCompletion *c = (librbd::io::AioCompletion *)pc;
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

  ImageOptions::ImageOptions(const ImageOptions &imgopts)
  {
    librbd::image_options_copy(&opts, imgopts);
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

  int ImageOptions::is_set(int optname, bool* is_set)
  {
    return librbd::image_options_is_set(opts, optname, is_set);
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

    ictx->state->close(new C_AioCompletion(ictx, librbd::io::AIO_TYPE_CLOSE,
                                           get_aio_completion(c)));
    ctx = NULL;

    tracepoint(librbd, aio_close_image_exit, 0);
    return 0;
  }

  int Image::resize(uint64_t size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    librbd::NoOpProgressContext prog_ctx;
    int r = ictx->operations->resize(size, true, prog_ctx);
    tracepoint(librbd, resize_exit, r);
    return r;
  }

  int Image::resize2(uint64_t size, bool allow_shrink, librbd::ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    int r = ictx->operations->resize(size, allow_shrink, pctx);
    tracepoint(librbd, resize_exit, r);
    return r;
  }

  int Image::resize_with_progress(uint64_t size, librbd::ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
    int r = ictx->operations->resize(size, true, pctx);
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

  int Image::get_group(group_info_t *group_info, size_t group_info_size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, image_get_group_enter, ictx->name.c_str());

    if (group_info_size != sizeof(group_info_t)) {
      tracepoint(librbd, image_get_group_exit, -ERANGE);
      return -ERANGE;
    }

    int r = librbd::api::Group<>::image_get_group(ictx, group_info);
    tracepoint(librbd, image_get_group_exit, r);
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
    int r = ictx->operations->update_features(features, enabled);
    tracepoint(librbd, update_features_exit, r);
    return r;
  }

  int Image::get_op_features(uint64_t *op_features)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Image<>::get_op_features(ictx, op_features);
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

  int Image::get_create_timestamp(struct timespec *timestamp)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_create_timestamp_enter, ictx, ictx->name.c_str(),
               ictx->read_only);
    utime_t time = ictx->get_create_timestamp();
    time.to_timespec(timestamp);
    tracepoint(librbd, get_create_timestamp_exit, 0, timestamp);
    return 0;
  }

  int Image::get_access_timestamp(struct timespec *timestamp)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_access_timestamp_enter, ictx, ictx->name.c_str(),
               ictx->read_only);
    {
      RWLock::RLocker timestamp_locker(ictx->timestamp_lock);
      utime_t time = ictx->get_access_timestamp();
      time.to_timespec(timestamp);
    }
    tracepoint(librbd, get_access_timestamp_exit, 0, timestamp);
    return 0;
  }

  int Image::get_modify_timestamp(struct timespec *timestamp)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_modify_timestamp_enter, ictx, ictx->name.c_str(),
               ictx->read_only);
    {
      RWLock::RLocker timestamp_locker(ictx->timestamp_lock);
      utime_t time = ictx->get_modify_timestamp();
      time.to_timespec(timestamp);
    }
    tracepoint(librbd, get_modify_timestamp_exit, 0, timestamp);
    return 0;
  }

  int Image::overlap(uint64_t *overlap)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, get_overlap_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::get_overlap(ictx, overlap);
    tracepoint(librbd, get_overlap_exit, r, *overlap);
    return r;
  }

  int Image::get_name(std::string *name)
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx *>(ctx);
    *name = ictx->name;
    return 0;
  }

  int Image::get_id(std::string *id)
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx *>(ctx);
    if (ictx->old_format) {
      return -EINVAL;
    }
    *id = ictx->id;
    return 0;
  }

  std::string Image::get_block_name_prefix()
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx *>(ctx);
    return ictx->object_prefix;
  }

  int64_t Image::get_data_pool_id()
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx *>(ctx);
    return ictx->data_ctx.get_id();
  }

  int Image::parent_info(string *parent_pool_name, string *parent_name,
			 string *parent_snap_name)
  {
    librbd::linked_image_spec_t parent_image;
    librbd::snap_spec_t parent_snap;
    int r = get_parent(&parent_image, &parent_snap);
    if (r >= 0) {
      if (parent_pool_name != nullptr) {
        *parent_pool_name = parent_image.pool_name;
      }
      if (parent_name != nullptr) {
        *parent_name = parent_image.image_name;
      }
      if (parent_snap_name != nullptr) {
        *parent_snap_name = parent_snap.name;
      }
    }
    return r;
  }

  int Image::parent_info2(string *parent_pool_name, string *parent_name,
                          string *parent_id, string *parent_snap_name)
  {
    librbd::linked_image_spec_t parent_image;
    librbd::snap_spec_t parent_snap;
    int r = get_parent(&parent_image, &parent_snap);
    if (r >= 0) {
      if (parent_pool_name != nullptr) {
        *parent_pool_name = parent_image.pool_name;
      }
      if (parent_name != nullptr) {
        *parent_name = parent_image.image_name;
      }
      if (parent_id != nullptr) {
        *parent_id = parent_image.image_id;
      }
      if (parent_snap_name != nullptr) {
        *parent_snap_name = parent_snap.name;
      }
    }
    return r;
  }

  int Image::get_parent(linked_image_spec_t *parent_image,
                        snap_spec_t *parent_snap)
  {
    auto ictx = reinterpret_cast<ImageCtx*>(ctx);
    tracepoint(librbd, get_parent_info_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(), ictx->read_only);

    int r = librbd::api::Image<>::get_parent(ictx, parent_image, parent_snap);

    tracepoint(librbd, get_parent_info_exit, r,
               parent_image->pool_name.c_str(),
               parent_image->image_name.c_str(),
               parent_image->image_id.c_str(),
               parent_snap->name.c_str());
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

  int Image::lock_acquire(rbd_lock_mode_t lock_mode)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, lock_acquire_enter, ictx, lock_mode);
    int r = librbd::lock_acquire(ictx, lock_mode);
    tracepoint(librbd, lock_acquire_exit, ictx, r);
    return r;
  }

  int Image::lock_release()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, lock_release_enter, ictx);
    int r = librbd::lock_release(ictx);
    tracepoint(librbd, lock_release_exit, ictx, r);
    return r;
  }

  int Image::lock_get_owners(rbd_lock_mode_t *lock_mode,
                             std::list<std::string> *lock_owners)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, lock_get_owners_enter, ictx);
    int r = librbd::lock_get_owners(ictx, lock_mode, lock_owners);
    tracepoint(librbd, lock_get_owners_exit, ictx, r);
    return r;
  }

  int Image::lock_break(rbd_lock_mode_t lock_mode,
                        const std::string &lock_owner)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, lock_break_enter, ictx, lock_mode, lock_owner.c_str());
    int r = librbd::lock_break(ictx, lock_mode, lock_owner);
    tracepoint(librbd, lock_break_exit, ictx, r);
    return r;
  }

  int Image::rebuild_object_map(ProgressContext &prog_ctx)
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx*>(ctx);
    return ictx->operations->rebuild_object_map(prog_ctx);
  }

  int Image::check_object_map(ProgressContext &prog_ctx)
  {
    ImageCtx *ictx = reinterpret_cast<ImageCtx*>(ctx);
    return ictx->operations->check_object_map(prog_ctx);
  }

  int Image::copy(IoCtx& dest_io_ctx, const char *destname)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
    ImageOptions opts;
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx, 0);
    tracepoint(librbd, copy_exit, r);
    return r;
  }

  int Image::copy2(Image& dest)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    tracepoint(librbd, copy2_enter, srcctx, srcctx->name.c_str(), srcctx->snap_name.c_str(), srcctx->read_only, destctx, destctx->name.c_str(), destctx->snap_name.c_str(), destctx->read_only);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(srcctx, destctx, prog_ctx, 0);
    tracepoint(librbd, copy2_exit, r);
    return r;
  }

  int Image::copy3(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy3_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, opts.opts);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx, 0);
    tracepoint(librbd, copy3_exit, r);
    return r;
  }

  int Image::copy4(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts, size_t sparse_size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy4_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, opts.opts, sparse_size);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx, sparse_size);
    tracepoint(librbd, copy4_exit, r);
    return r;
  }

  int Image::copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
				librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname);
    ImageOptions opts;
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, pctx, 0);
    tracepoint(librbd, copy_exit, r);
    return r;
  }

  int Image::copy_with_progress2(Image& dest, librbd::ProgressContext &pctx)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    tracepoint(librbd, copy2_enter, srcctx, srcctx->name.c_str(), srcctx->snap_name.c_str(), srcctx->read_only, destctx, destctx->name.c_str(), destctx->snap_name.c_str(), destctx->read_only);
    int r = librbd::copy(srcctx, destctx, pctx, 0);
    tracepoint(librbd, copy2_exit, r);
    return r;
  }

  int Image::copy_with_progress3(IoCtx& dest_io_ctx, const char *destname,
				 ImageOptions& opts,
				 librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy3_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, opts.opts);
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, pctx, 0);
    tracepoint(librbd, copy3_exit, r);
    return r;
  }

  int Image::copy_with_progress4(IoCtx& dest_io_ctx, const char *destname,
				 ImageOptions& opts,
				 librbd::ProgressContext &pctx,
				 size_t sparse_size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, copy4_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, opts.opts, sparse_size);
    int r = librbd::copy(ictx, dest_io_ctx, destname, opts, pctx, sparse_size);
    tracepoint(librbd, copy4_exit, r);
    return r;
  }

  int Image::deep_copy(IoCtx& dest_io_ctx, const char *destname,
                       ImageOptions& opts)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, deep_copy_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(), ictx->read_only,
               dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(),
               destname, opts.opts);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::api::Image<>::deep_copy(ictx, dest_io_ctx, destname, opts,
                                            prog_ctx);
    tracepoint(librbd, deep_copy_exit, r);
    return r;
  }

  int Image::deep_copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
                                     ImageOptions& opts,
                                     librbd::ProgressContext &prog_ctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, deep_copy_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(), ictx->read_only,
               dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(),
               destname, opts.opts);
    int r = librbd::api::Image<>::deep_copy(ictx, dest_io_ctx, destname, opts,
                                            prog_ctx);
    tracepoint(librbd, deep_copy_exit, r);
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
    std::vector<linked_image_spec_t> images;
    int r = list_children3(&images);
    if (r < 0) {
      return r;
    }

    for (auto& image : images) {
      if (!image.trash) {
        children->insert({image.pool_name, image.image_name});
      }
    }
    return 0;
  }

  int Image::list_children2(vector<librbd::child_info_t> *children)
  {
    std::vector<linked_image_spec_t> images;
    int r = list_children3(&images);
    if (r < 0) {
      return r;
    }

    for (auto& image : images) {
      children->push_back({
        .pool_name = image.pool_name,
        .image_name = image.image_name,
        .image_id = image.image_id,
        .trash = image.trash});
    }

    return 0;
  }

  int Image::list_children3(std::vector<linked_image_spec_t> *images)
  {
    auto ictx = reinterpret_cast<ImageCtx*>(ctx);
    tracepoint(librbd, list_children_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(), ictx->read_only);

    int r = librbd::api::Image<>::list_children(ictx, images);
    if (r >= 0) {
      for (auto& it : *images) {
        tracepoint(librbd, list_children_entry, it.pool_name.c_str(),
                   it.image_name.c_str());
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
    int r = ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					  snap_name);
    tracepoint(librbd, snap_create_exit, r);
    return r;
  }

  int Image::snap_remove(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_remove_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::snap_remove(ictx, snap_name, 0, prog_ctx);
    tracepoint(librbd, snap_remove_exit, r);
    return r;
  }

  int Image::snap_remove2(const char *snap_name, uint32_t flags, ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_remove2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name, flags);
    int r = librbd::snap_remove(ictx, snap_name, flags, pctx);
    tracepoint(librbd, snap_remove_exit, r);
    return r;
  }

  int Image::snap_remove_by_id(uint64_t snap_id)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Snapshot<>::remove(ictx, snap_id);
  }

  int Image::snap_rollback(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    librbd::NoOpProgressContext prog_ctx;
    int r = ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(), snap_name, prog_ctx);
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
    int r = ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(), snap_name, prog_ctx);
    tracepoint(librbd, snap_rollback_exit, r);
    return r;
  }

  int Image::snap_protect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_protect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(), snap_name);
    tracepoint(librbd, snap_protect_exit, r);
    return r;
  }

  int Image::snap_unprotect(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_unprotect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(), snap_name);
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
    int r = librbd::snap_exists(ictx, cls::rbd::UserSnapshotNamespace(), snap_name, &exists);
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
    int r = librbd::snap_exists(ictx, cls::rbd::UserSnapshotNamespace(), snap_name, exists);
    tracepoint(librbd, snap_exists_exit, r, *exists);
    return r;
  }

  int Image::snap_get_timestamp(uint64_t snap_id, struct timespec *timestamp)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_get_timestamp_enter, ictx, ictx->name.c_str());
    int r = librbd::snap_get_timestamp(ictx, snap_id, timestamp);
    tracepoint(librbd, snap_get_timestamp_exit, r);
    return r;
  }

  int Image::snap_get_limit(uint64_t *limit)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_get_limit_enter, ictx, ictx->name.c_str());
    int r = librbd::snap_get_limit(ictx, limit);
    tracepoint(librbd, snap_get_limit_exit, r, *limit);
    return r;
  }

  int Image::snap_get_namespace_type(uint64_t snap_id,
				     snap_namespace_type_t *namespace_type) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_get_namespace_type_enter, ictx, ictx->name.c_str());
    int r = librbd::api::Snapshot<>::get_namespace_type(ictx, snap_id, namespace_type);
    tracepoint(librbd, snap_get_namespace_type_exit, r);
    return r;
  }

  int Image::snap_get_group_namespace(uint64_t snap_id,
			              snap_group_namespace_t *group_snap,
                                      size_t group_snap_size) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_get_group_namespace_enter, ictx,
               ictx->name.c_str());

    if (group_snap_size != sizeof(snap_group_namespace_t)) {
      tracepoint(librbd, snap_get_group_namespace_exit, -ERANGE);
      return -ERANGE;
    }

    int r = librbd::api::Snapshot<>::get_group_namespace(ictx, snap_id,
                                                         group_snap);
    tracepoint(librbd, snap_get_group_namespace_exit, r);
    return r;
  }

  int Image::snap_get_trash_namespace(uint64_t snap_id,
                                      std::string* original_name) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Snapshot<>::get_trash_namespace(ictx, snap_id,
                                                        original_name);
  }

  int Image::snap_set_limit(uint64_t limit)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;

    tracepoint(librbd, snap_set_limit_enter, ictx, ictx->name.c_str(), limit);
    int r = ictx->operations->snap_set_limit(limit);
    tracepoint(librbd, snap_set_limit_exit, r);
    return r;
  }

  int Image::snap_set(const char *snap_name)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, snap_set_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
    int r = librbd::api::Image<>::snap_set(
      ictx, cls::rbd::UserSnapshotNamespace(), snap_name);
    tracepoint(librbd, snap_set_exit, r);
    return r;
  }

  int Image::snap_set_by_id(uint64_t snap_id)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Image<>::snap_set(ictx, snap_id);
  }

  ssize_t Image::read(uint64_t ofs, size_t len, bufferlist& bl)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    bufferptr ptr(len);
    bl.push_back(std::move(ptr));
    
    int r = ictx->io_work_queue->read(ofs, len, io::ReadResult{&bl}, 0);
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
    
    int r = ictx->io_work_queue->read(ofs, len, io::ReadResult{&bl}, op_flags);
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
    int r = librbd::api::DiffIterate<>::diff_iterate(ictx,
						     cls::rbd::UserSnapshotNamespace(),
						     fromsnapname, ofs,
                                                     len, true, false, cb, arg);
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
    int r = librbd::api::DiffIterate<>::diff_iterate(ictx,
						     cls::rbd::UserSnapshotNamespace(),
						     fromsnapname, ofs,
                                                     len, include_parent,
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

    int r = ictx->io_work_queue->write(ofs, len, bufferlist{bl}, 0);
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

    int r = ictx->io_work_queue->write(ofs, len, bufferlist{bl}, op_flags);
    tracepoint(librbd, write_exit, r);
    return r;
  }

  int Image::discard(uint64_t ofs, uint64_t len)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len);
    if (len > std::numeric_limits<int32_t>::max()) {
        tracepoint(librbd, discard_exit, -EINVAL);
        return -EINVAL;
    }
    int r = ictx->io_work_queue->discard(ofs, len, ictx->skip_partial_discard);
    tracepoint(librbd, discard_exit, r);
    return r;
  }

  ssize_t Image::writesame(uint64_t ofs, size_t len, bufferlist& bl, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, writesame_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
               ictx->read_only, ofs, len, bl.length() <= 0 ? NULL : bl.c_str(), bl.length(),
               op_flags);
    if (bl.length() <= 0 || len % bl.length() ||
        len > std::numeric_limits<int>::max()) {
      tracepoint(librbd, writesame_exit, -EINVAL);
      return -EINVAL;
    }

    bool discard_zero = ictx->config.get_val<bool>("rbd_discard_on_zeroed_write_same");
    if (discard_zero && mem_is_zero(bl.c_str(), bl.length())) {
      int r = ictx->io_work_queue->discard(ofs, len, false);
      tracepoint(librbd, writesame_exit, r);
      return r;
    }

    int r = ictx->io_work_queue->writesame(ofs, len, bufferlist{bl}, op_flags);
    tracepoint(librbd, writesame_exit, r);
    return r;
  }

  ssize_t Image::compare_and_write(uint64_t ofs, size_t len,
                                   ceph::bufferlist &cmp_bl, ceph::bufferlist& bl,
                                   uint64_t *mismatch_off, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, compare_and_write_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(),
               ictx->read_only, ofs, len, cmp_bl.length() < len ? NULL : cmp_bl.c_str(),
               bl.length() < len ? NULL : bl.c_str(), op_flags);

    if (bl.length() < len) {
      tracepoint(librbd, write_exit, -EINVAL);
      return -EINVAL;
    }

    int r = ictx->io_work_queue->compare_and_write(ofs, len, bufferlist{cmp_bl},
                                                   bufferlist{bl}, mismatch_off,
                                                   op_flags);

    tracepoint(librbd, compare_and_write_exit, r);

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
    ictx->io_work_queue->aio_write(get_aio_completion(c), off, len,
                                   bufferlist{bl}, 0);

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
    ictx->io_work_queue->aio_write(get_aio_completion(c), off, len,
                                   bufferlist{bl}, op_flags);

    tracepoint(librbd, aio_write_exit, 0);
    return 0;
  }

  int Image::aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, c->pc);
    ictx->io_work_queue->aio_discard(get_aio_completion(c), off, len, ictx->skip_partial_discard);
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

    ictx->io_work_queue->aio_read(get_aio_completion(c), off, len,
                                  io::ReadResult{&bl}, 0);
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

    ictx->io_work_queue->aio_read(get_aio_completion(c), off, len,
                                  io::ReadResult{&bl}, op_flags);
    tracepoint(librbd, aio_read_exit, 0);
    return 0;
  }

  int Image::flush()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = ictx->io_work_queue->flush();
    tracepoint(librbd, flush_exit, r);
    return r;
  }

  int Image::aio_flush(RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, c->pc);
    ictx->io_work_queue->aio_flush(get_aio_completion(c));
    tracepoint(librbd, aio_flush_exit, 0);
    return 0;
  }

  int Image::aio_writesame(uint64_t off, size_t len, bufferlist& bl,
                           RBD::AioCompletion *c, int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_writesame_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
               ictx->read_only, off, len, bl.length() <= len ? NULL : bl.c_str(), bl.length(),
               c->pc, op_flags);
    if (bl.length() <= 0 || len % bl.length()) {
      tracepoint(librbd, aio_writesame_exit, -EINVAL);
      return -EINVAL;
    }

    bool discard_zero = ictx->config.get_val<bool>("rbd_discard_on_zeroed_write_same");
    if (discard_zero && mem_is_zero(bl.c_str(), bl.length())) {
      ictx->io_work_queue->aio_discard(get_aio_completion(c), off, len, false);
      tracepoint(librbd, aio_writesame_exit, 0);
      return 0;
    }

    ictx->io_work_queue->aio_writesame(get_aio_completion(c), off, len,
                                       bufferlist{bl}, op_flags);
    tracepoint(librbd, aio_writesame_exit, 0);
    return 0;
  }

  int Image::aio_compare_and_write(uint64_t off, size_t len,
                                   ceph::bufferlist& cmp_bl, ceph::bufferlist& bl,
                                   RBD::AioCompletion *c, uint64_t *mismatch_off,
                                   int op_flags)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, aio_compare_and_write_enter, ictx, ictx->name.c_str(),
               ictx->snap_name.c_str(),
               ictx->read_only, off, len, cmp_bl.length() < len ? NULL : cmp_bl.c_str(),
               bl.length() < len ? NULL : bl.c_str(), c->pc, op_flags);

    if (bl.length() < len) {
      tracepoint(librbd, compare_and_write_exit, -EINVAL);
      return -EINVAL;
    }

    ictx->io_work_queue->aio_compare_and_write(get_aio_completion(c), off, len,
                                               bufferlist{cmp_bl}, bufferlist{bl},
                                               mismatch_off, op_flags, false);

    tracepoint(librbd, aio_compare_and_write_exit, 0);

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
    io::AioCompletion *cs[numcomp];
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, poll_io_events_enter, ictx, numcomp);
    int r = librbd::poll_io_events(ictx, cs, numcomp);
    tracepoint(librbd, poll_io_events_exit, r);
    if (r > 0) {
      for (int i = 0; i < r; ++i)
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
    int r = ictx->operations->metadata_set(key, value);
    tracepoint(librbd, metadata_set_exit, r);
    return r;
  }

  int Image::metadata_remove(const std::string &key)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, metadata_remove_enter, ictx, key.c_str());
    int r = ictx->operations->metadata_remove(key);
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
    return librbd::api::Mirror<>::image_enable(ictx, false);
  }

  int Image::mirror_image_disable(bool force) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Mirror<>::image_disable(ictx, force);
  }

  int Image::mirror_image_promote(bool force) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Mirror<>::image_promote(ictx, force);
  }

  int Image::mirror_image_demote() {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Mirror<>::image_demote(ictx);
  }

  int Image::mirror_image_resync()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Mirror<>::image_resync(ictx);
  }

  int Image::mirror_image_get_info(mirror_image_info_t *mirror_image_info,
                                   size_t info_size) {
    ImageCtx *ictx = (ImageCtx *)ctx;

    if (sizeof(mirror_image_info_t) != info_size) {
      return -ERANGE;
    }

    return librbd::api::Mirror<>::image_get_info(ictx, mirror_image_info);
  }

  int Image::mirror_image_get_status(mirror_image_status_t *mirror_image_status,
				     size_t status_size) {
    ImageCtx *ictx = (ImageCtx *)ctx;

    if (sizeof(mirror_image_status_t) != status_size) {
      return -ERANGE;
    }

    return librbd::api::Mirror<>::image_get_status(ictx, mirror_image_status);
  }

  int Image::mirror_image_get_instance_id(std::string *instance_id) {
    ImageCtx *ictx = (ImageCtx *)ctx;

    return librbd::api::Mirror<>::image_get_instance_id(ictx, instance_id);
  }

  int Image::aio_mirror_image_promote(bool force, RBD::AioCompletion *c) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::api::Mirror<>::image_promote(
      ictx, force, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                                       get_aio_completion(c)));
    return 0;
  }

  int Image::aio_mirror_image_demote(RBD::AioCompletion *c) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::api::Mirror<>::image_demote(
      ictx, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                                get_aio_completion(c)));
    return 0;
  }

  int Image::aio_mirror_image_get_info(mirror_image_info_t *mirror_image_info,
                                       size_t info_size,
                                       RBD::AioCompletion *c) {
    ImageCtx *ictx = (ImageCtx *)ctx;

    if (sizeof(mirror_image_info_t) != info_size) {
      return -ERANGE;
    }

    librbd::api::Mirror<>::image_get_info(
      ictx, mirror_image_info,
      new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                          get_aio_completion(c)));
    return 0;
  }

  int Image::aio_mirror_image_get_status(mirror_image_status_t *status,
                                         size_t status_size,
                                         RBD::AioCompletion *c) {
    ImageCtx *ictx = (ImageCtx *)ctx;

    if (sizeof(mirror_image_status_t) != status_size) {
      return -ERANGE;
    }

    librbd::api::Mirror<>::image_get_status(
      ictx, status, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                                        get_aio_completion(c)));
    return 0;
  }

  int Image::update_watch(UpdateWatchCtx *wctx, uint64_t *handle) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, update_watch_enter, ictx, wctx);
    int r = ictx->state->register_update_watcher(wctx, handle);
    tracepoint(librbd, update_watch_exit, r, *handle);
    return r;
  }

  int Image::update_unwatch(uint64_t handle) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, update_unwatch_enter, ictx, handle);
    int r = ictx->state->unregister_update_watcher(handle);
    tracepoint(librbd, update_unwatch_exit, r);
    return r;
  }

  int Image::list_watchers(std::list<librbd::image_watcher_t> &watchers) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    tracepoint(librbd, list_watchers_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
    int r = librbd::list_watchers(ictx, watchers);
    if (r >= 0) {
      for (auto &watcher : watchers) {
	tracepoint(librbd, list_watchers_entry, watcher.addr.c_str(), watcher.id, watcher.cookie);
      }
    }
    tracepoint(librbd, list_watchers_exit, r, watchers.size());
    return r;
  }

  int Image::config_list(std::vector<config_option_t> *options) {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::api::Config<>::list(ictx, options);
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

extern "C" int rbd_image_options_is_set(rbd_image_options_t opts, int optname,
                                        bool* is_set)
{
  return librbd::image_options_is_set(opts, optname, is_set);
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
  return librbd::api::Mirror<>::mode_get(io_ctx, mirror_mode);
}

extern "C" int rbd_mirror_mode_set(rados_ioctx_t p,
                                   rbd_mirror_mode_t mirror_mode) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::api::Mirror<>::mode_set(io_ctx, mirror_mode);
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
  int r = librbd::api::Mirror<>::peer_add(io_ctx, &uuid_str, cluster_name,
                                          client_name);
  if (r >= 0) {
    strncpy(uuid, uuid_str.c_str(), uuid_max_length);
    uuid[uuid_max_length - 1] = '\0';
  }
  return r;
}

extern "C" int rbd_mirror_peer_remove(rados_ioctx_t p, const char *uuid) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  int r = librbd::api::Mirror<>::peer_remove(io_ctx, uuid);
  return r;
}

extern "C" int rbd_mirror_peer_list(rados_ioctx_t p,
                                    rbd_mirror_peer_t *peers, int *max_peers) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  std::vector<librbd::mirror_peer_t> peer_vector;
  int r = librbd::api::Mirror<>::peer_list(io_ctx, &peer_vector);
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
  return librbd::api::Mirror<>::peer_set_client(io_ctx, uuid, client_name);
}

extern "C" int rbd_mirror_peer_set_cluster(rados_ioctx_t p, const char *uuid,
                                           const char *cluster_name) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::api::Mirror<>::peer_set_cluster(io_ctx, uuid, cluster_name);
}

extern "C" int rbd_mirror_peer_get_attributes(
    rados_ioctx_t p, const char *uuid, char *keys, size_t *max_key_len,
    char *values, size_t *max_val_len, size_t *key_value_count) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  std::map<std::string, std::string> attributes;
  int r = librbd::api::Mirror<>::peer_get_attributes(io_ctx, uuid, &attributes);
  if (r < 0) {
    return r;
  }

  size_t key_total_len = 0, val_total_len = 0;
  for (auto& it : attributes) {
    key_total_len += it.first.size() + 1;
    val_total_len += it.second.length() + 1;
  }

  bool too_short = ((*max_key_len < key_total_len) ||
                    (*max_val_len < val_total_len));

  *max_key_len = key_total_len;
  *max_val_len = val_total_len;
  *key_value_count = attributes.size();
  if (too_short) {
    return -ERANGE;
  }

  char *keys_p = keys;
  char *values_p = values;
  for (auto& it : attributes) {
    strncpy(keys_p, it.first.c_str(), it.first.size() + 1);
    keys_p += it.first.size() + 1;

    strncpy(values_p, it.second.c_str(), it.second.length() + 1);
    values_p += it.second.length() + 1;
  }

  return 0;
}

extern "C" int rbd_mirror_peer_set_attributes(
    rados_ioctx_t p, const char *uuid, const char *keys, const char *values,
    size_t count) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  std::map<std::string, std::string> attributes;

  for (size_t i = 0; i < count; ++i) {
    const char* key = keys;
    keys += strlen(key) + 1;
    const char* value = values;
    values += strlen(value) + 1;
    attributes[key] = value;
  }

  return librbd::api::Mirror<>::peer_set_attributes(io_ctx, uuid, attributes);
}

extern "C" int rbd_mirror_image_status_list(rados_ioctx_t p,
    const char *start_id, size_t max, char **image_ids,
    rbd_mirror_image_status_t *images, size_t *len) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  std::map<std::string, librbd::mirror_image_status_t> cpp_images;

  int r = librbd::api::Mirror<>::image_status_list(io_ctx, start_id, max,
                                                   &cpp_images);
  if (r < 0) {
    return r;
  }

  size_t i = 0;
  for (auto &it : cpp_images) {
    ceph_assert(i < max);
    const std::string &image_id = it.first;
    image_ids[i] = strdup(image_id.c_str());
    mirror_image_status_cpp_to_c(it.second, &images[i]);
    i++;
  }
  *len = i;
  return 0;
}

extern "C" void rbd_mirror_image_status_list_cleanup(char **image_ids,
    rbd_mirror_image_status_t *images, size_t len) {
  for (size_t i = 0; i < len; i++) {
    free(image_ids[i]);
    free(images[i].name);
    free(images[i].info.global_id);
    free(images[i].description);
  }
}

extern "C" int rbd_mirror_image_status_summary(rados_ioctx_t p,
    rbd_mirror_image_status_state_t *states, int *counts, size_t *maxlen) {

  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  std::map<librbd::mirror_image_status_state_t, int> states_;
  int r = librbd::api::Mirror<>::image_status_summary(io_ctx, &states_);
  if (r < 0) {
    return r;
  }

  size_t i = 0;
  for (auto &it : states_) {
    if (i == *maxlen) {
      return -ERANGE;
    }
    states[i] = it.first;
    counts[i] = it.second;
    i++;
  }
  *maxlen = i;
  return 0;
}

extern "C" int rbd_mirror_image_instance_id_list(
    rados_ioctx_t p, const char *start_id, size_t max, char **image_ids,
    char **instance_ids, size_t *len) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  std::map<std::string, std::string> cpp_instance_ids;

  int r = librbd::api::Mirror<>::image_instance_id_list(io_ctx, start_id, max,
                                                        &cpp_instance_ids);
  if (r < 0) {
    return r;
  }

  size_t i = 0;
  for (auto &it : cpp_instance_ids) {
    ceph_assert(i < max);
    image_ids[i] = strdup(it.first.c_str());
    instance_ids[i] = strdup(it.second.c_str());
    i++;
  }
  *len = i;
  return 0;
}

extern "C" void rbd_mirror_image_instance_id_list_cleanup(
    char **image_ids, char **instance_ids, size_t len) {
  for (size_t i = 0; i < len; i++) {
    free(image_ids[i]);
    free(instance_ids[i]);
  }
}

/* helpers */

extern "C" void rbd_image_spec_cleanup(rbd_image_spec_t *image)
{
  free(image->id);
  free(image->name);
}

extern "C" void rbd_image_spec_list_cleanup(rbd_image_spec_t *images,
                                            size_t num_images)
{
  for (size_t idx = 0; idx < num_images; ++idx) {
    rbd_image_spec_cleanup(&images[idx]);
  }
}

extern "C" void rbd_linked_image_spec_cleanup(rbd_linked_image_spec_t *image)
{
  free(image->pool_name);
  free(image->pool_namespace);
  free(image->image_id);
  free(image->image_name);
}

extern "C" void rbd_linked_image_spec_list_cleanup(
    rbd_linked_image_spec_t *images, size_t num_images)
{
  for (size_t idx = 0; idx < num_images; ++idx) {
    rbd_linked_image_spec_cleanup(&images[idx]);
  }
}

extern "C" void rbd_snap_spec_cleanup(rbd_snap_spec_t *snap)
{
  free(snap->name);
}

/* images */
extern "C" int rbd_list(rados_ioctx_t p, char *names, size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, list_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id());
  std::vector<librbd::image_spec_t> cpp_image_specs;
  int r = librbd::api::Image<>::list_images(io_ctx, &cpp_image_specs);
  if (r < 0) {
    tracepoint(librbd, list_exit, r, *size);
    return r;
  }

  size_t expected_size = 0;

  for (auto& it : cpp_image_specs) {
    expected_size += it.name.size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    tracepoint(librbd, list_exit, -ERANGE, *size);
    return -ERANGE;
  }

  if (names == NULL) {
    tracepoint(librbd, list_exit, -EINVAL, *size);
    return -EINVAL;
  }

  for (auto& it : cpp_image_specs) {
    const char* name = it.name.c_str();
    tracepoint(librbd, list_entry, name);
    strcpy(names, name);
    names += strlen(names) + 1;
  }
  tracepoint(librbd, list_exit, (int)expected_size, *size);
  return (int)expected_size;
}

extern "C" int rbd_list2(rados_ioctx_t p, rbd_image_spec_t *images,
                         size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, list_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id());
  memset(images, 0, sizeof(*images) * *size);
  std::vector<librbd::image_spec_t> cpp_image_specs;
  int r = librbd::api::Image<>::list_images(io_ctx, &cpp_image_specs);
  if (r < 0) {
    tracepoint(librbd, list_exit, r, *size);
    return r;
  }

  size_t expected_size = cpp_image_specs.size();
  if (*size < expected_size) {
    *size = expected_size;
    tracepoint(librbd, list_exit, -ERANGE, *size);
    return -ERANGE;
  }

  *size = expected_size;
  for (size_t idx = 0; idx < expected_size; ++idx) {
    images[idx].id = strdup(cpp_image_specs[idx].id.c_str());
    images[idx].name = strdup(cpp_image_specs[idx].name.c_str());
  }
  tracepoint(librbd, list_exit, 0, *size);
  return 0;
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
  int r = librbd::create(io_ctx, name, "", size, opts_, "", "", false);
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
  int r = librbd::clone(p_ioc, nullptr, p_name, p_snap_name, c_ioc, nullptr,
                        c_name, c_opts_, "", "");
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
  int r = librbd::api::Image<>::remove(io_ctx, name, "", prog_ctx);
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
  int r = librbd::api::Image<>::remove(io_ctx, name, "", prog_ctx);
  tracepoint(librbd, remove_exit, r);
  return r;
}

extern "C" int rbd_trash_move(rados_ioctx_t p, const char *name,
                              uint64_t delay) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, trash_move_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), name);
  int r = librbd::api::Trash<>::move(io_ctx, RBD_TRASH_IMAGE_SOURCE_USER, name,
                                     delay);
  tracepoint(librbd, trash_move_exit, r);
  return r;
}

extern "C" int rbd_trash_get(rados_ioctx_t io, const char *id,
                             rbd_trash_image_info_t *info) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  librbd::trash_image_info_t cpp_info;
  int r = librbd::api::Trash<>::get(io_ctx, id, &cpp_info);
  if (r < 0) {
    return r;
  }

  trash_image_info_cpp_to_c(cpp_info, info);
  return 0;
}

extern "C" void rbd_trash_get_cleanup(rbd_trash_image_info_t *info) {
  free(info->id);
  free(info->name);
}

extern "C" int rbd_trash_list(rados_ioctx_t p, rbd_trash_image_info_t *entries,
                              size_t *num_entries) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, trash_list_enter,
             io_ctx.get_pool_name().c_str(), io_ctx.get_id());
  memset(entries, 0, sizeof(*entries) * *num_entries);

  vector<librbd::trash_image_info_t> cpp_entries;
  int r = librbd::api::Trash<>::list(io_ctx, cpp_entries);
  if (r < 0) {
    tracepoint(librbd, trash_list_exit, r, *num_entries);
    return r;
  }

  if (*num_entries < cpp_entries.size()) {
    *num_entries = cpp_entries.size();
    tracepoint(librbd, trash_list_exit, -ERANGE, *num_entries);
    return -ERANGE;
  }

  int i=0;
  for (const auto &entry : cpp_entries) {
    trash_image_info_cpp_to_c(entry, &entries[i++]);
  }
  *num_entries = cpp_entries.size();

  return *num_entries;
}

extern "C" void rbd_trash_list_cleanup(rbd_trash_image_info_t *entries,
                                       size_t num_entries) {
  for (size_t i=0; i < num_entries; i++) {
    rbd_trash_get_cleanup(&entries[i]);
  }
}

extern "C" int rbd_trash_purge(rados_ioctx_t io, time_t expire_ts,
				                       float threshold) {
	librados::IoCtx io_ctx;
	librados::IoCtx::from_rados_ioctx_t(io, io_ctx);
	TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
	tracepoint(librbd, trash_purge_enter, io_ctx.get_pool_name().c_str(),
	           io_ctx.get_id(), expire_ts, threshold);
        librbd::NoOpProgressContext nop_pctx;
        int r = librbd::api::Trash<>::purge(io_ctx, expire_ts, threshold, nop_pctx);
	tracepoint(librbd, trash_purge_exit, r);
	return r;
}

extern "C" int rbd_trash_purge_with_progress(rados_ioctx_t io, time_t expire_ts,
				float threshold, librbd_progress_fn_t cb, void* cbdata) {
	librados::IoCtx io_ctx;
	librados::IoCtx::from_rados_ioctx_t(io, io_ctx);
	TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
	tracepoint(librbd, trash_purge_enter, io_ctx.get_pool_name().c_str(),
	           io_ctx.get_id(), expire_ts, threshold);
	librbd::CProgressContext pctx(cb, cbdata);
	int r = librbd::api::Trash<>::purge(io_ctx, expire_ts, threshold, pctx);
	tracepoint(librbd, trash_purge_exit, r);
	return r;
}

extern "C" int rbd_trash_remove(rados_ioctx_t p, const char *image_id,
                                bool force) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, trash_remove_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_id, force);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::api::Trash<>::remove(io_ctx, image_id, force, prog_ctx);
  tracepoint(librbd, trash_remove_exit, r);
  return r;
}

extern "C" int rbd_trash_remove_with_progress(rados_ioctx_t p,
                                              const char *image_id,
                                              bool force,
                                              librbd_progress_fn_t cb,
                                              void *cbdata) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, trash_remove_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_id, force);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::api::Trash<>::remove(io_ctx, image_id, force, prog_ctx);
  tracepoint(librbd, trash_remove_exit, r);
  return r;
}

extern "C" int rbd_trash_restore(rados_ioctx_t p, const char *id,
                                 const char *name) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, trash_undelete_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), id, name);
  int r = librbd::api::Trash<>::restore(io_ctx, RBD_TRASH_IMAGE_SOURCE_USER,
                                        id, name);
  tracepoint(librbd, trash_undelete_exit, r);
  return r;
}

extern "C" int rbd_namespace_create(rados_ioctx_t io,
                                    const char *namespace_name) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  return librbd::api::Namespace<>::create(io_ctx, namespace_name);
}

extern "C" int rbd_namespace_remove(rados_ioctx_t io,
                                    const char *namespace_name) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  return librbd::api::Namespace<>::remove(io_ctx, namespace_name);
}

extern "C" int rbd_namespace_list(rados_ioctx_t io, char *names, size_t *size) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  if (names == nullptr || size == nullptr) {
    return -EINVAL;
  }

  std::vector<std::string> cpp_names;
  int r = librbd::api::Namespace<>::list(io_ctx, &cpp_names);
  if (r < 0) {
    return r;
  }

  size_t expected_size = 0;
  for (size_t i = 0; i < cpp_names.size(); i++) {
    expected_size += cpp_names[i].size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    return -ERANGE;
  }

  *size = expected_size;
  for (int i = 0; i < (int)cpp_names.size(); i++) {
    const char* name = cpp_names[i].c_str();
    strcpy(names, name);
    names += strlen(names) + 1;
  }

  return (int)expected_size;
}

extern "C" int rbd_namespace_exists(rados_ioctx_t io,
                                    const char *namespace_name,
                                    bool *exists) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  return librbd::api::Namespace<>::exists(io_ctx, namespace_name, exists);
}

extern "C" int rbd_pool_init(rados_ioctx_t io, bool force) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  return librbd::api::Pool<>::init(io_ctx, force);
}

extern "C" void rbd_pool_stats_create(rbd_pool_stats_t *stats) {
  *stats = reinterpret_cast<rbd_pool_stats_t>(
    new librbd::api::Pool<>::StatOptions{});
}

extern "C" void rbd_pool_stats_destroy(rbd_pool_stats_t stats) {
  auto pool_stat_options =
    reinterpret_cast<librbd::api::Pool<>::StatOptions*>(stats);
  delete pool_stat_options;
}

extern "C" int rbd_pool_stats_option_add_uint64(rbd_pool_stats_t stats,
                                                int stat_option,
                                                uint64_t* stat_val) {
  auto pool_stat_options =
    reinterpret_cast<librbd::api::Pool<>::StatOptions*>(stats);
  return librbd::api::Pool<>::add_stat_option(
    pool_stat_options, static_cast<rbd_pool_stat_option_t>(stat_option),
    stat_val);
}

extern "C" int rbd_pool_stats_get(
    rados_ioctx_t io, rbd_pool_stats_t pool_stats) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(io, io_ctx);

  auto pool_stat_options =
    reinterpret_cast<librbd::api::Pool<>::StatOptions*>(pool_stats);
  return librbd::api::Pool<>::get_stats(io_ctx, pool_stat_options);
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
  int r = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx, 0);
  tracepoint(librbd, copy_exit, r);
  return r;
}

extern "C" int rbd_copy2(rbd_image_t srcp, rbd_image_t destp)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  tracepoint(librbd, copy2_enter, src, src->name.c_str(), src->snap_name.c_str(), src->read_only, dest, dest->name.c_str(), dest->snap_name.c_str(), dest->read_only);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(src, dest, prog_ctx, 0);
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
  int r = librbd::copy(ictx, dest_io_ctx, destname, c_opts_, prog_ctx, 0);
  tracepoint(librbd, copy3_exit, r);
  return r;
}

extern "C" int rbd_copy4(rbd_image_t image, rados_ioctx_t dest_p,
                         const char *destname, rbd_image_options_t c_opts, size_t sparse_size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, copy4_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, c_opts, sparse_size);
  librbd::ImageOptions c_opts_(c_opts);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::copy(ictx, dest_io_ctx, destname, c_opts_, prog_ctx, sparse_size);
  tracepoint(librbd, copy4_exit, r);
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
  int ret = librbd::copy(ictx, dest_io_ctx, destname, opts, prog_ctx, 0);
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
  int ret = librbd::copy(src, dest, prog_ctx, 0);
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
  tracepoint(librbd, copy3_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, dest_opts);
  librbd::ImageOptions dest_opts_(dest_opts);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, dest_opts_, prog_ctx, 0);
  tracepoint(librbd, copy3_exit, ret);
  return ret;
}

extern "C" int rbd_copy_with_progress4(rbd_image_t image, rados_ioctx_t dest_p,
                                       const char *destname,
                                       rbd_image_options_t dest_opts,
                                       librbd_progress_fn_t fn, void *data, size_t sparse_size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, copy4_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(), destname, dest_opts, sparse_size);
  librbd::ImageOptions dest_opts_(dest_opts);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, dest_opts_, prog_ctx, sparse_size);
  tracepoint(librbd, copy4_exit, ret);
  return ret;
}

extern "C" int rbd_deep_copy(rbd_image_t image, rados_ioctx_t dest_p,
                             const char *destname, rbd_image_options_t c_opts)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, deep_copy_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only,
             dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(),
             destname, c_opts);
  librbd::ImageOptions opts(c_opts);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::api::Image<>::deep_copy(ictx, dest_io_ctx, destname, opts,
                                          prog_ctx);
  tracepoint(librbd, deep_copy_exit, r);
  return r;
}

extern "C" int rbd_deep_copy_with_progress(rbd_image_t image,
                                           rados_ioctx_t dest_p,
                                           const char *destname,
                                           rbd_image_options_t dest_opts,
                                           librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, deep_copy_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only,
             dest_io_ctx.get_pool_name().c_str(), dest_io_ctx.get_id(),
             destname, dest_opts);
  librbd::ImageOptions opts(dest_opts);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::api::Image<>::deep_copy(ictx, dest_io_ctx, destname, opts,
                                            prog_ctx);
  tracepoint(librbd, deep_copy_exit, ret);
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

extern "C" int rbd_migration_prepare(rados_ioctx_t p, const char *image_name,
                                     rados_ioctx_t dest_p,
                                     const char *dest_image_name,
                                     rbd_image_options_t opts_)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  tracepoint(librbd, migration_prepare_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_name, dest_io_ctx.get_pool_name().c_str(),
             dest_io_ctx.get_id(), dest_image_name, opts_);
  librbd::ImageOptions opts(opts_);
  int r = librbd::api::Migration<>::prepare(io_ctx, image_name, dest_io_ctx,
                                            dest_image_name, opts);
  tracepoint(librbd, migration_prepare_exit, r);
  return r;
}

extern "C" int rbd_migration_execute(rados_ioctx_t p, const char *image_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_execute_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_name);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::api::Migration<>::execute(io_ctx, image_name, prog_ctx);
  tracepoint(librbd, migration_execute_exit, r);
  return r;
}

extern "C" int rbd_migration_execute_with_progress(rados_ioctx_t p,
                                                   const char *name,
                                                   librbd_progress_fn_t fn,
                                                   void *data)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_execute_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), name);
  librbd::CProgressContext prog_ctx(fn, data);
  int r = librbd::api::Migration<>::execute(io_ctx, name, prog_ctx);
  tracepoint(librbd, migration_execute_exit, r);
  return r;
}

extern "C" int rbd_migration_abort(rados_ioctx_t p, const char *image_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_abort_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_name);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::api::Migration<>::abort(io_ctx, image_name, prog_ctx);
  tracepoint(librbd, migration_abort_exit, r);
  return r;
}

extern "C" int rbd_migration_abort_with_progress(rados_ioctx_t p,
                                                 const char *name,
                                                 librbd_progress_fn_t fn,
                                                 void *data)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_abort_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), name);
  librbd::CProgressContext prog_ctx(fn, data);
  int r = librbd::api::Migration<>::abort(io_ctx, name, prog_ctx);
  tracepoint(librbd, migration_abort_exit, r);
  return r;
}

extern "C" int rbd_migration_commit(rados_ioctx_t p, const char *image_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_commit_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_name);
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::api::Migration<>::commit(io_ctx, image_name, prog_ctx);
  tracepoint(librbd, migration_commit_exit, r);
  return r;
}

extern "C" int rbd_migration_commit_with_progress(rados_ioctx_t p,
                                                  const char *name,
                                                  librbd_progress_fn_t fn,
                                                  void *data)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_commit_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), name);
  librbd::CProgressContext prog_ctx(fn, data);
  int r = librbd::api::Migration<>::commit(io_ctx, name, prog_ctx);
  tracepoint(librbd, migration_commit_exit, r);
  return r;
}

extern "C" int rbd_migration_status(rados_ioctx_t p, const char *image_name,
                                    rbd_image_migration_status_t *status,
                                    size_t status_size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, migration_status_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), image_name);

  if (status_size != sizeof(rbd_image_migration_status_t)) {
    tracepoint(librbd, migration_status_exit, -ERANGE);
    return -ERANGE;
  }

  librbd::image_migration_status_t cpp_status;
  int r = librbd::api::Migration<>::status(io_ctx, image_name, &cpp_status);
  if (r >= 0) {
    status->source_pool_id = cpp_status.source_pool_id;
    status->source_pool_namespace =
      strdup(cpp_status.source_pool_namespace.c_str());
    status->source_image_name = strdup(cpp_status.source_image_name.c_str());
    status->source_image_id = strdup(cpp_status.source_image_id.c_str());
    status->dest_pool_id = cpp_status.dest_pool_id;
    status->dest_pool_namespace =
      strdup(cpp_status.dest_pool_namespace.c_str());
    status->dest_image_name = strdup(cpp_status.dest_image_name.c_str());
    status->dest_image_id = strdup(cpp_status.dest_image_id.c_str());
    status->state = cpp_status.state;
    status->state_description = strdup(cpp_status.state_description.c_str());
  }

  tracepoint(librbd, migration_status_exit, r);
  return r;
}

extern "C" void rbd_migration_status_cleanup(rbd_image_migration_status_t *s)
{
  free(s->source_pool_namespace);
  free(s->source_image_name);
  free(s->source_image_id);
  free(s->dest_pool_namespace);
  free(s->dest_image_name);
  free(s->dest_image_id);
  free(s->state_description);
}

extern "C" int rbd_pool_metadata_get(rados_ioctx_t p, const char *key,
                                     char *value, size_t *vallen)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  string val_s;
  int r = librbd::api::PoolMetadata<>::get(io_ctx, key, &val_s);
  if (*vallen < val_s.size() + 1) {
    r = -ERANGE;
    *vallen = val_s.size() + 1;
  } else {
    strncpy(value, val_s.c_str(), val_s.size() + 1);
  }

  return r;
}

extern "C" int rbd_pool_metadata_set(rados_ioctx_t p, const char *key,
                                     const char *value)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  int r = librbd::api::PoolMetadata<>::set(io_ctx, key, value);
  return r;
}

extern "C" int rbd_pool_metadata_remove(rados_ioctx_t p, const char *key)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  int r = librbd::api::PoolMetadata<>::remove(io_ctx, key);
  return r;
}

extern "C" int rbd_pool_metadata_list(rados_ioctx_t p, const char *start,
                                      uint64_t max, char *key, size_t *key_len,
                                      char *value, size_t *val_len)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  map<string, bufferlist> pairs;
  int r = librbd::api::PoolMetadata<>::list(io_ctx, start, max, &pairs);
  if (r < 0) {
    return r;
  }
  size_t key_total_len = 0, val_total_len = 0;
  for (auto &it : pairs) {
    key_total_len += it.first.size() + 1;
    val_total_len += it.second.length() + 1;
  }
  if (*key_len < key_total_len || *val_len < val_total_len) {
    *key_len = key_total_len;
    *val_len = val_total_len;
    return -ERANGE;
  }
  *key_len = key_total_len;
  *val_len = val_total_len;

  char *key_p = key, *value_p = value;
  for (auto &it : pairs) {
    strncpy(key_p, it.first.c_str(), it.first.size() + 1);
    key_p += it.first.size() + 1;
    strncpy(value_p, it.second.c_str(), it.second.length());
    value_p += it.second.length();
    *value_p = '\0';
    value_p++;
  }
  return 0;
}

extern "C" int rbd_config_pool_list(rados_ioctx_t p,
                                    rbd_config_option_t *options,
                                    int *max_options) {
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);

  std::vector<librbd::config_option_t> option_vector;
  int r = librbd::api::Config<>::list(io_ctx, &option_vector);
  if (r < 0) {
    return r;
  }

  if (*max_options < static_cast<int>(option_vector.size())) {
    *max_options = static_cast<int>(option_vector.size());
    return -ERANGE;
  }

  for (int i = 0; i < static_cast<int>(option_vector.size()); ++i) {
    config_option_cpp_to_c(option_vector[i], &options[i]);
  }
  *max_options = static_cast<int>(option_vector.size());
  return 0;
}

extern "C" void rbd_config_pool_list_cleanup(rbd_config_option_t *options,
                                             int max_options) {
  for (int i = 0; i < max_options; ++i) {
    config_option_cleanup(options[i]);
  }
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

  int r = ictx->state->open(0);
  if (r >= 0) {
    *image = (rbd_image_t)ictx;
  }
  tracepoint(librbd, open_image_exit, r);
  return r;
}

extern "C" int rbd_open_by_id(rados_ioctx_t p, const char *id,
                              rbd_image_t *image, const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx("", id, snap_name, io_ctx,
						false);
  tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(),
             ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

  int r = ictx->state->open(0);
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
  ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(comp),
                                          image));
  tracepoint(librbd, aio_open_image_exit, 0);
  return 0;
}

extern "C" int rbd_aio_open_by_id(rados_ioctx_t p, const char *id,
			          rbd_image_t *image, const char *snap_name,
			          rbd_completion_t c)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx("", id, snap_name, io_ctx,
						false);
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_open_image_enter, ictx, ictx->name.c_str(),
             ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only,
             comp->pc);
  ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(comp),
                                          image));
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

  int r = ictx->state->open(0);
  if (r >= 0) {
    *image = (rbd_image_t)ictx;
  }
  tracepoint(librbd, open_image_exit, r);
  return r;
}

extern "C" int rbd_open_by_id_read_only(rados_ioctx_t p, const char *id,
			                rbd_image_t *image, const char *snap_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx("", id, snap_name, io_ctx,
						true);
  tracepoint(librbd, open_image_enter, ictx, ictx->name.c_str(),
             ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only);

  int r = ictx->state->open(0);
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
  ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(comp),
                                          image));
  tracepoint(librbd, aio_open_image_exit, 0);
  return 0;
}

extern "C" int rbd_aio_open_by_id_read_only(rados_ioctx_t p, const char *id,
				            rbd_image_t *image,
                                            const char *snap_name,
                                            rbd_completion_t c)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx("", id, snap_name, io_ctx,
						true);
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_open_image_enter, ictx, ictx->name.c_str(),
             ictx->id.c_str(), ictx->snap_name.c_str(), ictx->read_only, comp->pc);
  ictx->state->open(0, new C_OpenComplete(ictx, get_aio_completion(comp),
                                          image));
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
  ictx->state->close(new C_AioCompletion(ictx, librbd::io::AIO_TYPE_CLOSE,
                                         get_aio_completion(comp)));
  tracepoint(librbd, aio_close_image_exit, 0);
  return 0;
}

extern "C" int rbd_resize(rbd_image_t image, uint64_t size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::NoOpProgressContext prog_ctx;
  int r = ictx->operations->resize(size, true, prog_ctx);
  tracepoint(librbd, resize_exit, r);
  return r;
}

extern "C" int rbd_resize2(rbd_image_t image, uint64_t size, bool allow_shrink,
					librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = ictx->operations->resize(size, allow_shrink, prog_ctx);
  tracepoint(librbd, resize_exit, r);
  return r;
}

extern "C" int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
					librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, resize_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, size);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = ictx->operations->resize(size, true, prog_ctx);
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
  int r = ictx->operations->update_features(features, features_enabled);
  tracepoint(librbd, update_features_exit, r);
  return r;
}

extern "C" int rbd_get_op_features(rbd_image_t image, uint64_t *op_features)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Image<>::get_op_features(ictx, op_features);
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

extern "C"  int rbd_get_create_timestamp(rbd_image_t image,
                                           struct timespec *timestamp)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_create_timestamp_enter, ictx, ictx->name.c_str(),
             ictx->read_only);
  utime_t time = ictx->get_create_timestamp();
  time.to_timespec(timestamp);
  tracepoint(librbd, get_create_timestamp_exit, 0, timestamp);
  return 0;
}

extern "C"  int rbd_get_access_timestamp(rbd_image_t image,
                                           struct timespec *timestamp)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_access_timestamp_enter, ictx, ictx->name.c_str(),
             ictx->read_only);
  utime_t time = ictx->get_access_timestamp();
  time.to_timespec(timestamp);
  tracepoint(librbd, get_access_timestamp_exit, 0, timestamp);
  return 0;
}

extern "C"  int rbd_get_modify_timestamp(rbd_image_t image,
                                           struct timespec *timestamp)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_modify_timestamp_enter, ictx, ictx->name.c_str(),
             ictx->read_only);
  utime_t time = ictx->get_modify_timestamp();
  time.to_timespec(timestamp);
  tracepoint(librbd, get_modify_timestamp_exit, 0, timestamp);
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

extern "C" int rbd_get_name(rbd_image_t image, char *name, size_t *name_len)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx *>(image);
  if (*name_len <= ictx->name.size()) {
    *name_len = ictx->name.size() + 1;
    return -ERANGE;
  }

  strncpy(name, ictx->name.c_str(), ictx->name.size());
  name[ictx->name.size()] = '\0';
  *name_len = ictx->name.size() + 1;
  return 0;
}

extern "C" int rbd_get_id(rbd_image_t image, char *id, size_t id_len)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx *>(image);
  if (ictx->old_format) {
    return -EINVAL;
  }
  if (ictx->id.size() >= id_len) {
    return -ERANGE;
  }

  strncpy(id, ictx->id.c_str(), id_len - 1);
  id[id_len - 1] = '\0';
  return 0;
}

extern "C" int rbd_get_block_name_prefix(rbd_image_t image, char *prefix,
                                         size_t prefix_len)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx *>(image);
  if (ictx->object_prefix.size() >= prefix_len) {
    return -ERANGE;
  }

  strncpy(prefix, ictx->object_prefix.c_str(), prefix_len - 1);
  prefix[prefix_len - 1] = '\0';
  return 0;
}

extern "C" int64_t rbd_get_data_pool_id(rbd_image_t image)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx *>(image);
  return ictx->data_ctx.get_id();
}

extern "C" int rbd_get_parent_info(rbd_image_t image,
                                   char *parent_pool_name, size_t ppool_namelen,
                                   char *parent_name, size_t pnamelen,
                                   char *parent_snap_name, size_t psnap_namelen)
{
  auto ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, get_parent_info_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only);

  librbd::linked_image_spec_t parent_image;
  librbd::snap_spec_t parent_snap;
  int r = librbd::api::Image<>::get_parent(ictx, &parent_image, &parent_snap);
  if (r >= 0) {
    if (parent_pool_name) {
      if (parent_image.pool_name.length() + 1 > ppool_namelen) {
        r = -ERANGE;
      } else {
        strcpy(parent_pool_name, parent_image.pool_name.c_str());
      }
    }
    if (parent_name) {
      if (parent_image.image_name.length() + 1 > pnamelen) {
        r = -ERANGE;
      } else {
        strcpy(parent_name, parent_image.image_name.c_str());
      }
    }
    if (parent_snap_name) {
      if (parent_snap.name.length() + 1 > psnap_namelen) {
        r = -ERANGE;
      } else {
        strcpy(parent_snap_name, parent_snap.name.c_str());
      }
    }
  }

  if (r < 0) {
    tracepoint(librbd, get_parent_info_exit, r, NULL, NULL, NULL, NULL);
    return r;
  }

  tracepoint(librbd, get_parent_info_exit, r,
             parent_image.pool_name.c_str(),
             parent_image.image_name.c_str(),
             parent_image.image_id.c_str(),
             parent_snap.name.c_str());
  return 0;
}

extern "C" int rbd_get_parent_info2(rbd_image_t image,
                                    char *parent_pool_name,
                                    size_t ppool_namelen,
                                    char *parent_name, size_t pnamelen,
                                    char *parent_id, size_t pidlen,
                                    char *parent_snap_name,
                                    size_t psnap_namelen)
{
  auto ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, get_parent_info_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only);

  librbd::linked_image_spec_t parent_image;
  librbd::snap_spec_t parent_snap;
  int r = librbd::api::Image<>::get_parent(ictx, &parent_image, &parent_snap);
  if (r >= 0) {
    if (parent_pool_name) {
      if (parent_image.pool_name.length() + 1 > ppool_namelen) {
        r = -ERANGE;
      } else {
        strcpy(parent_pool_name, parent_image.pool_name.c_str());
      }
    }
    if (parent_name) {
      if (parent_image.image_name.length() + 1 > pnamelen) {
        r = -ERANGE;
      } else {
        strcpy(parent_name, parent_image.image_name.c_str());
      }
    }
    if (parent_id) {
      if (parent_image.image_id.length() + 1 > pidlen) {
        r = -ERANGE;
      } else {
        strcpy(parent_id, parent_image.image_id.c_str());
      }
  }
    if (parent_snap_name) {
      if (parent_snap.name.length() + 1 > psnap_namelen) {
        r = -ERANGE;
      } else {
        strcpy(parent_snap_name, parent_snap.name.c_str());
      }
    }
  }

  if (r < 0) {
    tracepoint(librbd, get_parent_info_exit, r, NULL, NULL, NULL, NULL);
    return r;
  }

  tracepoint(librbd, get_parent_info_exit, r,
             parent_image.pool_name.c_str(),
             parent_image.image_name.c_str(),
             parent_image.image_id.c_str(),
             parent_snap.name.c_str());
  return 0;
}

extern "C" int rbd_get_parent(rbd_image_t image,
                              rbd_linked_image_spec_t *parent_image,
                              rbd_snap_spec_t *parent_snap)
{
  auto ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, get_parent_info_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only);

  librbd::linked_image_spec_t cpp_parent_image;
  librbd::snap_spec_t cpp_parent_snap;
  int r = librbd::api::Image<>::get_parent(ictx, &cpp_parent_image,
                                           &cpp_parent_snap);
  if (r < 0) {
    memset(parent_image, 0, sizeof(rbd_linked_image_spec_t));
    memset(parent_snap, 0, sizeof(rbd_snap_spec_t));
  } else {
    *parent_image = {
      .pool_id = cpp_parent_image.pool_id,
      .pool_name = strdup(cpp_parent_image.pool_name.c_str()),
      .pool_namespace = strdup(cpp_parent_image.pool_namespace.c_str()),
      .image_id = strdup(cpp_parent_image.image_id.c_str()),
      .image_name = strdup(cpp_parent_image.image_name.c_str()),
      .trash = cpp_parent_image.trash};
    *parent_snap = {
      .id = cpp_parent_snap.id,
      .namespace_type = cpp_parent_snap.namespace_type,
      .name = strdup(cpp_parent_snap.name.c_str())};
  }

  tracepoint(librbd, get_parent_info_exit, r,
             parent_image->pool_name,
             parent_image->image_name,
             parent_image->image_id,
             parent_snap->name);
  return r;
}

extern "C" int rbd_get_flags(rbd_image_t image, uint64_t *flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, get_flags_enter, ictx);
  int r = librbd::get_flags(ictx, flags);
  tracepoint(librbd, get_flags_exit, ictx, r, *flags);
  return r;
}

extern "C" int rbd_get_group(rbd_image_t image, rbd_group_info_t *group_info,
                             size_t group_info_size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, image_get_group_enter, ictx->name.c_str());

  if (group_info_size != sizeof(rbd_group_info_t)) {
    tracepoint(librbd, image_get_group_exit, -ERANGE);
    return -ERANGE;
  }

  librbd::group_info_t cpp_group_info;
  int r = librbd::api::Group<>::image_get_group(ictx, &cpp_group_info);
  if (r >= 0) {
    group_info_cpp_to_c(cpp_group_info, group_info);
  } else {
    group_info->name = NULL;
  }

  tracepoint(librbd, image_get_group_exit, r);
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

extern "C" int rbd_lock_acquire(rbd_image_t image, rbd_lock_mode_t lock_mode)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, lock_acquire_enter, ictx, lock_mode);
  int r = librbd::lock_acquire(ictx, lock_mode);
  tracepoint(librbd, lock_acquire_exit, ictx, r);
  return r;
}

extern "C" int rbd_lock_release(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, lock_release_enter, ictx);
  int r = librbd::lock_release(ictx);
  tracepoint(librbd, lock_release_exit, ictx, r);
  return r;
}

extern "C" int rbd_lock_get_owners(rbd_image_t image,
                                   rbd_lock_mode_t *lock_mode,
                                   char **lock_owners,
                                   size_t *max_lock_owners)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, lock_get_owners_enter, ictx);
  memset(lock_owners, 0, sizeof(*lock_owners) * *max_lock_owners);
  std::list<std::string> lock_owner_list;
  int r = librbd::lock_get_owners(ictx, lock_mode, &lock_owner_list);
  if (r >= 0) {
    if (*max_lock_owners >= lock_owner_list.size()) {
      *max_lock_owners = 0;
      for (auto &lock_owner : lock_owner_list) {
        lock_owners[(*max_lock_owners)++] = strdup(lock_owner.c_str());
      }
    } else {
      *max_lock_owners = lock_owner_list.size();
      r = -ERANGE;
    }
  }
  tracepoint(librbd, lock_get_owners_exit, ictx, r);
  return r;
}

extern "C" void rbd_lock_get_owners_cleanup(char **lock_owners,
                                            size_t lock_owner_count)
{
  for (size_t i = 0; i < lock_owner_count; ++i) {
    free(lock_owners[i]);
  }
}

extern "C" int rbd_lock_break(rbd_image_t image, rbd_lock_mode_t lock_mode,
                              const char *lock_owner)
{
  librbd::ImageCtx *ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, lock_break_enter, ictx, lock_mode, lock_owner);
  int r = librbd::lock_break(ictx, lock_mode, lock_owner);
  tracepoint(librbd, lock_break_exit, ictx, r);
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
  int r = ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					snap_name);
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
  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::snap_remove(ictx, snap_name, 0, prog_ctx);
  tracepoint(librbd, snap_remove_exit, r);
  return r;
}

extern "C" int rbd_snap_remove2(rbd_image_t image, const char *snap_name, uint32_t flags,
				librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_remove2_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name, flags);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::snap_remove(ictx, snap_name, flags, prog_ctx);
  tracepoint(librbd, snap_remove_exit, r);
  return r;
}

extern "C" int rbd_snap_remove_by_id(rbd_image_t image, uint64_t snap_id)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Snapshot<>::remove(ictx, snap_id);
}

extern "C" int rbd_snap_rollback(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_rollback_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  librbd::NoOpProgressContext prog_ctx;
  int r = ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(), snap_name, prog_ctx);
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
  int r = ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(), snap_name, prog_ctx);
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
  memset(snaps, 0, sizeof(*snaps) * *max_snaps);

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
  int r = ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(), snap_name);
  tracepoint(librbd, snap_protect_exit, r);
  return r;
}

extern "C" int rbd_snap_unprotect(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_unprotect_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(), snap_name);
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

extern "C" int rbd_snap_get_limit(rbd_image_t image, uint64_t *limit)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_get_limit_enter, ictx, ictx->name.c_str());
  int r = librbd::snap_get_limit(ictx, limit);
  tracepoint(librbd, snap_get_limit_exit, r, *limit);
  return r;
}

extern "C" int rbd_snap_get_timestamp(rbd_image_t image, uint64_t snap_id, struct timespec *timestamp)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_get_timestamp_enter, ictx, ictx->name.c_str());
  int r = librbd::snap_get_timestamp(ictx, snap_id, timestamp);
  tracepoint(librbd, snap_get_timestamp_exit, r);
  return r;
}

extern "C" int rbd_snap_set_limit(rbd_image_t image, uint64_t limit)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_set_limit_enter, ictx, ictx->name.c_str(), limit);
  int r = librbd::snap_set_limit(ictx, limit);
  tracepoint(librbd, snap_set_limit_exit, r);
  return r;
}

extern "C" int rbd_snap_set(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_set_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, snap_name);
  int r = librbd::api::Image<>::snap_set(
    ictx, cls::rbd::UserSnapshotNamespace(), snap_name);
  tracepoint(librbd, snap_set_exit, r);
  return r;
}

extern "C" int rbd_snap_set_by_id(rbd_image_t image, uint64_t snap_id)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Image<>::snap_set(ictx, snap_id);
}

extern "C" ssize_t rbd_list_children(rbd_image_t image, char *pools,
				     size_t *pools_len, char *images,
				     size_t *images_len)
{
  auto ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, list_children_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only);

  std::vector<librbd::linked_image_spec_t> cpp_images;
  int r = librbd::api::Image<>::list_children(ictx, &cpp_images);
  if (r < 0) {
    tracepoint(librbd, list_children_exit, r);
    return r;
  }

  std::set<std::pair<std::string, std::string>> image_set;
  for (auto& image : cpp_images) {
    if (!image.trash) {
      image_set.insert({image.pool_name, image.image_name});
    }
  }

  size_t pools_total = 0;
  size_t images_total = 0;
  for (auto it : image_set) {
    pools_total += it.first.length() + 1;
    images_total += it.second.length() + 1;
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
  for (auto it : image_set) {
    const char* pool = it.first.c_str();
    strcpy(pools_p, pool);
    pools_p += it.first.length() + 1;
    const char* image = it.second.c_str();
    strcpy(images_p, image);
    images_p += it.second.length() + 1;
    tracepoint(librbd, list_children_entry, pool, image);
  }

  ssize_t ret = image_set.size();
  tracepoint(librbd, list_children_exit, ret);
  return ret;
}

extern "C" int rbd_list_children2(rbd_image_t image,
                                  rbd_child_info_t *children,
	                          int *max_children)
{
  auto ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, list_children_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only);
  memset(children, 0, sizeof(*children) * *max_children);

  if (!max_children) {
    tracepoint(librbd, list_children_exit, -EINVAL);
    return -EINVAL;
  }

  std::vector<librbd::linked_image_spec_t> cpp_children;
  int r = librbd::api::Image<>::list_children(ictx, &cpp_children);
  if (r < 0) {
    tracepoint(librbd, list_children_exit, r);
    return r;
  }

  if (*max_children < (int)cpp_children.size() + 1) {
    *max_children = (int)cpp_children.size() + 1;
    tracepoint(librbd, list_children_exit, *max_children);
    return -ERANGE;
  }

  int i;
  for (i = 0; i < (int)cpp_children.size(); i++) {
    children[i].pool_name = strdup(cpp_children[i].pool_name.c_str());
    children[i].image_name = strdup(cpp_children[i].image_name.c_str());
    children[i].image_id = strdup(cpp_children[i].image_id.c_str());
    children[i].trash = cpp_children[i].trash;
    tracepoint(librbd, list_children_entry, children[i].pool_name,
               children[i].image_name);
  }
  children[i].pool_name = NULL;
  children[i].image_name = NULL;
  children[i].image_id = NULL;

  r = (int)cpp_children.size();
  tracepoint(librbd, list_children_exit, *max_children);
  return r;
}

extern "C" void rbd_list_child_cleanup(rbd_child_info_t *child)
{
  free((void *)child->pool_name);
  free((void *)child->image_name);
  free((void *)child->image_id);
}

extern "C" void rbd_list_children_cleanup(rbd_child_info_t *children,
                                          size_t num_children)
{
  for (size_t i=0; i < num_children; i++) {
    free((void *)children[i].pool_name);
    free((void *)children[i].image_name);
    free((void *)children[i].image_id);
  }
}

extern "C" int rbd_list_children3(rbd_image_t image,
                                    rbd_linked_image_spec_t *images,
                                    size_t *max_images)
{
  auto ictx = reinterpret_cast<librbd::ImageCtx*>(image);
  tracepoint(librbd, list_children_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only);
  memset(images, 0, sizeof(*images) * *max_images);

  std::vector<librbd::linked_image_spec_t> cpp_children;
  int r = librbd::api::Image<>::list_children(ictx, &cpp_children);
  if (r < 0) {
    tracepoint(librbd, list_children_exit, r);
    return r;
  }

  if (*max_images < cpp_children.size()) {
    *max_images = cpp_children.size();
    return -ERANGE;
  }

  *max_images = cpp_children.size();
  for (size_t idx = 0; idx < cpp_children.size(); ++idx) {
    images[idx] = {
      .pool_id = cpp_children[idx].pool_id,
      .pool_name = strdup(cpp_children[idx].pool_name.c_str()),
      .pool_namespace = strdup(cpp_children[idx].pool_namespace.c_str()),
      .image_id = strdup(cpp_children[idx].image_id.c_str()),
      .image_name = strdup(cpp_children[idx].image_name.c_str()),
      .trash = cpp_children[idx].trash};
    tracepoint(librbd, list_children_entry, images[idx].pool_name,
               images[idx].image_name);
  }
  return 0;
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
  int r = ictx->io_work_queue->read(ofs, len, librbd::io::ReadResult{buf, len},
                                    0);
  tracepoint(librbd, read_exit, r);
  return r;
}

extern "C" ssize_t rbd_read2(rbd_image_t image, uint64_t ofs, size_t len,
			      char *buf, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, read2_enter, ictx, ictx->name.c_str(),
	      ictx->snap_name.c_str(), ictx->read_only, ofs, len, op_flags);
  int r = ictx->io_work_queue->read(ofs, len, librbd::io::ReadResult{buf, len},
                                    op_flags);
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
  int r = librbd::api::DiffIterate<>::diff_iterate(ictx,
						   cls::rbd::UserSnapshotNamespace(),
						   fromsnapname, ofs, len,
                                                   true, false, cb, arg);
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
  int r = librbd::api::DiffIterate<>::diff_iterate(ictx,
						   cls::rbd::UserSnapshotNamespace(),
						   fromsnapname, ofs, len,
                                                   include_parent, whole_object,
                                                   cb, arg);
  tracepoint(librbd, diff_iterate_exit, r);
  return r;
}

extern "C" ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len,
			     const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, ofs, len, buf);

  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, len));
  int r = ictx->io_work_queue->write(ofs, len, std::move(bl), 0);
  tracepoint(librbd, write_exit, r);
  return r;
}

extern "C" ssize_t rbd_write2(rbd_image_t image, uint64_t ofs, size_t len,
			      const char *buf, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, write2_enter, ictx, ictx->name.c_str(),
	      ictx->snap_name.c_str(), ictx->read_only, ofs, len, buf, op_flags);

  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, len));
  int r = ictx->io_work_queue->write(ofs, len, std::move(bl), op_flags);
  tracepoint(librbd, write_exit, r);
  return r;
}


extern "C" int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, discard_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only, ofs, len);
  if (len > std::numeric_limits<int>::max()) {
    tracepoint(librbd, discard_exit, -EINVAL);
    return -EINVAL;
  }

  int r = ictx->io_work_queue->discard(ofs, len, ictx->skip_partial_discard);
  tracepoint(librbd, discard_exit, r);
  return r;
}

extern "C" ssize_t rbd_writesame(rbd_image_t image, uint64_t ofs, size_t len,
                                 const char *buf, size_t data_len, int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, writesame_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
             ictx->read_only, ofs, len, data_len == 0 ? NULL : buf, data_len, op_flags);

  if (data_len == 0 || len % data_len ||
      len > std::numeric_limits<int>::max()) {
    tracepoint(librbd, writesame_exit, -EINVAL);
    return -EINVAL;
  }

  bool discard_zero = ictx->config.get_val<bool>("rbd_discard_on_zeroed_write_same");
  if (discard_zero && mem_is_zero(buf, data_len)) {
    int r = ictx->io_work_queue->discard(ofs, len, false);
    tracepoint(librbd, writesame_exit, r);
    return r;
  }

  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, data_len));
  int r = ictx->io_work_queue->writesame(ofs, len, std::move(bl), op_flags);
  tracepoint(librbd, writesame_exit, r);
  return r;
}

extern "C" ssize_t rbd_compare_and_write(rbd_image_t image,
                                         uint64_t ofs, size_t len,
                                         const char *cmp_buf,
                                         const char *buf,
                                         uint64_t *mismatch_off,
                                         int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, compare_and_write_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only, ofs,
             len, cmp_buf, buf, op_flags);

  bufferlist cmp_bl;
  cmp_bl.push_back(create_write_raw(ictx, cmp_buf, len));
  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, len));

  int r = ictx->io_work_queue->compare_and_write(ofs, len, std::move(cmp_bl),
                                                 std::move(bl), mismatch_off,
                                                 op_flags);
  tracepoint(librbd, compare_and_write_exit, r);
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

  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, len));
  ictx->io_work_queue->aio_write(get_aio_completion(comp), off, len,
                                 std::move(bl), 0);
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

  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, len));
  ictx->io_work_queue->aio_write(get_aio_completion(comp), off, len,
                                 std::move(bl), op_flags);
  tracepoint(librbd, aio_write_exit, 0);
  return 0;
}

extern "C" int rbd_aio_writev(rbd_image_t image, const struct iovec *iov,
                              int iovcnt, uint64_t off, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;

  // convert the scatter list into a bufferlist
  ssize_t len = 0;
  bufferlist bl;
  for (int i = 0; i < iovcnt; ++i) {
    const struct iovec &io = iov[i];
    len += io.iov_len;
    if (len < 0) {
      break;
    }

    bl.push_back(create_write_raw(ictx, static_cast<char*>(io.iov_base),
                                  io.iov_len));
  }

  int r = 0;
  if (iovcnt <= 0 || len < 0) {
    r = -EINVAL;
  }

  tracepoint(librbd, aio_write_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only, off, len, NULL,
             comp->pc);
  if (r == 0) {
    ictx->io_work_queue->aio_write(get_aio_completion(comp), off, len,
                                   std::move(bl), 0);
  }
  tracepoint(librbd, aio_write_exit, r);
  return r;
}

extern "C" int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
			       rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_discard_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, comp->pc);
  ictx->io_work_queue->aio_discard(get_aio_completion(comp), off, len, ictx->skip_partial_discard);
  tracepoint(librbd, aio_discard_exit, 0);
  return 0;
}

extern "C" int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
			    char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_read_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, off, len, buf, comp->pc);
  ictx->io_work_queue->aio_read(get_aio_completion(comp), off, len,
                                librbd::io::ReadResult{buf, len}, 0);
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
  ictx->io_work_queue->aio_read(get_aio_completion(comp), off, len,
                                librbd::io::ReadResult{buf, len},op_flags);
  tracepoint(librbd, aio_read_exit, 0);
  return 0;
}

extern "C" int rbd_aio_readv(rbd_image_t image, const struct iovec *iov,
                             int iovcnt, uint64_t off, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;

  ssize_t len = 0;
  for (int i = 0; i < iovcnt; ++i) {
    len += iov[i].iov_len;
    if (len < 0) {
      break;
    }
  }

  int r = 0;
  if (iovcnt == 0 || len < 0) {
    r = -EINVAL;
  }

  tracepoint(librbd, aio_read_enter, ictx, ictx->name.c_str(),
             ictx->snap_name.c_str(), ictx->read_only, off, len, NULL,
             comp->pc);
  if (r == 0) {
    librbd::io::ReadResult read_result;
    if (iovcnt == 1) {
      read_result = librbd::io::ReadResult(
        static_cast<char *>(iov[0].iov_base), iov[0].iov_len);
    } else {
      read_result = librbd::io::ReadResult(iov, iovcnt);
    }
    ictx->io_work_queue->aio_read(get_aio_completion(comp), off, len,
                                  std::move(read_result), 0);
  }
  tracepoint(librbd, aio_read_exit, r);
  return r;
}

extern "C" int rbd_flush(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  int r = ictx->io_work_queue->flush();
  tracepoint(librbd, flush_exit, r);
  return r;
}

extern "C" int rbd_aio_flush(rbd_image_t image, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_flush_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only, comp->pc);
  ictx->io_work_queue->aio_flush(get_aio_completion(comp));
  tracepoint(librbd, aio_flush_exit, 0);
  return 0;
}

extern "C" int rbd_aio_writesame(rbd_image_t image, uint64_t off, size_t len,
                                 const char *buf, size_t data_len, rbd_completion_t c,
                                 int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_writesame_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
             ictx->read_only, off, len, data_len == 0 ? NULL : buf, data_len, comp->pc,
             op_flags);

  if (data_len == 0 || len % data_len) {
    tracepoint(librbd, aio_writesame_exit, -EINVAL);
    return -EINVAL;
  }

  bool discard_zero = ictx->config.get_val<bool>("rbd_discard_on_zeroed_write_same");
  if (discard_zero && mem_is_zero(buf, data_len)) {
    ictx->io_work_queue->aio_discard(get_aio_completion(comp), off, len, false);
    tracepoint(librbd, aio_writesame_exit, 0);
    return 0;
  }

  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, data_len));
  ictx->io_work_queue->aio_writesame(get_aio_completion(comp), off, len,
                                     std::move(bl), op_flags);
  tracepoint(librbd, aio_writesame_exit, 0);
  return 0;
}

extern "C" ssize_t rbd_aio_compare_and_write(rbd_image_t image, uint64_t off,
                                             size_t len, const char *cmp_buf,
                                             const char *buf, rbd_completion_t c,
                                             uint64_t *mismatch_off,
                                             int op_flags)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  tracepoint(librbd, aio_compare_and_write_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(),
	      ictx->read_only, off, len, cmp_buf, buf, comp->pc, op_flags);

  bufferlist cmp_bl;
  cmp_bl.push_back(create_write_raw(ictx, cmp_buf, len));
  bufferlist bl;
  bl.push_back(create_write_raw(ictx, buf, len));
  ictx->io_work_queue->aio_compare_and_write(get_aio_completion(comp), off, len,
                                             std::move(cmp_bl), std::move(bl),
                                             mismatch_off, op_flags, false);

  tracepoint(librbd, aio_compare_and_write_exit, 0);
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
  librbd::io::AioCompletion *cs[numcomp];
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
  if (*vallen < val_s.size() + 1) {
    r = -ERANGE;
    *vallen = val_s.size() + 1;
    tracepoint(librbd, metadata_get_exit, r, key, NULL);
  } else {
    strncpy(value, val_s.c_str(), val_s.size() + 1);
    tracepoint(librbd, metadata_get_exit, r, key, value);
  }
  return r;
}

extern "C" int rbd_metadata_set(rbd_image_t image, const char *key, const char *value)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, metadata_set_enter, ictx, key, value);
  int r = ictx->operations->metadata_set(key, value);
  tracepoint(librbd, metadata_set_exit, r);
  return r;
}

extern "C" int rbd_metadata_remove(rbd_image_t image, const char *key)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, metadata_remove_enter, ictx, key);
  int r = ictx->operations->metadata_remove(key);
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
  if (*key_len < key_total_len || *val_len < val_total_len)
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
    strncpy(key_p, it->first.c_str(), it->first.size() + 1);
    key_p += it->first.size() + 1;
    strncpy(value_p, it->second.c_str(), it->second.length());
    value_p += it->second.length();
    *value_p = '\0';
    value_p++;
    tracepoint(librbd, metadata_list_entry, it->first.c_str(), it->second.c_str());
  }
  tracepoint(librbd, metadata_list_exit, r);
  return r;
}

extern "C" int rbd_mirror_image_enable(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Mirror<>::image_enable(ictx, false);
}

extern "C" int rbd_mirror_image_disable(rbd_image_t image, bool force)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Mirror<>::image_disable(ictx, force);
}

extern "C" int rbd_mirror_image_promote(rbd_image_t image, bool force)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Mirror<>::image_promote(ictx, force);
}

extern "C" int rbd_mirror_image_demote(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Mirror<>::image_demote(ictx);
}

extern "C" int rbd_mirror_image_resync(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::api::Mirror<>::image_resync(ictx);
}

extern "C" int rbd_mirror_image_get_info(rbd_image_t image,
                                         rbd_mirror_image_info_t *mirror_image_info,
                                         size_t info_size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  if (sizeof(rbd_mirror_image_info_t) != info_size) {
    return -ERANGE;
  }

  librbd::mirror_image_info_t cpp_mirror_image;
  int r = librbd::api::Mirror<>::image_get_info(ictx, &cpp_mirror_image);
  if (r < 0) {
    return r;
  }

  mirror_image_info_cpp_to_c(cpp_mirror_image, mirror_image_info);
  return 0;
}

extern "C" int rbd_mirror_image_get_status(rbd_image_t image,
					   rbd_mirror_image_status_t *status,
					   size_t status_size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  if (sizeof(rbd_mirror_image_status_t) != status_size) {
    return -ERANGE;
  }

  librbd::mirror_image_status_t cpp_status;
  int r = librbd::api::Mirror<>::image_get_status(ictx, &cpp_status);
  if (r < 0) {
    return r;
  }

  mirror_image_status_cpp_to_c(cpp_status, status);
  return 0;
}

extern "C" int rbd_mirror_image_get_instance_id(rbd_image_t image,
                                                char *instance_id,
                                                size_t *instance_id_max_length)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  std::string cpp_instance_id;
  int r = librbd::api::Mirror<>::image_get_instance_id(ictx, &cpp_instance_id);
  if (r < 0) {
    return r;
  }

  if (cpp_instance_id.size() >= *instance_id_max_length) {
    *instance_id_max_length = cpp_instance_id.size() + 1;
    return -ERANGE;
  }

  strcpy(instance_id, cpp_instance_id.c_str());
  *instance_id_max_length = cpp_instance_id.size() + 1;
  return 0;
}

extern "C" int rbd_aio_mirror_image_promote(rbd_image_t image, bool force,
                                            rbd_completion_t c) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  librbd::api::Mirror<>::image_promote(
    ictx, force, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                                     get_aio_completion(comp)));
  return 0;
}

extern "C" int rbd_aio_mirror_image_demote(rbd_image_t image,
                                           rbd_completion_t c) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  librbd::api::Mirror<>::image_demote(
    ictx, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                              get_aio_completion(comp)));
  return 0;
}

extern "C" int rbd_aio_mirror_image_get_info(rbd_image_t image,
                                             rbd_mirror_image_info_t *info,
                                             size_t info_size,
                                             rbd_completion_t c) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;

  if (sizeof(rbd_mirror_image_info_t) != info_size) {
    return -ERANGE;
  }

  auto ctx = new C_MirrorImageGetInfo(
    info, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                              get_aio_completion(comp)));
  librbd::api::Mirror<>::image_get_info(
    ictx, &ctx->cpp_mirror_image_info, ctx);
  return 0;
}

extern "C" int rbd_aio_mirror_image_get_status(rbd_image_t image,
                                               rbd_mirror_image_status_t *status,
                                               size_t status_size,
                                               rbd_completion_t c) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;

  if (sizeof(rbd_mirror_image_status_t) != status_size) {
    return -ERANGE;
  }

  auto ctx = new C_MirrorImageGetStatus(
    status, new C_AioCompletion(ictx, librbd::io::AIO_TYPE_GENERIC,
                                get_aio_completion(comp)));
  librbd::api::Mirror<>::image_get_status(ictx, &ctx->cpp_mirror_image_status,
                                          ctx);
  return 0;
}

extern "C" int rbd_update_watch(rbd_image_t image, uint64_t *handle,
				rbd_update_callback_t watch_cb, void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  C_UpdateWatchCB *wctx = new C_UpdateWatchCB(watch_cb, arg);
  tracepoint(librbd, update_watch_enter, ictx, wctx);
  int r = ictx->state->register_update_watcher(wctx, &wctx->handle);
  tracepoint(librbd, update_watch_exit, r, wctx->handle);
  *handle = reinterpret_cast<uint64_t>(wctx);
  return r;
}

extern "C" int rbd_update_unwatch(rbd_image_t image, uint64_t handle)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  C_UpdateWatchCB *wctx = reinterpret_cast<C_UpdateWatchCB *>(handle);
  tracepoint(librbd, update_unwatch_enter, ictx, wctx->handle);
  int r = ictx->state->unregister_update_watcher(wctx->handle);
  delete wctx;
  tracepoint(librbd, update_unwatch_exit, r);
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

extern "C" int rbd_group_create(rados_ioctx_t p, const char *name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, group_create_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), name);
  int r = librbd::api::Group<>::create(io_ctx, name);
  tracepoint(librbd, group_create_exit, r);
  return r;
}

extern "C" int rbd_group_remove(rados_ioctx_t p, const char *name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, group_remove_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), name);
  int r = librbd::api::Group<>::remove(io_ctx, name);
  tracepoint(librbd, group_remove_exit, r);
  return r;
}

extern "C" int rbd_group_list(rados_ioctx_t p, char *names, size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, group_list_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id());

  vector<string> cpp_names;
  int r = librbd::api::Group<>::list(io_ctx, &cpp_names);

  if (r < 0) {
    tracepoint(librbd, group_list_exit, r);
    return r;
  }

  size_t expected_size = 0;

  for (size_t i = 0; i < cpp_names.size(); i++) {
    expected_size += cpp_names[i].size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    tracepoint(librbd, group_list_exit, -ERANGE);
    return -ERANGE;
  }

  if (names == NULL) {
    tracepoint(librbd, group_list_exit, -EINVAL);
    return -EINVAL;
  }

  for (int i = 0; i < (int)cpp_names.size(); i++) {
    const char* name = cpp_names[i].c_str();
    tracepoint(librbd, group_list_entry, name);
    strcpy(names, name);
    names += strlen(names) + 1;
  }
  tracepoint(librbd, group_list_exit, (int)expected_size);
  return (int)expected_size;
}

extern "C" int rbd_group_rename(rados_ioctx_t p, const char *src_name,
                                const char *dest_name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  TracepointProvider::initialize<tracepoint_traits>(get_cct(io_ctx));
  tracepoint(librbd, group_rename_enter, io_ctx.get_pool_name().c_str(),
             io_ctx.get_id(), src_name, dest_name);
  int r = librbd::api::Group<>::rename(io_ctx, src_name, dest_name);
  tracepoint(librbd, group_rename_exit, r);
  return r;
}

extern "C" int rbd_group_image_add(rados_ioctx_t group_p,
                                   const char *group_name,
                                   rados_ioctx_t image_p,
                                   const char *image_name)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx image_ioctx;

  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);
  librados::IoCtx::from_rados_ioctx_t(image_p, image_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_image_add_enter, group_ioctx.get_pool_name().c_str(),
	     group_ioctx.get_id(), group_name, image_ioctx.get_pool_name().c_str(),
	     image_ioctx.get_id(), image_name);

  int r = librbd::api::Group<>::image_add(group_ioctx, group_name, image_ioctx,
                                          image_name);

  tracepoint(librbd, group_image_add_exit, r);
  return r;
}

extern "C" int rbd_group_image_remove(rados_ioctx_t group_p,
                                      const char *group_name,
                                      rados_ioctx_t image_p,
                                      const char *image_name)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx image_ioctx;

  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);
  librados::IoCtx::from_rados_ioctx_t(image_p, image_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_image_remove_enter, group_ioctx.get_pool_name().c_str(),
	     group_ioctx.get_id(), group_name, image_ioctx.get_pool_name().c_str(),
	     image_ioctx.get_id(), image_name);

  int r = librbd::api::Group<>::image_remove(group_ioctx, group_name,
                                             image_ioctx, image_name);

  tracepoint(librbd, group_image_remove_exit, r);
  return r;
}

extern "C" int rbd_group_image_remove_by_id(rados_ioctx_t group_p,
                                            const char *group_name,
                                            rados_ioctx_t image_p,
                                            const char *image_id)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx image_ioctx;

  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);
  librados::IoCtx::from_rados_ioctx_t(image_p, image_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_image_remove_by_id_enter,
             group_ioctx.get_pool_name().c_str(),
             group_ioctx.get_id(), group_name,
             image_ioctx.get_pool_name().c_str(),
             image_ioctx.get_id(), image_id);

  int r = librbd::api::Group<>::image_remove_by_id(group_ioctx, group_name,
                                                   image_ioctx, image_id);

  tracepoint(librbd, group_image_remove_by_id_exit, r);
  return r;
}

extern "C" int rbd_group_image_list(rados_ioctx_t group_p,
				    const char *group_name,
				    rbd_group_image_info_t *images,
                                    size_t group_image_info_size,
				    size_t *image_size)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_image_list_enter,
             group_ioctx.get_pool_name().c_str(),
	     group_ioctx.get_id(), group_name);
  memset(images, 0, sizeof(*images) * *image_size);

  if (group_image_info_size != sizeof(rbd_group_image_info_t)) {
    *image_size = 0;
    tracepoint(librbd, group_image_list_exit, -ERANGE);
    return -ERANGE;
  }

  std::vector<librbd::group_image_info_t> cpp_images;
  int r = librbd::api::Group<>::image_list(group_ioctx, group_name,
                                           &cpp_images);

  if (r == -ENOENT) {
    tracepoint(librbd, group_image_list_exit, 0);
    return 0;
  }

  if (r < 0) {
    tracepoint(librbd, group_image_list_exit, r);
    return r;
  }

  if (*image_size < cpp_images.size()) {
    *image_size = cpp_images.size();
    tracepoint(librbd, group_image_list_exit, -ERANGE);
    return -ERANGE;
  }

  for (size_t i = 0; i < cpp_images.size(); ++i) {
    group_image_status_cpp_to_c(cpp_images[i], &images[i]);
  }

  r = *image_size = cpp_images.size();
  tracepoint(librbd, group_image_list_exit, r);
  return r;
}

extern "C" int rbd_group_info_cleanup(rbd_group_info_t *group_info,
                                      size_t group_info_size) {
  if (group_info_size != sizeof(rbd_group_info_t)) {
    return -ERANGE;
  }

  free(group_info->name);
  return 0;
}

extern "C" int rbd_group_image_list_cleanup(rbd_group_image_info_t *images,
                                            size_t group_image_info_size,
                                            size_t len) {
  if (group_image_info_size != sizeof(rbd_group_image_info_t)) {
    return -ERANGE;
  }

  for (size_t i = 0; i < len; ++i) {
    free(images[i].name);
  }
  return 0;
}

extern "C" int rbd_group_snap_create(rados_ioctx_t group_p,
                                     const char *group_name,
                                     const char *snap_name)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_snap_create_enter,
             group_ioctx.get_pool_name().c_str(),
	     group_ioctx.get_id(), group_name, snap_name);

  int r = librbd::api::Group<>::snap_create(group_ioctx, group_name, snap_name);

  tracepoint(librbd, group_snap_create_exit, r);

  return r;
}

extern "C" int rbd_group_snap_remove(rados_ioctx_t group_p,
                                     const char *group_name,
                                     const char *snap_name)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_snap_remove_enter,
             group_ioctx.get_pool_name().c_str(),
	     group_ioctx.get_id(), group_name, snap_name);

  int r = librbd::api::Group<>::snap_remove(group_ioctx, group_name, snap_name);

  tracepoint(librbd, group_snap_remove_exit, r);

  return r;
}

extern "C" int rbd_group_snap_rename(rados_ioctx_t group_p,
                                     const char *group_name,
                                     const char *old_snap_name,
                                     const char *new_snap_name)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_snap_rename_enter,
             group_ioctx.get_pool_name().c_str(), group_ioctx.get_id(),
             group_name, old_snap_name, new_snap_name);

  int r = librbd::api::Group<>::snap_rename(group_ioctx, group_name,
                                            old_snap_name, new_snap_name);

  tracepoint(librbd, group_snap_list_exit, r);
  return r;
}

extern "C" int rbd_group_snap_list(rados_ioctx_t group_p,
                                   const char *group_name,
                                   rbd_group_snap_info_t *snaps,
                                   size_t group_snap_info_size,
                                   size_t *snaps_size)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_snap_list_enter, group_ioctx.get_pool_name().c_str(),
	     group_ioctx.get_id(), group_name);
  memset(snaps, 0, sizeof(*snaps) * *snaps_size);

  if (group_snap_info_size != sizeof(rbd_group_snap_info_t)) {
    *snaps_size = 0;
    tracepoint(librbd, group_snap_list_exit, -ERANGE);
    return -ERANGE;
  }

  std::vector<librbd::group_snap_info_t> cpp_snaps;
  int r = librbd::api::Group<>::snap_list(group_ioctx, group_name, &cpp_snaps);

  if (r == -ENOENT) {
    *snaps_size = 0;
    tracepoint(librbd, group_snap_list_exit, 0);
    return 0;
  }

  if (r < 0) {
    tracepoint(librbd, group_snap_list_exit, r);
    return r;
  }

  if (*snaps_size < cpp_snaps.size()) {
    *snaps_size = cpp_snaps.size();
    tracepoint(librbd, group_snap_list_exit, -ERANGE);
    return -ERANGE;
  }

  for (size_t i = 0; i < cpp_snaps.size(); ++i) {
    group_snap_info_cpp_to_c(cpp_snaps[i], &snaps[i]);
  }

  r = *snaps_size = cpp_snaps.size();
  tracepoint(librbd, group_snap_list_exit, r);
  return r;
}

extern "C" int rbd_group_snap_list_cleanup(rbd_group_snap_info_t *snaps,
                                           size_t group_snap_info_size,
                                           size_t len) {
  if (group_snap_info_size != sizeof(rbd_group_snap_info_t)) {
    return -ERANGE;
  }

  for (size_t i = 0; i < len; ++i) {
    free(snaps[i].name);
  }
  return 0;
}

extern "C" int rbd_group_snap_rollback(rados_ioctx_t group_p,
                                       const char *group_name,
                                       const char *snap_name)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_snap_rollback_enter,
             group_ioctx.get_pool_name().c_str(),
             group_ioctx.get_id(), group_name, snap_name);

  librbd::NoOpProgressContext prog_ctx;
  int r = librbd::api::Group<>::snap_rollback(group_ioctx, group_name,
                                              snap_name, prog_ctx);

  tracepoint(librbd, group_snap_rollback_exit, r);

  return r;
}

extern "C" int rbd_group_snap_rollback_with_progress(rados_ioctx_t group_p,
                                                     const char *group_name,
                                                     const char *snap_name,
                                                     librbd_progress_fn_t cb,
                                                     void *cbdata)
{
  librados::IoCtx group_ioctx;
  librados::IoCtx::from_rados_ioctx_t(group_p, group_ioctx);

  TracepointProvider::initialize<tracepoint_traits>(get_cct(group_ioctx));
  tracepoint(librbd, group_snap_rollback_enter,
             group_ioctx.get_pool_name().c_str(),
             group_ioctx.get_id(), group_name, snap_name);

  librbd::CProgressContext prog_ctx(cb, cbdata);
  int r = librbd::api::Group<>::snap_rollback(group_ioctx, group_name,
                                              snap_name, prog_ctx);

  tracepoint(librbd, group_snap_rollback_exit, r);

  return r;
}

extern "C" int rbd_snap_get_namespace_type(rbd_image_t image,
					   uint64_t snap_id,
					   rbd_snap_namespace_type_t *namespace_type) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_get_namespace_type_enter, ictx, ictx->name.c_str());
  int r = librbd::api::Snapshot<>::get_namespace_type(ictx, snap_id,
                                                      namespace_type);
  tracepoint(librbd, snap_get_namespace_type_exit, r);
  return r;
}

extern "C" int rbd_snap_get_group_namespace(rbd_image_t image, uint64_t snap_id,
                                            rbd_snap_group_namespace_t *group_snap,
                                            size_t snap_group_namespace_size) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  tracepoint(librbd, snap_get_group_namespace_enter, ictx,
             ictx->name.c_str());

  if (snap_group_namespace_size != sizeof(rbd_snap_group_namespace_t)) {
    tracepoint(librbd, snap_get_group_namespace_exit, -ERANGE);
    return -ERANGE;
  }

  librbd::snap_group_namespace_t group_namespace;
  int r = librbd::api::Snapshot<>::get_group_namespace(ictx, snap_id,
                                                       &group_namespace);
  if (r >= 0) {
    group_snap->group_pool = group_namespace.group_pool;
    group_snap->group_name = strdup(group_namespace.group_name.c_str());
    group_snap->group_snap_name =
      strdup(group_namespace.group_snap_name.c_str());
  }

  tracepoint(librbd, snap_get_group_namespace_exit, r);
  return r;
}

extern "C" int rbd_snap_group_namespace_cleanup(rbd_snap_group_namespace_t *group_snap,
                                                size_t snap_group_namespace_size) {
  if (snap_group_namespace_size != sizeof(rbd_snap_group_namespace_t)) {
    tracepoint(librbd, snap_get_group_namespace_exit, -ERANGE);
    return -ERANGE;
  }

  free(group_snap->group_name);
  free(group_snap->group_snap_name);
  return 0;
}

extern "C" int rbd_snap_get_trash_namespace(rbd_image_t image, uint64_t snap_id,
                                            char *original_name,
                                            size_t max_length) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  std::string cpp_original_name;
  int r = librbd::api::Snapshot<>::get_trash_namespace(ictx, snap_id,
                                                       &cpp_original_name);
  if (r < 0) {
    return r;
  }

  if (cpp_original_name.length() >= max_length) {
    return -ERANGE;
  }

  strcpy(original_name, cpp_original_name.c_str());
  return 0;
}
extern "C" int rbd_watchers_list(rbd_image_t image,
				 rbd_image_watcher_t *watchers,
				 size_t *max_watchers) {
  std::list<librbd::image_watcher_t> watcher_list;
  librbd::ImageCtx *ictx = (librbd::ImageCtx*)image;

  tracepoint(librbd, list_watchers_enter, ictx, ictx->name.c_str(), ictx->snap_name.c_str(), ictx->read_only);
  memset(watchers, 0, sizeof(*watchers) * *max_watchers);
  int r = librbd::list_watchers(ictx, watcher_list);
  if (r < 0) {
    tracepoint(librbd, list_watchers_exit, r, 0);
    return r;
  }

  if (watcher_list.size() > *max_watchers) {
    *max_watchers = watcher_list.size();
    tracepoint(librbd, list_watchers_exit, -ERANGE, watcher_list.size());
    return -ERANGE;
  }

  *max_watchers = 0;
  for (auto &watcher : watcher_list) {
    tracepoint(librbd, list_watchers_entry, watcher.addr.c_str(), watcher.id, watcher.cookie);
    watchers[*max_watchers].addr = strdup(watcher.addr.c_str());
    watchers[*max_watchers].id = watcher.id;
    watchers[*max_watchers].cookie = watcher.cookie;
    *max_watchers += 1;
  }

  tracepoint(librbd, list_watchers_exit, r, watcher_list.size());
  return 0;
}

extern "C" void rbd_watchers_list_cleanup(rbd_image_watcher_t *watchers,
					  size_t num_watchers) {
  for (size_t i = 0; i < num_watchers; ++i) {
    free(watchers[i].addr);
  }
}

extern "C" int rbd_config_image_list(rbd_image_t image,
                                     rbd_config_option_t *options,
                                     int *max_options) {
  librbd::ImageCtx *ictx = (librbd::ImageCtx*)image;

  std::vector<librbd::config_option_t> option_vector;
  int r = librbd::api::Config<>::list(ictx, &option_vector);
  if (r < 0) {
    return r;
  }

  if (*max_options < static_cast<int>(option_vector.size())) {
    *max_options = static_cast<int>(option_vector.size());
    return -ERANGE;
  }

  for (int i = 0; i < static_cast<int>(option_vector.size()); ++i) {
    config_option_cpp_to_c(option_vector[i], &options[i]);
  }
  *max_options = static_cast<int>(option_vector.size());
  return 0;
}

extern "C" void rbd_config_image_list_cleanup(rbd_config_option_t *options,
                                              int max_options) {
  for (int i = 0; i < max_options; ++i) {
    config_option_cleanup(options[i]);
  }
}
