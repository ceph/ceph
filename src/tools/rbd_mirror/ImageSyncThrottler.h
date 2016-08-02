// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RBD_MIRROR_IMAGE_SYNC_THROTTLER_H
#define CEPH_RBD_MIRROR_IMAGE_SYNC_THROTTLER_H

#include <list>
#include <map>
#include <utility>
#include "common/Mutex.h"
#include "librbd/ImageCtx.h"
#include "include/Context.h"
#include "librbd/journal/TypeTraits.h"

class CephContext;
class Context;
class ContextWQ;
class SafeTimer;
namespace journal { class Journaler; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {

template <typename> class ImageSync;

class ProgressContext;

/**
 * Manage concurrent image-syncs
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageSyncThrottler : public md_config_obs_t {
public:

  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  ImageSyncThrottler();
  ~ImageSyncThrottler();
  ImageSyncThrottler(const ImageSyncThrottler&) = delete;
  ImageSyncThrottler& operator=(const ImageSyncThrottler&) = delete;

  void start_sync(ImageCtxT *local_image_ctx,
                  ImageCtxT *remote_image_ctx, SafeTimer *timer,
                  Mutex *timer_lock, const std::string &mirror_uuid,
                  Journaler *journaler, MirrorPeerClientMeta *client_meta,
                  ContextWQ *work_queue, Context *on_finish,
                  ProgressContext *progress_ctx = nullptr);

  void cancel_sync(librados::IoCtx &local_io_ctx,
                   const std::string local_image_id);

  void set_max_concurrent_syncs(uint32_t max);

  void print_status(Formatter *f, std::stringstream *ss);

private:
  typedef std::pair<int64_t, std::string> PoolImageId;

  struct C_SyncHolder : public Context {
    ImageSyncThrottler<ImageCtxT> *m_sync_throttler;
    PoolImageId m_local_pool_image_id;
    ImageSync<ImageCtxT> *m_sync = nullptr;
    Context *m_on_finish;

    C_SyncHolder(ImageSyncThrottler<ImageCtxT> *sync_throttler,
                 const PoolImageId &local_pool_image_id, Context *on_finish)
      : m_sync_throttler(sync_throttler),
        m_local_pool_image_id(local_pool_image_id), m_on_finish(on_finish) {
    }

    virtual void finish(int r) {
      m_sync_throttler->handle_sync_finished(this);
      m_on_finish->complete(r);
    }
  };

  void handle_sync_finished(C_SyncHolder *sync_holder);

  const char **get_tracked_conf_keys() const;
  void handle_conf_change(const struct md_config_t *conf,
                          const std::set<std::string> &changed);

  uint32_t m_max_concurrent_syncs;
  Mutex m_lock;
  std::list<C_SyncHolder *> m_sync_queue;
  std::map<PoolImageId, C_SyncHolder *> m_inflight_syncs;

};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_SYNC_THROTTLER_H
