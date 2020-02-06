// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Replayer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::" \
                           << "Replayer: " << this << " " << __func__ << ": "

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
Replayer<I>::Replayer(
    Threads<I>* threads,
    const std::string& local_mirror_uuid,
    PoolMetaCache* pool_meta_cache,
    StateBuilder<I>* state_builder,
    ReplayerListener* replayer_listener)
  : m_threads(threads),
    m_local_mirror_uuid(local_mirror_uuid),
    m_pool_meta_cache(pool_meta_cache),
    m_state_builder(state_builder),
    m_replayer_listener(replayer_listener),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "rbd::mirror::image_replayer::snapshot::Replayer", this))) {
  dout(10) << dendl;
}

template <typename I>
Replayer<I>::~Replayer() {
  dout(10) << dendl;
}

template <typename I>
void Replayer<I>::init(Context* on_finish) {
  dout(10) << dendl;

  // TODO
  m_state = STATE_REPLAYING;
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void Replayer<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  // TODO
  m_state = STATE_COMPLETE;
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void Replayer<I>::flush(Context* on_finish) {
  dout(10) << dendl;

  // TODO
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
bool Replayer<I>::get_replay_status(std::string* description,
                                    Context* on_finish) {
  dout(10) << dendl;

  *description = "NOT IMPLEMENTED";
  on_finish->complete(-EEXIST);
  return true;
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::snapshot::Replayer<librbd::ImageCtx>;
