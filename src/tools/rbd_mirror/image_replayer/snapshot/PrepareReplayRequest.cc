// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PrepareReplayRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/ImageMeta.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::" \
                           << "PrepareReplayRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

using librbd::util::create_context_callback;

template <typename I>
void PrepareReplayRequest<I>::send() {
  *m_resync_requested = false;
  *m_syncing = false;

  load_local_image_meta();
}

template <typename I>
void PrepareReplayRequest<I>::load_local_image_meta() {
  dout(15) << dendl;

  ceph_assert(m_state_builder->local_image_meta == nullptr);
  m_state_builder->local_image_meta =
    librbd::mirror::snapshot::ImageMeta<I>::create(
      m_state_builder->local_image_ctx, m_local_mirror_uuid);

  auto ctx = create_context_callback<
    PrepareReplayRequest<I>,
    &PrepareReplayRequest<I>::handle_load_local_image_meta>(this);
  m_state_builder->local_image_meta->load(ctx);
}

template <typename I>
void PrepareReplayRequest<I>::handle_load_local_image_meta(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to load local image-meta: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  *m_resync_requested = m_state_builder->local_image_meta->resync_requested;
  finish(0);
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::snapshot::PrepareReplayRequest<librbd::ImageCtx>;
