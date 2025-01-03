// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MirrorStatusWatcher.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::MirrorStatusWatcher: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

using librbd::util::create_rados_callback;

template <typename I>
MirrorStatusWatcher<I>::MirrorStatusWatcher(librados::IoCtx &io_ctx,
                                            librbd::asio::ContextWQ *work_queue)
  : Watcher(io_ctx, work_queue, RBD_MIRRORING) {
}

template <typename I>
MirrorStatusWatcher<I>::~MirrorStatusWatcher() {
}

template <typename I>
void MirrorStatusWatcher<I>::init(Context *on_finish) {
  dout(20) << dendl;

  on_finish = new LambdaContext(
    [this, on_finish] (int r) {
      if (r < 0) {
        derr << "error removing down statuses: " << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }
      register_watch(on_finish);
    });

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_status_remove_down(&op);
  librados::AioCompletion *aio_comp = create_rados_callback(on_finish);

  int r = m_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void MirrorStatusWatcher<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  unregister_watch(on_finish);
}

template <typename I>
void MirrorStatusWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                           uint64_t notifier_id,
                                           bufferlist &bl) {
  dout(20) << dendl;

  bufferlist out;
  acknowledge_notify(notify_id, handle, out);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::MirrorStatusWatcher<librbd::ImageCtx>;
