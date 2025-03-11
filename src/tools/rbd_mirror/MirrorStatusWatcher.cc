// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
  : Watcher(io_ctx, work_queue, RBD_MIRRORING),
    m_lock(ceph::make_mutex("rbd::mirror::MirrorStatusWatcher " +
                              stringify(io_ctx.get_id()))) {
}

template <typename I>
MirrorStatusWatcher<I>::~MirrorStatusWatcher() {
  ceph_assert(m_on_start_finish == nullptr);
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

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_on_start_finish == nullptr);
    std::swap(m_on_start_finish, on_finish);
  }

  remove_down_image_status();
}

template <typename I>
void MirrorStatusWatcher<I>::remove_down_image_status() {
  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_status_remove_down(&op);
  auto comp = create_rados_callback(
    new LambdaContext([this](int r) {
      handle_remove_down_image_status(r);
    }));

  int r = m_ioctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void MirrorStatusWatcher<I>::handle_remove_down_image_status(int r) {
  dout(20) << "r=" << r << dendl;

  remove_down_group_status();
}

template <typename I>
void MirrorStatusWatcher<I>::remove_down_group_status() {
  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_status_remove_down(&op);
  auto comp = create_rados_callback(
    new LambdaContext([this](int r) {
      handle_remove_down_group_status(r);
    }));

  int r = m_ioctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void MirrorStatusWatcher<I>::handle_remove_down_group_status(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};
    std::swap(m_on_start_finish, on_finish);
  }

  if (on_finish) {
    on_finish->complete(0);
  }
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
  dout(10) << "notify_id=" << notify_id << ", handle=" << handle
           << ", notifier_id=" << notifier_id << dendl;

  bufferlist out;
  acknowledge_notify(notify_id, handle, out);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::MirrorStatusWatcher<librbd::ImageCtx>;
