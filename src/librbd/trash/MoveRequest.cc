// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/trash/MoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::trash::MoveRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace trash {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void MoveRequest<I>::send() {
  trash_add();
}

template <typename I>
void MoveRequest<I>::trash_add() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::trash_add(&op, m_image_id, m_trash_image_spec);

  auto aio_comp = create_rados_callback<
    MoveRequest<I>, &MoveRequest<I>::handle_trash_add>(this);
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void MoveRequest<I>::handle_trash_add(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -EEXIST) {
    ldout(m_cct, 10) << "previous unfinished deferred remove for image: "
                     << m_image_id << dendl;
  } else if (r < 0) {
    lderr(m_cct) << "failed to add image to trash: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  remove_id();
}

template <typename I>
void MoveRequest<I>::remove_id() {
  ldout(m_cct, 10) << dendl;

  auto aio_comp = create_rados_callback<
    MoveRequest<I>, &MoveRequest<I>::handle_remove_id>(this);
  int r = m_io_ctx.aio_remove(util::id_obj_name(m_trash_image_spec.name),
                              aio_comp);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void MoveRequest<I>::handle_remove_id(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove image id object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  directory_remove();
}

template <typename I>
void MoveRequest<I>::directory_remove() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::dir_remove_image(&op, m_trash_image_spec.name,
                                       m_image_id);

  auto aio_comp = create_rados_callback<
    MoveRequest<I>, &MoveRequest<I>::handle_directory_remove>(this);
  int r = m_io_ctx.aio_operate(RBD_DIRECTORY, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void MoveRequest<I>::handle_directory_remove(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove image from directory: " << cpp_strerror(r)
                 << dendl;
  }

  finish(r);
}

template <typename I>
void MoveRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace trash
} // namespace librbd

template class librbd::trash::MoveRequest<librbd::ImageCtx>;
