// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageRefresh.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/image/RefreshRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageRefresh: "

namespace librbd {

template <typename I>
ImageRefresh<I>::ImageRefresh(I &image_ctx) : m_image_ctx(image_ctx) {
}

template <typename I>
bool ImageRefresh<I>::is_refresh_required() const {
  // TODO future entry point for AIO ops -- to replace ictx_check call
  return false;
}

template <typename I>
void ImageRefresh<I>::refresh(Context *on_finish) {
  // TODO simple state machine to restrict to a single in-progress refresh / snap set
  image::RefreshRequest<I> *req = image::RefreshRequest<I>::create(
    m_image_ctx, on_finish);
  req->send();
}

} // namespace librbd

template class librbd::ImageRefresh<librbd::ImageCtx>;
