// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_REFRESH_H
#define CEPH_LIBRBD_IMAGE_REFRESH_H

#include "include/int_types.h"

class Context;

namespace librbd {

class ImageCtx;

template <typename ImageCtxT = ImageCtx>
class ImageRefresh {
public:
  ImageRefresh(ImageCtxT &image_ctx);

  bool is_refresh_required() const;

  void refresh(Context *on_finish);

private:
  ImageCtxT &m_image_ctx;

};

} // namespace librbd

extern template class librbd::ImageRefresh<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_REFRESH_H
