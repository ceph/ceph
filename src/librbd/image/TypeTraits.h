// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_TYPE_TRAITS_H
#define CEPH_LIBRBD_IMAGE_TYPE_TRAITS_H

namespace librbd {

namespace asio { struct ContextWQ; }

namespace image {

template <typename ImageCtxT>
struct TypeTraits {
  typedef asio::ContextWQ ContextWQ;
};

} // namespace image
} // namespace librbd

#endif // CEPH_LIBRBD_IMAGE_TYPE_TRAITS_H
