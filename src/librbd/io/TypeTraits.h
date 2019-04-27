// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_TYPE_TRAITS_H
#define CEPH_LIBRBD_IO_TYPE_TRAITS_H

class SafeTimer;

namespace librbd {
namespace io {

template <typename IoCtxT>
struct TypeTraits {
  typedef ::SafeTimer SafeTimer;
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_TYPE_TRAITS_H
