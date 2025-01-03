// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_THREADS_H
#define CEPH_RBD_MIRROR_THREADS_H

#include "include/common_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include <memory>

class ThreadPool;

namespace librbd {
struct AsioEngine;
struct ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class Threads {
public:
  librbd::AsioEngine* asio_engine = nullptr;
  librbd::asio::ContextWQ* work_queue = nullptr;

  SafeTimer *timer = nullptr;
  ceph::mutex timer_lock = ceph::make_mutex("Threads::timer_lock");

  explicit Threads(std::shared_ptr<librados::Rados>& rados);
  Threads(const Threads&) = delete;
  Threads& operator=(const Threads&) = delete;

  ~Threads();
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::Threads<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_THREADS_H
