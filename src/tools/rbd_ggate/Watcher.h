// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_GGATE_WATCHER_H
#define CEPH_RBD_GGATE_WATCHER_H

#include "include/rbd/librbd.hpp"

namespace rbd {
namespace ggate {

class Driver;

class Watcher : public librbd::UpdateWatchCtx
{
public:
  Watcher(Driver *m_drv, librados::IoCtx &ioctx, librbd::Image &image,
          size_t size);

  void handle_notify() override;

private:
  Driver *m_drv;
  librados::IoCtx &m_ioctx;
  librbd::Image &m_image;
  size_t m_size;
};


} // namespace ggate
} // namespace rbd

#endif // CEPH_RBD_GGATE_WATCHER_H

