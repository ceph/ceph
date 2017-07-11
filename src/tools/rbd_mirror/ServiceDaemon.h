
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_SERVICE_DAEMON_H
#define CEPH_RBD_MIRROR_SERVICE_DAEMON_H

#include "tools/rbd_mirror/types.h"
#include <string>

struct CephContext;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class ServiceDaemon {
public:
  ServiceDaemon(CephContext *cct, RadosRef rados)
    : m_cct(cct), m_rados(rados) {
  }

  int init();

private:
  CephContext *m_cct;
  RadosRef m_rados;

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ServiceDaemon<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_SERVICE_DAEMON_H
