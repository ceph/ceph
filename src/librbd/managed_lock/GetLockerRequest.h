// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_GET_LOCKER_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_GET_LOCKER_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include <string>

class Context;

namespace librbd {

struct ImageCtx;

namespace managed_lock {

struct Locker;

template <typename ImageCtxT = ImageCtx>
class GetLockerRequest {
public:
  static GetLockerRequest* create(librados::IoCtx& ioctx,
                                  const std::string& oid, bool exclusive,
                                  Locker *locker, Context *on_finish) {
    return new GetLockerRequest(ioctx, oid, exclusive, locker, on_finish);
  }

  void send();

private:
  librados::IoCtx &m_ioctx;
  CephContext *m_cct;
  std::string m_oid;
  bool m_exclusive;
  Locker *m_locker;
  Context *m_on_finish;

  bufferlist m_out_bl;

  GetLockerRequest(librados::IoCtx& ioctx, const std::string& oid,
                   bool exclusive, Locker *locker, Context *on_finish);

  void send_get_lockers();
  void handle_get_lockers(int r);

  void finish(int r);

};

} // namespace managed_lock
} // namespace librbd

extern template class librbd::managed_lock::GetLockerRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MANAGED_LOCK_GET_LOCKER_REQUEST_H
