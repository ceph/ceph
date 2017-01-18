// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_REACQUIRE_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_REACQUIRE_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/int_types.h"
#include <string>

class Context;

namespace librbd {

class Watcher;

namespace managed_lock {

template <typename ImageCtxT>
class ReacquireRequest {
public:

  static ReacquireRequest *create(librados::IoCtx& ioctx,
                                  const std::string& oid,
                                  const std::string& old_cookie,
                                  const std::string &new_cookie,
                                  bool exclusive,
                                  Context *on_finish) {
    return new ReacquireRequest(ioctx, oid, old_cookie, new_cookie, exclusive,
                                on_finish);
  }

  ReacquireRequest(librados::IoCtx& ioctx, const std::string& oid,
                   const std::string& old_cookie,
                   const std::string &new_cookie, bool exclusive,
                   Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * SET_COOKIE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  librados::IoCtx& m_ioctx;
  std::string m_oid;
  std::string m_old_cookie;
  std::string m_new_cookie;
  bool m_exclusive;
  Context *m_on_finish;

  void set_cookie();
  void handle_set_cookie(int r);

};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_REACQUIRE_REQUEST_H
