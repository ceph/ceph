// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_BREAK_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_BREAK_REQUEST_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "msg/msg_types.h"
#include <list>
#include <string>
#include <boost/optional.hpp>
#include "librbd/managed_lock/Types.h"

class Context;
class ContextWQ;
class obj_watch_t;

namespace librbd {

class AsioEngine;
class ImageCtx;
template <typename> class Journal;
namespace asio { struct ContextWQ; }

namespace managed_lock {

template <typename ImageCtxT = ImageCtx>
class BreakRequest {
public:
  static BreakRequest* create(librados::IoCtx& ioctx,
                              AsioEngine& asio_engine,
                              const std::string& oid, const Locker &locker,
                              bool exclusive, bool blacklist_locker,
                              uint32_t blacklist_expire_seconds,
                              bool force_break_lock, Context *on_finish) {
    return new BreakRequest(ioctx, asio_engine, oid, locker, exclusive,
                            blacklist_locker, blacklist_expire_seconds,
                            force_break_lock, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_WATCHERS
   *    |
   *    v
   * GET_LOCKER
   *    |
   *    v
   * BLACKLIST (skip if disabled)
   *    |
   *    v
   * WAIT_FOR_OSD_MAP
   *    |
   *    v
   * BREAK_LOCK
   *    |
   *    v
   * <finish>
   *
   * @endvertbatim
   */

  librados::IoCtx &m_ioctx;
  CephContext *m_cct;
  AsioEngine& m_asio_engine;
  std::string m_oid;
  Locker m_locker;
  bool m_exclusive;
  bool m_blacklist_locker;
  uint32_t m_blacklist_expire_seconds;
  bool m_force_break_lock;
  Context *m_on_finish;

  bufferlist m_out_bl;

  std::list<obj_watch_t> m_watchers;
  int m_watchers_ret_val;

  Locker m_refreshed_locker;

  BreakRequest(librados::IoCtx& ioctx, AsioEngine& asio_engine,
               const std::string& oid, const Locker &locker,
               bool exclusive, bool blacklist_locker,
               uint32_t blacklist_expire_seconds, bool force_break_lock,
               Context *on_finish);

  void send_get_watchers();
  void handle_get_watchers(int r);

  void send_get_locker();
  void handle_get_locker(int r);

  void send_blacklist();
  void handle_blacklist(int r);

  void wait_for_osd_map();
  void handle_wait_for_osd_map(int r);

  void send_break_lock();
  void handle_break_lock(int r);

  void finish(int r);

};

} // namespace managed_lock
} // namespace librbd

extern template class librbd::managed_lock::BreakRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MANAGED_LOCK_BREAK_REQUEST_H
