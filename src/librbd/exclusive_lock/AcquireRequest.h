// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include "msg/msg_types.h"
#include <map>
#include <string>

class Context;

namespace librbd {

class Journal;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class AcquireRequest {
public:
  static AcquireRequest* create(ImageCtxT &image_ctx, const std::string &cookie,
                                Context *on_finish);

  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |     /---------------------------------------------------------\
   *    |     |                                                         |
   *    |     |             (no lockers)                                |
   *    |     |   . . . . . . . . . . . . . . . . . . . . .             |
   *    |     |   .                                       .             |
   *    |     v   v      (EBUSY)                          .             |
   *    \--> LOCK_IMAGE * * * * * * * > GET_LOCKERS . . . .             |
   *          .   |                       |                             |
   *    . . . .   |                       |                             |
   *    .         v                       v                             |
   *    .     OPEN_JOURNAL  . . .       GET_WATCHERS . . .              |
   *    .         |             .         |              .              |
   *    .         v             .         v              .              |
   *    . . > OPEN_OBJECT_MAP   .       BLACKLIST        . (blacklist   |
   *    .         |             .         |              .  disabled)   |
   *    .         v             .         v              .              |
   *    .     LOCK_OBJECT_MAP   .       BREAK_LOCK < . . .              |
   *    .         |             .         |                             |
   *    .         v             .         |                             |
   *    . . > <finish>  < . . . .         |                             |
   *                                      \-----------------------------/
   *
   * @endverbatim
   */

  AcquireRequest(ImageCtxT &image_ctx, const std::string &cookie,
                 Context *on_finish);

  ImageCtxT &m_image_ctx;
  std::string m_cookie;
  Context *m_on_finish;

  bufferlist m_out_bl;

  std::list<obj_watch_t> m_watchers;
  int m_watchers_ret_val;

  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;

  entity_name_t m_locker_entity;
  std::string m_locker_cookie;
  std::string m_locker_address;
  uint64_t m_locker_handle;

  void send_lock();
  Context *handle_lock(int *ret_val);

  Context *send_open_journal();
  Context *handle_open_journal(int *ret_val);

  Context *send_open_object_map();
  Context *handle_open_object_map(int *ret_val);

  Context *send_lock_object_map();
  Context *handle_lock_object_map(int *ret_val);

  void send_get_lockers();
  Context *handle_get_lockers(int *ret_val);

  void send_get_watchers();
  Context *handle_get_watchers(int *ret_val);

  void send_blacklist();
  Context *handle_blacklist(int *ret_val);

  void send_break_lock();
  Context *handle_break_lock(int *ret_val);

  void apply();
};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::AcquireRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
