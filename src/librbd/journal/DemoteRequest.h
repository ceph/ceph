// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_DEMOTE_REQUEST_H
#define CEPH_LIBRBD_JOURNAL_DEMOTE_REQUEST_H

#include "common/Mutex.h"
#include "cls/journal/cls_journal_types.h"
#include "journal/Future.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace journal {

template <typename ImageCtxT = librbd::ImageCtx>
class DemoteRequest {
public:
  static DemoteRequest *create(ImageCtxT &image_ctx, Context *on_finish) {
    return new DemoteRequest(image_ctx, on_finish);
  }

  DemoteRequest(ImageCtxT &image_ctx, Context *on_finish);
  ~DemoteRequest();

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_JOURNALER * * * * *
   *    |                   *
   *    v                   *
   * ALLOCATE_TAG * * * * * *
   *    |                   *
   *    v                   *
   * APPEND_EVENT * * *     *
   *    |             *     *
   *    v             *     *
   * COMMIT_EVENT     *     *
   *    |             *     *
   *    v             *     *
   * STOP_APPEND <* * *     *
   *    |                   *
   *    v                   *
   * SHUT_DOWN_JOURNALER <* *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  typedef typename TypeTraits<ImageCtxT>::Journaler Journaler;
  typedef typename TypeTraits<ImageCtxT>::Future Future;

  ImageCtxT &m_image_ctx;
  Context *m_on_finish;

  Journaler *m_journaler = nullptr;
  int m_ret_val = 0;

  Mutex m_lock;
  ImageClientMeta m_client_meta;
  uint64_t m_tag_tid = 0;
  TagData m_tag_data;

  cls::journal::Tag m_tag;
  Future m_future;

  void open_journaler();
  void handle_open_journaler(int r);

  void allocate_tag();
  void handle_allocate_tag(int r);

  void append_event();
  void handle_append_event(int r);

  void commit_event();
  void handle_commit_event(int r);

  void stop_append();
  void handle_stop_append(int r);

  void shut_down_journaler();
  void handle_shut_down_journaler(int r);

  void finish(int r);

};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::DemoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_DEMOTE_REQUEST_H
