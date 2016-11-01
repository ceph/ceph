// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_OPEN_REQUEST_H
#define CEPH_LIBRBD_JOURNAL_OPEN_REQUEST_H

#include "include/int_types.h"
#include "librbd/journal/TypeTraits.h"

struct Context;
struct Mutex;

namespace librbd {

struct ImageCtx;

namespace journal {

struct ImageClientMeta;
struct TagData;

template <typename ImageCtxT = ImageCtx>
class OpenRequest {
public:
  typedef typename TypeTraits<ImageCtxT>::Journaler Journaler;

  static OpenRequest* create(ImageCtxT *image_ctx, Journaler *journaler,
                             Mutex *lock, journal::ImageClientMeta *client_meta,
                             uint64_t *tag_tid, journal::TagData *tag_data,
                             Context *on_finish) {
    return new OpenRequest(image_ctx, journaler, lock, client_meta, tag_tid,
                           tag_data, on_finish);
  }

  OpenRequest(ImageCtxT *image_ctx, Journaler *journaler, Mutex *lock,
              journal::ImageClientMeta *client_meta, uint64_t *tag_tid,
              journal::TagData *tag_data, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   *  INIT
   *    |
   *    v
   * GET_TAGS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */


  ImageCtxT *m_image_ctx;
  Journaler *m_journaler;
  Mutex *m_lock;
  journal::ImageClientMeta *m_client_meta;
  uint64_t *m_tag_tid;
  journal::TagData *m_tag_data;
  Context *m_on_finish;

  uint64_t m_tag_class = 0;

  void send_init();
  void handle_init(int r);

  void send_get_tags();
  void handle_get_tags(int r);

  void finish(int r);

};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::OpenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_OPEN_REQUEST_H
