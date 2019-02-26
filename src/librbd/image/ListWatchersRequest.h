// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_LIST_WATCHERS_REQUEST_H
#define CEPH_LIBRBD_IMAGE_LIST_WATCHERS_REQUEST_H

#include "include/rados/rados_types.hpp"

#include <list>

class Context;

namespace librbd {

class ImageCtx;

namespace image {

enum {
  LIST_WATCHERS_FILTER_OUT_MY_INSTANCE = 1 << 0,
  LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES = 1 << 1,
};

template<typename ImageCtxT = ImageCtx>
class ListWatchersRequest {
public:
  static ListWatchersRequest *create(ImageCtxT &image_ctx, int flags,
                                     std::list<obj_watch_t> *watchers,
                                     Context *on_finish) {
    return new ListWatchersRequest(image_ctx, flags, watchers, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_IMAGE_WATCHERS
   *    |
   *    v
   * LIST_MIRROR_WATCHERS (skip if not needed)
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ListWatchersRequest(ImageCtxT &image_ctx, int flags, std::list<obj_watch_t> *watchers,
                      Context *on_finish);

  ImageCtxT& m_image_ctx;
  int m_flags;
  std::list<obj_watch_t> *m_watchers;
  Context *m_on_finish;

  CephContext *m_cct;
  int m_ret_val;
  bufferlist m_out_bl;
  std::list<obj_watch_t> m_object_watchers;
  std::list<obj_watch_t> m_mirror_watchers;

  void list_image_watchers();
  void handle_list_image_watchers(int r);

  void list_mirror_watchers();
  void handle_list_mirror_watchers(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::ListWatchersRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_LIST_WATCHERS_REQUEST_H
