// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_SHUTDOWN_REQUEST_H
#define CEPH_LIBRBD_CACHE_RWL_SHUTDOWN_REQUEST_H

class Context;

namespace librbd {

class ImageCtx;

namespace cache {
namespace rwl {

template<typename>
class ImageCacheState;

template <typename ImageCtxT = ImageCtx>
class ShutdownRequest {
public:
  static ShutdownRequest* create(ImageCtxT &image_ctx, Context *on_finish);

  void send();

private:

  /**
   * @verbatim
   *
   * Shutdown request goes through the following state machine:
   *
   * <start>
   *    |
   *    v
   * SHUTDOWN_IMAGE_CACHE
   *    |
   *    v
   * REMOVE_IMAGE_FEATURE_BIT
   *    |
   *    v
   * REMOVE_IMAGE_CACHE_STATE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ShutdownRequest(ImageCtxT &image_ctx, Context *on_finish);

  ImageCtxT &m_image_ctx;
  Context *m_on_finish;

  int m_error_result;

  void send_shutdown_image_cache();
  void handle_shutdown_image_cache(int r);

  void send_remove_feature_bit();
  void handle_remove_feature_bit(int r);

  void send_remove_image_cache_state();
  void handle_remove_image_cache_state(int r);

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }
};

} // namespace rwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::rwl::ShutdownRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_RWL_SHUTDOWN_REQUEST_H
