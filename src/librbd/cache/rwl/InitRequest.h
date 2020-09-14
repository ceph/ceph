// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_INIT_REQUEST_H
#define CEPH_LIBRBD_CACHE_RWL_INIT_REQUEST_H

class Context;

namespace librbd {

class ImageCtx;

namespace cache {
namespace rwl {

template<typename>
class ImageCacheState;

template <typename ImageCtxT = ImageCtx>
class InitRequest {
public:
  static InitRequest* create(ImageCtxT &image_ctx, Context *on_finish);

  void send();

private:

  /**
   * @verbatim
   *
   * Init request goes through the following state machine:
   *
   * <start>
   *    |
   *    v
   * GET_IMAGE_CACHE_STATE
   *    |
   *    v
   * INIT_IMAGE_CACHE
   *    |
   *    v
   * SET_FEATURE_BIT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  InitRequest(ImageCtxT &image_ctx, Context *on_finish);

  ImageCtxT &m_image_ctx;
  Context *m_on_finish;

  int m_error_result;

  bool is_rwl_enabled();

  void get_image_cache_state();

  void init_image_cache();
  void handle_init_image_cache(int r);

  void set_feature_bit();
  void handle_set_feature_bit(int r);

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

extern template class librbd::cache::rwl::InitRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_RWL_INIT_REQUEST_H
