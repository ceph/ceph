// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_INIT_REQUEST_H
#define CEPH_LIBRBD_CACHE_RWL_INIT_REQUEST_H

class Context;

namespace librbd {

class ImageCtx;

namespace io { class ImageDispatchInterface; }

namespace plugin { template <typename> struct Api; }

namespace cache {

class ImageWritebackInterface;

namespace pwl {

template<typename>
class AbstractWriteLog;

template<typename>
class ImageCacheState;

template <typename ImageCtxT = ImageCtx>
class InitRequest {
public:
  static InitRequest* create(
      ImageCtxT &image_ctx,
      librbd::cache::ImageWritebackInterface& image_writeback,
      plugin::Api<ImageCtxT>& plugin_api,
      Context *on_finish);

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
   * SET_FEATURE_BIT * * * > CLOSE_IMAGE_CACHE
   *    |                         |
   *    v                         |
   * <finish> <-------------------/
   *
   * @endverbatim
   */

  InitRequest(ImageCtxT &image_ctx,
              librbd::cache::ImageWritebackInterface& image_writeback,
	      plugin::Api<ImageCtxT>& plugin_api,
              Context *on_finish);

  ImageCtxT &m_image_ctx;
  librbd::cache::ImageWritebackInterface& m_image_writeback;
  plugin::Api<ImageCtxT>& m_plugin_api;
  AbstractWriteLog<ImageCtxT> *m_image_cache;
  Context *m_on_finish;

  int m_error_result;

  bool is_pwl_enabled();

  void get_image_cache_state();

  void init_image_cache();
  void handle_init_image_cache(int r);

  void set_feature_bit();
  void handle_set_feature_bit(int r);

  void shutdown_image_cache();
  void handle_shutdown_image_cache(int r);

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }
};

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::InitRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_RWL_INIT_REQUEST_H
