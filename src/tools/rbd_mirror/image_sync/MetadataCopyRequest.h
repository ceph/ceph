// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_METADATA_COPY_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_METADATA_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <map>
#include <string>

class Context;

namespace rbd {
namespace mirror {
namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class MetadataCopyRequest {
public:
  static MetadataCopyRequest* create(ImageCtxT *local_image_ctx,
                                     ImageCtxT *remote_image_ctx,
                                     Context *on_finish) {
    return new MetadataCopyRequest(local_image_ctx, remote_image_ctx,
                                   on_finish);
  }

  MetadataCopyRequest(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
                      Context *on_finish)
    : m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_REMOTE_METADATA <-----\
   *    |                       | (repeat if additional
   *    v                       |  metadata)
   * SET_LOCAL_METADATA --------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  typedef std::map<std::string, bufferlist> Metadata;

  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  Context *m_on_finish;

  bufferlist m_out_bl;

  std::string m_last_metadata_key;
  bool m_more_metadata = false;

  void list_remote_metadata();
  void handle_list_remote_data(int r);

  void set_local_metadata(const Metadata& metadata);
  void handle_set_local_metadata(int r);

  void finish(int r);

};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::MetadataCopyRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_METADATA_COPY_REQUEST_H
