// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_METADATA_COPY_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_METADATA_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <map>
#include <string>

class Context;

namespace librbd {
namespace deep_copy {

template <typename ImageCtxT = librbd::ImageCtx>
class MetadataCopyRequest {
public:
  static MetadataCopyRequest* create(ImageCtxT *src_image_ctx,
                                     ImageCtxT *dst_image_ctx,
                                     Context *on_finish) {
    return new MetadataCopyRequest(src_image_ctx, dst_image_ctx, on_finish);
  }

  MetadataCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                      Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_SRC_METADATA <------\
   *    |                     | (repeat if additional
   *    v                     |  metadata)
   * SET_DST_METADATA --------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  typedef std::map<std::string, bufferlist> Metadata;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  Context *m_on_finish;

  CephContext *m_cct;
  bufferlist m_out_bl;

  std::string m_last_metadata_key;
  bool m_more_metadata = false;

  void list_src_metadata();
  void handle_list_src_metadata(int r);

  void set_dst_metadata(const Metadata& metadata);
  void handle_set_dst_metadata(int r);

  void finish(int r);

};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::MetadataCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_METADATA_COPY_REQUEST_H
