// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_DEMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_DEMOTE_REQUEST_H

#include "include/buffer.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class DemoteRequest {
public:
  static DemoteRequest *create(ImageCtxT *image_ctx,
                               const std::string& global_image_id,
                               Context *on_finish) {
    return new DemoteRequest(image_ctx, global_image_id, on_finish);
  }

  DemoteRequest(ImageCtxT *image_ctx, const std::string& global_image_id,
                Context *on_finish)
    : m_image_ctx(image_ctx), m_global_image_id(global_image_id),
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
   * ENABLE_NON_PRIMARY_FEATURE
   *    |
   *    v
   * CREATE_SNAPSHOT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  std::string m_global_image_id;
  Context *m_on_finish;

  void enable_non_primary_feature();
  void handle_enable_non_primary_feature(int r);

  void create_snapshot();
  void handle_create_snapshot(int r);

  void finish(int r);

};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::DemoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_DEMOTE_REQUEST_H
