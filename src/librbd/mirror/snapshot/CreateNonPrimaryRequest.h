// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_CREATE_NON_PRIMARY_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_CREATE_NON_PRIMARY_REQUEST_H

#include "include/buffer.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/snapshot/Types.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class CreateNonPrimaryRequest {
public:
  static CreateNonPrimaryRequest *create(ImageCtxT *image_ctx,
                                         const std::string &primary_mirror_uuid,
                                         uint64_t primary_snap_id,
                                         const ImageState &image_state,
                                         uint64_t *snap_id,
                                         Context *on_finish) {
    return new CreateNonPrimaryRequest(image_ctx, primary_mirror_uuid,
                                       primary_snap_id, image_state, snap_id,
                                       on_finish);
  }

  CreateNonPrimaryRequest(ImageCtxT *image_ctx,
                          const std::string &primary_mirror_uuid,
                          uint64_t primary_snap_id,
                          const ImageState &image_state, uint64_t *snap_id,
                          Context *on_finish)
    : m_image_ctx(image_ctx), m_primary_mirror_uuid(primary_mirror_uuid),
      m_primary_snap_id(primary_snap_id), m_image_state(image_state),
      m_snap_id(snap_id), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * REFRESH_IMAGE
   *    |
   *    v
   * GET_MIRROR_IMAGE
   *    |
   *    v
   * CREATE_SNAPSHOT
   *    |
   *    v
   * WRITE_IMAGE_STATE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  std::string m_primary_mirror_uuid;
  uint64_t m_primary_snap_id;
  ImageState m_image_state;
  uint64_t *m_snap_id;
  Context *m_on_finish;

  std::string m_snap_name;

  bufferlist m_out_bl;

  bool is_orphan() const {
    return m_primary_mirror_uuid.empty();
  }

  void refresh_image();
  void handle_refresh_image(int r);

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void get_mirror_peers();
  void handle_get_mirror_peers(int r);

  void create_snapshot();
  void handle_create_snapshot(int r);

  void write_image_state();
  void handle_write_image_state(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::CreateNonPrimaryRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_CREATE_NON_PRIMARY_REQUEST_H
