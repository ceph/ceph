// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_APPLY_IMAGE_STATE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_APPLY_IMAGE_STATE_REQUEST_H

#include "common/ceph_mutex.h"
#include "librbd/mirror/snapshot/Types.h"
#include <map>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

template <typename> class EventPreprocessor;
template <typename> class ReplayStatusFormatter;
template <typename> class StateBuilder;

template <typename ImageCtxT>
class ApplyImageStateRequest {
public:
  static ApplyImageStateRequest* create(
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      ImageCtxT* local_image_ctx,
      ImageCtxT* remote_image_ctx,
      librbd::mirror::snapshot::ImageState image_state,
      Context* on_finish) {
    return new ApplyImageStateRequest(local_mirror_uuid, remote_mirror_uuid,
                                      local_image_ctx, remote_image_ctx,
                                      image_state, on_finish);
  }

  ApplyImageStateRequest(
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      ImageCtxT* local_image_ctx,
      ImageCtxT* remote_image_ctx,
      librbd::mirror::snapshot::ImageState image_state,
      Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * RENAME_IMAGE
   *    |
   *    |       /---------\
   *    |       |         |
   *    v       v         |
   * UPDATE_FEATURES -----/
   *    |
   *    v
   * GET_IMAGE_META
   *    |
   *    |       /---------\
   *    |       |         |
   *    v       v         |
   * UPDATE_IMAGE_META ---/
   *    |
   *    |       /---------\
   *    |       |         |
   *    v       v         |
   * UNPROTECT_SNAPSHOT   |
   *    |                 |
   *    v                 |
   * REMOVE_SNAPSHOT      |
   *    |                 |
   *    v                 |
   * PROTECT_SNAPSHOT     |
   *    |                 |
   *    v                 |
   * RENAME_SNAPSHOT -----/
   *    |
   *    v
   * SET_SNAPSHOT_LIMIT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  std::string m_local_mirror_uuid;
  std::string m_remote_mirror_uuid;
  ImageCtxT* m_local_image_ctx;
  ImageCtxT* m_remote_image_ctx;
  librbd::mirror::snapshot::ImageState m_image_state;
  Context* m_on_finish;

  std::map<uint64_t, uint64_t> m_local_to_remote_snap_ids;

  uint64_t m_features = 0;

  std::map<std::string, bufferlist> m_metadata;

  uint64_t m_prev_snap_id = 0;
  std::string m_snap_name;

  void rename_image();
  void handle_rename_image(int r);

  void update_features();
  void handle_update_features(int r);

  void get_image_meta();
  void handle_get_image_meta(int r);

  void update_image_meta();
  void handle_update_image_meta(int r);

  void unprotect_snapshot();
  void handle_unprotect_snapshot(int r);

  void remove_snapshot();
  void handle_remove_snapshot(int r);

  void protect_snapshot();
  void handle_protect_snapshot(int r);

  void rename_snapshot();
  void handle_rename_snapshot(int r);

  void set_snapshot_limit();
  void handle_set_snapshot_limit(int r);

  void finish(int r);

  uint64_t compute_remote_snap_id(uint64_t snap_id);
  void compute_local_to_remote_snap_ids();
};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::snapshot::ApplyImageStateRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_APPLY_IMAGE_STATE_REQUEST_H
