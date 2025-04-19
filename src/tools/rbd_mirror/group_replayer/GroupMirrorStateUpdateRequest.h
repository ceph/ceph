// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_GROUP_REPLAYER_GROUP_MIRROR_STATE_UPDATE_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_GROUP_MIRROR_STATE_UPDATE_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include <string>

struct Context;

namespace librbd {
struct ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {
namespace group_replayer {

template <typename> class GroupStateBuilder;

template <typename ImageCtxT = librbd::ImageCtx>
class GroupMirrorStateUpdateRequest {
public:
  static GroupMirrorStateUpdateRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &local_group_id,
      uint64_t num_images,
      Context *on_finish) {
    return new GroupMirrorStateUpdateRequest(io_ctx, local_group_id,
                                             num_images, on_finish);
  }

  GroupMirrorStateUpdateRequest(
      librados::IoCtx &io_ctx,
      const std::string &local_group_id,
      uint64_t num_images,
      Context *on_finish)
    : m_local_io_ctx(io_ctx), m_local_group_id(local_group_id),
      m_num_images(num_images), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_GROUP (state = CREATING)
   *    |
   *    v
   * ENABLE_MIRROR_GROUP
   *    |
   *    v
   * NOTIFY_MIRRORING_WATCHER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_local_io_ctx;
  std::string m_local_group_id;
  uint64_t m_num_images;
  Context *m_on_finish;

  cls::rbd::MirrorGroup m_mirror_group;
  bufferlist m_out_bl;

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void enable_mirror_group();
  void handle_enable_mirror_group(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void finish(int r);

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::GroupMirrorStateUpdateRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_GROUP_MIRROR_STATE_UPDATE_REQUEST_H

