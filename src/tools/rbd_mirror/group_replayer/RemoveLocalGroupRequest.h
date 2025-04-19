// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_REMOVE_LOCAL_GROUP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_REMOVE_LOCAL_GROUP_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
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
class RemoveLocalGroupRequest {
public:

  static RemoveLocalGroupRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      bool resync,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish) {
    return new RemoveLocalGroupRequest(io_ctx, global_group_id,
                                       resync, work_queue, on_finish);
  }

  RemoveLocalGroupRequest(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      bool resync,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish)
    : m_io_ctx(io_ctx), m_global_group_id(global_group_id),
      m_resync(resync), m_work_queue(work_queue), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_GROUP_ID
   *    |
   *    v
   * GET_GROUP_NAME
   *    |
   *    v
   * GET_MIRROR_GROUP
   *    |
   *    v
   * DISABLE_MIRROR_GROUP
   *    |
   *    v
   * REMOVE_IMAGE_FROM_GROUP <--
   *    |                      |
   *    v                      |
   * IMAGE_TRASH_MOVE ----------
   *    |
   *    v
   * REMOVE_LOCAL_GROUP
   *    |
   *    v
   * REMOVE_LOCAL_GROUP_ID
   *    |
   *    v
   * REMOVE_MIRROR_GROUP
   *    |
   *    v
   * NOTIFY_MIRRORING_WATCHER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_group_id;
  bool m_resync;
  librbd::asio::ContextWQ *m_work_queue;
  Context *m_on_finish;

  std::string m_group_id;
  std::string m_group_name;
  uint64_t m_num_images;

  bufferlist m_out_bl;
  std::list<cls::rbd::GroupImageStatus> m_images;

  cls::rbd::MirrorGroup m_mirror_group;
  librbd::mirror::PromotionState m_promotion_state;
  std::map<std::string /*global_image_id*/, std::pair<int64_t/*pool_id*/, std::string /*image_id*/>> m_trash_images;

  bool m_notify_watcher = false;

  void get_local_group_id();
  void handle_get_local_group_id(int r);

  void get_local_group_name();
  void handle_get_local_group_name(int r);

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void disable_mirror_group();
  void handle_disable_mirror_group(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void get_mirror_images();
  void handle_get_mirror_images(int r);

  void remove_image_from_group();
  void handle_remove_image_from_group(int r);

  void move_image_to_trash();
  void handle_move_image_to_trash(int r);

  void remove_local_group();
  void handle_remove_local_group(int r);

  void remove_local_group_id();
  void handle_remove_local_group_id(int r);

  void remove_mirror_group();
  void handle_remove_mirror_group(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void finish(int r);

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::RemoveLocalGroupRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REMOVE_LOCAL_GROUP_REQUEST_H

