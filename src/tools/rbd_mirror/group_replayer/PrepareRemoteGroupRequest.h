// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_GROUP_REPLAYER_PREPARE_REMOTE_GROUP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_PREPARE_REMOTE_GROUP_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include "tools/rbd_mirror/group_replayer/Types.h"
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
class PrepareRemoteGroupRequest {
public:
  static PrepareRemoteGroupRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      std::string *remote_group_name,
      GroupStateBuilder<ImageCtxT>** state_builder,
      Context *on_finish) {
    return new PrepareRemoteGroupRequest(io_ctx, global_group_id,
                                         remote_group_name, state_builder,
                                         on_finish);
  }

  PrepareRemoteGroupRequest(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      std::string *remote_group_name,
      GroupStateBuilder<ImageCtxT>** state_builder,
      Context *on_finish)
    : m_io_ctx(io_ctx), m_global_group_id(global_group_id),
      m_remote_group_name(remote_group_name), m_state_builder(state_builder),
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
   * GET_REMOTE_GROUP_ID
   *    |
   *    v
   * GET_REMOTE_GROUP_NAME
   *    |
   *    v
   * GET_MIRROR_INFO
   *    |
   *    |
   *    v
   * LIST_GROUP_IMAGES
   *    |
   *    v
   * GET_MIRROR_IMAGES
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_group_id;
  std::string *m_remote_group_name; // FIXME: Not required?
  GroupStateBuilder<ImageCtxT>** m_state_builder;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_remote_group_id;
  cls::rbd::MirrorGroup m_mirror_group;
  librbd::mirror::PromotionState m_promotion_state;

  std::list<cls::rbd::GroupImageStatus> m_images;
  std::set<GlobalImageId> m_remote_images;

  void get_remote_group_id();
  void handle_get_remote_group_id(int r);

  void get_remote_group_name();
  void handle_get_remote_group_name(int r);

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void get_mirror_images();
  void handle_get_mirror_images(int r);

  void finish(int r);

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::PrepareRemoteGroupRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_PREPARE_REMOTE_GROUP_REQUEST_H

