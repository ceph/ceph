// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_PREPARE_LOCAL_GROUP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_PREPARE_LOCAL_GROUP_REQUEST_H

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
class PrepareLocalGroupRequest {
public:
  static PrepareLocalGroupRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      std::string *local_group_name,
      GroupStateBuilder<ImageCtxT>** state_builder,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish) {
    return new PrepareLocalGroupRequest(io_ctx, global_group_id,
                                        local_group_name, state_builder,
                                        work_queue, on_finish);
  }

  PrepareLocalGroupRequest(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      std::string *local_group_name,
      GroupStateBuilder<ImageCtxT>** state_builder,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish)
    : m_io_ctx(io_ctx), m_global_group_id(global_group_id),
      m_local_group_name(local_group_name), m_state_builder(state_builder),
      m_work_queue(work_queue), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_LOCAL_GROUP_ID
   *    |
   *    v
   * GET_LOCAL_GROUP_NAME
   *    |
   *    v
   * GET_MIRROR_INFO ---------------
   *    |                          |
   *    v                   (if the group mirror state is CREATING)
   * LIST_GROUP_IMAGES             |
   *    |                          |
   *    v                          v
   * GET_MIRROR_IMAGES        REMOVE_LOCAL_GROUP
   *    |                          |
   *    v                          |
   * <finish>  <--------------------
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_group_id;
  std::string *m_local_group_name;
  GroupStateBuilder<ImageCtxT>** m_state_builder;
  librbd::asio::ContextWQ *m_work_queue;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_local_group_id;
  cls::rbd::MirrorGroup m_mirror_group;
  librbd::mirror::PromotionState m_promotion_state;
  std::list<cls::rbd::GroupImageStatus> m_images;
  std::map<std::string /*global-id*/, std::pair<int64_t /*pool_id*/, std::string /*image_id*/>> m_local_images;

  void get_local_group_id();
  void handle_get_local_group_id(int r);

  void get_local_group_name();
  void handle_get_local_group_name(int r);

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void get_mirror_images();
  void handle_get_mirror_images(int r);

  void remove_local_group();
  void handle_remove_local_group(int r);

  void finish(int r);

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::PrepareLocalGroupRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_PREPARE_LOCAL_GROUP_REQUEST_H

