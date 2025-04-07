// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_CREATE_LOCAL_GROUP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_CREATE_LOCAL_GROUP_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include <string>
#include "GroupStateBuilder.h"

struct Context;

namespace librbd {
struct ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {
namespace group_replayer {

//template <typename> class GroupStateBuilder;

template <typename ImageCtxT = librbd::ImageCtx>
class CreateLocalGroupRequest {
public:
  static CreateLocalGroupRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      GroupStateBuilder<ImageCtxT>* state_builder,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish) {
    return new CreateLocalGroupRequest(io_ctx, global_group_id,
                                       state_builder, work_queue, on_finish);
  }

  CreateLocalGroupRequest(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      GroupStateBuilder<ImageCtxT>* state_builder,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish)
    : m_local_io_ctx(io_ctx), m_global_group_id(global_group_id),
      m_state_builder(state_builder), m_work_queue(work_queue),
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
   * DISABLE_MIRROR_GROUP
   *    |
   *    v
   * REMOVE_MIRROR_GROUP
   *    |
   *    v
   * ADD_MIRROR_GROUP (state = CREATING)
   *    |
   *    v
   * CREATE_LOCAL_GROUP_ID
   *    |
   *    v
   * CREATE_LOCAL_GROUP
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
  std::string m_global_group_id;
  GroupStateBuilder<ImageCtxT>* m_state_builder;
  librbd::asio::ContextWQ *m_work_queue; // TODO: remove this
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_local_group_id;
  cls::rbd::MirrorGroup m_mirror_group;
  librbd::mirror::PromotionState m_promotion_state;
  std::string m_primary_mirror_uuid;

  void disable_mirror_group();
  void handle_disable_mirror_group(int r);

  void remove_mirror_group();
  void handle_remove_mirror_group(int r);

  void add_mirror_group();
  void handle_add_mirror_group(int r);

  void create_local_group_id();
  void handle_create_local_group_id(int r);

  void create_local_group();
  void handle_create_local_group(int r);

  void enable_mirror_group();
  void handle_enable_mirror_group(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void finish(int r);

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::CreateLocalGroupRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_CREATE_LOCAL_GROUP_REQUEST_H

