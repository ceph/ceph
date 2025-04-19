// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_CREATE_LOCAL_GROUP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_CREATE_LOCAL_GROUP_REQUEST_H

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
class CreateLocalGroupRequest {
public:
  static CreateLocalGroupRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      GroupStateBuilder<ImageCtxT>* state_builder,
      Context *on_finish) {
    return new CreateLocalGroupRequest(io_ctx, global_group_id,
                                       state_builder, on_finish);
  }

  CreateLocalGroupRequest(
      librados::IoCtx &io_ctx,
      const std::string &global_group_id,
      GroupStateBuilder<ImageCtxT>* state_builder,
      Context *on_finish)
    : m_local_io_ctx(io_ctx), m_global_group_id(global_group_id),
      m_state_builder(state_builder),
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
   * ADD_MIRROR_GROUP (state = CREATING)
   *    |
   *    v
   * CREATE_LOCAL_GROUP_ID
   *    |
   *    v
   * CREATE_LOCAL_GROUP
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_local_io_ctx;
  std::string m_global_group_id;
  GroupStateBuilder<ImageCtxT>* m_state_builder;
  Context *m_on_finish;

  cls::rbd::MirrorGroup m_mirror_group;

  void add_mirror_group();
  void handle_add_mirror_group(int r);

  void create_local_group_id();
  void handle_create_local_group_id(int r);

  void create_local_group();
  void handle_create_local_group(int r);

  void finish(int r);

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::CreateLocalGroupRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_CREATE_LOCAL_GROUP_REQUEST_H

