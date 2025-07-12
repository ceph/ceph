#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "GroupStateBuilder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "GroupStateBuilder: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

template <typename I>
GroupStateBuilder<I>::GroupStateBuilder(const std::string& global_group_id)
  : global_group_id(global_group_id) {
  dout(10) << "global_group_id=" << global_group_id << dendl;
}

template <typename I>
GroupStateBuilder<I>::~GroupStateBuilder() {
  local_images.clear();
  remote_images.clear();
}

template <typename I>
bool GroupStateBuilder<I>::is_local_primary() const {
  if (local_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY) {
    ceph_assert(!local_group_id.empty());
    return true;
  }
  return false;
}

template <typename I>
bool GroupStateBuilder<I>::is_remote_primary() const {
  if (remote_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY) {
    ceph_assert(!remote_group_id.empty());
    return true;
  }
  return false;
}

template <typename I>
bool GroupStateBuilder<I>::is_linked() const {
  if (local_promotion_state == librbd::mirror::PROMOTION_STATE_NON_PRIMARY) {
    ceph_assert(!local_group_id.empty());
    return true;
  }
  return false;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::GroupStateBuilder<librbd::ImageCtx>;
