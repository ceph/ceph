#ifndef CEPH_RBD_MIRROR_GROUP_REPLAYER_STATE_BUILDER_H
#define CEPH_RBD_MIRROR_GROUP_REPLAYER_STATE_BUILDER_H

#include "tools/rbd_mirror/group_replayer/Types.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class Threads;

namespace group_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupStateBuilder {

public:
  static GroupStateBuilder* create(const std::string& global_image_id) {
    return new GroupStateBuilder(global_image_id);
  }

  GroupStateBuilder(const std::string& global_image_id);

  ~GroupStateBuilder();

  void destroy() {
    delete this;
  }

  bool is_local_primary() const;
  bool is_remote_primary() const;
  bool is_linked() const; // FIXME: Required?

  std::string global_group_id;
  std::string group_name;

  std::string local_group_id;
  librbd::mirror::PromotionState local_promotion_state =
    librbd::mirror::PROMOTION_STATE_UNKNOWN;
  std::map<std::string /*global-id*/, std::pair<int64_t /*pool_id*/, std::string /*image_id*/>> local_images;

  std::string remote_group_id;
  librbd::mirror::PromotionState remote_promotion_state =
    librbd::mirror::PROMOTION_STATE_UNKNOWN;
  std::string remote_mirror_peer_uuid;
  std::set<GlobalImageId> remote_images;

};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::GroupStateBuilder<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_GROUP_REPLAYER_STATE_BUILDER_H

