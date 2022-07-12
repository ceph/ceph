// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_STATE_BUILDER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_STATE_BUILDER_H

#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

struct BaseRequest;
template <typename> class InstanceWatcher;
struct PoolMetaCache;
struct ProgressContext;
template <typename> class Threads;

namespace image_sync { struct SyncPointHandler; }

namespace image_replayer {

struct Replayer;
struct ReplayerListener;

template <typename ImageCtxT>
class StateBuilder {
public:
  StateBuilder(const StateBuilder&) = delete;
  StateBuilder& operator=(const StateBuilder&) = delete;

  virtual ~StateBuilder();

  virtual void destroy() {
    delete this;
  }

  virtual void close(Context* on_finish) = 0;

  virtual bool is_disconnected() const = 0;

  bool is_local_primary() const;
  bool is_linked() const;

  virtual cls::rbd::MirrorImageMode get_mirror_image_mode() const = 0;

  virtual image_sync::SyncPointHandler* create_sync_point_handler() = 0;
  void destroy_sync_point_handler();

  virtual bool replay_requires_remote_image() const = 0;

  void close_remote_image(Context* on_finish);

  virtual BaseRequest* create_local_image_request(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& local_io_ctx,
      const std::string& global_image_id,
      PoolMetaCache* pool_meta_cache,
      ProgressContext* progress_ctx,
      Context* on_finish) = 0;

  virtual BaseRequest* create_prepare_replay_request(
      const std::string& local_mirror_uuid,
      ProgressContext* progress_ctx,
      bool* resync_requested,
      bool* syncing,
      Context* on_finish) = 0;

  virtual Replayer* create_replayer(
      Threads<ImageCtxT>* threads,
      InstanceWatcher<ImageCtxT>* instance_watcher,
      const std::string& local_mirror_uuid,
      PoolMetaCache* pool_meta_cache,
      ReplayerListener* replayer_listener) = 0;

  std::string global_image_id;

  std::string local_image_id;
  librbd::mirror::PromotionState local_promotion_state =
    librbd::mirror::PROMOTION_STATE_PRIMARY;
  ImageCtxT* local_image_ctx = nullptr;

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  librbd::mirror::PromotionState remote_promotion_state =
    librbd::mirror::PROMOTION_STATE_NON_PRIMARY;
  ImageCtxT* remote_image_ctx = nullptr;

protected:
  image_sync::SyncPointHandler* m_sync_point_handler = nullptr;

  StateBuilder(const std::string& global_image_id);

  void close_local_image(Context* on_finish);

private:
  virtual bool is_linked_impl() const = 0;

  void handle_close_local_image(int r, Context* on_finish);
  void handle_close_remote_image(int r, Context* on_finish);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::StateBuilder<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_STATE_BUILDER_H
