// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_PREPARE_REPLAY_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_PREPARE_REPLAY_REQUEST_H

#include "include/int_types.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Types.h"
#include "librbd/mirror/Types.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include <list>
#include <string>

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

class ProgressContext;

namespace image_replayer {
namespace journal {

template <typename> class StateBuilder;

template <typename ImageCtxT>
class PrepareReplayRequest : public BaseRequest {
public:
  static PrepareReplayRequest* create(
      const std::string& local_mirror_uuid,
      ProgressContext* progress_ctx,
      StateBuilder<ImageCtxT>* state_builder,
      bool* resync_requested,
      bool* syncing,
      Context* on_finish) {
    return new PrepareReplayRequest(
      local_mirror_uuid, progress_ctx, state_builder, resync_requested,
      syncing, on_finish);
  }

  PrepareReplayRequest(
      const std::string& local_mirror_uuid,
      ProgressContext* progress_ctx,
      StateBuilder<ImageCtxT>* state_builder,
      bool* resync_requested,
      bool* syncing,
      Context* on_finish)
    : BaseRequest(on_finish),
      m_local_mirror_uuid(local_mirror_uuid),
      m_progress_ctx(progress_ctx),
      m_state_builder(state_builder),
      m_resync_requested(resync_requested),
      m_syncing(syncing) {
  }

  void send() override;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UPDATE_CLIENT_STATE
   *    |
   *    v
   * GET_REMOTE_TAG_CLASS
   *    |
   *    v
   * GET_REMOTE_TAGS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  typedef std::list<cls::journal::Tag> Tags;

  std::string m_local_mirror_uuid;
  ProgressContext* m_progress_ctx;
  StateBuilder<ImageCtxT>* m_state_builder;
  bool* m_resync_requested;
  bool* m_syncing;

  uint64_t m_local_tag_tid = 0;
  librbd::journal::TagData m_local_tag_data;

  uint64_t m_remote_tag_class = 0;
  Tags m_remote_tags;
  cls::journal::Client m_client;

  void update_client_state();
  void handle_update_client_state(int r);

  void get_remote_tag_class();
  void handle_get_remote_tag_class(int r);

  void get_remote_tags();
  void handle_get_remote_tags(int r);

  void update_progress(const std::string& description);

};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::journal::PrepareReplayRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_PREPARE_REPLAY_REQUEST_H
