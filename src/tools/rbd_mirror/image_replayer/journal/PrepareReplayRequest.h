// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_PREPARE_REPLAY_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_PREPARE_REPLAY_REQUEST_H

#include "include/int_types.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/Types.h"
#include <list>
#include <string>

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

class ProgressContext;

namespace image_replayer {
namespace journal {

template <typename ImageCtxT>
class PrepareReplayRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  static PrepareReplayRequest* create(
      ImageCtxT* local_image_ctx, Journaler* remote_journaler,
      librbd::mirror::PromotionState remote_promotion_state,
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      MirrorPeerClientMeta* client_meta, ProgressContext* progress_ctx,
      bool* resync_requested, bool* syncing, Context* on_finish) {
    return new PrepareReplayRequest(
      local_image_ctx, remote_journaler, remote_promotion_state,
      local_mirror_uuid, remote_mirror_uuid, client_meta, progress_ctx,
      resync_requested, syncing, on_finish);
  }

  PrepareReplayRequest(
      ImageCtxT* local_image_ctx, Journaler* remote_journaler,
      librbd::mirror::PromotionState remote_promotion_state,
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      MirrorPeerClientMeta* client_meta, ProgressContext* progress_ctx,
      bool* resync_requested, bool* syncing, Context* on_finish)
    : m_local_image_ctx(local_image_ctx), m_remote_journaler(remote_journaler),
      m_remote_promotion_state(remote_promotion_state),
      m_local_mirror_uuid(local_mirror_uuid),
      m_remote_mirror_uuid(remote_mirror_uuid), m_client_meta(client_meta),
      m_progress_ctx(progress_ctx), m_resync_requested(resync_requested),
      m_syncing(syncing), m_on_finish(on_finish) {
  }

  void send();

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

  ImageCtxT* m_local_image_ctx;
  Journaler* m_remote_journaler;
  librbd::mirror::PromotionState m_remote_promotion_state;
  std::string m_local_mirror_uuid;
  std::string m_remote_mirror_uuid;
  MirrorPeerClientMeta* m_client_meta;
  ProgressContext* m_progress_ctx;
  bool* m_resync_requested;
  bool* m_syncing;
  Context* m_on_finish;

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

  void finish(int r);
  void update_progress(const std::string& description);

};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::journal::PrepareReplayRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_PREPARE_REPLAY_REQUEST_H
