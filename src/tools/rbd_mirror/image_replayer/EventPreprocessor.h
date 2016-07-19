// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_EVENT_PREPROCESSOR_H
#define RBD_MIRROR_IMAGE_REPLAYER_EVENT_PREPROCESSOR_H

#include "include/int_types.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include <map>
#include <string>
#include <boost/variant/static_visitor.hpp>

struct Context;
struct ContextWQ;
namespace journal { class Journaler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class EventPreprocessor {
public:
  using Journaler = typename librbd::journal::TypeTraits<ImageCtxT>::Journaler;
  using EventEntry = librbd::journal::EventEntry;
  using MirrorPeerClientMeta = librbd::journal::MirrorPeerClientMeta;

  static EventPreprocessor *create(ImageCtxT &local_image_ctx,
                                   Journaler &remote_journaler,
                                   const std::string &local_mirror_uuid,
                                   MirrorPeerClientMeta *client_meta,
                                   ContextWQ *work_queue) {
    return new EventPreprocessor(local_image_ctx, remote_journaler,
                                 local_mirror_uuid, client_meta, work_queue);
  }

  EventPreprocessor(ImageCtxT &local_image_ctx, Journaler &remote_journaler,
                    const std::string &local_mirror_uuid,
                    MirrorPeerClientMeta *client_meta, ContextWQ *work_queue);
  ~EventPreprocessor();

  bool is_required(const EventEntry &event_entry);
  void preprocess(EventEntry *event_entry, Context *on_finish);

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v (skip if not required)
   * REFRESH_IMAGE
   *    |
   *    v (skip if not required)
   * PREPROCESS_EVENT
   *    |
   *    v (skip if not required)
   * UPDATE_CLIENT
   *
   * @endverbatim
   */

  typedef std::map<uint64_t, uint64_t> SnapSeqs;

  class PreprocessEventVisitor : public boost::static_visitor<int> {
  public:
    EventPreprocessor *event_preprocessor;

    PreprocessEventVisitor(EventPreprocessor *event_preprocessor)
      : event_preprocessor(event_preprocessor) {
    }

    template <typename T>
    inline int operator()(T&) const {
      return 0;
    }
    inline int operator()(librbd::journal::SnapRenameEvent &event) const {
      return event_preprocessor->preprocess_snap_rename(event);
    }
  };

  ImageCtxT &m_local_image_ctx;
  Journaler &m_remote_journaler;
  std::string m_local_mirror_uuid;
  MirrorPeerClientMeta *m_client_meta;
  ContextWQ *m_work_queue;

  bool m_in_progress = false;
  EventEntry *m_event_entry = nullptr;
  Context *m_on_finish = nullptr;

  SnapSeqs m_snap_seqs;
  bool m_snap_seqs_updated = false;

  bool prune_snap_map(SnapSeqs *snap_seqs);

  void refresh_image();
  void handle_refresh_image(int r);

  void preprocess_event();
  int preprocess_snap_rename(librbd::journal::SnapRenameEvent &event);

  void update_client();
  void handle_update_client(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::EventPreprocessor<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_EVENT_PREPROCESSOR_H
