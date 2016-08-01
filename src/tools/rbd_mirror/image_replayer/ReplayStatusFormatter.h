// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_REPLAY_STATUS_FORMATTER_H
#define RBD_MIRROR_IMAGE_REPLAYER_REPLAY_STATUS_FORMATTER_H

#include "include/Context.h"
#include "common/Mutex.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"

namespace journal { class Journaler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class ReplayStatusFormatter {
public:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;

  static ReplayStatusFormatter* create(Journaler *journaler,
				       const std::string &mirror_uuid) {
    return new ReplayStatusFormatter(journaler, mirror_uuid);
  }

  ReplayStatusFormatter(Journaler *journaler, const std::string &mirror_uuid);

  bool get_or_send_update(std::string *description, Context *on_finish);

private:
  Journaler *m_journaler;
  std::string m_mirror_uuid;
  Mutex m_lock;
  Context *m_on_finish = nullptr;
  cls::journal::ObjectPosition m_master_position;
  cls::journal::ObjectPosition m_mirror_position;
  int m_entries_behind_master = 0;
  cls::journal::Tag m_tag;
  std::map<uint64_t, librbd::journal::TagData> m_tag_cache;

  bool calculate_behind_master_or_send_update();
  void send_update_tag_cache(uint64_t master_tag_tid, uint64_t mirror_tag_tid);
  void handle_update_tag_cache(uint64_t master_tag_tid, uint64_t mirror_tag_tid,
			       int r);
  void format(std::string *description);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAY_STATUS_FORMATTER_H
