// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_REPLAY_STATUS_FORMATTER_H
#define RBD_MIRROR_IMAGE_REPLAYER_REPLAY_STATUS_FORMATTER_H

#include "include/Context.h"
#include "common/ceph_mutex.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/image_replayer/TimeRollingMean.h"

namespace journal { class Journaler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

template <typename ImageCtxT = librbd::ImageCtx>
class ReplayStatusFormatter {
public:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;

  static ReplayStatusFormatter* create(Journaler *journaler,
				       const std::string &mirror_uuid) {
    return new ReplayStatusFormatter(journaler, mirror_uuid);
  }

  static void destroy(ReplayStatusFormatter* formatter) {
    delete formatter;
  }

  ReplayStatusFormatter(Journaler *journaler, const std::string &mirror_uuid);

  void handle_entry_processed(uint32_t bytes);

  bool get_or_send_update(std::string *description, Context *on_finish);

private:
  Journaler *m_journaler;
  std::string m_mirror_uuid;
  ceph::mutex m_lock;
  Context *m_on_finish = nullptr;
  cls::journal::ObjectPosition m_master_position;
  cls::journal::ObjectPosition m_mirror_position;
  int64_t m_entries_behind_master = 0;
  cls::journal::Tag m_tag;
  std::map<uint64_t, librbd::journal::TagData> m_tag_cache;

  TimeRollingMean m_bytes_per_second;
  TimeRollingMean m_entries_per_second;

  bool calculate_behind_master_or_send_update();
  void send_update_tag_cache(uint64_t master_tag_tid, uint64_t mirror_tag_tid);
  void handle_update_tag_cache(uint64_t master_tag_tid, uint64_t mirror_tag_tid,
			       int r);
  void format(std::string *description);
};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::journal::ReplayStatusFormatter<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAY_STATUS_FORMATTER_H
