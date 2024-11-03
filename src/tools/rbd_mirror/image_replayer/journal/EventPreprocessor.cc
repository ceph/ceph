// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "EventPreprocessor.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/journal/Types.h"
#include <boost/variant.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror

#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "EventPreprocessor: " << this << " " << __func__ \
                           << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using librbd::util::create_context_callback;

template <typename I>
EventPreprocessor<I>::EventPreprocessor(I &local_image_ctx,
                                        Journaler &remote_journaler,
                                        const std::string &local_mirror_uuid,
                                        MirrorPeerClientMeta *client_meta,
                                        librbd::asio::ContextWQ *work_queue)
  : m_local_image_ctx(local_image_ctx), m_remote_journaler(remote_journaler),
    m_local_mirror_uuid(local_mirror_uuid), m_client_meta(client_meta),
    m_work_queue(work_queue) {
}

template <typename I>
EventPreprocessor<I>::~EventPreprocessor() {
  ceph_assert(!m_in_progress);
}

template <typename I>
bool EventPreprocessor<I>::is_required(const EventEntry &event_entry) {
  SnapSeqs snap_seqs(m_client_meta->snap_seqs);
  return (prune_snap_map(&snap_seqs) ||
          event_entry.get_event_type() ==
            librbd::journal::EVENT_TYPE_SNAP_RENAME);
}

template <typename I>
void EventPreprocessor<I>::preprocess(EventEntry *event_entry,
                                      Context *on_finish) {
  ceph_assert(!m_in_progress);
  m_in_progress = true;
  m_event_entry = event_entry;
  m_on_finish = on_finish;

  refresh_image();
}

template <typename I>
void EventPreprocessor<I>::refresh_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    EventPreprocessor<I>, &EventPreprocessor<I>::handle_refresh_image>(this);
  m_local_image_ctx.state->refresh(ctx);
}

template <typename I>
void EventPreprocessor<I>::handle_refresh_image(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error encountered during image refresh: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  preprocess_event();
}

template <typename I>
void EventPreprocessor<I>::preprocess_event() {
  dout(20) << dendl;

  m_snap_seqs = m_client_meta->snap_seqs;
  m_snap_seqs_updated = prune_snap_map(&m_snap_seqs);

  int r = boost::apply_visitor(PreprocessEventVisitor(this),
                               m_event_entry->event);
  if (r < 0) {
    finish(r);
    return;
  }

  update_client();
}

template <typename I>
int EventPreprocessor<I>::preprocess_snap_rename(
    librbd::journal::SnapRenameEvent &event) {
  dout(20) << "remote_snap_id=" << event.snap_id << ", "
           << "src_snap_name=" << event.src_snap_name << ", "
           << "dest_snap_name=" << event.dst_snap_name << dendl;

  auto snap_seq_it = m_snap_seqs.find(event.snap_id);
  if (snap_seq_it != m_snap_seqs.end()) {
    dout(20) << "remapping remote snap id " << snap_seq_it->first << " "
             << "to local snap id " << snap_seq_it->second << dendl;
    event.snap_id = snap_seq_it->second;
    return 0;
  }

  auto snap_id_it = m_local_image_ctx.snap_ids.find({cls::rbd::UserSnapshotNamespace(),
						     event.src_snap_name});
  if (snap_id_it == m_local_image_ctx.snap_ids.end()) {
    dout(20) << "cannot map remote snapshot '" << event.src_snap_name << "' "
             << "to local snapshot" << dendl;
    event.snap_id = CEPH_NOSNAP;
    return -ENOENT;
  }

  dout(20) << "mapping remote snap id " << event.snap_id << " "
           << "to local snap id " << snap_id_it->second << dendl;
  m_snap_seqs_updated = true;
  m_snap_seqs[event.snap_id] = snap_id_it->second;
  event.snap_id = snap_id_it->second;
  return 0;
}

template <typename I>
void EventPreprocessor<I>::update_client() {
  if (!m_snap_seqs_updated) {
    finish(0);
    return;
  }

  dout(20) << dendl;
  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);
  client_meta.snap_seqs = m_snap_seqs;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    EventPreprocessor<I>, &EventPreprocessor<I>::handle_update_client>(
      this);
  m_remote_journaler.update_client(data_bl, ctx);
}

template <typename I>
void EventPreprocessor<I>::handle_update_client(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to update mirror peer journal client: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_client_meta->snap_seqs = m_snap_seqs;
  finish(0);
}

template <typename I>
bool EventPreprocessor<I>::prune_snap_map(SnapSeqs *snap_seqs) {
  bool pruned = false;

  std::shared_lock image_locker{m_local_image_ctx.image_lock};
  for (auto it = snap_seqs->begin(); it != snap_seqs->end(); ) {
    auto current_it(it++);
    if (m_local_image_ctx.snap_info.count(current_it->second) == 0) {
      snap_seqs->erase(current_it);
      pruned = true;
    }
  }
  return pruned;
}

template <typename I>
void EventPreprocessor<I>::finish(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = m_on_finish;
  m_on_finish = nullptr;
  m_event_entry = nullptr;
  m_in_progress = false;
  m_snap_seqs_updated = false;
  m_work_queue->queue(on_finish, r);
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::EventPreprocessor<librbd::ImageCtx>;
