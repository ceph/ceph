// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReplayStatusFormatter.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::ReplayStatusFormatter: " \
    << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::unique_lock_name;

template <typename I>
ReplayStatusFormatter<I>::ReplayStatusFormatter(Journaler *journaler,
						const std::string &mirror_uuid)
  : m_journaler(journaler),
    m_mirror_uuid(mirror_uuid),
    m_lock(unique_lock_name("ReplayStatusFormatter::m_lock", this)) {
}

template <typename I>
bool ReplayStatusFormatter<I>::get_or_send_update(std::string *description,
						  Context *on_finish) {
  dout(20) << dendl;

  bool in_progress = false;
  {
    Mutex::Locker locker(m_lock);
    if (m_on_finish) {
      in_progress = true;
    } else {
      m_on_finish = on_finish;
    }
  }

  if (in_progress) {
    dout(10) << "previous request is still in progress, ignoring" << dendl;
    on_finish->complete(-EAGAIN);
    return false;
  }

  m_master_position = cls::journal::ObjectPosition();
  m_mirror_position = cls::journal::ObjectPosition();

  cls::journal::Client master_client, mirror_client;
  int r;

  r = m_journaler->get_cached_client(librbd::Journal<>::IMAGE_CLIENT_ID,
                                     &master_client);
  if (r < 0) {
    derr << "error retrieving registered master client: "
	 << cpp_strerror(r) << dendl;
  } else {
    r = m_journaler->get_cached_client(m_mirror_uuid, &mirror_client);
    if (r < 0) {
      derr << "error retrieving registered mirror client: "
	   << cpp_strerror(r) << dendl;
    }
  }

  if (!master_client.commit_position.object_positions.empty()) {
    m_master_position =
      *(master_client.commit_position.object_positions.begin());
  }

  if (!mirror_client.commit_position.object_positions.empty()) {
    m_mirror_position =
      *(mirror_client.commit_position.object_positions.begin());
  }

  if (!calculate_behind_master_or_send_update()) {
    dout(20) << "need to update tag cache" << dendl;
    return false;
  }

  format(description);

  {
    Mutex::Locker locker(m_lock);
    assert(m_on_finish == on_finish);
    m_on_finish = nullptr;
  }

  on_finish->complete(-EEXIST);
  return true;
}

template <typename I>
bool ReplayStatusFormatter<I>::calculate_behind_master_or_send_update() {
  dout(20) << "m_master_position=" << m_master_position
	   << ", m_mirror_position=" << m_mirror_position << dendl;

  m_entries_behind_master = 0;

  if (m_master_position == cls::journal::ObjectPosition() ||
      m_master_position.tag_tid < m_mirror_position.tag_tid) {
    return true;
  }

  cls::journal::ObjectPosition master = m_master_position;
  uint64_t mirror_tag_tid = m_mirror_position.tag_tid;

  while (master.tag_tid != mirror_tag_tid) {
    auto tag_it = m_tag_cache.find(master.tag_tid);
    if (tag_it == m_tag_cache.end()) {
      send_update_tag_cache(master.tag_tid, mirror_tag_tid);
      return false;
    }
    librbd::journal::TagData &tag_data = tag_it->second;
    m_entries_behind_master += master.entry_tid;
    master = cls::journal::ObjectPosition(0, tag_data.predecessor.tag_tid,
					  tag_data.predecessor.entry_tid);
  }
  m_entries_behind_master += master.entry_tid - m_mirror_position.entry_tid;

  dout(20) << "clearing tags not needed any more (below mirror position)"
	   << dendl;

  uint64_t tag_tid = mirror_tag_tid;
  size_t old_size = m_tag_cache.size();
  while (tag_tid != 0) {
    auto tag_it = m_tag_cache.find(tag_tid);
    if (tag_it == m_tag_cache.end()) {
      break;
    }
    librbd::journal::TagData &tag_data = tag_it->second;

    dout(20) << "erasing tag " <<  tag_data << "for tag_tid " << tag_tid
	     << dendl;

    tag_tid = tag_data.predecessor.tag_tid;
    m_tag_cache.erase(tag_it);
  }

  dout(20) << old_size - m_tag_cache.size() << " entries cleared" << dendl;

  return true;
}

template <typename I>
void ReplayStatusFormatter<I>::send_update_tag_cache(uint64_t master_tag_tid,
						     uint64_t mirror_tag_tid) {

  dout(20) << "master_tag_tid=" << master_tag_tid << ", mirror_tag_tid="
	   << mirror_tag_tid << dendl;

  if (master_tag_tid == mirror_tag_tid) {
    Context *on_finish = nullptr;
    {
      Mutex::Locker locker(m_lock);
      std::swap(m_on_finish, on_finish);
    }

    assert(on_finish);
    on_finish->complete(0);
    return;
  }

  FunctionContext *ctx = new FunctionContext(
    [this, master_tag_tid, mirror_tag_tid](int r) {
      handle_update_tag_cache(master_tag_tid, mirror_tag_tid, r);
    });
  m_journaler->get_tag(master_tag_tid, &m_tag, ctx);
}

template <typename I>
void ReplayStatusFormatter<I>::handle_update_tag_cache(uint64_t master_tag_tid,
						       uint64_t mirror_tag_tid,
						       int r) {
  librbd::journal::TagData tag_data;

  if (r < 0) {
    derr << "error retrieving tag " << master_tag_tid << ": " << cpp_strerror(r)
	 << dendl;
  } else {
    dout(20) << "retrieved tag " << master_tag_tid << ": " << m_tag << dendl;

    bufferlist::iterator it = m_tag.data.begin();
    try {
      ::decode(tag_data, it);
    } catch (const buffer::error &err) {
      derr << "error decoding tag " << master_tag_tid << ": " << err.what()
	   << dendl;
    }
  }

  if (tag_data.predecessor.tag_tid == 0) {
    // We failed. Don't consider this fatal, just terminate retrieving.
    dout(20) << "making fake tag" << dendl;
    tag_data.predecessor.tag_tid = mirror_tag_tid;
  }

  dout(20) << "decoded tag " << master_tag_tid << ": " << tag_data << dendl;

  m_tag_cache.insert(std::make_pair(master_tag_tid, tag_data));
  send_update_tag_cache(tag_data.predecessor.tag_tid, mirror_tag_tid);
}

template <typename I>
void ReplayStatusFormatter<I>::format(std::string *description) {

  dout(20) << "m_master_position=" << m_master_position
	   << ", m_mirror_position=" << m_mirror_position
	   << ", m_entries_behind_master=" << m_entries_behind_master << dendl;

  std::stringstream ss;
  ss << "master_position=";
  if (m_master_position == cls::journal::ObjectPosition()) {
    ss << "[]";
  } else {
    ss << m_master_position;
  }
  ss << ", mirror_position=";
  if (m_mirror_position == cls::journal::ObjectPosition()) {
    ss << "[]";
  } else {
    ss << m_mirror_position;
  }
  ss << ", entries_behind_master="
     << (m_entries_behind_master > 0 ? m_entries_behind_master : 0);

  *description = ss.str();
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class
rbd::mirror::image_replayer::ReplayStatusFormatter<librbd::ImageCtx>;
