// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReplayStatusFormatter.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "json_spirit/json_spirit.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "ReplayStatusFormatter: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using librbd::util::unique_lock_name;

namespace {

double round_to_two_places(double value) {
  return abs(round(value * 100) / 100);
}

json_spirit::mObject to_json_object(
    const cls::journal::ObjectPosition& position) {
  json_spirit::mObject object;
  if (position != cls::journal::ObjectPosition{}) {
    object["object_number"] = position.object_number;
    object["tag_tid"] = position.tag_tid;
    object["entry_tid"] = position.entry_tid;
  }
  return object;
}

} // anonymous namespace

template <typename I>
ReplayStatusFormatter<I>::ReplayStatusFormatter(Journaler *journaler,
						const std::string &mirror_uuid)
  : m_journaler(journaler),
    m_mirror_uuid(mirror_uuid),
    m_lock(ceph::make_mutex(unique_lock_name("ReplayStatusFormatter::m_lock", this))) {
}

template <typename I>
void ReplayStatusFormatter<I>::handle_entry_processed(uint32_t bytes) {
  dout(20) << dendl;

  m_bytes_per_second(bytes);
  m_entries_per_second(1);
}

template <typename I>
bool ReplayStatusFormatter<I>::get_or_send_update(std::string *description,
						  Context *on_finish) {
  dout(20) << dendl;

  bool in_progress = false;
  {
    std::lock_guard locker{m_lock};
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
    std::lock_guard locker{m_lock};
    ceph_assert(m_on_finish == on_finish);
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

  while (master.tag_tid > mirror_tag_tid) {
    auto tag_it = m_tag_cache.find(master.tag_tid);
    if (tag_it == m_tag_cache.end()) {
      send_update_tag_cache(master.tag_tid, mirror_tag_tid);
      return false;
    }
    librbd::journal::TagData &tag_data = tag_it->second;
    m_entries_behind_master += master.entry_tid;
    master = {0, tag_data.predecessor.tag_tid, tag_data.predecessor.entry_tid};
  }
  if (master.tag_tid == mirror_tag_tid &&
      master.entry_tid > m_mirror_position.entry_tid) {
    m_entries_behind_master += master.entry_tid - m_mirror_position.entry_tid;
  }

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
  if (master_tag_tid <= mirror_tag_tid ||
      m_tag_cache.find(master_tag_tid) != m_tag_cache.end()) {
    Context *on_finish = nullptr;
    {
      std::lock_guard locker{m_lock};
      std::swap(m_on_finish, on_finish);
    }

    ceph_assert(on_finish);
    on_finish->complete(0);
    return;
  }

  dout(20) << "master_tag_tid=" << master_tag_tid << ", mirror_tag_tid="
	   << mirror_tag_tid << dendl;

  auto ctx = new LambdaContext(
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

    auto it = m_tag.data.cbegin();
    try {
      decode(tag_data, it);
    } catch (const buffer::error &err) {
      derr << "error decoding tag " << master_tag_tid << ": " << err.what()
	   << dendl;
    }
  }

  if (tag_data.predecessor.mirror_uuid !=
        librbd::Journal<>::LOCAL_MIRROR_UUID &&
      tag_data.predecessor.mirror_uuid !=
        librbd::Journal<>::ORPHAN_MIRROR_UUID) {
    dout(20) << "hit remote image non-primary epoch" << dendl;
    tag_data.predecessor = {};
  }

  dout(20) << "decoded tag " << master_tag_tid << ": " << tag_data << dendl;

  m_tag_cache[master_tag_tid] = tag_data;
  send_update_tag_cache(tag_data.predecessor.tag_tid, mirror_tag_tid);
}

template <typename I>
void ReplayStatusFormatter<I>::format(std::string *description) {
  dout(20) << "m_master_position=" << m_master_position
	   << ", m_mirror_position=" << m_mirror_position
	   << ", m_entries_behind_master=" << m_entries_behind_master << dendl;

  json_spirit::mObject root_obj;
  root_obj["primary_position"] = to_json_object(m_master_position);
  root_obj["non_primary_position"] = to_json_object(m_mirror_position);
  root_obj["entries_behind_primary"] = (
    m_entries_behind_master > 0 ? m_entries_behind_master : 0);

  m_bytes_per_second(0);
  root_obj["bytes_per_second"] = round_to_two_places(
    m_bytes_per_second.get_average());

  m_entries_per_second(0);
  auto entries_per_second = m_entries_per_second.get_average();
  root_obj["entries_per_second"] = round_to_two_places(entries_per_second);

  if (m_entries_behind_master > 0 && entries_per_second > 0) {
    std::uint64_t seconds_until_synced = round_to_two_places(
      m_entries_behind_master / entries_per_second);
    if (seconds_until_synced >= std::numeric_limits<uint64_t>::max()) {
      seconds_until_synced = std::numeric_limits<uint64_t>::max();
    }

    root_obj["seconds_until_synced"] = seconds_until_synced;
  }

  *description = json_spirit::write(
    root_obj, json_spirit::remove_trailing_zeros);
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::ReplayStatusFormatter<librbd::ImageCtx>;
