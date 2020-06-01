// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LogMap.h"
#include "include/ceph_assert.h"
#include "librbd/Utils.h"

namespace librbd {
namespace cache {
namespace rwl {

#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::LogMap: " << this << " " \
                           <<  __func__ << ": "
template <typename T>
std::ostream &operator<<(std::ostream &os,
                         LogMapEntry<T> &e) {
  os << "block_extent=" << e.block_extent << ", "
     << "log_entry=[" << e.log_entry << "]";
  return os;
}

template <typename T>
LogMapEntry<T>::LogMapEntry(const BlockExtent block_extent,
                            std::shared_ptr<T> log_entry)
  : block_extent(block_extent) , log_entry(log_entry) {
}

template <typename T>
LogMapEntry<T>::LogMapEntry(std::shared_ptr<T> log_entry)
  : block_extent(log_entry->block_extent()) , log_entry(log_entry) {
}

template <typename T>
LogMap<T>::LogMap(CephContext *cct)
  : m_cct(cct),
    m_lock(ceph::make_mutex(util::unique_lock_name(
           "librbd::cache::rwl::LogMap::m_lock", this))) {
}

/**
 * Add a write log entry to the map. Subsequent queries for blocks
 * within this log entry's extent will find this log entry. Portions
 * of prior write log entries overlapping with this log entry will
 * be replaced in the map by this log entry.
 *
 * The map_entries field of the log entry object will be updated to
 * contain this map entry.
 *
 * The map_entries fields of all log entries overlapping with this
 * entry will be updated to remove the regions that overlap with
 * this.
 */
template <typename T>
void LogMap<T>::add_log_entry(std::shared_ptr<T> log_entry) {
  std::lock_guard locker(m_lock);
  add_log_entry_locked(log_entry);
}

template <typename T>
void LogMap<T>::add_log_entries(std::list<std::shared_ptr<T>> &log_entries) {
  std::lock_guard locker(m_lock);
  ldout(m_cct, 20) << dendl;
  for (auto &log_entry : log_entries) {
    add_log_entry_locked(log_entry);
  }
}

/**
 * Remove any map entries that refer to the supplied write log
 * entry.
 */
template <typename T>
void LogMap<T>::remove_log_entry(std::shared_ptr<T> log_entry) {
  std::lock_guard locker(m_lock);
  remove_log_entry_locked(log_entry);
}

template <typename T>
void LogMap<T>::remove_log_entries(std::list<std::shared_ptr<T>> &log_entries) {
  std::lock_guard locker(m_lock);
  ldout(m_cct, 20) << dendl;
  for (auto &log_entry : log_entries) {
    remove_log_entry_locked(log_entry);
  }
}

/**
 * Returns the list of all write log entries that overlap the specified block
 * extent. This doesn't tell you which portions of these entries overlap the
 * extent, or each other. For that, use find_map_entries(). A log entry may
 * appear in the list more than once, if multiple map entries refer to it
 * (e.g. the middle of that write log entry has been overwritten).
 */
template <typename T>
std::list<std::shared_ptr<T>> LogMap<T>::find_log_entries(BlockExtent block_extent) {
  std::lock_guard locker(m_lock);
  ldout(m_cct, 20) << dendl;
  return find_log_entries_locked(block_extent);
}

/**
 * Returns the list of all write log map entries that overlap the
 * specified block extent.
 */
template <typename T>
LogMapEntries<T> LogMap<T>::find_map_entries(BlockExtent block_extent) {
  std::lock_guard locker(m_lock);
  ldout(m_cct, 20) << dendl;
  return find_map_entries_locked(block_extent);
}

template <typename T>
void LogMap<T>::add_log_entry_locked(std::shared_ptr<T> log_entry) {
  LogMapEntry<T> map_entry(log_entry);
  ldout(m_cct, 20) << "block_extent=" << map_entry.block_extent
                   << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  LogMapEntries<T> overlap_entries = find_map_entries_locked(map_entry.block_extent);
  for (auto &entry : overlap_entries) {
    ldout(m_cct, 20) << entry << dendl;
    if (map_entry.block_extent.block_start <= entry.block_extent.block_start) {
      if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
        ldout(m_cct, 20) << "map entry completely occluded by new log entry" << dendl;
        remove_map_entry_locked(entry);
      } else {
        ceph_assert(map_entry.block_extent.block_end < entry.block_extent.block_end);
        /* The new entry occludes the beginning of the old entry */
        BlockExtent adjusted_extent(map_entry.block_extent.block_end,
                                    entry.block_extent.block_end);
        adjust_map_entry_locked(entry, adjusted_extent);
      }
    } else {
      if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
        /* The new entry occludes the end of the old entry */
        BlockExtent adjusted_extent(entry.block_extent.block_start,
                                    map_entry.block_extent.block_start);
        adjust_map_entry_locked(entry, adjusted_extent);
      } else {
        /* The new entry splits the old entry */
        split_map_entry_locked(entry, map_entry.block_extent);
      }
    }
  }
  add_map_entry_locked(map_entry);
}

template <typename T>
void LogMap<T>::remove_log_entry_locked(std::shared_ptr<T> log_entry) {
  ldout(m_cct, 20) << "*log_entry=" << *log_entry << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  LogMapEntries<T> possible_hits = find_map_entries_locked(log_entry->block_extent());
  for (auto &possible_hit : possible_hits) {
    if (possible_hit.log_entry == log_entry) {
      /* This map entry refers to the specified log entry */
      remove_map_entry_locked(possible_hit);
    }
  }
}

template <typename T>
void LogMap<T>::add_map_entry_locked(LogMapEntry<T> &map_entry) {
  ceph_assert(map_entry.log_entry);
  m_block_to_log_entry_map.insert(map_entry);
  map_entry.log_entry->inc_map_ref();
}

template <typename T>
void LogMap<T>::remove_map_entry_locked(LogMapEntry<T> &map_entry) {
  auto it = m_block_to_log_entry_map.find(map_entry);
  ceph_assert(it != m_block_to_log_entry_map.end());

  LogMapEntry<T> erased = *it;
  m_block_to_log_entry_map.erase(it);
  erased.log_entry->dec_map_ref();
  if (0 == erased.log_entry->get_map_ref()) {
    ldout(m_cct, 20) << "log entry has zero map entries: " << erased.log_entry << dendl;
  }
}

template <typename T>
void LogMap<T>::adjust_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &new_extent) {
  auto it = m_block_to_log_entry_map.find(map_entry);
  ceph_assert(it != m_block_to_log_entry_map.end());

  LogMapEntry<T> adjusted = *it;
  m_block_to_log_entry_map.erase(it);

  m_block_to_log_entry_map.insert(LogMapEntry<T>(new_extent, adjusted.log_entry));
}

template <typename T>
void LogMap<T>::split_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &removed_extent) {
  auto it = m_block_to_log_entry_map.find(map_entry);
  ceph_assert(it != m_block_to_log_entry_map.end());

  LogMapEntry<T> split = *it;
  m_block_to_log_entry_map.erase(it);

  BlockExtent left_extent(split.block_extent.block_start,
                          removed_extent.block_start);
  m_block_to_log_entry_map.insert(LogMapEntry<T>(left_extent, split.log_entry));

  BlockExtent right_extent(removed_extent.block_end,
                           split.block_extent.block_end);
  m_block_to_log_entry_map.insert(LogMapEntry<T>(right_extent, split.log_entry));

  split.log_entry->inc_map_ref();
}

template <typename T>
std::list<std::shared_ptr<T>> LogMap<T>::find_log_entries_locked(const BlockExtent &block_extent) {
  std::list<std::shared_ptr<T>> overlaps;
  ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  LogMapEntries<T> map_entries = find_map_entries_locked(block_extent);
  for (auto &map_entry : map_entries) {
    overlaps.emplace_back(map_entry.log_entry);
  }
  return overlaps;
}

/**
 * TODO: Generalize this to do some arbitrary thing to each map
 * extent, instead of returning a list.
 */
template <typename T>
LogMapEntries<T> LogMap<T>::find_map_entries_locked(const BlockExtent &block_extent) {
  LogMapEntries<T> overlaps;

  ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  auto p = m_block_to_log_entry_map.equal_range(LogMapEntry<T>(block_extent));
  ldout(m_cct, 20) << "count=" << std::distance(p.first, p.second) << dendl;
  for ( auto i = p.first; i != p.second; ++i ) {
    LogMapEntry<T> entry = *i;
    overlaps.emplace_back(entry);
    ldout(m_cct, 20) << entry << dendl;
  }
  return overlaps;
}

/* We map block extents to write log entries, or portions of write log
 * entries. These are both represented by a WriteLogMapEntry. When a
 * GenericWriteLogEntry is added to this map, a WriteLogMapEntry is created to
 * represent the entire block extent of the GenericWriteLogEntry, and the
 * WriteLogMapEntry is added to the set.
 *
 * The set must not contain overlapping WriteLogMapEntrys. WriteLogMapEntrys
 * in the set that overlap with one being added are adjusted (shrunk, split,
 * or removed) before the new entry is added.
 *
 * This comparison works despite the ambiguity because we ensure the set
 * contains no overlapping entries. This comparison works to find entries
 * that overlap with a given block extent because equal_range() returns the
 * first entry in which the extent doesn't end before the given extent
 * starts, and the last entry for which the extent starts before the given
 * extent ends (the first entry that the key is less than, and the last entry
 * that is less than the key).
 */
template <typename T>
bool LogMap<T>::LogMapEntryCompare::operator()(const LogMapEntry<T> &lhs,
                                               const LogMapEntry<T> &rhs) const {
  if (lhs.block_extent.block_end <= rhs.block_extent.block_start) {
    return true;
  }
  return false;
}

} //namespace rwl
} //namespace cache
} //namespace librbd
