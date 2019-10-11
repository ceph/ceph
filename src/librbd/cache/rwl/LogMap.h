// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_LOG_MAP
#define CEPH_LIBRBD_CACHE_RWL_LOG_MAP

#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include <functional>
#include <list>
#include "common/Finisher.h"
#include "include/ceph_assert.h"

namespace librbd {
namespace cache {
namespace rwl {

using namespace librbd::cache::rwl;

static const bool LOGMAP_VERBOSE_LOGGING = false;

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::LogMap: " << this << " " \
			   <<  __func__ << ": "
/**
 * WriteLogMap: maps block extents to GeneralWriteLogEntries
 */
/* A WriteLogMapEntry (based on LogMapEntry) refers to a portion of a GeneralWriteLogEntry */
template <typename T>
class LogMapEntry {
public:
  BlockExtent block_extent;
  std::shared_ptr<T> log_entry;

  LogMapEntry(BlockExtent block_extent,
	      std::shared_ptr<T> log_entry = nullptr);
  LogMapEntry(std::shared_ptr<T> log_entry);
  friend std::ostream &operator<<(std::ostream &os,
        			  LogMapEntry &e) {
    os << "block_extent=" << e.block_extent << ", "
       << "log_entry=[" << e.log_entry << "]";
    return os;
  };
};

template <typename T>
using LogMapEntries = std::list<LogMapEntry<T>>;

template <typename T, typename T_S>
class LogMap {
public:
  LogMap(CephContext *cct);
  LogMap(const LogMap&) = delete;
  LogMap &operator=(const LogMap&) = delete;

  void add_log_entry(std::shared_ptr<T> log_entry);
  void add_log_entries(T_S &log_entries);
  void remove_log_entry(std::shared_ptr<T> log_entry);
  void remove_log_entries(T_S &log_entries);
  T_S find_log_entries(BlockExtent block_extent);
  LogMapEntries<T> find_map_entries(BlockExtent block_extent);

private:
  void add_log_entry_locked(std::shared_ptr<T> log_entry);
  void remove_log_entry_locked(std::shared_ptr<T> log_entry);
  void add_map_entry_locked(LogMapEntry<T> &map_entry);
  void remove_map_entry_locked(LogMapEntry<T> &map_entry);
  void adjust_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &new_extent);
  void split_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &removed_extent);
  T_S find_log_entries_locked(BlockExtent &block_extent);
  LogMapEntries<T> find_map_entries_locked(BlockExtent &block_extent);

  using LogMapEntryT = LogMapEntry<T>;

  class LogMapEntryCompare {
  public:
    bool operator()(const LogMapEntryT &lhs,
		    const LogMapEntryT &rhs) const;
  };

  using BlockExtentToLogMapEntries = std::set<LogMapEntryT,
					      LogMapEntryCompare>;

  LogMapEntry<T> block_extent_to_map_key(const BlockExtent &block_extent);

  CephContext *m_cct;

  ceph::mutex m_lock;
  BlockExtentToLogMapEntries m_block_to_log_entry_map;
};

template <typename T>
LogMapEntry<T>::LogMapEntry(const BlockExtent block_extent,
			    std::shared_ptr<T> log_entry)
  : block_extent(block_extent) , log_entry(log_entry) {
}

template <typename T>
LogMapEntry<T>::LogMapEntry(std::shared_ptr<T> log_entry)
  : block_extent(log_entry->block_extent()) , log_entry(log_entry) {
}

template <typename T, typename T_S>
LogMap<T, T_S>::LogMap(CephContext *cct)
  : m_cct(cct), m_lock("librbd::cache::rwl::LogMap::m_lock") {
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
template <typename T, typename T_S>
void LogMap<T, T_S>::add_log_entry(std::shared_ptr<T> log_entry) {
  assert(log_entry->is_writer());
  std::lock_guard locker(m_lock);
  add_log_entry_locked(log_entry);
}

template <typename T, typename T_S>
void LogMap<T, T_S>::add_log_entries(T_S &log_entries) {
  std::lock_guard locker(m_lock);
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << dendl;
  }
  for (auto &log_entry : log_entries) {
    add_log_entry_locked(log_entry);
  }
}

/**
 * Remove any map entries that refer to the supplied write log
 * entry.
 */
template <typename T, typename T_S>
void LogMap<T, T_S>::remove_log_entry(std::shared_ptr<T> log_entry) {
  if (!log_entry->is_writer()) { return; }
  std::lock_guard locker(m_lock);
  remove_log_entry_locked(log_entry);
}

template <typename T, typename T_S>
void LogMap<T, T_S>::remove_log_entries(T_S &log_entries) {
  std::lock_guard locker(m_lock);
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << dendl;
  }
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
template <typename T, typename T_S>
T_S LogMap<T, T_S>::find_log_entries(BlockExtent block_extent) {
  std::lock_guard locker(m_lock);
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << dendl;
  }
  return find_log_entries_locked(block_extent);
}

/**
 * Returns the list of all write log map entries that overlap the
 * specified block extent.
 */
template <typename T, typename T_S>
LogMapEntries<T> LogMap<T, T_S>::find_map_entries(BlockExtent block_extent) {
  std::lock_guard locker(m_lock);
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << dendl;
  }
  return find_map_entries_locked(block_extent);
}

template <typename T, typename T_S>
void LogMap<T, T_S>::add_log_entry_locked(std::shared_ptr<T> log_entry) {
  LogMapEntry<T> map_entry(log_entry);
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << "block_extent=" << map_entry.block_extent
		     << dendl;
  }
  assert(m_lock.is_locked_by_me());
  assert(log_entry->is_writer());
  LogMapEntries<T> overlap_entries = find_map_entries_locked(map_entry.block_extent);
  if (overlap_entries.size()) {
    for (auto &entry : overlap_entries) {
      if (LOGMAP_VERBOSE_LOGGING) {
	ldout(m_cct, 20) << entry << dendl;
      }
      if (map_entry.block_extent.block_start <= entry.block_extent.block_start) {
	if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
	  if (LOGMAP_VERBOSE_LOGGING) {
	    ldout(m_cct, 20) << "map entry completely occluded by new log entry" << dendl;
	  }
	  remove_map_entry_locked(entry);
	} else {
	  assert(map_entry.block_extent.block_end < entry.block_extent.block_end);
	  /* The new entry occludes the beginning of the old entry */
	  BlockExtent adjusted_extent(map_entry.block_extent.block_end+1,
				      entry.block_extent.block_end);
	  adjust_map_entry_locked(entry, adjusted_extent);
	}
      } else {
	assert(map_entry.block_extent.block_start > entry.block_extent.block_start);
	if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
	  /* The new entry occludes the end of the old entry */
	  BlockExtent adjusted_extent(entry.block_extent.block_start,
				      map_entry.block_extent.block_start-1);
	  adjust_map_entry_locked(entry, adjusted_extent);
	} else {
	  /* The new entry splits the old entry */
	  split_map_entry_locked(entry, map_entry.block_extent);
	}
      }
    }
  }
  add_map_entry_locked(map_entry);
}

template <typename T, typename T_S>
void LogMap<T, T_S>::remove_log_entry_locked(std::shared_ptr<T> log_entry) {
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << "*log_entry=" << *log_entry << dendl;
  }
  assert(m_lock.is_locked_by_me());

  if (!log_entry->is_writer()) { return; }
  BlockExtent log_entry_extent(log_entry->block_extent());
  LogMapEntries<T> possible_hits = find_map_entries_locked(log_entry_extent);
  for (auto &possible_hit : possible_hits) {
    if (possible_hit.log_entry == log_entry) {
      /* This map entry refers to the specified log entry */
      remove_map_entry_locked(possible_hit);
    }
  }
}

template <typename T, typename T_S>
void LogMap<T, T_S>::add_map_entry_locked(LogMapEntry<T> &map_entry)
{
  assert(map_entry.log_entry);
  m_block_to_log_entry_map.insert(map_entry);
  map_entry.log_entry->inc_map_ref();
}

template <typename T, typename T_S>
void LogMap<T, T_S>::remove_map_entry_locked(LogMapEntry<T> &map_entry)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  LogMapEntry<T> erased = *it;
  m_block_to_log_entry_map.erase(it);
  erased.log_entry->dec_map_ref();
  if (0 == erased.log_entry->get_map_ref()) {
    if (LOGMAP_VERBOSE_LOGGING) {
      ldout(m_cct, 20) << "log entry has zero map entries: " << erased.log_entry << dendl;
    }
  }
}

template <typename T, typename T_S>
void LogMap<T, T_S>::adjust_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &new_extent)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  LogMapEntry<T> adjusted = *it;
  m_block_to_log_entry_map.erase(it);

  m_block_to_log_entry_map.insert(LogMapEntry<T>(new_extent, adjusted.log_entry));
}

template <typename T, typename T_S>
void LogMap<T, T_S>::split_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &removed_extent)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  LogMapEntry<T> split = *it;
  m_block_to_log_entry_map.erase(it);

  BlockExtent left_extent(split.block_extent.block_start,
			  removed_extent.block_start-1);
  m_block_to_log_entry_map.insert(LogMapEntry<T>(left_extent, split.log_entry));

  BlockExtent right_extent(removed_extent.block_end+1,
			   split.block_extent.block_end);
  m_block_to_log_entry_map.insert(LogMapEntry<T>(right_extent, split.log_entry));

  split.log_entry->inc_map_ref();
}

template <typename T, typename T_S>
T_S LogMap<T, T_S>::find_log_entries_locked(BlockExtent &block_extent) {
  T_S overlaps;
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;
  }

  assert(m_lock.is_locked_by_me());
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
template <typename T, typename T_S>
LogMapEntries<T> LogMap<T, T_S>::find_map_entries_locked(BlockExtent &block_extent) {
  LogMapEntries<T> overlaps;

  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;
  }
  assert(m_lock.is_locked_by_me());
  auto p = m_block_to_log_entry_map.equal_range(LogMapEntry<T>(block_extent));
  if (LOGMAP_VERBOSE_LOGGING) {
    ldout(m_cct, 20) << "count=" << std::distance(p.first, p.second) << dendl;
  }
  for ( auto i = p.first; i != p.second; ++i ) {
    LogMapEntry<T> entry = *i;
    overlaps.emplace_back(entry);
    if (LOGMAP_VERBOSE_LOGGING) {
      ldout(m_cct, 20) << entry << dendl;
    }
  }
  return overlaps;
}

/* We map block extents to write log entries, or portions of write log
 * entries. These are both represented by a WriteLogMapEntry. When a
 * GeneralWriteLogEntry is added to this map, a WriteLogMapEntry is created to
 * represent the entire block extent of the GeneralWriteLogEntry, and the
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
template <typename T, typename T_S>
bool LogMap<T, T_S>::LogMapEntryCompare::operator()(const LogMapEntry<T> &lhs,
						    const LogMapEntry<T> &rhs) const {
  if (lhs.block_extent.block_end < rhs.block_extent.block_start) {
    return true;
  }
  return false;
}

template <typename T, typename T_S>
LogMapEntry<T> LogMap<T, T_S>::block_extent_to_map_key(const BlockExtent &block_extent) {
  return LogMapEntry<T>(block_extent);
}

} //namespace rwl
} //namespace cache
} //namespace librbd

#endif //CEPH_LIBRBD_CACHE_RWL_LOG_MAP
