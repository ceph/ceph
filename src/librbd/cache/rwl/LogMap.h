// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_LOG_MAP_H
#define CEPH_LIBRBD_CACHE_RWL_LOG_MAP_H

#include "librbd/BlockGuard.h"
#include <list>

namespace librbd {
namespace cache {
namespace rwl {

/**
 * WriteLogMap: maps block extents to GenericWriteLogEntries
 *
 * A WriteLogMapEntry (based on LogMapEntry) refers to a portion of a GenericWriteLogEntry
 */
template <typename T>
class LogMapEntry {
public:
  BlockExtent block_extent;
  std::shared_ptr<T> log_entry;

  LogMapEntry(BlockExtent block_extent,
              std::shared_ptr<T> log_entry = nullptr);
  LogMapEntry(std::shared_ptr<T> log_entry);

  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  LogMapEntry<U> &e);
};

template <typename T>
using LogMapEntries = std::list<LogMapEntry<T>>;

template <typename T>
class LogMap {
public:
  LogMap(CephContext *cct);
  LogMap(const LogMap&) = delete;
  LogMap &operator=(const LogMap&) = delete;

  void add_log_entry(std::shared_ptr<T> log_entry);
  void add_log_entries(std::list<std::shared_ptr<T>> &log_entries);
  void remove_log_entry(std::shared_ptr<T> log_entry);
  void remove_log_entries(std::list<std::shared_ptr<T>> &log_entries);
  std::list<std::shared_ptr<T>> find_log_entries(BlockExtent block_extent);
  LogMapEntries<T> find_map_entries(BlockExtent block_extent);

private:
  void add_log_entry_locked(std::shared_ptr<T> log_entry);
  void remove_log_entry_locked(std::shared_ptr<T> log_entry);
  void add_map_entry_locked(LogMapEntry<T> &map_entry);
  void remove_map_entry_locked(LogMapEntry<T> &map_entry);
  void adjust_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &new_extent);
  void split_map_entry_locked(LogMapEntry<T> &map_entry, BlockExtent &removed_extent);
  std::list<std::shared_ptr<T>> find_log_entries_locked(const BlockExtent &block_extent);
  LogMapEntries<T> find_map_entries_locked(const BlockExtent &block_extent);

  using LogMapEntryT = LogMapEntry<T>;

  class LogMapEntryCompare {
  public:
    bool operator()(const LogMapEntryT &lhs,
                    const LogMapEntryT &rhs) const;
  };

  using BlockExtentToLogMapEntries = std::set<LogMapEntryT,
                                              LogMapEntryCompare>;

  CephContext *m_cct;
  ceph::mutex m_lock;
  BlockExtentToLogMapEntries m_block_to_log_entry_map;
};

} //namespace rwl
} //namespace cache
} //namespace librbd

#endif //CEPH_LIBRBD_CACHE_RWL_LOG_MAP_H
