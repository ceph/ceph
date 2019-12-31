// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H
#define CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H

#include "librbd/cache/rwl/Types.h"

namespace librbd {
namespace cache {
namespace rwl {

class GenericLogEntry {
public:
  WriteLogPmemEntry ram_entry;
  WriteLogPmemEntry *pmem_entry = nullptr;
  uint32_t log_entry_index = 0;
  bool completed = false;
  GenericLogEntry(const uint64_t image_offset_bytes = 0, const uint64_t write_bytes = 0)
    : ram_entry(image_offset_bytes, write_bytes) {
  };
  virtual ~GenericLogEntry() { };
  GenericLogEntry(const GenericLogEntry&) = delete;
  GenericLogEntry &operator=(const GenericLogEntry&) = delete;
  virtual unsigned int write_bytes() = 0;
  bool is_sync_point();
  bool is_discard();
  bool is_writesame();
  bool is_write();
  bool is_writer();
  virtual std::ostream &format(std::ostream &os, const GenericLogEntry &entry) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const GenericLogEntry &entry);
};

} // namespace rwl 
} // namespace cache 
} // namespace librbd 

#endif // CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H
