// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H
#define CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H

#include "librbd/Utils.h"
#include "librbd/cache/rwl/Types.h"
#include <atomic>
#include <memory>

namespace librbd {
namespace cache {
namespace rwl {

class SyncPointLogEntry;
class GeneralWriteLogEntry;
class WriteLogEntry;

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
  virtual const GenericLogEntry* get_log_entry() = 0;
  virtual const SyncPointLogEntry* get_sync_point_log_entry() {
    return nullptr;
  }
  virtual const GeneralWriteLogEntry* get_gen_write_log_entry() {
    return nullptr;
  }
  virtual const WriteLogEntry* get_write_log_entry() {
    return nullptr;
  }
  virtual std::ostream& format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const GenericLogEntry &entry);
};

class SyncPointLogEntry : public GenericLogEntry {
public:
  /* Writing entries using this sync gen number */
  std::atomic<unsigned int> writes = {0};
  /* Total bytes for all writing entries using this sync gen number */
  std::atomic<uint64_t> bytes = {0};
  /* Writing entries using this sync gen number that have completed */
  std::atomic<unsigned int> writes_completed = {0};
  /* Writing entries using this sync gen number that have completed flushing to the writeback interface */
  std::atomic<unsigned int> writes_flushed = {0};
  /* All writing entries using all prior sync gen numbers have been flushed */
  std::atomic<bool> prior_sync_point_flushed = {true};
  std::shared_ptr<SyncPointLogEntry> next_sync_point_entry = nullptr;
  SyncPointLogEntry(const uint64_t sync_gen_number) {
    ram_entry.sync_gen_number = sync_gen_number;
    ram_entry.sync_point = 1;
  };
  SyncPointLogEntry(const SyncPointLogEntry&) = delete;
  SyncPointLogEntry &operator=(const SyncPointLogEntry&) = delete;
  virtual inline unsigned int write_bytes() {
    return 0;
  }
  const GenericLogEntry* get_log_entry() override {
    return get_sync_point_log_entry();
  }
  const SyncPointLogEntry* get_sync_point_log_entry() override {
    return this;
  }
  std::ostream& format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const SyncPointLogEntry &entry);
};

class GeneralWriteLogEntry : public GenericLogEntry {
public:
  uint32_t referring_map_entries = 0;
  bool flushing = false;
  bool flushed = false; /* or invalidated */
  std::shared_ptr<SyncPointLogEntry> sync_point_entry;
  GeneralWriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                       const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(sync_point_entry) { }
  GeneralWriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(nullptr) { }
  GeneralWriteLogEntry(const GeneralWriteLogEntry&) = delete;
  GeneralWriteLogEntry &operator=(const GeneralWriteLogEntry&) = delete;
  virtual inline unsigned int write_bytes() {
    /* The valid bytes in this ops data buffer. Discard and WS override. */
    return ram_entry.write_bytes;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. Discard and WS override. */
    return write_bytes();
  };
  const BlockExtent block_extent() {
    return ram_entry.block_extent();
  }
  const GenericLogEntry* get_log_entry() override {
    return get_gen_write_log_entry();
  }
  const GeneralWriteLogEntry* get_gen_write_log_entry() override {
    return this;
  }
  uint32_t get_map_ref() {
    return(referring_map_entries);
  }
  void inc_map_ref() { referring_map_entries++; }
  void dec_map_ref() { referring_map_entries--; }
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const GeneralWriteLogEntry &entry);
};

class WriteLogEntry : public GeneralWriteLogEntry {
protected:
  buffer::ptr pmem_bp;
  buffer::list pmem_bl;
  std::atomic<int> bl_refs = {0}; /* The refs held on pmem_bp by pmem_bl */

  void init_pmem_bp();

  /* Write same will override */
  virtual void init_bl(buffer::ptr &bp, buffer::list &bl) {
    bl.append(bp);
  }

  void init_pmem_bl();

public:
  uint8_t *pmem_buffer = nullptr;
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) { }
  WriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) { }
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  const BlockExtent block_extent();
  unsigned int reader_count();
  /* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
  buffer::list &get_pmem_bl(ceph::mutex &entry_bl_lock);
  /* Constructs a new bl containing copies of pmem_bp */
  void copy_pmem_bl(ceph::mutex &entry_bl_lock, bufferlist *out_bl);
  virtual const GenericLogEntry* get_log_entry() override {
    return get_write_log_entry();
  }
  const WriteLogEntry* get_write_log_entry() override {
    return this;
  }
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogEntry &entry);
};

} // namespace rwl 
} // namespace cache 
} // namespace librbd 

#endif // CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H
