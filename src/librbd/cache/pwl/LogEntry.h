// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_LOG_ENTRY_H
#define CEPH_LIBRBD_CACHE_PWL_LOG_ENTRY_H

#include "common/ceph_mutex.h"
#include "librbd/Utils.h"
#include "librbd/cache/pwl/Types.h"
#include <atomic>
#include <memory>

namespace librbd {
namespace cache {
class ImageWritebackInterface;
namespace pwl {

class SyncPointLogEntry;
class GenericWriteLogEntry;
class WriteLogEntry;

typedef std::list<std::shared_ptr<GenericWriteLogEntry>> GenericWriteLogEntries;

class GenericLogEntry {
public:
  WriteLogCacheEntry ram_entry;
  WriteLogCacheEntry *cache_entry = nullptr;
  uint64_t log_entry_index = 0;
  bool completed = false;
  BlockGuardCell* m_cell = nullptr;
  GenericLogEntry(uint64_t image_offset_bytes = 0, uint64_t write_bytes = 0)
    : ram_entry(image_offset_bytes, write_bytes) {
  };
  virtual ~GenericLogEntry() { };
  GenericLogEntry(const GenericLogEntry&) = delete;
  GenericLogEntry &operator=(const GenericLogEntry&) = delete;
  virtual bool can_writeback() const {
    return false;
  }
  virtual bool can_retire() const {
    return false;
  }
  virtual void set_flushed(bool flushed) {
    ceph_assert(false);
  }
  virtual unsigned int write_bytes() const {
    return 0;
  };
  virtual unsigned int bytes_dirty() const {
    return 0;
  };
  virtual std::shared_ptr<SyncPointLogEntry> get_sync_point_entry() {
    return nullptr;
  }
  virtual void writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                         Context *ctx) {
    ceph_assert(false);
  };
  virtual void writeback_bl(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx, ceph::bufferlist &&bl) {
    ceph_assert(false);
  }
  virtual bool is_write_entry() const {
    return false;
  }
  virtual bool is_writesame_entry() const {
    return false;
  }
  virtual bool is_sync_point() const {
    return false;
  }
  virtual unsigned int get_aligned_data_size() const {
    return 0;
  }
  virtual void remove_cache_bl() {}
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
  SyncPointLogEntry(uint64_t sync_gen_number) {
    ram_entry.sync_gen_number = sync_gen_number;
    ram_entry.set_sync_point(true);
  };
  ~SyncPointLogEntry() override {};
  SyncPointLogEntry(const SyncPointLogEntry&) = delete;
  SyncPointLogEntry &operator=(const SyncPointLogEntry&) = delete;
  bool can_retire() const override {
    return this->completed;
  }
  bool is_sync_point() const override {
    return true;
  }
  std::ostream& format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const SyncPointLogEntry &entry);
};

class GenericWriteLogEntry : public GenericLogEntry {
public:
  uint32_t referring_map_entries = 0;
  std::shared_ptr<SyncPointLogEntry> sync_point_entry;
  GenericWriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                       uint64_t image_offset_bytes, uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(sync_point_entry) { }
  GenericWriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(nullptr) { }
  ~GenericWriteLogEntry() override {};
  GenericWriteLogEntry(const GenericWriteLogEntry&) = delete;
  GenericWriteLogEntry &operator=(const GenericWriteLogEntry&) = delete;
  unsigned int write_bytes() const override {
    /* The valid bytes in this ops data buffer. Discard and WS override. */
    return ram_entry.write_bytes;
  };
  unsigned int bytes_dirty() const override {
    /* The bytes in the image this op makes dirty. Discard and WS override. */
    return write_bytes();
  };
  BlockExtent block_extent() {
    return ram_entry.block_extent();
  }
  uint32_t get_map_ref() {
    return(referring_map_entries);
  }
  void inc_map_ref() { referring_map_entries++; }
  void dec_map_ref() { referring_map_entries--; }
  bool can_writeback() const override;
  std::shared_ptr<SyncPointLogEntry> get_sync_point_entry() override {
    return sync_point_entry;
  }
  virtual void copy_cache_bl(bufferlist *out_bl) = 0;
  void set_flushed(bool flushed) override {
    m_flushed = flushed;
  }
  bool get_flushed() const {
    return m_flushed;
  }
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const GenericWriteLogEntry &entry);

private:
  bool m_flushed = false; /* or invalidated */
};

class WriteLogEntry : public GenericWriteLogEntry {
protected:
  bool is_writesame = false;
  buffer::ptr cache_bp;
  buffer::list cache_bl;
  std::atomic<int> bl_refs = {0}; /* The refs held on cache_bp by cache_bl */
  /* Used in WriteLogEntry::get_cache_bl() to synchronize between threads making entries readable */
  mutable ceph::mutex m_entry_bl_lock;

  virtual void init_cache_bp() {}

  virtual void init_bl(buffer::ptr &bp, buffer::list &bl) {}
public:
  uint8_t *cache_buffer = nullptr;
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                uint64_t image_offset_bytes, uint64_t write_bytes)
    : GenericWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes),
      m_entry_bl_lock(ceph::make_mutex(pwl::unique_lock_name(
        "librbd::cache::pwl::WriteLogEntry::m_entry_bl_lock", this)))
  { }
  WriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes)
    : GenericWriteLogEntry(nullptr, image_offset_bytes, write_bytes),
      m_entry_bl_lock(ceph::make_mutex(pwl::unique_lock_name(
        "librbd::cache::pwl::WriteLogEntry::m_entry_bl_lock", this)))
  { }
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                    uint64_t image_offset_bytes, uint64_t write_bytes,
                    uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.set_writesame(true);
    ram_entry.ws_datalen = data_length;
    is_writesame = true;
  };
  WriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes,
                    uint32_t data_length)
    : WriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.set_writesame(true);
    ram_entry.ws_datalen = data_length;
    is_writesame = true;
  };
 ~WriteLogEntry() override {};
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  unsigned int write_bytes() const override {
    // The valid bytes in this ops data buffer.
    if(is_writesame) {
      return ram_entry.ws_datalen;
    }
    return ram_entry.write_bytes;
  };
  unsigned int bytes_dirty() const override {
    // The bytes in the image this op makes dirty.
    return ram_entry.write_bytes;
  };
  void init(bool has_data,
            uint64_t current_sync_gen, uint64_t last_op_sequence_num, bool persist_on_flush);
  virtual void init_cache_buffer(std::vector<WriteBufferAllocation>::iterator allocation) {}
  virtual void init_cache_bl(bufferlist &src_bl, uint64_t off, uint64_t len) {}
  /* Returns a ref to a bl containing bufferptrs to the entry cache buffer */
  virtual buffer::list &get_cache_bl() = 0;

  BlockExtent block_extent();
  virtual unsigned int reader_count() const = 0;
  /* Constructs a new bl containing copies of cache_bp */
  bool can_retire() const override {
    return (this->completed && this->get_flushed() && (0 == reader_count()));
  }
  bool is_write_entry() const override {
    return true;
  }
  bool is_writesame_entry() const override {
    return is_writesame;
  }
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogEntry &entry);
};

class DiscardLogEntry : public GenericWriteLogEntry {
public:
  DiscardLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                  uint64_t image_offset_bytes, uint64_t write_bytes,
                  uint32_t discard_granularity_bytes)
    : GenericWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes),
      m_discard_granularity_bytes(discard_granularity_bytes) {
    ram_entry.set_discard(true);
  };
  DiscardLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes)
    : GenericWriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.set_discard(true);
  };
  DiscardLogEntry(const DiscardLogEntry&) = delete;
  DiscardLogEntry &operator=(const DiscardLogEntry&) = delete;
  unsigned int write_bytes() const override {
    /* The valid bytes in this ops data buffer. */
    return 0;
  };
  unsigned int bytes_dirty() const override {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };
  bool can_retire() const override {
    return this->completed;
  }
  void copy_cache_bl(bufferlist *out_bl) override {
    ceph_assert(false);
  }
  void writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx) override;
  void init(uint64_t current_sync_gen, bool persist_on_flush, uint64_t last_op_sequence_num);
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const DiscardLogEntry &entry);
private:
  uint32_t m_discard_granularity_bytes;
};

} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_LOG_ENTRY_H
