// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H
#define CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H

#include "common/ceph_mutex.h"
#include "librbd/Utils.h"
#include "librbd/cache/rwl/Types.h"
#include <atomic>
#include <memory>

namespace librbd {
namespace cache {
class ImageWritebackInterface;
namespace rwl {

class SyncPointLogEntry;
class GenericWriteLogEntry;
class WriteLogEntry;

typedef std::list<std::shared_ptr<GenericWriteLogEntry>> GenericWriteLogEntries;

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
  virtual bool can_writeback() const {
    return false;
  }
  // TODO: discard need to override this
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
  ~SyncPointLogEntry() override {};
  SyncPointLogEntry(const SyncPointLogEntry&) = delete;
  SyncPointLogEntry &operator=(const SyncPointLogEntry&) = delete;
  bool can_retire() const override {
    return this->completed;
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
                       const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(sync_point_entry) { }
  GenericWriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
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
  virtual void copy_pmem_bl(bufferlist *out_bl) = 0;
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
  buffer::ptr pmem_bp;
  buffer::list pmem_bl;
  std::atomic<int> bl_refs = {0}; /* The refs held on pmem_bp by pmem_bl */
  /* Used in WriteLogEntry::get_pmem_bl() to syncronize between threads making entries readable */
  mutable ceph::mutex m_entry_bl_lock;

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
    : GenericWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes),
      m_entry_bl_lock(ceph::make_mutex(util::unique_lock_name(
        "librbd::cache::rwl::WriteLogEntry::m_entry_bl_lock", this)))
  { }
  WriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericWriteLogEntry(nullptr, image_offset_bytes, write_bytes),
      m_entry_bl_lock(ceph::make_mutex(util::unique_lock_name(
        "librbd::cache::rwl::WriteLogEntry::m_entry_bl_lock", this)))
  { }
  ~WriteLogEntry() override {};
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  void init(bool has_data, std::vector<WriteBufferAllocation>::iterator allocation,
            uint64_t current_sync_gen, uint64_t last_op_sequence_num, bool persist_on_flush);
  BlockExtent block_extent();
  unsigned int reader_count() const;
  /* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
  buffer::list &get_pmem_bl();
  /* Constructs a new bl containing copies of pmem_bp */
  void copy_pmem_bl(bufferlist *out_bl) override;
  void writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx) override;
  bool can_retire() const override {
    return (this->completed && this->get_flushed() && (0 == reader_count()));
  }
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogEntry &entry);
};

class DiscardLogEntry : public GenericWriteLogEntry {
public:
  DiscardLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                  const uint64_t image_offset_bytes, const uint64_t write_bytes,
                  uint32_t discard_granularity_bytes)
    : GenericWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes),
      m_discard_granularity_bytes(discard_granularity_bytes) {
    ram_entry.discard = 1;
  };
  DiscardLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericWriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
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
  void copy_pmem_bl(bufferlist *out_bl) override {
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

class WriteSameLogEntry : public WriteLogEntry {
protected:
  void init_bl(buffer::ptr &bp, buffer::list &bl) override;

public:
  WriteSameLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                    const uint64_t image_offset_bytes, const uint64_t write_bytes,
                    const uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.writesame = 1;
    ram_entry.ws_datalen = data_length;
  };
  WriteSameLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes,
                    const uint32_t data_length)
    : WriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.writesame = 1;
    ram_entry.ws_datalen = data_length;
  };
  WriteSameLogEntry(const WriteSameLogEntry&) = delete;
  WriteSameLogEntry &operator=(const WriteSameLogEntry&) = delete;
  unsigned int write_bytes() const override {
    /* The valid bytes in this ops data buffer. */
    return ram_entry.ws_datalen;
  };
  unsigned int bytes_dirty() const override {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };
  void writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx) override;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteSameLogEntry &entry);
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_RWL_LOG_ENTRY_H
