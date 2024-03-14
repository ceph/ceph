// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// // vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_SSD_LOG_ENTRY_H
#define CEPH_LIBRBD_CACHE_PWL_SSD_LOG_ENTRY_H

#include "librbd/cache/pwl/LogEntry.h"

namespace librbd {
namespace cache {
class ImageWritebackInterface;
namespace pwl {
namespace ssd {

class WriteLogEntry : public pwl::WriteLogEntry {
public:
  WriteLogEntry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes)
    : pwl::WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {}
  WriteLogEntry(
      uint64_t image_offset_bytes, uint64_t write_bytes)
    : pwl::WriteLogEntry(image_offset_bytes, write_bytes) {}
  WriteLogEntry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length)
    : pwl::WriteLogEntry(sync_point_entry, image_offset_bytes,
                         write_bytes, data_length) {}
  WriteLogEntry(
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length)
    : pwl::WriteLogEntry(image_offset_bytes, write_bytes, data_length) {}
  ~WriteLogEntry() {}
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  void writeback_bl(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx, ceph::bufferlist &&bl) override;
  void init_cache_bl(bufferlist &src_bl, uint64_t off, uint64_t len) override;
  buffer::list &get_cache_bl() override;
  void copy_cache_bl(bufferlist *out) override;
  void remove_cache_bl() override;
  unsigned int get_aligned_data_size() const override;
  void inc_bl_refs() { bl_refs++; };
  void dec_bl_refs() { bl_refs--; };
  unsigned int reader_count() const override {
    return bl_refs;
  }
};

class WriteSameLogEntry : public WriteLogEntry {
public:
  WriteSameLogEntry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes,
                        write_bytes, data_length) {}
  WriteSameLogEntry(
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length)
    : WriteLogEntry(image_offset_bytes, write_bytes, data_length) {}
  ~WriteSameLogEntry() {}
  WriteSameLogEntry(const WriteSameLogEntry&) = delete;
  WriteSameLogEntry &operator=(const WriteSameLogEntry&) = delete;
  void writeback_bl(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx, ceph::bufferlist &&bl) override;
};

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_SSD_LOG_ENTRY_H
