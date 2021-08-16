// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_RWL_LOG_ENTRY_H
#define CEPH_LIBRBD_CACHE_PWL_RWL_LOG_ENTRY_H

#include "librbd/cache/pwl/LogEntry.h"

namespace librbd {
namespace cache {
class ImageWritebackInterface;
namespace pwl {
namespace rwl {

class WriteLogEntry : public pwl::WriteLogEntry {
public:
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                uint64_t image_offset_bytes, uint64_t write_bytes)
    : pwl::WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {}
  WriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes)
    : pwl::WriteLogEntry(image_offset_bytes, write_bytes) {}
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                uint64_t image_offset_bytes, uint64_t write_bytes,
                uint32_t data_length)
    : pwl::WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes,
                         data_length) {}
  WriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes,
                uint32_t data_length)
    : pwl::WriteLogEntry(image_offset_bytes, write_bytes, data_length) {}
 ~WriteLogEntry() {}
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;

  void writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx) override;
  void init_cache_bp() override;
  void init_bl(buffer::ptr &bp, buffer::list &bl) override;
  void init_cache_buffer(
      std::vector<WriteBufferAllocation>::iterator allocation) override;
  buffer::list &get_cache_bl() override;
  void copy_cache_bl(bufferlist *out_bl) override;
  unsigned int reader_count() const override;
};

class WriteSameLogEntry : public WriteLogEntry {
public:
  WriteSameLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                    uint64_t image_offset_bytes, uint64_t write_bytes,
                    uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes,
                    data_length) {}
  WriteSameLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes,
                   uint32_t data_length)
    : WriteLogEntry(image_offset_bytes, write_bytes, data_length) {}
  ~WriteSameLogEntry() {}
  WriteSameLogEntry(const WriteSameLogEntry&) = delete;
  WriteSameLogEntry &operator=(const WriteSameLogEntry&) = delete;

  void writeback(librbd::cache::ImageWritebackInterface &image_writeback,
                 Context *ctx) override;
};

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_RWL_LOG_ENTRY_H
