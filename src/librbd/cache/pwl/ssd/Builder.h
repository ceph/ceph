// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_SSD_BUILDER_H
#define CEPH_LIBRBD_CACHE_PWL_SSD_BUILDER_H

#include <iostream>
#include "LogEntry.h"
#include "ReadRequest.h"
#include "Request.h"
#include "LogOperation.h"

#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/pwl/Builder.h"

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

template <typename T>
class Builder : public pwl::Builder<T> {
public:
  std::shared_ptr<pwl::WriteLogEntry> create_write_log_entry(
      uint64_t image_offset_bytes, uint64_t write_bytes) override {
    return std::make_shared<WriteLogEntry>(image_offset_bytes, write_bytes);
  }
  std::shared_ptr<pwl::WriteLogEntry> create_write_log_entry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes) override {
    return std::make_shared<WriteLogEntry>(
        sync_point_entry, image_offset_bytes, write_bytes);
  }
  std::shared_ptr<pwl::WriteLogEntry> create_writesame_log_entry(
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length) override {
    return std::make_shared<WriteSameLogEntry>(
        image_offset_bytes, write_bytes, data_length);
  }
  std::shared_ptr<pwl::WriteLogEntry> create_writesame_log_entry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length) override {
    return std::make_shared<WriteSameLogEntry>(
        sync_point_entry, image_offset_bytes, write_bytes, data_length);
  }
  pwl::C_WriteRequest<T> *create_write_request(
      T &pwl, utime_t arrived, io::Extents &&image_extents,
      bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req) override {
    return new C_WriteRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(bl),
        fadvise_flags, lock, perfcounter, user_req);
  }
  pwl::C_WriteSameRequest<T> *create_writesame_request(
      T &pwl, utime_t arrived, io::Extents &&image_extents,
      bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req) override {
    return new C_WriteSameRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(bl),
        fadvise_flags, lock, perfcounter, user_req);
  }
  pwl::C_WriteRequest<T> *create_comp_and_write_request(
      T &pwl, utime_t arrived, io::Extents &&image_extents,
      bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
      const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req) override {
    return new C_CompAndWriteRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(cmp_bl),
        std::move(bl), mismatch_offset, fadvise_flags,
        lock, perfcounter, user_req);
  }
  std::shared_ptr<pwl::WriteLogOperation> create_write_log_operation(
      WriteLogOperationSet &set, uint64_t image_offset_bytes,
      uint64_t write_bytes, CephContext *cct,
      std::shared_ptr<pwl::WriteLogEntry> write_log_entry) {
    return std::make_shared<WriteLogOperation>(
        set, image_offset_bytes, write_bytes, cct, write_log_entry);
  }
  std::shared_ptr<pwl::WriteLogOperation> create_write_log_operation(
      WriteLogOperationSet &set, uint64_t image_offset_bytes,
      uint64_t write_bytes, uint32_t data_len, CephContext *cct,
      std::shared_ptr<pwl::WriteLogEntry> writesame_log_entry) {
    return std::make_shared<WriteLogOperation>(
        set, image_offset_bytes, write_bytes, data_len, cct,
        writesame_log_entry);
  }
  std::shared_ptr<pwl::DiscardLogOperation> create_discard_log_operation(
      std::shared_ptr<SyncPoint> sync_point, uint64_t image_offset_bytes,
      uint64_t write_bytes, uint32_t discard_granularity_bytes,
      utime_t dispatch_time, PerfCounters *perfcounter, CephContext *cct) {
    return std::make_shared<DiscardLogOperation>(
        sync_point, image_offset_bytes, write_bytes, discard_granularity_bytes,
        dispatch_time, perfcounter, cct);
  }
  C_ReadRequest *create_read_request(CephContext *cct, utime_t arrived,
      PerfCounters *perfcounter, ceph::bufferlist *bl, Context *on_finish) {
    return new C_ReadRequest(cct, arrived, perfcounter, bl, on_finish);
  }
};


} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_SSD_BUILDER_H
