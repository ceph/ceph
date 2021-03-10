// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_BUILDER_H
#define CEPH_LIBRBD_CACHE_PWL_BUILDER_H

namespace librbd {
namespace cache {
namespace pwl {

template <typename T>
class Builder {
public:
  virtual ~Builder() {}
  virtual std::shared_ptr<WriteLogEntry> create_write_log_entry(
      uint64_t image_offset_bytes, uint64_t write_bytes) = 0;
  virtual std::shared_ptr<WriteLogEntry> create_write_log_entry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes) = 0;
  virtual std::shared_ptr<WriteLogEntry> create_writesame_log_entry(
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length) = 0;
  virtual std::shared_ptr<WriteLogEntry> create_writesame_log_entry(
      std::shared_ptr<SyncPointLogEntry> sync_point_entry,
      uint64_t image_offset_bytes, uint64_t write_bytes,
      uint32_t data_length) = 0;
  virtual C_WriteRequest<T> *create_write_request(
      T &pwl, utime_t arrived, io::Extents &&image_extents,
      bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req) = 0;
  virtual C_WriteSameRequest<T> *create_writesame_request(
      T &pwl, utime_t arrived, io::Extents &&image_extents,
      bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req) = 0;
  virtual C_WriteRequest<T> *create_comp_and_write_request(
      T &pwl, utime_t arrived, io::Extents &&image_extents,
      bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
      const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req) = 0;
  virtual std::shared_ptr<WriteLogOperation> create_write_log_operation(
      WriteLogOperationSet &set, uint64_t image_offset_bytes,
      uint64_t write_bytes, CephContext *cct,
      std::shared_ptr<WriteLogEntry> write_log_entry) = 0;
  virtual std::shared_ptr<WriteLogOperation> create_write_log_operation(
      WriteLogOperationSet &set, uint64_t image_offset_bytes,
      uint64_t write_bytes, uint32_t data_len, CephContext *cct,
      std::shared_ptr<WriteLogEntry> writesame_log_entry) = 0;
  virtual std::shared_ptr<pwl::DiscardLogOperation> create_discard_log_operation(
      std::shared_ptr<SyncPoint> sync_point, uint64_t image_offset_bytes,
      uint64_t write_bytes, uint32_t discard_granularity_bytes,
      utime_t dispatch_time, PerfCounters *perfcounter, CephContext *cct) = 0;
  virtual C_ReadRequest *create_read_request(CephContext *cct, utime_t arrived,
      PerfCounters *perfcounter, ceph::bufferlist *bl, Context *on_finish) = 0;

};

} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_BUILDER_H
