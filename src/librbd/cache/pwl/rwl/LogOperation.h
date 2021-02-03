// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_RWL_LOG_OPERATION_H
#define CEPH_LIBRBD_CACHE_PWL_RWL_LOG_OPERATION_H

#include "librbd/cache/pwl/LogOperation.h"


namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

class WriteLogOperation : public pwl::WriteLogOperation {
public:
  WriteLogOperation(
      WriteLogOperationSet &set, uint64_t image_offset_bytes,
      uint64_t write_bytes, CephContext *cct,
      std::shared_ptr<pwl::WriteLogEntry> write_log_entry)
    : pwl::WriteLogOperation(set, image_offset_bytes, write_bytes, cct,
                             write_log_entry) {}

  WriteLogOperation(
      WriteLogOperationSet &set, uint64_t image_offset_bytes,
      uint64_t write_bytes, uint32_t data_len, CephContext *cct,
      std::shared_ptr<pwl::WriteLogEntry> writesame_log_entry)
    : pwl::WriteLogOperation(set, image_offset_bytes, write_bytes, cct,
                             writesame_log_entry) {}

  void copy_bl_to_cache_buffer(
          std::vector<WriteBufferAllocation>::iterator allocation) override;
};

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_RWL_LOG_OPERATION_H
