// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Request.h"
#include "librbd/cache/pwl/AbstractWriteLog.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::Request: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

template <typename T>
void C_WriteRequest<T>::setup_buffer_resources(
    uint64_t *bytes_cached, uint64_t *bytes_dirtied, uint64_t *bytes_allocated,
    uint64_t *number_lanes, uint64_t *number_log_entries,
    uint64_t *number_unpublished_reserves) {

  ceph_assert(!this->m_resources.allocated);

  auto image_extents_size = this->image_extents.size();
  this->m_resources.buffers.reserve(image_extents_size);

  *bytes_cached = 0;
  *bytes_allocated = 0;
  *number_lanes = image_extents_size;
  *number_log_entries = image_extents_size;
  *number_unpublished_reserves = image_extents_size;

  for (auto &extent : this->image_extents) {
    this->m_resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = this->m_resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    buffer.allocated = false;
    *bytes_cached += extent.second;
    if (extent.second > buffer.allocation_size) {
      buffer.allocation_size = extent.second;
    }
    *bytes_allocated += buffer.allocation_size;
  }
  *bytes_dirtied = *bytes_cached;
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_CompAndWriteRequest<T> &req) {
  os << (C_WriteRequest<T>&)req
     << " cmp_bl=" << req.cmp_bl
     << ", read_bl=" << req.read_bl
     << ", compare_succeeded=" << req.compare_succeeded
     << ", mismatch_offset=" << req.mismatch_offset;
  return os;
}

template <typename T>
void C_WriteSameRequest<T>::setup_buffer_resources(
    uint64_t *bytes_cached, uint64_t *bytes_dirtied, uint64_t *bytes_allocated,
    uint64_t *number_lanes, uint64_t *number_log_entries,
    uint64_t *number_unpublished_reserves) {
  ceph_assert(this->image_extents.size() == 1);
  *number_log_entries = 1;
  *bytes_dirtied += this->image_extents[0].second;
  auto pattern_length = this->bl.length();
  this->m_resources.buffers.emplace_back();
  struct WriteBufferAllocation &buffer = this->m_resources.buffers.back();
  buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
  buffer.allocated = false;
  *bytes_cached += pattern_length;
  if (pattern_length > buffer.allocation_size) {
    buffer.allocation_size = pattern_length;
  }
  *bytes_allocated += buffer.allocation_size;
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::rwl::C_WriteRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::rwl::C_WriteSameRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::rwl::C_CompAndWriteRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
