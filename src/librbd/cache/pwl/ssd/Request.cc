// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Request.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ssd::Request: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

template <typename T>
void C_WriteRequest<T>::setup_buffer_resources(
    uint64_t *bytes_cached, uint64_t *bytes_dirtied, uint64_t *bytes_allocated,
    uint64_t *number_lanes, uint64_t *number_log_entries,
    uint64_t *number_unpublished_reserves) {

  *bytes_cached = 0;
  *bytes_allocated = 0;
  *number_log_entries = this->image_extents.size();

  for (auto &extent : this->image_extents) {
    *bytes_cached += extent.second;
    *bytes_allocated += round_up_to(extent.second, MIN_WRITE_ALLOC_SSD_SIZE);
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
  *bytes_dirtied = this->image_extents[0].second;
  *bytes_cached = this->bl.length();
  *bytes_allocated = round_up_to(*bytes_cached, MIN_WRITE_ALLOC_SSD_SIZE);
}

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ssd::C_WriteRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::ssd::C_WriteSameRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::ssd::C_CompAndWriteRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
