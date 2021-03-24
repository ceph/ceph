// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_SSD_REQUEST_H
#define CEPH_LIBRBD_CACHE_SSD_REQUEST_H

#include "librbd/cache/pwl/Request.h"

namespace librbd {
class BlockGuardCell;

namespace cache {
namespace pwl {

template<typename T>
class AbstractWriteLog;

namespace ssd {

template <typename T>
class C_WriteRequest : public pwl::C_WriteRequest<T> {
public:
  C_WriteRequest(
      T &pwl, const utime_t arrived, io::Extents &&image_extents,
      bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
      const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req)
    : pwl::C_WriteRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(cmp_bl),
        std::move(bl), mismatch_offset, fadvise_flags,
        lock, perfcounter, user_req) {}

  C_WriteRequest(
      T &pwl, const utime_t arrived, io::Extents &&image_extents,
      bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req)
    : pwl::C_WriteRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(bl),
        fadvise_flags, lock, perfcounter, user_req) {}
protected:
  void setup_buffer_resources(
      uint64_t *bytes_cached, uint64_t *bytes_dirtied,
      uint64_t *bytes_allocated, uint64_t *number_lanes,
      uint64_t *number_log_entries,
      uint64_t *number_unpublished_reserves) override;
};

template <typename T>
class C_CompAndWriteRequest : public C_WriteRequest<T> {
public:
  C_CompAndWriteRequest(
      T &pwl, const utime_t arrived, io::Extents &&image_extents,
      bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
      const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req)
    : C_WriteRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(cmp_bl),
        std::move(bl), mismatch_offset,fadvise_flags,
        lock, perfcounter, user_req) {}

  const char *get_name() const override {
    return "C_CompAndWriteRequest";
  }
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_CompAndWriteRequest<U> &req);
};

template <typename T>
class C_WriteSameRequest : public pwl::C_WriteSameRequest<T> {
public:
  C_WriteSameRequest(
      T &pwl, const utime_t arrived, io::Extents &&image_extents,
      bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
      PerfCounters *perfcounter, Context *user_req)
    : pwl::C_WriteSameRequest<T>(
        pwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags,
        lock, perfcounter, user_req) {}

  void setup_buffer_resources(
      uint64_t *bytes_cached, uint64_t *bytes_dirtied,
      uint64_t *bytes_allocated, uint64_t *number_lanes,
      uint64_t *number_log_entries,
      uint64_t *number_unpublished_reserves) override;
};

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_SSD_REQUEST_H
