// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/future.hh>

#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"

#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"
#include "device.h"

namespace crimson::os::seastore {

using std::vector;
struct block_shard_info_t {
  std::size_t size;
  std::size_t segments;
  uint64_t tracker_offset;
  uint64_t first_segment_offset;

  DENC(block_shard_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.segments, p);
    denc(v.tracker_offset, p);
    denc(v.first_segment_offset, p);
    DENC_FINISH(p);
  }
};

struct block_sm_superblock_t {
  unsigned int shard_num = 0;
  size_t segment_size = 0;
  size_t block_size = 0;

  std::vector<block_shard_info_t> shard_infos;

  device_config_t config;

  DENC(block_sm_superblock_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.shard_num, p);
    denc(v.segment_size, p);
    denc(v.block_size, p);
    denc(v.shard_infos, p);
    denc(v.config, p);
    DENC_FINISH(p);
  }

  void validate() const {
    ceph_assert(shard_num == seastar::smp::count);
    ceph_assert(block_size > 0);
    ceph_assert(segment_size > 0 &&
                segment_size % block_size == 0);
    ceph_assert_always(segment_size <= SEGMENT_OFF_MAX);
    for (unsigned int i = 0; i < seastar::smp::count; i ++) {
      ceph_assert(shard_infos[i].size > segment_size &&
                  shard_infos[i].size % block_size == 0);
      ceph_assert_always(shard_infos[i].size <= DEVICE_OFF_MAX);
      ceph_assert(shard_infos[i].segments > 0);
      ceph_assert_always(shard_infos[i].segments <= DEVICE_SEGMENT_ID_MAX);
      ceph_assert(shard_infos[i].tracker_offset > 0 &&
                  shard_infos[i].tracker_offset % block_size == 0);
      ceph_assert(shard_infos[i].first_segment_offset > shard_infos[i].tracker_offset &&
                  shard_infos[i].first_segment_offset % block_size == 0);
    }
    ceph_assert(config.spec.magic != 0);
    ceph_assert(get_default_backend_of_device(config.spec.dtype) ==
		backend_type_t::SEGMENTED);
    ceph_assert(config.spec.id <= DEVICE_ID_MAX_VALID);
    if (!config.major_dev) {
      ceph_assert(config.secondary_devices.size() == 0);
    }
    for (const auto& [k, v] : config.secondary_devices) {
      ceph_assert(k != config.spec.id);
      ceph_assert(k <= DEVICE_ID_MAX_VALID);
      ceph_assert(k == v.id);
      ceph_assert(v.magic != 0);
      ceph_assert(v.dtype > device_type_t::NONE);
      ceph_assert(v.dtype < device_type_t::NUM_TYPES);
    }
  }
};

std::ostream& operator<<(std::ostream&, const block_shard_info_t&);
std::ostream& operator<<(std::ostream&, const block_sm_superblock_t&);

class Segment : public boost::intrusive_ref_counter<
  Segment,
  boost::thread_unsafe_counter>{
public:

  enum class segment_state_t : uint8_t {
    EMPTY = 0,
    OPEN = 1,
    CLOSED = 2
  };

  /**
   * get_segment_id
   */
  virtual segment_id_t get_segment_id() const = 0;

  /**
   * min next write location
   */
  virtual segment_off_t get_write_ptr() const = 0;

  /**
   * max capacity
   */
  virtual segment_off_t get_write_capacity() const = 0;

  /**
   * close
   *
   * Closes segment for writes.  Won't complete until
   * outstanding writes to this segment are complete.
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual close_ertr::future<> close() = 0;


  /**
   * write
   *
   * @param offset offset of write, must be aligned to <> and >= write pointer, advances
   *               write pointer
   * @param bl     buffer to write, will be padded if not aligned
  */
  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >;
  virtual write_ertr::future<> write(
    segment_off_t offset, ceph::bufferlist bl) = 0;

  /**
   * advance_wp
   *
   * advance the segment write pointer,
   * needed when writing at wp is strictly implemented. ex: ZBD backed segments
   * @param offset: advance write pointer till the given offset
   */
  virtual write_ertr::future<> advance_wp(
    segment_off_t offset) = 0;

  virtual ~Segment() {}
};
using SegmentRef = boost::intrusive_ptr<Segment>;

std::ostream& operator<<(std::ostream& out, Segment::segment_state_t);

constexpr size_t PADDR_SIZE = sizeof(paddr_t);
class SegmentManager;

using SegmentManagerRef = std::unique_ptr<SegmentManager>;

class SegmentManager : public Device {
public:
  backend_type_t get_backend_type() const final {
    return backend_type_t::SEGMENTED;
  }

  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual open_ertr::future<SegmentRef> open(segment_id_t id) = 0;

  using release_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual release_ertr::future<> release(segment_id_t id) = 0;

  /* Methods for discovering device geometry, segmentid set, etc */
  virtual segment_off_t get_segment_size() const = 0;
  virtual device_segment_id_t get_num_segments() const {
    ceph_assert(get_available_size() % get_segment_size() == 0);
    return ((device_segment_id_t)(get_available_size() / get_segment_size()));
  }

  virtual ~SegmentManager() {}

  static seastar::future<SegmentManagerRef>
  get_segment_manager(const std::string &device, device_type_t dtype);
};

}

WRITE_CLASS_DENC(
  crimson::os::seastore::block_shard_info_t
)
WRITE_CLASS_DENC(
  crimson::os::seastore::block_sm_superblock_t
)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::block_shard_info_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::block_sm_superblock_t> : fmt::ostream_formatter {};
#endif
