// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/transaction.h"

#include "crimson/common/layout.h"
#include "include/buffer.h"
#include "crimson/os/seastore/device.h"

namespace crimson::os::seastore {

struct alloc_paddr_result {
  paddr_t start;
  extent_len_t len;
};

struct rbm_shard_info_t {
  std::size_t size = 0;
  uint64_t start_offset = 0;

  DENC(rbm_shard_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.start_offset, p);
    DENC_FINISH(p);
  }
};

struct rbm_metadata_header_t {
  size_t size = 0;
  size_t block_size = 0;
  uint64_t feature = 0;
  uint64_t journal_size = 0;
  checksum_t crc = 0;
  device_config_t config;
  unsigned int shard_num = 0;
  std::vector<rbm_shard_info_t> shard_infos;

  DENC(rbm_metadata_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.block_size, p);
    denc(v.feature, p);

    denc(v.journal_size, p);
    denc(v.crc, p);
    denc(v.config, p);
    denc(v.shard_num, p);
    denc(v.shard_infos, p);
    DENC_FINISH(p);
  }

  void validate() const {
    ceph_assert(shard_num == seastar::smp::count);
    ceph_assert(block_size > 0);
    for (unsigned int i = 0; i < seastar::smp::count; i ++) {
      ceph_assert(shard_infos[i].size > block_size &&
                  shard_infos[i].size % block_size == 0);
      ceph_assert_always(shard_infos[i].size <= DEVICE_OFF_MAX);
      ceph_assert(journal_size > 0 &&
                  journal_size % block_size == 0);
      ceph_assert(shard_infos[i].start_offset < size &&
		  shard_infos[i].start_offset % block_size == 0);
    }
    ceph_assert(config.spec.magic != 0);
    ceph_assert(get_default_backend_of_device(config.spec.dtype) ==
		backend_type_t::RANDOM_BLOCK);
    ceph_assert(config.spec.id <= DEVICE_ID_MAX_VALID);
  }
};

enum class rbm_extent_state_t {
  FREE,		// not allocated
  RESERVED,	// extent is reserved by alloc_new_extent, but is not persistent
  ALLOCATED,	// extent is persistent
};

class Device;
using rbm_abs_addr = uint64_t;
constexpr rbm_abs_addr RBM_START_ADDRESS = 0;
class RandomBlockManager {
public:

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_ertr::future<> read(paddr_t addr, bufferptr &buffer) = 0;

  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::ebadf,
    crimson::ct_error::enospc,
    crimson::ct_error::erange
    >;
  virtual write_ertr::future<> write(paddr_t addr, bufferptr &buf) = 0;

  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual open_ertr::future<> open() = 0;

  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg>;
  virtual close_ertr::future<> close() = 0;

  using allocate_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enospc
    >;
  using allocate_ret = allocate_ertr::future<paddr_t>;
  // allocator, return start addr of allocated blocks
  virtual paddr_t alloc_extent(size_t size) = 0;

  using allocate_ret_bare = std::list<alloc_paddr_result>;
  using allo_extents_ret = allocate_ertr::future<allocate_ret_bare>;
  virtual allocate_ret_bare alloc_extents(size_t size) = 0;

  virtual void mark_space_used(paddr_t paddr, size_t len) = 0;
  virtual void mark_space_free(paddr_t paddr, size_t len) = 0;

  virtual void complete_allocation(paddr_t addr, size_t size) = 0;

  virtual size_t get_size() const = 0;
  virtual extent_len_t get_block_size() const = 0;
  virtual uint64_t get_free_blocks() const = 0;
  virtual device_id_t get_device_id() const = 0;
  virtual const seastore_meta_t &get_meta() const = 0;
  virtual Device* get_device() = 0;
  virtual paddr_t get_start() = 0;
  virtual rbm_extent_state_t get_extent_state(paddr_t addr, size_t size) = 0;
  virtual size_t get_journal_size() const = 0;
  virtual ~RandomBlockManager() {}
#ifdef UNIT_TESTS_BUILT
  virtual void prefill_fragmented_device() = 0;
#endif
};
using RandomBlockManagerRef = std::unique_ptr<RandomBlockManager>;

inline rbm_abs_addr convert_paddr_to_abs_addr(const paddr_t& paddr) {
  const blk_paddr_t& blk_addr = paddr.as_blk_paddr();
  return blk_addr.get_device_off();
}

inline paddr_t convert_abs_addr_to_paddr(rbm_abs_addr addr, device_id_t d_id) {
  return paddr_t::make_blk_paddr(d_id, addr);
}

namespace random_block_device {
  class RBMDevice;
}

seastar::future<std::unique_ptr<random_block_device::RBMDevice>> 
  get_rb_device(const std::string &device);

std::ostream &operator<<(std::ostream &out, const rbm_metadata_header_t &header);
std::ostream &operator<<(std::ostream &out, const rbm_shard_info_t &shard);
}

WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::rbm_shard_info_t
)
WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::rbm_metadata_header_t
)

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::rbm_metadata_header_t> : fmt::ostream_formatter {};
template<> struct fmt::formatter<crimson::os::seastore::rbm_shard_info_t> : fmt::ostream_formatter {};
#endif
