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
#include "include/uuid.h"


namespace crimson::os::seastore {

class RandomBlockManager {
public:

  struct mkfs_config_t {
    std::string path;
    paddr_t start;
    paddr_t end;
    size_t block_size = 0;
    size_t total_size = 0;
    device_id_t device_id = 0;
    seastore_meta_t meta;
  };
  using mkfs_ertr = crimson::errorator<
	crimson::ct_error::input_output_error,
	crimson::ct_error::invarg
	>;
  virtual mkfs_ertr::future<> mkfs(mkfs_config_t) = 0;

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
  virtual open_ertr::future<> open(const std::string &path, paddr_t start) = 0;

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
  virtual allocate_ret alloc_extent(Transaction &t, size_t size) = 0;

  using abort_allocation_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg
    >;
  virtual abort_allocation_ertr::future<> abort_allocation(Transaction &t) = 0;

  using complete_allocation_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange
    >;
  virtual write_ertr::future<> complete_allocation(Transaction &t) = 0;

  virtual size_t get_size() const = 0;
  virtual size_t get_block_size() const = 0;
  virtual uint64_t get_free_blocks() const = 0;
  virtual device_id_t get_device_id() const = 0;
  virtual ~RandomBlockManager() {}
};
using RandomBlockManagerRef = std::unique_ptr<RandomBlockManager>;
using blk_no_t = uint64_t;
using rbm_abs_addr = uint64_t;

inline rbm_abs_addr convert_paddr_to_abs_addr(paddr_t& paddr) {
  blk_paddr_t& blk_addr = paddr.as_blk_paddr();
  return blk_addr.get_block_off();
}

inline paddr_t convert_abs_addr_to_paddr(rbm_abs_addr addr, device_id_t d_id) {
  return paddr_t::make_blk_paddr(d_id, addr);
}
}
