// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "block_driver.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "test/crimson/seastore/test_block.h"

class TMDriver final : public BlockDriver {
public:
  TMDriver(config_t config) : config(config) {}
  ~TMDriver() final {}

  bufferptr get_buffer(size_t size) final {
    return ceph::buffer::create_page_aligned(size);
  }

  seastar::future<> write(
    off_t offset,
    bufferptr ptr) final;

  seastar::future<bufferlist> read(
    off_t offset,
    size_t size) final;

  size_t get_size() const final;

  seastar::future<> mount() final;

  seastar::future<> close() final;

private:
  const config_t config;

  using SegmentManagerRef = crimson::os::seastore::SegmentManagerRef;
  SegmentManagerRef segment_manager;

  using TransactionManager = crimson::os::seastore::TransactionManager;
  using TransactionManagerRef = crimson::os::seastore::TransactionManagerRef;
  TransactionManagerRef tm;

  seastar::future<> mkfs();
  void init();
  void clear();

  using read_extents_iertr = TransactionManager::read_extent_iertr;
  using read_extents_ret = read_extents_iertr::future<
    crimson::os::seastore::lextent_list_t<crimson::os::seastore::TestBlock>
    >;
  read_extents_ret read_extents(
    crimson::os::seastore::Transaction &t,
    crimson::os::seastore::laddr_t offset,
    crimson::os::seastore::extent_len_t length);
};
