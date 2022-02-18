// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "crimson/os/seastore/segment_manager.h"

#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace crimson::os::seastore::segment_manager {

class EphemeralSegmentManager;
using EphemeralSegmentManagerRef = std::unique_ptr<EphemeralSegmentManager>;

struct ephemeral_config_t {
  size_t size = 0;
  size_t block_size = 0;
  size_t segment_size = 0;
  magic_t magic = 0;
  device_type_t dtype = device_type_t::NONE;
  device_id_t id = 0;
};

constexpr ephemeral_config_t DEFAULT_TEST_EPHEMERAL = {
  1 << 30,
  4 << 10,
  8 << 20,
  0xabcd,
  device_type_t::SEGMENTED,
  0
};

std::ostream &operator<<(std::ostream &, const ephemeral_config_t &);
EphemeralSegmentManagerRef create_test_ephemeral();

class EphemeralSegment final : public Segment {
  friend class EphemeralSegmentManager;
  EphemeralSegmentManager &manager;
  const segment_id_t id;
  seastore_off_t write_pointer = 0;
public:
  EphemeralSegment(EphemeralSegmentManager &manager, segment_id_t id);

  segment_id_t get_segment_id() const final { return id; }
  seastore_off_t get_write_capacity() const final;
  seastore_off_t get_write_ptr() const final { return write_pointer; }
  close_ertr::future<> close() final;
  write_ertr::future<> write(seastore_off_t offset, ceph::bufferlist bl) final;

  ~EphemeralSegment() {}
};

class EphemeralSegmentManager final : public SegmentManager {
  friend class EphemeralSegment;
  using segment_state_t = Segment::segment_state_t;

  const ephemeral_config_t config;
  std::optional<seastore_meta_t> meta;

  size_t get_offset(paddr_t addr) {
    auto& seg_addr = addr.as_seg_paddr();
    return (seg_addr.get_segment_id().device_segment_id() * config.segment_size) +
	     seg_addr.get_segment_off();
  }

  std::vector<segment_state_t> segment_state;

  char *buffer = nullptr;

  Segment::close_ertr::future<> segment_close(segment_id_t id);

  secondary_device_set_t sec_device_set;

public:
  EphemeralSegmentManager(
    ephemeral_config_t config)
    : config(config) {}
  ~EphemeralSegmentManager();

  close_ertr::future<> close() final {
    return close_ertr::now();
  }

  device_id_t get_device_id() const {
    return config.id;
  }

  using init_ertr = crimson::errorator<
    crimson::ct_error::enospc,
    crimson::ct_error::invarg,
    crimson::ct_error::erange>;
  init_ertr::future<> init();

  mount_ret mount() {
    return mount_ertr::now();
  }

  mkfs_ret mkfs(segment_manager_config_t) {
    return mkfs_ertr::now();
  }

  open_ertr::future<SegmentRef> open(segment_id_t id) final;

  release_ertr::future<> release(segment_id_t id) final;

  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) final;

  size_t get_size() const final {
    return config.size;
  }
  seastore_off_t get_block_size() const final {
    return config.block_size;
  }
  seastore_off_t get_segment_size() const final {
    return config.segment_size;
  }

  const seastore_meta_t &get_meta() const final {
    assert(meta);
    return *meta;
  }

  secondary_device_set_t& get_secondary_devices() final {
    return sec_device_set;
  }

  device_spec_t get_device_spec() const final {
    return {config.magic, config.dtype, config.id};
  }

  magic_t get_magic() const final {
    return config.magic;
  }

  void remount();

  // public so tests can bypass segment interface when simpler
  Segment::write_ertr::future<> segment_write(
    paddr_t addr,
    ceph::bufferlist bl,
    bool ignore_check=false);
};

}
