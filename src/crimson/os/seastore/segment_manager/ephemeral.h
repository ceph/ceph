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
class EphemeralSegment final : public Segment {
  friend class EphemeralSegmentManager;
  EphemeralSegmentManager &manager;
  const segment_id_t id;
  segment_off_t write_pointer = 0;
public:
  EphemeralSegment(EphemeralSegmentManager &manager, segment_id_t id);

  segment_id_t get_segment_id() const final { return id; }
  segment_off_t get_write_capacity() const final;
  segment_off_t get_write_ptr() const final { return write_pointer; }
  close_ertr::future<> close() final;
  write_ertr::future<> write(segment_off_t offset, ceph::bufferlist bl) final;

  ~EphemeralSegment() {}
};

class EphemeralSegmentManager final : public SegmentManager {
  friend class EphemeralSegment;

  const ephemeral_config_t config;

  size_t get_offset(paddr_t addr) {
    return (addr.segment * config.segment_size) + addr.offset;
  }

  enum class segment_state_t {
    EMPTY,
    OPEN,
    CLOSED
  };
  std::vector<segment_state_t> segment_state;

  char *buffer = nullptr;

  Segment::close_ertr::future<> segment_close(segment_id_t id);

public:
  EphemeralSegmentManager(ephemeral_config_t config);
  ~EphemeralSegmentManager();

  init_ertr::future<> init() final;

  open_ertr::future<SegmentRef> open(segment_id_t id) final;

  release_ertr::future<> release(segment_id_t id) final;

  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) final;

  size_t get_size() const final {
    return config.size;
  }
  segment_off_t get_block_size() const {
    return config.block_size;
  }
  segment_off_t get_segment_size() const {
    return config.segment_size;
  }

  // public so tests can bypass segment interface when simpler
  Segment::write_ertr::future<> segment_write(
    paddr_t addr,
    ceph::bufferlist bl,
    bool ignore_check=false);
};

}
