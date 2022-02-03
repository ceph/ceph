// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <linux/blkzoned.h>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include "crimson/common/layout.h"

#include "crimson/os/seastore/segment_manager.h"

#include "include/uuid.h"

namespace crimson::os::seastore::segment_manager::zns {

  struct zns_sm_metadata_t {
    size_t size = 0;
    size_t segment_size = 0;
    size_t segment_capacity = 0;
    size_t zones_per_segment = 0;
    size_t zone_capacity = 0;
    size_t block_size = 0;
    size_t segments = 0;
    size_t zone_size = 0;
    uint64_t first_segment_offset = 0;
    seastore_meta_t meta;
    
    bool major_dev = false;
    magic_t magic = 0;
    device_type_t dtype = device_type_t::NONE;
    device_id_t device_id = 0;
    secondary_device_set_t secondary_devices;

    DENC(zns_sm_metadata_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.size, p);
      denc(v.segment_size, p);
      denc(v.zone_capacity, p);
      denc(v.zones_per_segment, p);
      denc(v.block_size, p);
      denc(v.segments, p);
      denc(v.zone_size, p);
      denc(v.first_segment_offset, p);
      denc(v.meta, p);
      denc(v.magic, p);
      denc(v.dtype, p);
      denc(v.device_id, p);
      if (v.major_dev) {
	denc(v.secondary_devices, p);
      }
      DENC_FINISH(p);
    }
  };

  using write_ertr = crimson::errorator<crimson::ct_error::input_output_error>;
  using read_ertr = crimson::errorator<crimson::ct_error::input_output_error>;

  class ZNSSegmentManager;

  class ZNSSegment final : public Segment {
  public:
    ZNSSegment(ZNSSegmentManager &man, segment_id_t i) : manager(man), id(i){};

    segment_id_t get_segment_id() const final { return id; }
    seastore_off_t get_write_capacity() const final;
    seastore_off_t get_write_ptr() const final { return write_pointer; }
    close_ertr::future<> close() final;
    write_ertr::future<> write(seastore_off_t offset, ceph::bufferlist bl) final;

    ~ZNSSegment() {}
  private:
    friend class ZNSSegmentManager;
    ZNSSegmentManager &manager;
    const segment_id_t id;
    seastore_off_t write_pointer = 0;
  };

  class ZNSSegmentManager final : public SegmentManager{
  public:
    mount_ret mount() final;
    mkfs_ret mkfs(segment_manager_config_t meta) final;
    open_ertr::future<SegmentRef> open(segment_id_t id) final;
    close_ertr::future<> close() final;

    release_ertr::future<> release(segment_id_t id) final;

    read_ertr::future<> read(
      paddr_t addr, 
      size_t len, 
      ceph::bufferptr &out) final;

    size_t get_size() const final {
      return metadata.size;
    };

    seastore_off_t get_block_size() const final {
      return metadata.block_size;
    };

    seastore_off_t get_segment_size() const final {
      return metadata.segment_size;
    };

    const seastore_meta_t &get_meta() const {
      return metadata.meta;
    };

    device_id_t get_device_id() const final;

    secondary_device_set_t& get_secondary_devices() final;

    device_spec_t get_device_spec() const final;

    magic_t get_magic() const final;

    ZNSSegmentManager(const std::string &path) : device_path(path) {}

    ~ZNSSegmentManager() final = default;

    Segment::write_ertr::future<> segment_write(
    paddr_t addr,
    ceph::bufferlist bl,
    bool ignore_check=false);

  private:
    friend class ZNSSegment;
    std::string device_path;
    zns_sm_metadata_t metadata;
    seastar::file device;
    uint32_t nr_zones;
    struct effort_t {
      uint64_t num = 0;
      uint64_t bytes = 0;

      void increment(uint64_t read_bytes) {
        ++num;
        bytes += read_bytes;
      }
    };

    struct zns_sm_stats {
      effort_t data_read = {};
      effort_t data_write = {};
      effort_t metadata_write = {};
      uint64_t opened_segments = 0;
      uint64_t closed_segments = 0;
      uint64_t closed_segments_unused_bytes = 0;
      uint64_t released_segments = 0;

      void reset() {
	*this = zns_sm_stats{};
      }
    } stats;

    void register_metrics();
    seastar::metrics::metric_group metrics;

    Segment::close_ertr::future<> segment_close(
      segment_id_t id, seastore_off_t write_pointer);

    uint64_t get_offset(paddr_t addr) {
      auto& seg_addr = addr.as_seg_paddr();
      const auto default_sector_size = 512;
      return (metadata.first_segment_offset +
	      (seg_addr.get_segment_id().device_segment_id() * 
	       metadata.zone_size)) * default_sector_size + 
	seg_addr.get_segment_off();
    }
  };

}

WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::segment_manager::zns::zns_sm_metadata_t
)
