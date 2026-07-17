// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

namespace crimson::os::seastore::segment_manager::zbd {

  using write_ertr = crimson::errorator<crimson::ct_error::input_output_error>;
  using read_ertr = crimson::errorator<crimson::ct_error::input_output_error>;

  enum class zone_op {
    OPEN,
    FINISH,
    CLOSE,
    RESET,
  };

  class ZBDSegmentManager;

  class ZBDSegment final : public Segment {
  public:
    ZBDSegment(ZBDSegmentManager &man, segment_id_t i) : manager(man), id(i){};

    segment_id_t get_segment_id() const override { return id; }
    segment_off_t get_write_capacity() const override;
    segment_off_t get_write_ptr() const override { return write_pointer; }
    close_ertr::future<> close() override;
    write_ertr::future<> write(segment_off_t offset, ceph::bufferlist bl) override;
    write_ertr::future<> advance_wp(segment_off_t offset) override;

    ~ZBDSegment() {}
  private:
    friend class ZBDSegmentManager;
    ZBDSegmentManager &manager;
    const segment_id_t id;
    segment_off_t write_pointer = 0;
    write_ertr::future<> write_padding_bytes(size_t padding_bytes);
  };

  class ZBDSegmentManager final : public SegmentManager {
  // interfaces used by Device
  public:
    seastar::future<> start(uint32_t shard_nums) override;

    seastar::future<> stop() override;

    Device& get_sharded_device(store_index_t store_index = 0) override;

    mount_ret mount() override;
    mkfs_ret mkfs(device_config_t meta) override;

    ZBDSegmentManager(const std::string &path, store_index_t store_index = 0)
    : device_path(path),
      store_index(store_index) {}

    ~ZBDSegmentManager() override = default;

  //interfaces used by each shard device
  public:
    open_ertr::future<SegmentRef> open(segment_id_t id) override;
    close_ertr::future<> close() override;

    release_ertr::future<> release(segment_id_t id) override;

    read_ertr::future<> read(
      paddr_t addr, 
      size_t len, 
      ceph::bufferptr &out) override;

    read_ertr::future<> readv(
      paddr_t addr,
      std::vector<bufferptr> ptrs) override;

    read_ertr::future<uint32_t> get_shard_nums() override;

    device_type_t get_device_type() const override {
      return device_type_t::ZBD;
    };

    size_t get_available_size() const override {
      return shard_info.size;
    };

    extent_len_t get_block_size() const override {
      return metadata.block_size;
    };

    segment_off_t get_segment_size() const override {
      return metadata.segment_capacity;
    };

    const seastore_meta_t &get_meta() const {
      return metadata.config.meta;
    };

    device_id_t get_device_id() const override;

    secondary_device_set_t& get_secondary_devices() override;

    magic_t get_magic() const override;

    Segment::write_ertr::future<> segment_write(
    paddr_t addr,
    ceph::bufferlist bl,
    bool ignore_check=false);

  private:
    friend class ZBDSegment;
    std::string device_path;
    device_shard_info_t shard_info;
    device_superblock_t metadata;
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

    struct zbd_sm_stats {
      effort_t data_read = {};
      effort_t data_write = {};
      effort_t metadata_write = {};
      uint64_t opened_segments = 0;
      uint64_t closed_segments = 0;
      uint64_t closed_segments_unused_bytes = 0;
      uint64_t released_segments = 0;

      void reset() {
	*this = zbd_sm_stats{};
      }
    } stats;

    void register_metrics(store_index_t store_index);
    seastar::metrics::metric_group metrics;

    Segment::close_ertr::future<> segment_close(
      segment_id_t id, segment_off_t write_pointer);

    uint64_t get_offset(paddr_t addr) {
      auto& seg_addr = addr.as_seg_paddr();
      return (shard_info.first_segment_offset +
	      (seg_addr.get_segment_id().device_segment_id() * 
	       metadata.segment_size)) + seg_addr.get_segment_off();
    }
  private:
    // shard 0 mkfs
    mkfs_ret primary_mkfs(device_config_t meta);
    // all shards mkfs
    mkfs_ret shard_mkfs();

    mount_ret shard_mount();

    uint32_t device_shard_nums = 0;
    store_index_t store_index = 0;
    bool shard_status = true;

    class MultiShardDevices {
    public:
      std::vector<std::unique_ptr<ZBDSegmentManager>> mshard_devices;

    public:
    MultiShardDevices(size_t count,
                      const std::string path)
    : mshard_devices() {
      mshard_devices.reserve(count);
      for (size_t store_index = 0; store_index < count; ++store_index) {
        mshard_devices.emplace_back(std::make_unique<ZBDSegmentManager>(
          path, store_index));
      }
    }
    ~MultiShardDevices() {
     mshard_devices.clear();
    }
  };
  seastar::sharded<MultiShardDevices> shard_devices;
  };

}

