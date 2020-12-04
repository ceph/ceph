// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <chrono>
#include <seastar/core/sleep.hh>

#include "include/buffer_raw.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"

/**
 * dummy.h
 *
 * Dummy backend implementations for test purposes.
 */

namespace crimson::os::seastore::onode {

class DummySuper final: public Super {
 public:
  DummySuper(Transaction& t, RootNodeTracker& tracker, laddr_t* p_root_laddr)
      : Super(t, tracker), p_root_laddr{p_root_laddr} {}
  ~DummySuper() override = default;
 protected:
  laddr_t get_root_laddr() const override { return *p_root_laddr; }
  void write_root_laddr(context_t, laddr_t addr) override {
    logger().info("OTree::Dummy: update root {:#x} ...", addr);
    *p_root_laddr = addr;
  }
 private:
  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
  laddr_t* p_root_laddr;
};

class DummyNodeExtent final: public NodeExtent {
 public:
  DummyNodeExtent(ceph::bufferptr &&ptr) : NodeExtent(std::move(ptr)) {
    state = extent_state_t::INITIAL_WRITE_PENDING;
  }
  ~DummyNodeExtent() override = default;
 protected:
  NodeExtentRef mutate(context_t, DeltaRecorderURef&&) override {
    ceph_abort("impossible path"); }
  DeltaRecorder* get_recorder() const override {
    return nullptr; }
  CachedExtentRef duplicate_for_write() override {
    ceph_abort("impossible path"); }
  extent_types_t get_type() const override {
    ceph_abort("impossible path"); }
  ceph::bufferlist get_delta() override {
    ceph_abort("impossible path"); }
  void apply_delta(const ceph::bufferlist&) override {
    ceph_abort("impossible path"); }
};

template <bool SYNC>
class DummyNodeExtentManager final: public NodeExtentManager {
  static constexpr size_t ALIGNMENT = 4096;
 public:
  ~DummyNodeExtentManager() override = default;
 protected:
  bool is_read_isolated() const override { return false; }

  tm_future<NodeExtentRef> read_extent(
      Transaction& t, laddr_t addr, extent_len_t len) override {
    logger().trace("OTree::Dummy: reading {}B at {:#x} ...", len, addr);
    if constexpr (SYNC) {
      return read_extent_sync(t, addr, len);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, addr, len] {
        return read_extent_sync(t, addr, len);
      });
    }
  }

  tm_future<NodeExtentRef> alloc_extent(
      Transaction& t, extent_len_t len) override {
    logger().trace("OTree::Dummy: allocating {}B ...", len);
    if constexpr (SYNC) {
      return alloc_extent_sync(t, len);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, len] {
        return alloc_extent_sync(t, len);
      });
    }
  }

  tm_future<Super::URef> get_super(
      Transaction& t, RootNodeTracker& tracker) override {
    logger().trace("OTree::Dummy: get root ...");
    if constexpr (SYNC) {
      return get_super_sync(t, tracker);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, &tracker] {
        return get_super_sync(t, tracker);
      });
    }
  }

 private:
  tm_future<NodeExtentRef> read_extent_sync(
      Transaction& t, laddr_t addr, extent_len_t len) {
    auto iter = allocate_map.find(addr);
    assert(iter != allocate_map.end());
    auto extent = iter->second;
    logger().trace("OTree::Dummy: read {}B at {:#x}",
                   extent->get_length(), extent->get_laddr());
    assert(extent->get_laddr() == addr);
    assert(extent->get_length() == len);
    return tm_ertr::make_ready_future<NodeExtentRef>(extent);
  }

  tm_future<NodeExtentRef> alloc_extent_sync(
      Transaction& t, extent_len_t len) {
    assert(len % ALIGNMENT == 0);
    auto r = ceph::buffer::create_aligned(len, ALIGNMENT);
    auto addr = reinterpret_cast<laddr_t>(r->get_data());
    auto bp = ceph::bufferptr(std::move(r));
    auto extent = Ref<DummyNodeExtent>(new DummyNodeExtent(std::move(bp)));
    extent->set_laddr(addr);
    assert(allocate_map.find(extent->get_laddr()) == allocate_map.end());
    allocate_map.insert({extent->get_laddr(), extent});
    logger().debug("OTree::Dummy: allocated {}B at {:#x}",
                   extent->get_length(), extent->get_laddr());
    assert(extent->get_length() == len);
    return tm_ertr::make_ready_future<NodeExtentRef>(extent);
  }

  tm_future<Super::URef> get_super_sync(
      Transaction& t, RootNodeTracker& tracker) {
    logger().debug("OTree::Dummy: got root {:#x}", root_laddr);
    return tm_ertr::make_ready_future<Super::URef>(
        Super::URef(new DummySuper(t, tracker, &root_laddr)));
  }

  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }

  std::map<laddr_t, Ref<DummyNodeExtent>> allocate_map;
  laddr_t root_laddr = L_ADDR_NULL;
};

}
