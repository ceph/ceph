// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <chrono>
#include <seastar/core/sleep.hh>

#include "include/buffer_raw.h"
#include "crimson/os/seastore/logging.h"

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
  void write_root_laddr(context_t c, laddr_t addr) override {
    LOG_PREFIX(OTree::Dummy);
    SUBDEBUGT(seastore_onode, "update root {:#x} ...", c.t, addr);
    *p_root_laddr = addr;
  }
 private:
  laddr_t* p_root_laddr;
};

class DummyNodeExtent final: public NodeExtent {
 public:
  DummyNodeExtent(ceph::bufferptr &&ptr) : NodeExtent(std::move(ptr)) {
    state = extent_state_t::INITIAL_WRITE_PENDING;
  }
  DummyNodeExtent(const DummyNodeExtent& other) = delete;
  ~DummyNodeExtent() override = default;

  void retire() {
    assert(state == extent_state_t::INITIAL_WRITE_PENDING);
    state = extent_state_t::INVALID;
    bufferptr empty_bptr;
    get_bptr().swap(empty_bptr);
  }

 protected:
  NodeExtentRef mutate(context_t, DeltaRecorderURef&&) override {
    ceph_abort("impossible path"); }
  DeltaRecorder* get_recorder() const override {
    return nullptr; }
  CachedExtentRef duplicate_for_write(Transaction&) override {
    ceph_abort("impossible path"); }
  extent_types_t get_type() const override {
    return extent_types_t::TEST_BLOCK; }
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
  std::size_t size() const { return allocate_map.size(); }

 protected:
  bool is_read_isolated() const override { return false; }

  read_iertr::future<NodeExtentRef> read_extent(
      Transaction& t, laddr_t addr) override {
    SUBTRACET(seastore_onode, "reading at {:#x} ...", t, addr);
    if constexpr (SYNC) {
      return read_extent_sync(t, addr);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, addr] {
        return read_extent_sync(t, addr);
      });
    }
  }

  alloc_iertr::future<NodeExtentRef> alloc_extent(
      Transaction& t, laddr_t hint, extent_len_t len) override {
    SUBTRACET(seastore_onode, "allocating {}B with hint {:#x} ...", t, len, hint);
    if constexpr (SYNC) {
      return alloc_extent_sync(t, len);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, len] {
        return alloc_extent_sync(t, len);
      });
    }
  }

  retire_iertr::future<> retire_extent(
      Transaction& t, NodeExtentRef extent) override {
    SUBTRACET(seastore_onode,
        "retiring {}B at {:#x} -- {} ...",
        t, extent->get_length(), extent->get_laddr(), *extent);
    if constexpr (SYNC) {
      return retire_extent_sync(t, extent);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, extent] {
        return retire_extent_sync(t, extent);
      });
    }
  }

  getsuper_iertr::future<Super::URef> get_super(
      Transaction& t, RootNodeTracker& tracker) override {
    SUBTRACET(seastore_onode, "get root ...", t);
    if constexpr (SYNC) {
      return get_super_sync(t, tracker);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, &tracker] {
        return get_super_sync(t, tracker);
      });
    }
  }

  std::ostream& print(std::ostream& os) const override {
    return os << "DummyNodeExtentManager(sync=" << SYNC << ")";
  }

 private:
  read_iertr::future<NodeExtentRef> read_extent_sync(
      Transaction& t, laddr_t addr) {
    auto iter = allocate_map.find(addr);
    assert(iter != allocate_map.end());
    auto extent = iter->second;
    SUBTRACET(seastore_onode,
        "read {}B at {:#x} -- {}",
        t, extent->get_length(), extent->get_laddr(), *extent);
    assert(extent->get_laddr() == addr);
    return read_iertr::make_ready_future<NodeExtentRef>(extent);
  }

  alloc_iertr::future<NodeExtentRef> alloc_extent_sync(
      Transaction& t, extent_len_t len) {
    assert(len % ALIGNMENT == 0);
    auto r = ceph::buffer::create_aligned(len, ALIGNMENT);
    auto addr = reinterpret_cast<laddr_t>(r->get_data());
    auto bp = ceph::bufferptr(std::move(r));
    auto extent = Ref<DummyNodeExtent>(new DummyNodeExtent(std::move(bp)));
    extent->set_laddr(addr);
    assert(allocate_map.find(extent->get_laddr()) == allocate_map.end());
    allocate_map.insert({extent->get_laddr(), extent});
    SUBDEBUGT(seastore_onode,
        "allocated {}B at {:#x} -- {}",
        t, extent->get_length(), extent->get_laddr(), *extent);
    assert(extent->get_length() == len);
    return alloc_iertr::make_ready_future<NodeExtentRef>(extent);
  }

  retire_iertr::future<> retire_extent_sync(
      Transaction& t, NodeExtentRef _extent) {
    auto& extent = static_cast<DummyNodeExtent&>(*_extent.get());
    auto addr = extent.get_laddr();
    auto len = extent.get_length();
    extent.retire();
    auto iter = allocate_map.find(addr);
    assert(iter != allocate_map.end());
    allocate_map.erase(iter);
    SUBDEBUGT(seastore_onode, "retired {}B at {:#x}", t, len, addr);
    return retire_iertr::now();
  }

  getsuper_iertr::future<Super::URef> get_super_sync(
      Transaction& t, RootNodeTracker& tracker) {
    SUBTRACET(seastore_onode, "got root {:#x}", t, root_laddr);
    return getsuper_iertr::make_ready_future<Super::URef>(
        Super::URef(new DummySuper(t, tracker, &root_laddr)));
  }

  static LOG_PREFIX(OTree::Dummy);

  std::map<laddr_t, Ref<DummyNodeExtent>> allocate_map;
  laddr_t root_laddr = L_ADDR_NULL;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::onode::DummyNodeExtent> : fmt::ostream_formatter {};
#endif
