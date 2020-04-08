// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/buffer_raw.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"

namespace crimson::os::seastore::onode {

class DummySuper final: public Super {
 public:
  DummySuper(Transaction& t, RootNodeTracker& tracker, laddr_t* p_root_laddr)
      : Super(t, tracker), p_root_laddr{p_root_laddr} {}
  ~DummySuper() override = default;
 protected:
  laddr_t get_root_laddr() const override { return *p_root_laddr; }
  void write_root_laddr(context_t, laddr_t addr) override { *p_root_laddr = addr; }
  laddr_t* p_root_laddr;
};

class DummyNodeExtent final: public NodeExtent {
 public:
  DummyNodeExtent(ceph::bufferptr &&ptr) : NodeExtent(std::move(ptr)) {
    state = extent_state_t::INITIAL_WRITE_PENDING;
  }
  ~DummyNodeExtent() override = default;
 protected:
  Ref mutate(context_t) override {
    assert(false && "impossible path"); }
  CachedExtentRef duplicate_for_write() override {
    assert(false && "impossible path"); }
  extent_types_t get_type() const override {
    assert(false && "impossible path"); }
  ceph::bufferlist get_delta() override {
    assert(false && "impossible path"); }
  void apply_delta(const ceph::bufferlist&) override {
    assert(false && "impossible path"); }
};

class DummyNodeExtentManager final: public NodeExtentManager {
  static constexpr size_t ALIGNMENT = 4096;
 public:
  ~DummyNodeExtentManager() override = default;
 protected:
  bool is_read_isolated() const { return false; }

  tm_future<NodeExtent::Ref> read_extent(
      Transaction& t, laddr_t addr, extent_len_t len) {
    auto iter = allocate_map.find(addr);
    assert(iter != allocate_map.end());
    assert(iter->second->get_length() == len);
    return tm_ertr::make_ready_future<NodeExtent::Ref>(iter->second);
  }

  tm_future<NodeExtent::Ref> alloc_extent(
      Transaction& t, extent_len_t len) {
    assert(len % ALIGNMENT == 0);
    auto r = ceph::buffer::create_aligned(len, ALIGNMENT);
    auto addr = reinterpret_cast<laddr_t>(r->get_data());
    auto bp = ceph::bufferptr(std::move(r));
    auto extent = Ref<DummyNodeExtent>(new DummyNodeExtent(std::move(bp)));
    extent->set_laddr(addr);
    assert(allocate_map.find(extent->get_laddr()) == allocate_map.end());
    allocate_map.insert({extent->get_laddr(), extent});
    return tm_ertr::make_ready_future<NodeExtent::Ref>(extent);
  }

  tm_future<Super::URef> get_super(Transaction& t, RootNodeTracker& tracker) {
    return tm_ertr::make_ready_future<Super::URef>(
        Super::URef(new DummySuper(t, tracker, &root_laddr)));
  }

  std::map<laddr_t, Ref<DummyNodeExtent>> allocate_map;
  laddr_t root_laddr = L_ADDR_NULL;
};

}
