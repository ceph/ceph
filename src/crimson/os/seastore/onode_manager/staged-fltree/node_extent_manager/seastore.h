// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <random>

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_delta_recorder.h"

/**
 * seastore.h
 *
 * Seastore backend implementations.
 */

namespace crimson::os::seastore::onode {

class SeastoreSuper final: public Super {
 public:
  SeastoreSuper(Transaction& t, RootNodeTracker& tracker,
                laddr_t root_addr, InterruptedTransactionManager& tm)
    : Super(t, tracker), root_addr{root_addr}, tm{tm} {}
  ~SeastoreSuper() override = default;
 protected:
  laddr_t get_root_laddr() const override {
    return root_addr;
  }
  void write_root_laddr(context_t c, laddr_t addr) override {
    LOG_PREFIX(OTree::Seastore);
    DEBUGT("update root {:#x} ...", c.t, addr);
    root_addr = addr;
    tm.write_onode_root(c.t, addr);
  }
 private:
  laddr_t root_addr;
  InterruptedTransactionManager tm;
};

class SeastoreNodeExtent final: public NodeExtent {
 public:
  SeastoreNodeExtent(ceph::bufferptr &&ptr)
    : NodeExtent(std::move(ptr)) {}
  SeastoreNodeExtent(const SeastoreNodeExtent& other)
    : NodeExtent(other) {}
  ~SeastoreNodeExtent() override = default;
 protected:
  NodeExtentRef mutate(context_t, DeltaRecorderURef&&) override;

  DeltaRecorder* get_recorder() const override {
    return recorder.get();
  }

  CachedExtentRef duplicate_for_write() override {
    return CachedExtentRef(new SeastoreNodeExtent(*this));
  }
  extent_types_t get_type() const override {
    return extent_types_t::ONODE_BLOCK_STAGED;
  }
  ceph::bufferlist get_delta() override {
    assert(recorder);
    return recorder->get_delta();
  }
  void apply_delta(const ceph::bufferlist&) override;
 private:
  DeltaRecorderURef recorder;
};

class TransactionManagerHandle : public NodeExtentManager {
 public:
  TransactionManagerHandle(InterruptedTransactionManager tm) : tm{tm} {}
  InterruptedTransactionManager tm;
};

template <bool INJECT_EAGAIN=false>
class SeastoreNodeExtentManager final: public TransactionManagerHandle {
 public:
  SeastoreNodeExtentManager(
      InterruptedTransactionManager tm, laddr_t min, double p_eagain)
      : TransactionManagerHandle(tm), addr_min{min}, p_eagain{p_eagain} {
    if constexpr (INJECT_EAGAIN) {
      assert(p_eagain > 0.0 && p_eagain < 1.0);
    } else {
      assert(p_eagain == 0.0);
    }
  }

  ~SeastoreNodeExtentManager() override = default;

  void set_generate_eagain(bool enable) {
    generate_eagain = enable;
  }

 protected:
  bool is_read_isolated() const override { return true; }

  read_ertr::future<NodeExtentRef> read_extent(
      Transaction& t, laddr_t addr) override {
    TRACET("reading at {:#x} ...", t, addr);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        DEBUGT("reading at {:#x}: trigger eagain", t, addr);
        return crimson::ct_error::eagain::make();
      }
    }
    return tm.read_extent<SeastoreNodeExtent>(t, addr
    ).safe_then([addr, &t](auto&& e) -> read_ertr::future<NodeExtentRef> {
      TRACET("read {}B at {:#x} -- {}",
             t, e->get_length(), e->get_laddr(), *e);
      if (t.is_conflicted()) {
        ERRORT("transaction conflict detected on extent read {}", t, *e);
	assert(t.is_conflicted());
	return crimson::ct_error::eagain::make();
      }
      assert(e->is_valid());
      assert(e->get_laddr() == addr);
      std::ignore = addr;
      return read_ertr::make_ready_future<NodeExtentRef>(e);
    });
  }

  alloc_ertr::future<NodeExtentRef> alloc_extent(
      Transaction& t, extent_len_t len) override {
    TRACET("allocating {}B ...", t, len);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        DEBUGT("allocating {}B: trigger eagain", t, len);
        return crimson::ct_error::eagain::make();
      }
    }
    return tm.alloc_extent<SeastoreNodeExtent>(t, addr_min, len
    ).safe_then([len, &t](auto extent) {
      DEBUGT("allocated {}B at {:#x} -- {}",
             t, extent->get_length(), extent->get_laddr(), *extent);
      if (!extent->is_initial_pending()) {
        ERRORT("allocated {}B but got invalid extent: {}",
               t, len, *extent);
        ceph_abort("fatal error");
      }
      assert(extent->get_length() == len);
      std::ignore = len;
      return NodeExtentRef(extent);
    });
  }

  retire_ertr::future<> retire_extent(
      Transaction& t, NodeExtentRef _extent) override {
    LogicalCachedExtentRef extent = _extent;
    auto addr = extent->get_laddr();
    auto len = extent->get_length();
    DEBUGT("retiring {}B at {:#x} -- {} ...",
           t, len, addr, *extent);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        DEBUGT("retiring {}B at {:#x} -- {} : trigger eagain",
               t, len, addr, *extent);
        return crimson::ct_error::eagain::make();
      }
    }
    return tm.dec_ref(t, extent).safe_then([addr, len, &t] (unsigned cnt) {
      assert(cnt == 0);
      TRACET("retired {}B at {:#x} ...", t, len, addr);
    });
  }

  getsuper_ertr::future<Super::URef> get_super(
      Transaction& t, RootNodeTracker& tracker) override {
    TRACET("get root ...", t);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        DEBUGT("get root: trigger eagain", t);
        return crimson::ct_error::eagain::make();
      }
    }
    return tm.read_onode_root(t).safe_then([this, &t, &tracker](auto root_addr) {
      TRACET("got root {:#x}", t, root_addr);
      return Super::URef(new SeastoreSuper(t, tracker, root_addr, tm));
    });
  }

  std::ostream& print(std::ostream& os) const override {
    os << "SeastoreNodeExtentManager";
    if constexpr (INJECT_EAGAIN) {
      os << "(p_eagain=" << p_eagain << ")";
    }
    return os;
  }

 private:
  static LOG_PREFIX(OTree::Seastore);

  const laddr_t addr_min;

  // XXX: conditional members by INJECT_EAGAIN
  bool trigger_eagain() {
    if (generate_eagain) {
      double dice = rd();
      assert(rd.min() == 0);
      dice /= rd.max();
      return dice <= p_eagain;
    } else {
      return false;
    }
  }
  bool generate_eagain = true;
  std::random_device rd;
  double p_eagain;
};

}
