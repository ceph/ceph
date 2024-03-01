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
                laddr_t root_addr, TransactionManager& tm)
    : Super(t, tracker), root_addr{root_addr}, tm{tm} {}
  ~SeastoreSuper() override = default;
 protected:
  laddr_t get_root_laddr() const override {
    return root_addr;
  }
  void write_root_laddr(context_t c, laddr_t addr) override {
    LOG_PREFIX(OTree::Seastore);
    SUBDEBUGT(seastore_onode, "update root {:#x} ...", c.t, addr);
    root_addr = addr;
    tm.write_onode_root(c.t, addr);
  }
 private:
  laddr_t root_addr;
  TransactionManager &tm;
};

class SeastoreNodeExtent final: public NodeExtent {
 public:
  SeastoreNodeExtent(ceph::bufferptr &&ptr)
    : NodeExtent(std::move(ptr)) {}
  SeastoreNodeExtent(const SeastoreNodeExtent& other)
    : NodeExtent(other) {}
  ~SeastoreNodeExtent() override = default;

  constexpr static extent_types_t TYPE = extent_types_t::ONODE_BLOCK_STAGED;
  extent_types_t get_type() const override {
    return TYPE;
  }

 protected:
  NodeExtentRef mutate(context_t, DeltaRecorderURef&&) override;

  DeltaRecorder* get_recorder() const override {
    return recorder.get();
  }

  CachedExtentRef duplicate_for_write(Transaction&) override {
    return CachedExtentRef(new SeastoreNodeExtent(*this));
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
  TransactionManagerHandle(TransactionManager &tm) : tm{tm} {}
  TransactionManager &tm;
};

template <bool INJECT_EAGAIN=false>
class SeastoreNodeExtentManager final: public TransactionManagerHandle {
 public:
  SeastoreNodeExtentManager(
      TransactionManager &tm, laddr_t min, double p_eagain)
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

  read_iertr::future<NodeExtentRef> read_extent(
      Transaction& t, laddr_t addr) override {
    SUBTRACET(seastore_onode, "reading at {:#x} ...", t, addr);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        SUBDEBUGT(seastore_onode, "reading at {:#x}: trigger eagain", t, addr);
        t.test_set_conflict();
        return read_iertr::make_ready_future<NodeExtentRef>();
      }
    }
    return tm.read_extent<SeastoreNodeExtent>(t, addr
    ).si_then([addr, &t](auto&& e) -> read_iertr::future<NodeExtentRef> {
      SUBTRACET(seastore_onode,
          "read {}B at {:#x} -- {}",
          t, e->get_length(), e->get_laddr(), *e);
      assert(e->get_laddr() == addr);
      std::ignore = addr;
      return read_iertr::make_ready_future<NodeExtentRef>(e);
    });
  }

  alloc_iertr::future<NodeExtentRef> alloc_extent(
      Transaction& t, laddr_t hint, extent_len_t len) override {
    SUBTRACET(seastore_onode, "allocating {}B with hint {:#x} ...", t, len, hint);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        SUBDEBUGT(seastore_onode, "allocating {}B: trigger eagain", t, len);
        t.test_set_conflict();
        return alloc_iertr::make_ready_future<NodeExtentRef>();
      }
    }
    return tm.alloc_non_data_extent<SeastoreNodeExtent>(t, hint, len
    ).si_then([len, &t](auto extent) {
      SUBDEBUGT(seastore_onode,
          "allocated {}B at {:#x} -- {}",
          t, extent->get_length(), extent->get_laddr(), *extent);
      if (!extent->is_initial_pending()) {
        SUBERRORT(seastore_onode,
            "allocated {}B but got invalid extent: {}",
            t, len, *extent);
        ceph_abort("fatal error");
      }
      assert(extent->get_length() == len);
      std::ignore = len;
      return NodeExtentRef(extent);
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      alloc_iertr::pass_further{}
    );
  }

  retire_iertr::future<> retire_extent(
      Transaction& t, NodeExtentRef _extent) override {
    LogicalCachedExtentRef extent = _extent;
    auto addr = extent->get_laddr();
    auto len = extent->get_length();
    SUBDEBUGT(seastore_onode,
        "retiring {}B at {:#x} -- {} ...",
        t, len, addr, *extent);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        SUBDEBUGT(seastore_onode,
            "retiring {}B at {:#x} -- {} : trigger eagain",
            t, len, addr, *extent);
        t.test_set_conflict();
        return retire_iertr::now();
      }
    }
    return tm.remove(t, extent).si_then([addr, len, &t] (unsigned cnt) {
      assert(cnt == 0);
      SUBTRACET(seastore_onode, "retired {}B at {:#x} ...", t, len, addr);
    });
  }

  getsuper_iertr::future<Super::URef> get_super(
      Transaction& t, RootNodeTracker& tracker) override {
    SUBTRACET(seastore_onode, "get root ...", t);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        SUBDEBUGT(seastore_onode, "get root: trigger eagain", t);
        t.test_set_conflict();
        return getsuper_iertr::make_ready_future<Super::URef>();
      }
    }
    return tm.read_onode_root(t).si_then([this, &t, &tracker](auto root_addr) {
      SUBTRACET(seastore_onode, "got root {:#x}", t, root_addr);
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

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::onode::SeastoreNodeExtent> : fmt::ostream_formatter {};
#endif
