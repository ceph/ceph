// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <random>

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"

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
  // meta collection: root lives in root_t::meta_onode_root, no coll_node.
  SeastoreSuper(Transaction& t, RootNodeTracker& tracker,
                laddr_t root_addr, TransactionManager& tm)
    : Super(t, tracker), root_addr{root_addr}, tm{tm} {}
  // root lives in that collection's coll_info_t
  SeastoreSuper(Transaction& t, RootNodeTracker& tracker,
                laddr_t root_addr, TransactionManager& tm, coll_t cid,
                collection_manager::CollectionNode::CollectionNodeRef coll_node,
                unsigned split_bits)
    : Super(t, tracker), root_addr{root_addr}, tm{tm}, cid{cid},
      coll_node{std::move(coll_node)}, split_bits{split_bits} {}
  ~SeastoreSuper() override = default;
 protected:
  laddr_t get_root_laddr() const override {
    return root_addr;
  }
  void write_root_laddr(context_t c, laddr_t addr) override {
    LOG_PREFIX(OTree::Seastore);
    root_addr = addr;
    if (coll_node) {
      SUBDEBUGT(seastore_onode, "update coll {} onode root {} ...",
                c.t, cid, addr);
      // todo: coll_node already exists so we can avoid chaining the future here.
      //       switch to get? 
      std::ignore = coll_node->update(
        collection_manager::coll_context_t{tm, c.t}, cid,
        collection_manager::coll_value_t{split_bits, addr});
    } else {
      SUBDEBUGT(seastore_onode, "update meta onode root {} ...", c.t, addr);
      tm.write_meta_onode_root(c.t, addr);
    }
  }
 private:
  laddr_t root_addr;
  TransactionManager &tm;
  coll_t cid;
  collection_manager::CollectionNode::CollectionNodeRef coll_node;
  unsigned split_bits = 0;
};

class SeastoreNodeExtent final: public NodeExtent {
 public:
  explicit SeastoreNodeExtent(ceph::bufferptr &&ptr)
    : NodeExtent(std::move(ptr)) {}
  explicit SeastoreNodeExtent(extent_len_t length)
    : NodeExtent(length) {}
  SeastoreNodeExtent(const SeastoreNodeExtent& other)
    : NodeExtent(other) {}
  ~SeastoreNodeExtent() override = default;

  constexpr static extent_types_t TYPE = extent_types_t::ONODE_BLOCK_STAGED;
  extent_types_t get_type() const override {
    return TYPE;
  }

 protected:
  NodeExtentRef mutate(context_t, DeltaRecorderURef&&) override;

  void do_on_state_commit() override {
    auto &prior = static_cast<SeastoreNodeExtent&>(*get_prior_instance());
    prior.recorder = std::move(recorder);
  }

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
      TransactionManager &tm, laddr_t min, double p_eagain,
      coll_t cid, collection_manager::FlatCollectionManager &collection_manager)
      : TransactionManagerHandle(tm), addr_min{min},
        cid{cid}, collection_manager{collection_manager}, p_eagain{p_eagain} {
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
    SUBTRACET(seastore_onode, "reading at {} ...", t, addr);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        SUBDEBUGT(seastore_onode, "reading at {}: trigger eagain", t, addr);
        t.test_set_conflict();
        return read_iertr::make_ready_future<NodeExtentRef>();
      }
    }
    return tm.read_extent<SeastoreNodeExtent>(t, addr
    ).si_then([addr, &t](auto maybe_indirect_extent)
              -> read_iertr::future<NodeExtentRef> {
      auto e = maybe_indirect_extent.extent;
      SUBTRACET(seastore_onode,
          "read {}B at {} -- {}",
          t, e->get_length(), e->get_laddr(), *e);
      assert(!maybe_indirect_extent.is_indirect());
      assert(!maybe_indirect_extent.is_clone);
      assert(e->get_laddr() == addr);
      std::ignore = addr;
      return read_iertr::make_ready_future<NodeExtentRef>(e);
    });
  }

  alloc_iertr::future<NodeExtentRef> alloc_extent(
      Transaction& t, laddr_hint_t hint, extent_len_t len) override {
    SUBTRACET(seastore_onode, "allocating {}B with hint {} ...", t, len, hint);
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
          "allocated {}B at {} -- {}",
          t, extent->get_length(), extent->get_laddr(), *extent);
      if (!extent->is_initial_pending()) {
        SUBERRORT(seastore_onode,
            "allocated {}B but got invalid extent: {}",
            t, len, *extent);
        ceph_abort_msg("fatal error");
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
    LogicalChildNodeRef extent = _extent;
    auto addr = extent->get_laddr();
    auto len = extent->get_length();
    SUBDEBUGT(seastore_onode,
        "retiring {}B at {} -- {} ...",
        t, len, addr, *extent);
    if constexpr (INJECT_EAGAIN) {
      if (trigger_eagain()) {
        SUBDEBUGT(seastore_onode,
            "retiring {}B at {} -- {} : trigger eagain",
            t, len, addr, *extent);
        t.test_set_conflict();
        return retire_iertr::now();
      }
    }
    return tm.remove(t, extent).si_then([addr, len, &t] (unsigned cnt) {
      assert(cnt == 0);
      SUBTRACET(seastore_onode, "retired {}B at {} ...", t, len, addr);
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
    if (cid == coll_t::meta()) {
      return tm.read_meta_onode_root(t).si_then(
          [this, &t, &tracker](auto root_addr) {
        SUBTRACET(seastore_onode, "meta got root {}", t, root_addr);
        return Super::URef(new SeastoreSuper(t, tracker, root_addr, tm));
      });
    }
    return tm.read_collection_root(t).si_then(
        [this, &t](auto coll_root) {
      return collection_manager.get_coll_node(coll_root, t);
    }).handle_error_interruptible(
      getsuper_iertr::pass_further{},
      crimson::ct_error::assert_all(
        "SeastoreNodeExtentManager::get_super: unexpected error reading "
        "collection node")
    ).si_then([this, &t, &tracker](auto coll_node) {
      auto &value = coll_node->get_value(cid);
      SUBTRACET(seastore_onode, "coll {} got root {}", t, cid, value.onode_root);
      return Super::URef(new SeastoreSuper(
        t, tracker, value.onode_root, tm, cid, coll_node, value.bits));
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

  // collection_manager of this cid.
  const coll_t cid;
  collection_manager::FlatCollectionManager &collection_manager;

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
