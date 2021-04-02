// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/value.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree.h"

namespace crimson::os::seastore::onode {

struct FLTreeOnode : Onode, Value {
  static constexpr value_magic_t HEADER_MAGIC = value_magic_t::ONODE;

  enum class status_t {
    STABLE,
    MUTATED,
    DELETED
  } status = status_t::STABLE;

  FLTreeOnode(FLTreeOnode&&) = default;
  FLTreeOnode& operator=(FLTreeOnode&&) = delete;

  FLTreeOnode(const FLTreeOnode&) = default;
  FLTreeOnode& operator=(const FLTreeOnode&) = delete;

  template <typename... T>
  FLTreeOnode(T&&... args) : Value(std::forward<T>(args)...) {}



  struct Recorder : public ValueDeltaRecorder {
    Recorder(bufferlist &bl) : ValueDeltaRecorder(bl) {}

    value_magic_t get_header_magic() const final {
      return value_magic_t::ONODE;
    }

    void apply_value_delta(
      ceph::bufferlist::const_iterator &bliter,
      NodeExtentMutable &value,
      laddr_t) final {
      assert(value.get_length() == sizeof(onode_layout_t));
      bliter.copy(value.get_length(), value.get_write());
    }

    void record_delta(NodeExtentMutable &value) {
      // TODO: probably could use versioning, etc
      assert(value.get_length() == sizeof(onode_layout_t));
      ceph::buffer::ptr bptr(value.get_length());
      memcpy(bptr.c_str(), value.get_read(), value.get_length());
      get_encoded(value).append(bptr);
    }
  };

  const onode_layout_t &get_layout() const final {
    return *read_payload<onode_layout_t>();
  }

  onode_layout_t &get_mutable_layout(Transaction &t) final {
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    status = status_t::MUTATED;
    return *reinterpret_cast<onode_layout_t*>(p.first.get_write());
  };

  void populate_recorder(Transaction &t) {
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    if (p.second) {
      p.second->record_delta(
        p.first);
    }
    status = status_t::STABLE;
  }

  ~FLTreeOnode() final {}
};

using OnodeTree = Btree<FLTreeOnode>;

class FLTreeOnodeManager : public crimson::os::seastore::OnodeManager {
  OnodeTree tree;

public:
  FLTreeOnodeManager(TransactionManager &tm) :
    tree(std::make_unique<SeastoreNodeExtentManager>(
	   tm, laddr_t{})) {}

  mkfs_ret mkfs(Transaction &t) {
    return tree.mkfs(t
    ).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in FLTreeOnodeManager::mkfs"
      }
    );
  }

  get_onode_ret get_onode(
    Transaction &trans,
    const ghobject_t &hoid) final;

  get_or_create_onode_ret get_or_create_onode(
    Transaction &trans,
    const ghobject_t &hoid) final;

  get_or_create_onodes_ret get_or_create_onodes(
    Transaction &trans,
    const std::vector<ghobject_t> &hoids) final;

  write_dirty_ret write_dirty(
    Transaction &trans,
    const std::vector<OnodeRef> &onodes) final;

  ~FLTreeOnodeManager();
};
using FLTreeOnodeManagerRef = std::unique_ptr<FLTreeOnodeManager>;

}
