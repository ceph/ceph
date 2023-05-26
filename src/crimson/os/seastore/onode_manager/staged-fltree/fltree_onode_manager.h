// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/value.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree.h"

namespace crimson::os::seastore::onode {

struct FLTreeOnode final : Onode, Value {
  static constexpr tree_conf_t TREE_CONF = {
    value_magic_t::ONODE,
    256,        // max_ns_size
                //   same to option osd_max_object_namespace_len
    2048,       // max_oid_size
                //   same to option osd_max_object_name_len
    1200,       // max_value_payload_size
                //   see crimson::os::seastore::onode_layout_t
    8192,       // internal_node_size
                //   see the formula in validate_tree_config
    16384       // leaf_node_size
                //   see the formula in validate_tree_config
  };

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
  FLTreeOnode(uint32_t ddr, uint32_t dmr, T&&... args)
    : Onode(ddr, dmr),
      Value(std::forward<T>(args)...) {}

  template <typename... T>
  FLTreeOnode(T&&... args)
    : Onode(0, 0),
      Value(std::forward<T>(args)...) {}

  struct Recorder : public ValueDeltaRecorder {
    Recorder(bufferlist &bl) : ValueDeltaRecorder(bl) {}

    value_magic_t get_header_magic() const final {
      return TREE_CONF.value_magic;
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

  bool is_alive() const {
    return status != status_t::DELETED;
  }
  const onode_layout_t &get_layout() const final {
    assert(status != status_t::DELETED);
    return *read_payload<onode_layout_t>();
  }

  onode_layout_t &get_mutable_layout(Transaction &t) final {
    assert(status != status_t::DELETED);
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    status = status_t::MUTATED;
    return *reinterpret_cast<onode_layout_t*>(p.first.get_write());
  };

  void populate_recorder(Transaction &t) {
    assert(status == status_t::MUTATED);
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    if (p.second) {
      p.second->record_delta(
        p.first);
    }
    status = status_t::STABLE;
  }

  void mark_delete() {
    assert(status != status_t::DELETED);
    status = status_t::DELETED;
  }

  laddr_t get_hint() const final {
    return Value::get_hint();
  }
  ~FLTreeOnode() final {}
};

using OnodeTree = Btree<FLTreeOnode>;

using crimson::common::get_conf;

class FLTreeOnodeManager : public crimson::os::seastore::OnodeManager {
  OnodeTree tree;

  uint32_t default_data_reservation = 0;
  uint32_t default_metadata_offset = 0;
  uint32_t default_metadata_range = 0;
public:
  FLTreeOnodeManager(TransactionManager &tm) :
    tree(NodeExtentManager::create_seastore(tm)),
    default_data_reservation(
      get_conf<uint64_t>("seastore_default_max_object_size")),
    default_metadata_offset(default_data_reservation),
    default_metadata_range(
      get_conf<uint64_t>("seastore_default_object_metadata_reservation"))
  {}

  mkfs_ret mkfs(Transaction &t) {
    return tree.mkfs(t);
  }

  contains_onode_ret contains_onode(
    Transaction &trans,
    const ghobject_t &hoid) final;

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

  erase_onode_ret erase_onode(
    Transaction &trans,
    OnodeRef &onode) final;

  list_onodes_ret list_onodes(
    Transaction &trans,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) final;

  ~FLTreeOnodeManager();
};
using FLTreeOnodeManagerRef = std::unique_ptr<FLTreeOnodeManager>;

}
