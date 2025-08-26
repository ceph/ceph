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
    ALIVE,
    DELETED
  } status = status_t::ALIVE;

  FLTreeOnode(FLTreeOnode&&) = default;
  FLTreeOnode& operator=(FLTreeOnode&&) = delete;

  FLTreeOnode(const FLTreeOnode&) = default;
  FLTreeOnode& operator=(const FLTreeOnode&) = delete;

  template <typename... T>
  FLTreeOnode(const hobject_t &hobj, T&&... args)
    : Onode(hobj),
      Value(std::forward<T>(args)...) {}

  struct Recorder : public ValueDeltaRecorder {
    enum class delta_op_t : uint8_t {
      UPDATE_ONODE_SIZE,
      UPDATE_OMAP_ROOT,
      UPDATE_LOG_ROOT,
      UPDATE_XATTR_ROOT,
      UPDATE_OBJECT_DATA,
      UPDATE_OBJECT_INFO,
      UPDATE_SNAPSET,
      CLEAR_OBJECT_INFO,
      CLEAR_SNAPSET,
      CREATE_DEFAULT,
      SET_NEED_COW,
      UNSET_NEED_COW
    };
    Recorder(bufferlist &bl) : ValueDeltaRecorder(bl) {}

    value_magic_t get_header_magic() const final {
      return TREE_CONF.value_magic;
    }

    void apply_value_delta(
      ceph::bufferlist::const_iterator &bliter,
      NodeExtentMutable &value,
      laddr_offset_t value_addr_offset) final;

    void encode_update(NodeExtentMutable &payload_mut, delta_op_t op);
  };

  bool is_alive() const {
    return status != status_t::DELETED;
  }
  const onode_layout_t &get_layout() const final {
    assert(status != status_t::DELETED);
    return *read_payload<onode_layout_t>();
  }

  template <typename layout_func_t>
  void with_mutable_layout(
    Transaction &t,
    layout_func_t &&layout_func) {
    assert(status != status_t::DELETED);
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    layout_func(p.first, p.second);
  }

  void swap_layout(Transaction &t, Onode &onode) final {
    _swap_layout(t, static_cast<FLTreeOnode&>(onode));
  }

  void _swap_layout(Transaction &t, FLTreeOnode &other) {
    assert(status != status_t::DELETED);
    assert(other.status != status_t::DELETED);
    auto [payload_mut, recorder] = prepare_mutate_payload<
      onode_layout_t, Recorder>(t);
    auto &mlayout = *reinterpret_cast<onode_layout_t*>(
      payload_mut.get_write());
    auto [o_payload_mut, o_recorder] = other.prepare_mutate_payload<
      onode_layout_t, Recorder>(t);
    auto &o_mlayout = *reinterpret_cast<onode_layout_t*>(
      o_payload_mut.get_write());
    std::swap(mlayout.object_data, o_mlayout.object_data);
    std::swap(mlayout.omap_root, o_mlayout.omap_root);
    std::swap(mlayout.log_root, o_mlayout.log_root);
    std::swap(mlayout.xattr_root, o_mlayout.xattr_root);
    if (recorder) {
      recorder->encode_update(
	payload_mut, Recorder::delta_op_t::UPDATE_OBJECT_DATA);
      recorder->encode_update(
	payload_mut, Recorder::delta_op_t::UPDATE_OMAP_ROOT);
      recorder->encode_update(
	payload_mut, Recorder::delta_op_t::UPDATE_LOG_ROOT);
      recorder->encode_update(
	payload_mut, Recorder::delta_op_t::UPDATE_XATTR_ROOT);
    }
    if (o_recorder) {
      o_recorder->encode_update(
	o_payload_mut, Recorder::delta_op_t::UPDATE_OBJECT_DATA);
      o_recorder->encode_update(
	o_payload_mut, Recorder::delta_op_t::UPDATE_OMAP_ROOT);
      o_recorder->encode_update(
	o_payload_mut, Recorder::delta_op_t::UPDATE_LOG_ROOT);
      o_recorder->encode_update(
	o_payload_mut, Recorder::delta_op_t::UPDATE_XATTR_ROOT);
    }
  }

  void create_default_layout(Transaction &t) {
    with_mutable_layout(
      t,
      [](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
	  payload_mut.get_write());
	mlayout = onode_layout_t{};
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::CREATE_DEFAULT);
	}
    });
  }

  void set_need_cow(Transaction &t) final {
    with_mutable_layout(
      t,
      [](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	mlayout.need_cow = true;
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::SET_NEED_COW);
	}
    });
  }

  void unset_need_cow(Transaction &t) final {
    with_mutable_layout(
      t,
      [](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	mlayout.need_cow = false;
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UNSET_NEED_COW);
	}
    });
  }

  void update_onode_size(Transaction &t, uint32_t size) final {
    with_mutable_layout(
      t,
      [size](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	mlayout.size = size;
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_ONODE_SIZE);
	}
    });
  }

  void update_omap_root(Transaction &t, omap_root_t &oroot) final {
    assert(oroot.get_type() == omap_type_t::OMAP);
    with_mutable_layout(
      t,
      [&oroot](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	mlayout.omap_root.update(oroot);
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_OMAP_ROOT);
	}
    });
  }

  void update_log_root(Transaction &t, omap_root_t &lroot) final {
    assert(lroot.get_type() == omap_type_t::LOG);
    with_mutable_layout(
      t,
      [&lroot](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	mlayout.log_root.update(lroot);
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_LOG_ROOT);
	}
    });
  }

  void update_xattr_root(Transaction &t, omap_root_t &xroot) final {
    assert(xroot.get_type() == omap_type_t::XATTR);
    with_mutable_layout(
      t,
      [&xroot](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
	  payload_mut.get_write());
	mlayout.xattr_root.update(xroot);
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_XATTR_ROOT);
	}
    });
  }

  void update_object_data(Transaction &t, object_data_t &odata) final {
    with_mutable_layout(
      t,
      [&odata](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	mlayout.object_data.update(odata);
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_OBJECT_DATA);
	}
    });
  }

  void update_object_info(Transaction &t, ceph::bufferlist &oi_bl) final {
    with_mutable_layout(
      t,
      [&oi_bl](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	maybe_inline_memcpy(
	  &mlayout.oi[0],
	  oi_bl.c_str(),
	  oi_bl.length(),
	  onode_layout_t::MAX_OI_LENGTH);
	mlayout.oi_size = oi_bl.length();
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_OBJECT_INFO);
	}
    });
  }

  void clear_object_info(Transaction &t) final {
    with_mutable_layout(
      t, [](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	memset(&mlayout.oi[0], 0, mlayout.oi_size);
	mlayout.oi_size = 0;
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::CLEAR_OBJECT_INFO);
	}
    });
  }

  void update_snapset(Transaction &t, ceph::bufferlist &ss_bl) final {
    with_mutable_layout(
      t,
      [&ss_bl](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	maybe_inline_memcpy(
	  &mlayout.ss[0],
	  ss_bl.c_str(),
	  ss_bl.length(),
	  onode_layout_t::MAX_OI_LENGTH);
	mlayout.ss_size = ss_bl.length();
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::UPDATE_SNAPSET);
	}
    });
  }

  void clear_snapset(Transaction &t) final {
    with_mutable_layout(
      t,
      [](NodeExtentMutable &payload_mut, Recorder *recorder) {
	auto &mlayout = *reinterpret_cast<onode_layout_t*>(
          payload_mut.get_write());
	memset(&mlayout.ss[0], 0, mlayout.ss_size);
	mlayout.ss_size = 0;
	if (recorder) {
	  recorder->encode_update(
	    payload_mut, Recorder::delta_op_t::CLEAR_SNAPSET);
	}
    });
  }

  void mark_delete() {
    assert(status != status_t::DELETED);
    status = status_t::DELETED;
  }

  laddr_hint_t init_hint(
    extent_len_t block_size,
    bool is_metadata) const final {
    return Value::init_hint(block_size, is_metadata);
  }
  laddr_hint_t generate_clone_hint(
    local_object_id_t object_id,
    extent_len_t block_size,
    bool is_metadata) const final {
    return Value::generate_clone_hint(object_id, block_size, is_metadata);
  }
  ~FLTreeOnode() final {}
};

using OnodeTree = Btree<FLTreeOnode>;

using crimson::common::get_conf;

class FLTreeOnodeManager : public crimson::os::seastore::OnodeManager {
  OnodeTree tree;

  uint32_t default_data_reservation = 0;
public:
  FLTreeOnodeManager(TransactionManager &tm) :
    tree(NodeExtentManager::create_seastore(tm)),
    default_data_reservation(
      get_conf<uint64_t>("seastore_default_max_object_size"))
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
