// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <map>
#include <memory>
#include <optional>

#include "common/hobject.h"
#ifndef WITH_SEASTAR
#include "osd/osd_internal_types.h"
#else
#include "crimson/osd/object_context.h"
#endif
#include "common/interval_map.h"
#include "common/inline_variant.h"

/**
 * This class represents transactions which can be submitted to
 * a PGBackend.  For expediency, there are some constraints on
 * the operations submitted:
 * 1) Rename sources may only be referenced prior to the rename
 *    operation to the destination.
 * 2) The graph formed by edges of source->destination for clones
 *    (Create) and Renames must be acyclic.
 * 3) clone_range sources must not be modified by the same
 *    transaction
 */
class PGTransaction {
public:
  std::map<hobject_t, ObjectContextRef> obc_map;

  class ObjectOperation {
  public:
    struct Init
    {
      struct None {};
      struct Create {};
      struct Clone {
	hobject_t source;
      };
      struct Rename {
	hobject_t source; // must be temp object
      };
    };
    using InitType = boost::variant<
      Init::None,
      Init::Create,
      Init::Clone,
      Init::Rename>;

    InitType init_type = Init::None();
    bool delete_first = false;

    /**
     * is_none() && is_delete() indicates that we are deleting an
     * object which already exists and not recreating it. delete_first means
     * that the transaction logically removes the object.

     * There are really 4 cases:

     * 1) We are modifying an existing object (is_none() &&
     *    !is_delete())
     *    a) If it's an append, we just write into the log entry the old size
     *    b) If it's an actual overwrite, we save the old versions of the
     *       extents being overwritten and write those offsets into the log
     *       entry
     * 2) We are removing and then recreating an object (!is_none() && is_delete())
     *    -- stash
     * 3) We are removing an object (is_none() && is_delete()) -- stash
     * 4) We are creating an object (!is_none() && !is_delete()) -- create (no
     *    stash)
     *
     * Create, Clone, Rename are the three ways we can recreate it.
     * ECBackend transaction planning needs this context
     * to figure out how to perform the transaction.
     */
    bool deletes_first() const {
      return delete_first;
    }
    bool is_delete() const {
      return boost::get<Init::None>(&init_type) != nullptr && delete_first;
    }
    bool is_none() const {
      return boost::get<Init::None>(&init_type) != nullptr && !delete_first;
    }
    bool is_fresh_object() const {
      return boost::get<Init::None>(&init_type) == nullptr;
    }
    bool is_rename() const {
      return boost::get<Init::Rename>(&init_type) != nullptr;
    }
    bool has_source(hobject_t *source = nullptr) const {
      return match(
	init_type,
	[&](const Init::Clone &op) -> bool {
	  if (source)
	    *source = op.source;
	  return true;
	},
	[&](const Init::Rename &op) -> bool {
	  if (source)
	    *source = op.source;
	  return true;
	},
	[&](const Init::None &) -> bool { return false; },
	[&](const Init::Create &) -> bool { return false; });
    }

    bool clear_omap = false;

    /**
     * truncate
     * <lowest, last> ?
     *
     * truncate is represented as a pair because in the event of
     * multiple truncates within a single transaction we need to
     * remember the lowest truncate and the final object size
     * (the last truncate).  We also adjust the buffers map
     * to account for truncates overriding previous writes */
    std::optional<std::pair<uint64_t, uint64_t> > truncate = std::nullopt;

    std::map<std::string, std::optional<ceph::buffer::list> > attr_updates;

    enum class OmapUpdateType {Remove, Insert, RemoveRange};
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list> > omap_updates;

    std::optional<ceph::buffer::list> omap_header;

    /// (old, new) -- only valid with no truncate or buffer updates
    std::optional<std::pair<std::set<snapid_t>, std::set<snapid_t>>> updated_snaps;

    struct alloc_hint_t {
      uint64_t expected_object_size;
      uint64_t expected_write_size;
      uint32_t flags;
    };
    std::optional<alloc_hint_t> alloc_hint;

    struct BufferUpdate {
      struct Write {
	ceph::buffer::list buffer;
	uint32_t fadvise_flags;
      };
      struct Zero {
	uint64_t len;
      };
      struct CloneRange {
	hobject_t from;
	uint64_t offset;
	uint64_t len;
      };
    };
    using BufferUpdateType = boost::variant<
      BufferUpdate::Write,
      BufferUpdate::Zero,
      BufferUpdate::CloneRange>;

  private:
    struct SplitMerger {
      BufferUpdateType split(
	uint64_t offset,
	uint64_t len,
	const BufferUpdateType &bu) const {
	return match(
	  bu,
	  [&](const BufferUpdate::Write &w) -> BufferUpdateType {
	    ceph::buffer::list bl;
	    bl.substr_of(w.buffer, offset, len);
	    return BufferUpdate::Write{bl, w.fadvise_flags};
	  },
	  [&](const BufferUpdate::Zero &) -> BufferUpdateType {
	    return BufferUpdate::Zero{len};
	  },
	  [&](const BufferUpdate::CloneRange &c) -> BufferUpdateType {
	    return BufferUpdate::CloneRange{c.from, c.offset + offset, len};
	  });
      }
      uint64_t length(
	const BufferUpdateType &left) const {
	return match(
	  left,
	  [&](const BufferUpdate::Write &w) -> uint64_t {
	    return w.buffer.length();
	  },
	  [&](const BufferUpdate::Zero &z) -> uint64_t {
	    return z.len;
	  },
	  [&](const BufferUpdate::CloneRange &c) -> uint64_t {
	    return c.len;
	  });
      }
      bool can_merge(
	const BufferUpdateType &left,
	const BufferUpdateType &right) const {
	return match(
	  left,
	  [&](const BufferUpdate::Write &w) -> bool {
	    auto r = boost::get<BufferUpdate::Write>(&right);
	    return r != nullptr && (w.fadvise_flags == r->fadvise_flags);
	  },
	  [&](const BufferUpdate::Zero &) -> bool {
	    auto r = boost::get<BufferUpdate::Zero>(&right);
	    return r != nullptr;
	  },
	  [&](const BufferUpdate::CloneRange &c) -> bool {
	    return false;
	  });
      }
      BufferUpdateType merge(
	BufferUpdateType &&left,
	BufferUpdateType &&right) const {
	return match(
	  left,
	  [&](const BufferUpdate::Write &w) -> BufferUpdateType {
	    auto r = boost::get<BufferUpdate::Write>(&right);
	    ceph_assert(r && w.fadvise_flags == r->fadvise_flags);
	    ceph::buffer::list bl = w.buffer;
	    bl.append(r->buffer);
	    return BufferUpdate::Write{bl, w.fadvise_flags};
	  },
	  [&](const BufferUpdate::Zero &z) -> BufferUpdateType {
	    auto r = boost::get<BufferUpdate::Zero>(&right);
	    ceph_assert(r);
	    return BufferUpdate::Zero{z.len + r->len};
	  },
	  [&](const BufferUpdate::CloneRange &c) -> BufferUpdateType {
	    ceph_abort_msg("violates can_merge condition");
	    return left;
	  });
      }
    };
  public:
    using buffer_update_type = interval_map<
      uint64_t, BufferUpdateType, SplitMerger>;
    buffer_update_type buffer_updates;

    friend class PGTransaction;
  };
  std::map<hobject_t, ObjectOperation> op_map;
private:
  ObjectOperation &get_object_op_for_modify(const hobject_t &hoid) {
    auto &op = op_map[hoid];
    ceph_assert(!op.is_delete());
    return op;
  }
  ObjectOperation &get_object_op(const hobject_t &hoid) {
    return op_map[hoid];
  }
public:
  void add_obc(
    ObjectContextRef obc) {
    ceph_assert(obc);
    obc_map[obc->obs.oi.soid] = obc;
  }
  /// Sets up state for new object
  void create(
    const hobject_t &hoid
    ) {
    auto &op = op_map[hoid];
    ceph_assert(op.is_none() || op.is_delete());
    op.init_type = ObjectOperation::Init::Create();
  }

  /// Sets up state for target cloned from source
  void clone(
    const hobject_t &target,       ///< [in] obj to clone to
    const hobject_t &source        ///< [in] obj to clone from
    ) {
    auto &op = op_map[target];
    ceph_assert(op.is_none() || op.is_delete());
    op.init_type = ObjectOperation::Init::Clone{source};
  }

  /// Sets up state for target renamed from source
  void rename(
    const hobject_t &target,       ///< [in] to, must not exist, be non-temp
    const hobject_t &source        ///< [in] source (must be a temp object)
    ) {
    ceph_assert(source.is_temp());
    ceph_assert(!target.is_temp());
    auto &op = op_map[target];
    ceph_assert(op.is_none() || op.is_delete());

    bool del_first = op.is_delete();
    auto iter = op_map.find(source);
    if (iter != op_map.end()) {
      op = iter->second;
      op_map.erase(iter);
      op.delete_first = del_first;
    }

    op.init_type = ObjectOperation::Init::Rename{source};
  }

  /// Remove -- must not be called on rename target
  void remove(
    const hobject_t &hoid          ///< [in] obj to remove
    ) {
    auto &op = get_object_op_for_modify(hoid);
    if (!op.is_fresh_object()) {
      ceph_assert(!op.updated_snaps);
      op = ObjectOperation();
      op.delete_first = true;
    } else {
      ceph_assert(!op.is_rename());
      op_map.erase(hoid); // make it a noop if it's a fresh object
    }
  }

  void update_snaps(
    const hobject_t &hoid,         ///< [in] object for snaps
    const std::set<snapid_t> &old_snaps,///< [in] old snaps value
    const std::set<snapid_t> &new_snaps ///< [in] new snaps value
    ) {
    auto &op = get_object_op(hoid);
    ceph_assert(!op.updated_snaps);
    ceph_assert(op.buffer_updates.empty());
    ceph_assert(!op.truncate);
    op.updated_snaps = make_pair(
      old_snaps,
      new_snaps);
  }

  /// Clears, truncates
  void omap_clear(
    const hobject_t &hoid          ///< [in] object to clear omap
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.clear_omap = true;
    op.omap_updates.clear();
    op.omap_header = std::nullopt;
  }
  void truncate(
    const hobject_t &hoid,         ///< [in] object
    uint64_t off                   ///< [in] offset to truncate to
    ) {
    auto &op = get_object_op_for_modify(hoid);
    ceph_assert(!op.updated_snaps);
    op.buffer_updates.erase(
      off,
      std::numeric_limits<uint64_t>::max() - off);
    if (!op.truncate || off < op.truncate->first) {
      op.truncate = std::pair<uint64_t, uint64_t>(off, off);
    } else {
      op.truncate->second = off;
    }
  }

  /// Attr ops
  void setattrs(
    const hobject_t &hoid,         ///< [in] object to write
    std::map<std::string, ceph::buffer::list, std::less<>> &attrs ///< [in] attrs, may be cleared
    ) {
    auto &op = get_object_op_for_modify(hoid);
    for (auto &[key, val]: attrs) {
      auto& d = op.attr_updates[key];
      d = val;
      d->rebuild();
    }
  }
  void setattr(
    const hobject_t &hoid,         ///< [in] object to write
    const std::string &attrname,        ///< [in] attr to write
    ceph::buffer::list &bl                 ///< [in] val to write, may be claimed
    ) {
    auto &op = get_object_op_for_modify(hoid);
    auto& d = op.attr_updates[attrname];
    d = bl;
    d->rebuild();
  }
  void rmattr(
    const hobject_t &hoid,         ///< [in] object to write
    const std::string &attrname         ///< [in] attr to remove
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.attr_updates[attrname] = std::nullopt;
  }

  /// set alloc hint
  void set_alloc_hint(
    const hobject_t &hoid,         ///< [in] object (must exist)
    uint64_t expected_object_size, ///< [in]
    uint64_t expected_write_size,
    uint32_t flags
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.alloc_hint = ObjectOperation::alloc_hint_t{
      expected_object_size, expected_write_size, flags};
  }

  /// Buffer updates
  void write(
    const hobject_t &hoid,         ///< [in] object to write
    uint64_t off,                  ///< [in] off at which to write
    uint64_t len,                  ///< [in] len to write from bl
    ceph::buffer::list &bl,                ///< [in] bl to write will be claimed to len
    uint32_t fadvise_flags = 0     ///< [in] fadvise hint
    ) {
    auto &op = get_object_op_for_modify(hoid);
    ceph_assert(!op.updated_snaps);
    ceph_assert(len > 0);
    ceph_assert(len == bl.length());
    op.buffer_updates.insert(
      off,
      len,
      ObjectOperation::BufferUpdate::Write{bl, fadvise_flags});
  }
  void clone_range(
    const hobject_t &from,         ///< [in] from
    const hobject_t &to,           ///< [in] to
    uint64_t fromoff,              ///< [in] offset
    uint64_t len,                  ///< [in] len
    uint64_t tooff                 ///< [in] offset
    ) {
    auto &op = get_object_op_for_modify(to);
    ceph_assert(!op.updated_snaps);
    op.buffer_updates.insert(
      tooff,
      len,
      ObjectOperation::BufferUpdate::CloneRange{from, fromoff, len});
  }
  void zero(
    const hobject_t &hoid,         ///< [in] object
    uint64_t off,                  ///< [in] offset to start zeroing at
    uint64_t len                   ///< [in] amount to zero
    ) {
    auto &op = get_object_op_for_modify(hoid);
    ceph_assert(!op.updated_snaps);
    op.buffer_updates.insert(
      off,
      len,
      ObjectOperation::BufferUpdate::Zero{len});
  }

  /// Omap updates
  void omap_setkeys(
    const hobject_t &hoid,         ///< [in] object to write
    ceph::buffer::list &keys_bl            ///< [in] encoded map<string, ceph::buffer::list>
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_updates.emplace_back(
      std::make_pair(
	ObjectOperation::OmapUpdateType::Insert,
	keys_bl));
  }
  void omap_setkeys(
    const hobject_t &hoid,         ///< [in] object to write
    std::map<std::string, ceph::buffer::list> &keys  ///< [in] omap keys, may be cleared
    ) {
    using ceph::encode;
    ceph::buffer::list bl;
    encode(keys, bl);
    omap_setkeys(hoid, bl);
  }
  void omap_rmkeys(
    const hobject_t &hoid,         ///< [in] object to write
    ceph::buffer::list &keys_bl            ///< [in] encode set<string>
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_updates.emplace_back(
      std::make_pair(
	ObjectOperation::OmapUpdateType::Remove,
	keys_bl));
  }
  void omap_rmkeys(
    const hobject_t &hoid,         ///< [in] object to write
    std::set<std::string> &keys              ///< [in] omap keys, may be cleared
    ) {
    using ceph::encode;
    ceph::buffer::list bl;
    encode(keys, bl);
    omap_rmkeys(hoid, bl);
  }
  void omap_rmkeyrange(
    const hobject_t &hoid,         ///< [in] object to write
    ceph::buffer::list &range_bl           ///< [in] encode string[2]
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_updates.emplace_back(
      std::make_pair(
	ObjectOperation::OmapUpdateType::RemoveRange,
	range_bl));
  }
  void omap_rmkeyrange(
    const hobject_t &hoid,         ///< [in] object to write
    std::string& key_begin,        ///< [in] first key in range
    std::string& key_end           ///< [in] first key past range, range is [first,last)
    ) {
    ceph::buffer::list bl;
    ::encode(key_begin, bl);
    ::encode(key_end, bl);
    omap_rmkeyrange(hoid, bl);
  }
  void omap_setheader(
    const hobject_t &hoid,         ///< [in] object to write
    ceph::buffer::list &header             ///< [in] header
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_header = header;
  }

  bool empty() const {
    return op_map.empty();
  }

  uint64_t get_bytes_written() const {
    uint64_t ret = 0;
    for (auto &&i: op_map) {
      for (auto &&j: i.second.buffer_updates) {
	ret += j.get_len();
      }
    }
    return ret;
  }

  void nop(
    const hobject_t &hoid ///< [in] obj to which we are doing nothing
    ) {
    get_object_op_for_modify(hoid);
  }

  /* Calls t() on all pair<hobject_t, ObjectOperation> & such that clone/rename
   * sinks are always called before clone sources
   *
   * TODO: add a fast path for the single object case and possibly the single
   * object clone from source case (make_writeable made a clone).
   *
   * This structure only requires that the source->sink graph be acyclic.
   * This is much more general than is actually required by PrimaryLogPG.
   * Only 4 flavors of multi-object transactions actually happen:
   * 1) rename temp -> object for copyfrom
   * 2) clone head -> clone, modify head for make_writeable on normal head write
   * 3) clone clone -> head for rollback
   * 4) 2 + 3
   *
   * We can bypass the below logic for single object transactions trivially
   * (including case 1 above since temp doesn't show up again).
   * For 2-3, we could add something ad-hoc to ensure that they happen in the
   * right order, but it actually seems easier to just do the graph construction.
   */
  template <typename T>
  void safe_create_traverse(T &&t) {
    std::map<hobject_t, std::list<hobject_t>> dgraph;
    std::list<hobject_t> stack;

    // Populate stack with roots, dgraph with edges
    for (auto &&opair: op_map) {
      hobject_t source;
      if (opair.second.has_source(&source)) {
	auto &l = dgraph[source];
	if (l.empty() && !op_map.count(source)) {
	  /* Source oids not in op_map need to be added as roots
	   * (but only once!) */
	  stack.push_back(source);
	}
	l.push_back(opair.first);
      } else {
	stack.push_back(opair.first);
      }
    }

    /* Why don't we need to worry about accessing the same node
     * twice?  dgraph nodes always have in-degree at most 1 because
     * the inverse graph nodes (source->dest) can have out-degree
     * at most 1 (only one possible source).  We do a post-order
     * depth-first traversal here to ensure we call f on children
     * before parents.
     */
    while (!stack.empty()) {
      hobject_t &cur = stack.front();
      auto diter = dgraph.find(cur);
      if (diter == dgraph.end()) {
	/* Leaf: pop and call t() */
	auto opiter = op_map.find(cur);
	if (opiter != op_map.end())
	  t(*opiter);
	stack.pop_front();
      } else {
	/* Internal node: push children onto stack, remove edge,
	 * recurse.  When this node is encountered again, it'll
	 * be a leaf */
	ceph_assert(!diter->second.empty());
	stack.splice(stack.begin(), diter->second);
	dgraph.erase(diter);
      }
    }
  }
};
using PGTransactionUPtr = std::unique_ptr<PGTransaction>;

#endif
