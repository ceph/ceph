// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "common/hobject.h"
#include "crimson/common/type_helpers.h"

#include "fwd.h"
#include "tree_types.h"

/**
 * tree.h
 *
 * An example implementation to expose tree interfaces to users. The current
 * interface design is based on:
 * - ceph::os::Transaction::create/touch/remove()
 * - ceph::ObjectStore::collection_list()
 * - ceph::BlueStore::get_onode()
 * - db->get_iterator(PREFIIX_OBJ) by ceph::BlueStore::fsck()
 *
 * TODO: Redesign the interfaces based on real onode manager requirements.
 */

namespace crimson::os::seastore::onode {

class Node;
class Btree {
 public:
  using btree_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  template <class ValueT=void>
  using btree_future = btree_ertr::future<ValueT>;

  Btree(NodeExtentManagerURef&& nm);
  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree& operator=(const Btree&) = delete;
  Btree& operator=(Btree&&) = delete;
  ~Btree();

  btree_future<> mkfs(Transaction&);

  class Cursor;
  // lookup
  btree_future<Cursor> begin(Transaction&);
  btree_future<Cursor> last(Transaction&);
  Cursor end();
  btree_future<bool> contains(Transaction&, const ghobject_t&);
  btree_future<Cursor> find(Transaction&, const ghobject_t&);
  btree_future<Cursor> lower_bound(Transaction&, const ghobject_t&);

  // modifiers
  // TODO: replace onode_t
  btree_future<std::pair<Cursor, bool>>
  insert(Transaction&, const ghobject_t&, const onode_t&);
  btree_future<size_t> erase(Transaction&, const ghobject_t& key);
  btree_future<Cursor> erase(Cursor& pos);
  btree_future<Cursor> erase(Cursor& first, Cursor& last);

  // stats
  btree_future<size_t> height(Transaction&);
  btree_future<tree_stats_t> get_stats_slow(Transaction&);
  std::ostream& dump(Transaction&, std::ostream&);

  // test_only
  bool test_is_clean() const;
  btree_future<> test_clone_from(Transaction& t, Transaction& t_from, Btree& from);

 private:
  context_t get_context(Transaction& t) { return {*nm, t}; }
  btree_future<Ref<Node>> get_root(Transaction& t);

  NodeExtentManagerURef nm;
  RootNodeTrackerURef root_tracker;

  friend class DummyChildPool;
};

class tree_cursor_t;
class Btree::Cursor {
 public:
  Cursor(const Cursor&);
  Cursor(Cursor&&) noexcept;
  Cursor& operator=(const Cursor&);
  Cursor& operator=(Cursor&&);
  ~Cursor();

  bool is_end() const;
  // XXX: return key_view_t to avoid unecessary ghobject_t constructions
  ghobject_t get_ghobj() const;
  const onode_t* value() const;
  bool operator==(const Cursor& x) const;
  bool operator!=(const Cursor& x) const { return !(*this == x); }
  Cursor& operator++();
  Cursor operator++(int);

 private:
  Cursor(Btree*, Ref<tree_cursor_t>);
  Cursor(Btree*);

  static Cursor make_end(Btree*);

  Btree* p_tree;
  Ref<tree_cursor_t> p_cursor;

  friend class Btree;
};

}
