// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "common/hobject.h"
#include "crimson/common/type_helpers.h"
#include "crimson/os/seastore/logging.h"

#include "fwd.h"
#include "node.h"
#include "node_extent_manager.h"
#include "stages/key_layout.h"
#include "super.h"
#include "value.h"

/**
 * tree.h
 *
 * A special-purpose and b-tree-based implementation that:
 * - Fulfills requirements of OnodeManager to index ordered onode key-values;
 * - Runs above seastore block and transaction layer;
 * - Specially optimized for onode key structures and seastore
 *   delta/transaction semantics;
 *
 * Note: Cursor/Value are transactional, they cannot be used outside the scope
 * of the according transaction, or the behavior is undefined.
 */

namespace crimson::os::seastore::onode {

class Node;
class tree_cursor_t;

template <typename ValueImpl>
class Btree {
 public:
  Btree(NodeExtentManagerURef&& _nm)
    : nm{std::move(_nm)},
      root_tracker{RootNodeTracker::create(nm->is_read_isolated())} {}
  ~Btree() { assert(root_tracker->is_clean()); }

  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree& operator=(const Btree&) = delete;
  Btree& operator=(Btree&&) = delete;

  eagain_ifuture<> mkfs(Transaction& t) {
    return Node::mkfs(get_context(t), *root_tracker);
  }

  class Cursor {
   public:
    Cursor(const Cursor&) = default;
    Cursor(Cursor&&) noexcept = default;
    Cursor& operator=(const Cursor&) = default;
    Cursor& operator=(Cursor&&) = default;
    ~Cursor() = default;

    bool is_end() const {
      if (p_cursor->is_tracked()) {
        return false;
      } else if (p_cursor->is_invalid()) {
        return true;
      } else {
        // we don't actually store end cursor because it will hold a reference
        // to an end leaf node and is not kept updated.
        assert(p_cursor->is_end());
        ceph_abort("impossible");
      }
    }

    /// Invalidate the Cursor before submitting transaction.
    void invalidate() {
      p_cursor.reset();
    }

    // XXX: return key_view_t to avoid unecessary ghobject_t constructions
    ghobject_t get_ghobj() const {
      assert(!is_end());
      auto view = p_cursor->get_key_view(
          p_tree->value_builder.get_header_magic());
      assert(view.nspace().size() <=
             p_tree->value_builder.get_max_ns_size());
      assert(view.oid().size() <=
             p_tree->value_builder.get_max_oid_size());
      return view.to_ghobj();
    }

    ValueImpl value() {
      assert(!is_end());
      return p_tree->value_builder.build_value(
        get_ghobj().hobj, *p_tree->nm, p_tree->value_builder, p_cursor);
    }

    bool operator==(const Cursor& o) const { return operator<=>(o) == 0; }

    eagain_ifuture<Cursor> get_next(Transaction& t) {
      assert(!is_end());
      auto this_obj = *this;
      return p_cursor->get_next(p_tree->get_context(t)
      ).si_then([this_obj] (Ref<tree_cursor_t> next_cursor) {
        next_cursor->assert_next_to(
            *this_obj.p_cursor, this_obj.p_tree->value_builder.get_header_magic());
        auto ret = Cursor{this_obj.p_tree, next_cursor};
        assert(this_obj < ret);
        return ret;
      });
    }

    template <bool FORCE_MERGE = false>
    eagain_ifuture<Cursor> erase(Transaction& t) {
      assert(!is_end());
      auto this_obj = *this;
      return p_cursor->erase<FORCE_MERGE>(p_tree->get_context(t), true
      ).si_then([this_obj, this] (Ref<tree_cursor_t> next_cursor) {
        assert(p_cursor->is_invalid());
        if (next_cursor) {
          assert(!next_cursor->is_end());
          return Cursor{p_tree, next_cursor};
        } else {
          return Cursor{p_tree};
        }
      });
    }

   private:
    Cursor(Btree* p_tree, Ref<tree_cursor_t> _p_cursor) : p_tree(p_tree) {
      if (_p_cursor->is_invalid()) {
        // we don't create Cursor from an invalid tree_cursor_t.
        ceph_abort("impossible");
      } else if (_p_cursor->is_end()) {
        // we don't actually store end cursor because it will hold a reference
        // to an end leaf node and is not kept updated.
      } else {
        assert(_p_cursor->is_tracked());
        p_cursor = _p_cursor;
      }
    }
    Cursor(Btree* p_tree) : p_tree{p_tree} {}

    std::strong_ordering operator<=>(const Cursor& o) const {
      assert(p_tree == o.p_tree);
      return p_cursor->compare_to(
          *o.p_cursor, p_tree->value_builder.get_header_magic());
    }

    static Cursor make_end(Btree* p_tree) {
      return {p_tree};
    }

    Btree* p_tree;
    Ref<tree_cursor_t> p_cursor = tree_cursor_t::get_invalid();

    friend class Btree;
  };

  /*
   * lookup
   */

  eagain_ifuture<Cursor> begin(Transaction& t) {
    return get_root(t).si_then([this, &t](auto root) {
      return root->lookup_smallest(get_context(t));
    }).si_then([this](auto cursor) {
      return Cursor{this, cursor};
    });
  }

  eagain_ifuture<Cursor> last(Transaction& t) {
    return get_root(t).si_then([this, &t](auto root) {
      return root->lookup_largest(get_context(t));
    }).si_then([this](auto cursor) {
      return Cursor(this, cursor);
    });
  }

  Cursor end() {
    return Cursor::make_end(this);
  }

  eagain_ifuture<bool> contains(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      key_hobj_t{obj},
      [this, &t](auto& key) -> eagain_ifuture<bool> {
        return get_root(t).si_then([this, &t, &key](auto root) {
          // TODO: improve lower_bound()
          return root->lower_bound(get_context(t), key);
        }).si_then([](auto result) {
          return MatchKindBS::EQ == result.match();
        });
      }
    );
  }

  eagain_ifuture<Cursor> find(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      key_hobj_t{obj},
      [this, &t](auto& key) -> eagain_ifuture<Cursor> {
        return get_root(t).si_then([this, &t, &key](auto root) {
          // TODO: improve lower_bound()
          return root->lower_bound(get_context(t), key);
        }).si_then([this](auto result) {
          if (result.match() == MatchKindBS::EQ) {
            return Cursor(this, result.p_cursor);
          } else {
            return Cursor::make_end(this);
          }
        });
      }
    );
  }

  /**
   * lower_bound
   *
   * Returns a Cursor pointing to the element that is equal to the key, or the
   * first element larger than the key, or the end Cursor if that element
   * doesn't exist.
   */
  eagain_ifuture<Cursor> lower_bound(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      key_hobj_t{obj},
      [this, &t](auto& key) -> eagain_ifuture<Cursor> {
        return get_root(t).si_then([this, &t, &key](auto root) {
          return root->lower_bound(get_context(t), key);
        }).si_then([this](auto result) {
          return Cursor(this, result.p_cursor);
        });
      }
    );
  }

  eagain_ifuture<Cursor> get_next(Transaction& t, Cursor& cursor) {
    return cursor.get_next(t);
  }

  /*
   * modifiers
   */

  struct tree_value_config_t {
    value_size_t payload_size = 256;
  };
  using insert_iertr = eagain_iertr::extend<
    crimson::ct_error::value_too_large>;
  insert_iertr::future<std::pair<Cursor, bool>>
  insert(Transaction& t, const ghobject_t& obj, tree_value_config_t _vconf) {
    LOG_PREFIX(OTree::insert);
    if (_vconf.payload_size > value_builder.get_max_value_payload_size()) {
      SUBERRORT(seastore_onode, "value payload size {} too large to insert {}",
                t, _vconf.payload_size, key_hobj_t{obj});
      return crimson::ct_error::value_too_large::make();
    }
    if (obj.hobj.nspace.size() > value_builder.get_max_ns_size()) {
      SUBERRORT(seastore_onode, "namespace size {} too large to insert {}",
                t, obj.hobj.nspace.size(), key_hobj_t{obj});
      return crimson::ct_error::value_too_large::make();
    }
    if (obj.hobj.oid.name.size() > value_builder.get_max_oid_size()) {
      SUBERRORT(seastore_onode, "oid size {} too large to insert {}",
                t, obj.hobj.oid.name.size(), key_hobj_t{obj});
      return crimson::ct_error::value_too_large::make();
    }
    value_config_t vconf{value_builder.get_header_magic(), _vconf.payload_size};
    return seastar::do_with(
      key_hobj_t{obj},
      [this, &t, vconf](auto& key) -> eagain_ifuture<std::pair<Cursor, bool>> {
        ceph_assert(key.is_valid());
        return get_root(t).si_then([this, &t, &key, vconf](auto root) {
          return root->insert(get_context(t), key, vconf, std::move(root));
        }).si_then([this](auto ret) {
          auto& [cursor, success] = ret;
          return std::make_pair(Cursor(this, cursor), success);
        });
      }
    );
  }

  eagain_ifuture<std::size_t> erase(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      key_hobj_t{obj},
      [this, &t](auto& key) -> eagain_ifuture<std::size_t> {
        return get_root(t).si_then([this, &t, &key](auto root) {
          return root->erase(get_context(t), key, std::move(root));
        });
      }
    );
  }

  eagain_ifuture<Cursor> erase(Transaction& t, Cursor& pos) {
    return pos.erase(t);
  }

  eagain_ifuture<> erase(Transaction& t, Value& value) {
    assert(value.is_tracked());
    auto ref_cursor = value.p_cursor;
    return ref_cursor->erase(get_context(t), false
    ).si_then([ref_cursor] (auto next_cursor) {
      assert(ref_cursor->is_invalid());
      assert(!next_cursor);
    });
  }

  /*
   * stats
   */

  eagain_ifuture<size_t> height(Transaction& t) {
    return get_root(t).si_then([](auto root) {
      return size_t(root->level() + 1);
    });
  }

  eagain_ifuture<tree_stats_t> get_stats_slow(Transaction& t) {
    return get_root(t).si_then([this, &t](auto root) {
      unsigned height = root->level() + 1;
      return root->get_tree_stats(get_context(t)
      ).si_then([height](auto stats) {
        stats.height = height;
        return seastar::make_ready_future<tree_stats_t>(stats);
      });
    });
  }

  std::ostream& dump(Transaction& t, std::ostream& os) {
    auto root = root_tracker->get_root(t);
    if (root) {
      root->dump(os);
    } else {
      os << "empty tree!";
    }
    return os;
  }

  std::ostream& print(std::ostream& os) const {
    return os << "BTree-" << *nm;
  }

  /*
   * test_only
   */

  bool test_is_clean() const {
    return root_tracker->is_clean();
  }

  eagain_ifuture<> test_clone_from(
      Transaction& t, Transaction& t_from, Btree& from) {
    // Note: assume the tree to clone is tracked correctly in memory.
    // In some unit tests, parts of the tree are stubbed out that they
    // should not be loaded from NodeExtentManager.
    return from.get_root(t_from
    ).si_then([this, &t](auto root_from) {
      return root_from->test_clone_root(get_context(t), *root_tracker);
    });
  }

 private:
  context_t get_context(Transaction& t) {
    return {*nm, value_builder, t};
  }

  eagain_ifuture<Ref<Node>> get_root(Transaction& t) {
    auto root = root_tracker->get_root(t);
    if (root) {
      return seastar::make_ready_future<Ref<Node>>(root);
    } else {
      return Node::load_root(get_context(t), *root_tracker);
    }
  }

  NodeExtentManagerURef nm;
  const ValueBuilderImpl<ValueImpl> value_builder;
  RootNodeTrackerURef root_tracker;

  friend class DummyChildPool;
};

template <typename ValueImpl>
inline std::ostream& operator<<(std::ostream& os, const Btree<ValueImpl>& tree) {
  return tree.print(os);
}

}
