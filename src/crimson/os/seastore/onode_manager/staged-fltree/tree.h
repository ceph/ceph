// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "common/hobject.h"
#include "crimson/common/type_helpers.h"

#include "fwd.h"
#include "node.h"
#include "node_extent_manager.h"
#include "stages/key_layout.h"
#include "super.h"
#include "value.h"

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
class tree_cursor_t;

template <typename ValueImpl>
class Btree {
 public:
  using btree_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange,
    crimson::ct_error::eagain>;
  template <class ValueT=void>
  using btree_future = btree_ertr::future<ValueT>;

  Btree(NodeExtentManagerURef&& _nm)
    : nm{std::move(_nm)},
      root_tracker{RootNodeTracker::create(nm->is_read_isolated())} {}
  ~Btree() { assert(root_tracker->is_clean()); }

  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree& operator=(const Btree&) = delete;
  Btree& operator=(Btree&&) = delete;

  btree_future<> mkfs(Transaction& t) {
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
      if (p_cursor) {
        assert(!p_cursor->is_end());
        return false;
      } else {
        return true;
      }
    }

    // XXX: return key_view_t to avoid unecessary ghobject_t constructions
    ghobject_t get_ghobj() const {
      assert(!is_end());
      return p_cursor->get_key_view(
          p_tree->value_builder.get_header_magic()).to_ghobj();
    }

    ValueImpl value() {
      assert(!is_end());
      return p_tree->value_builder.build_value(
          *p_tree->nm, p_tree->value_builder, p_cursor);
    }

    bool operator>(const Cursor& o) const { return (int)compare_to(o) > 0; }
    bool operator>=(const Cursor& o) const { return (int)compare_to(o) >= 0; }
    bool operator<(const Cursor& o) const { return (int)compare_to(o) < 0; }
    bool operator<=(const Cursor& o) const { return (int)compare_to(o) <= 0; }
    bool operator==(const Cursor& o) const { return (int)compare_to(o) == 0; }
    bool operator!=(const Cursor& o) const { return (int)compare_to(o) != 0; }

    btree_future<Cursor> get_next(Transaction& t) {
      assert(!is_end());
      auto this_obj = *this;
      return p_cursor->get_next(p_tree->get_context(t)
      ).safe_then([this_obj] (Ref<tree_cursor_t> next_cursor) {
        next_cursor->assert_next_to(
            *this_obj.p_cursor, this_obj.p_tree->value_builder.get_header_magic());
        auto ret = Cursor{this_obj.p_tree, next_cursor};
        assert(this_obj < ret);
        return ret;
      });
    }

   private:
    Cursor(Btree* p_tree, Ref<tree_cursor_t> _p_cursor) : p_tree(p_tree) {
      if (_p_cursor->is_end()) {
        // no need to hold the leaf node
      } else {
        p_cursor = _p_cursor;
      }
    }
    Cursor(Btree* p_tree) : p_tree{p_tree} {}

    MatchKindCMP compare_to(const Cursor& o) const {
      assert(p_tree == o.p_tree);
      if (p_cursor && o.p_cursor) {
        return p_cursor->compare_to(
            *o.p_cursor, p_tree->value_builder.get_header_magic());
      } else if (!p_cursor && !o.p_cursor) {
        return MatchKindCMP::EQ;
      } else if (!p_cursor) {
        return MatchKindCMP::GT;
      } else { // !o.p_cursor
        return MatchKindCMP::LT;
      }
    }

    static Cursor make_end(Btree* p_tree) {
      return {p_tree};
    }

    Btree* p_tree;
    Ref<tree_cursor_t> p_cursor;

    friend class Btree;
  };

  /*
   * lookup
   */

  btree_future<Cursor> begin(Transaction& t) {
    return get_root(t).safe_then([this, &t](auto root) {
      return root->lookup_smallest(get_context(t));
    }).safe_then([this](auto cursor) {
      return Cursor{this, cursor};
    });
  }

  btree_future<Cursor> last(Transaction& t) {
    return get_root(t).safe_then([this, &t](auto root) {
      return root->lookup_largest(get_context(t));
    }).safe_then([this](auto cursor) {
      return Cursor(this, cursor);
    });
  }

  Cursor end() {
    return Cursor::make_end(this);
  }

  btree_future<bool> contains(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      full_key_t<KeyT::HOBJ>(obj),
      [this, &t](auto& key) -> btree_future<bool> {
        return get_root(t).safe_then([this, &t, &key](auto root) {
          // TODO: improve lower_bound()
          return root->lower_bound(get_context(t), key);
        }).safe_then([](auto result) {
          return MatchKindBS::EQ == result.match();
        });
      }
    );
  }

  btree_future<Cursor> find(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      full_key_t<KeyT::HOBJ>(obj),
      [this, &t](auto& key) -> btree_future<Cursor> {
        return get_root(t).safe_then([this, &t, &key](auto root) {
          // TODO: improve lower_bound()
          return root->lower_bound(get_context(t), key);
        }).safe_then([this](auto result) {
          if (result.match() == MatchKindBS::EQ) {
            return Cursor(this, result.p_cursor);
          } else {
            return Cursor::make_end(this);
          }
        });
      }
    );
  }

  btree_future<Cursor> lower_bound(Transaction& t, const ghobject_t& obj) {
    return seastar::do_with(
      full_key_t<KeyT::HOBJ>(obj),
      [this, &t](auto& key) -> btree_future<Cursor> {
        return get_root(t).safe_then([this, &t, &key](auto root) {
          return root->lower_bound(get_context(t), key);
        }).safe_then([this](auto result) {
          return Cursor(this, result.p_cursor);
        });
      }
    );
  }

  /*
   * modifiers
   */

  struct tree_value_config_t {
    value_size_t payload_size = 256;
  };
  btree_future<std::pair<Cursor, bool>>
  insert(Transaction& t, const ghobject_t& obj, tree_value_config_t _vconf) {
    value_config_t vconf{value_builder.get_header_magic(), _vconf.payload_size};
    return seastar::do_with(
      full_key_t<KeyT::HOBJ>(obj),
      [this, &t, vconf](auto& key) -> btree_future<std::pair<Cursor, bool>> {
        return get_root(t).safe_then([this, &t, &key, vconf](auto root) {
          return root->insert(get_context(t), key, vconf);
        }).safe_then([this](auto ret) {
          auto& [cursor, success] = ret;
          return std::make_pair(Cursor(this, cursor), success);
        });
      }
    );
  }

  btree_future<size_t> erase(Transaction& t, const ghobject_t& obj) {
    // TODO
    return btree_ertr::make_ready_future<size_t>(0u);
  }

  btree_future<Cursor> erase(Transaction &t, Cursor& pos) {
    // TODO
    return btree_ertr::make_ready_future<Cursor>(
        Cursor::make_end(this));
  }

  btree_future<Cursor> erase(Transaction &t, Cursor& first, Cursor& last) {
    // TODO
    return btree_ertr::make_ready_future<Cursor>(
        Cursor::make_end(this));
  }

  btree_future<Cursor> erase(Transaction &t, Value &value) {
    // TODO
    return btree_ertr::make_ready_future<Cursor>(
        Cursor::make_end(this));
  }

  /*
   * stats
   */

  btree_future<size_t> height(Transaction& t) {
    return get_root(t).safe_then([](auto root) {
      return size_t(root->level() + 1);
    });
  }

  btree_future<tree_stats_t> get_stats_slow(Transaction& t) {
    return get_root(t).safe_then([this, &t](auto root) {
      unsigned height = root->level() + 1;
      return root->get_tree_stats(get_context(t)
      ).safe_then([height](auto stats) {
        stats.height = height;
        return btree_ertr::make_ready_future<tree_stats_t>(stats);
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

  btree_future<> test_clone_from(
      Transaction& t, Transaction& t_from, Btree& from) {
    // Note: assume the tree to clone is tracked correctly in memory.
    // In some unit tests, parts of the tree are stubbed out that they
    // should not be loaded from NodeExtentManager.
    return from.get_root(t_from
    ).safe_then([this, &t](auto root_from) {
      return root_from->test_clone_root(get_context(t), *root_tracker);
    });
  }

 private:
  context_t get_context(Transaction& t) {
    return {*nm, value_builder, t};
  }

  btree_future<Ref<Node>> get_root(Transaction& t) {
    auto root = root_tracker->get_root(t);
    if (root) {
      return btree_ertr::make_ready_future<Ref<Node>>(root);
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
