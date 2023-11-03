// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <boost/container/static_vector.hpp>
#include <sys/mman.h>
#include <memory>
#include <string.h>

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/root_block.h"

#define RESERVATION_PTR reinterpret_cast<ChildableCachedExtent*>(0x1)

namespace crimson::os::seastore::lba_manager::btree {
struct lba_map_val_t;
}

namespace crimson::os::seastore {

bool is_valid_child_ptr(ChildableCachedExtent* child);

template <typename T>
phy_tree_root_t& get_phy_tree_root(root_t& r);

using get_child_iertr =
  ::crimson::interruptible::interruptible_errorator<
    typename trans_intr::condition,
    get_child_ertr>;
using get_phy_tree_root_node_ret =
  std::pair<bool, get_child_iertr::future<CachedExtentRef>>;

template <typename T, typename key_t>
const get_phy_tree_root_node_ret get_phy_tree_root_node(
  const RootBlockRef &root_block,
  op_context_t<key_t> c);

template <typename ROOT_T>
void link_phy_tree_root_node(RootBlockRef &root_block, ROOT_T* root_node);

template <typename T>
void unlink_phy_tree_root_node(RootBlockRef &root_block);

template <typename T>
Transaction::tree_stats_t& get_tree_stats(Transaction &t);

template <
  typename node_key_t,
  typename node_val_t,
  typename internal_node_t,
  typename leaf_node_t,
  typename pin_t,
  size_t node_size,
  bool leaf_has_children>
class FixedKVBtree {
  static constexpr size_t MAX_DEPTH = 16;
  using self_type = FixedKVBtree<
    node_key_t,
    node_val_t,
    internal_node_t,
    leaf_node_t,
    pin_t,
    node_size,
    leaf_has_children>;
public:
  using InternalNodeRef = TCachedExtentRef<internal_node_t>;
  using LeafNodeRef = TCachedExtentRef<leaf_node_t>;

  using base_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using base_iertr = trans_iertr<base_ertr>;

  class iterator;
  using iterator_fut = base_iertr::future<iterator>;

  using mapped_space_visitor_t = std::function<
    void(paddr_t, node_key_t, extent_len_t, depth_t, extent_types_t, iterator&)>;

  class iterator {
  public:
    iterator(const iterator &rhs) noexcept :
      internal(rhs.internal), leaf(rhs.leaf) {}
    iterator(iterator &&rhs) noexcept :
      internal(std::move(rhs.internal)), leaf(std::move(rhs.leaf)) {}

    iterator &operator=(const iterator &) = default;
    iterator &operator=(iterator &&) = default;

    iterator_fut next(
      op_context_t<node_key_t> c,
      mapped_space_visitor_t *visitor=nullptr) const
    {
      assert_valid();
      assert(!is_end());

      auto ret = *this;
      ret.leaf.pos++;
      if (ret.at_boundary()) {
        return seastar::do_with(
          ret,
          [c, visitor](auto &ret) mutable {
            return ret.handle_boundary(
              c, visitor
            ).si_then([&ret] {
              return std::move(ret);
            });
          });
      } else {
        return iterator_fut(
          interruptible::ready_future_marker{},
          ret);
      }

    }

    iterator_fut prev(op_context_t<node_key_t> c) const
    {
      assert_valid();
      assert(!is_begin());

      auto ret = *this;

      if (ret.leaf.pos > 0) {
        ret.leaf.pos--;
        return iterator_fut(
          interruptible::ready_future_marker{},
          ret);
      }

      depth_t depth_with_space = 2;
      for (; depth_with_space <= get_depth(); ++depth_with_space) {
        if (ret.get_internal(depth_with_space).pos > 0) {
          break;
        }
      }

      assert(depth_with_space <= ret.get_depth()); // must not be begin()
      return seastar::do_with(
        std::move(ret),
        [](const internal_node_t &internal) { return --internal.end(); },
        [](const leaf_node_t &leaf) { return --leaf.end(); },
        [c, depth_with_space](auto &ret, auto &li, auto &ll) {
          for (depth_t depth = 2; depth < depth_with_space; ++depth) {
            ret.get_internal(depth).reset();
          }
          ret.leaf.reset();
          ret.get_internal(depth_with_space).pos--;
          // note, cannot result in at_boundary() by construction
          return lookup_depth_range(
            c, ret, depth_with_space - 1, 0, li, ll, nullptr
          ).si_then([&ret] {
            assert(!ret.at_boundary());
            return std::move(ret);
          });
        });
    }

    void assert_valid() const {
      assert(leaf.node);
      assert(leaf.pos <= leaf.node->get_size());

      for (auto &i: internal) {
	(void)i;
	assert(i.node);
	assert(i.pos < i.node->get_size());
      }
    }

    depth_t get_depth() const {
      return internal.size() + 1;
    }

    auto &get_internal(depth_t depth) {
      assert(depth > 1);
      assert((depth - 2) < internal.size());
      return internal[depth - 2];
    }

    const auto &get_internal(depth_t depth) const {
      assert(depth > 1);
      assert((depth - 2) < internal.size());
      return internal[depth - 2];
    }

    node_key_t get_key() const {
      assert(!is_end());
      return leaf.node->iter_idx(leaf.pos).get_key();
    }
    node_val_t get_val() const {
      assert(!is_end());
      auto ret = leaf.node->iter_idx(leaf.pos).get_val();
      if constexpr (
        std::is_same_v<crimson::os::seastore::lba_manager::btree::lba_map_val_t,
                       node_val_t>) {
        if (ret.pladdr.is_paddr()) {
          ret.pladdr = ret.pladdr.get_paddr().maybe_relative_to(
            leaf.node->get_paddr());
        }
      }
      return ret;
    }

    bool is_end() const {
      // external methods may only resolve at a boundary if at end
      return at_boundary();
    }

    bool is_begin() const {
      for (auto &i: internal) {
	if (i.pos != 0)
	  return false;
      }
      return leaf.pos == 0;
    }

    std::unique_ptr<pin_t> get_pin(op_context_t<node_key_t> ctx) const {
      assert(!is_end());
      auto val = get_val();
      auto key = get_key();
      return std::make_unique<pin_t>(
        ctx,
	leaf.node,
        leaf.pos,
	val,
	fixed_kv_node_meta_t<node_key_t>{ key, key + val.len, 0 });
    }

    typename leaf_node_t::Ref get_leaf_node() {
      return leaf.node;
    }

    uint16_t get_leaf_pos() {
      return leaf.pos;
    }
  private:
    iterator() noexcept {}
    iterator(depth_t depth) noexcept : internal(depth - 1) {}

    friend class FixedKVBtree;
    static constexpr uint16_t INVALID = std::numeric_limits<uint16_t>::max();
    template <typename NodeType>
    struct node_position_t {
      typename NodeType::Ref node;
      uint16_t pos = INVALID;

      node_position_t() = default;
      node_position_t(
        typename NodeType::Ref node,
        uint16_t pos)
        : node(node), pos(pos) {}

      void reset() {
	*this = node_position_t{};
      }

      auto get_iter() {
	assert(pos != INVALID);
	assert(pos < node->get_size());
	return node->iter_idx(pos);
      }
    };
    boost::container::static_vector<
      node_position_t<internal_node_t>, MAX_DEPTH> internal;
    node_position_t<leaf_node_t> leaf;

    bool at_boundary() const {
      assert(leaf.pos <= leaf.node->get_size());
      return leaf.pos == leaf.node->get_size();
    }

    using handle_boundary_ertr = base_iertr;
    using handle_boundary_ret = handle_boundary_ertr::future<>;
    handle_boundary_ret handle_boundary(
      op_context_t<node_key_t> c,
      mapped_space_visitor_t *visitor)
    {
      assert(at_boundary());
      depth_t depth_with_space = 2;
      for (; depth_with_space <= get_depth(); ++depth_with_space) {
        if ((get_internal(depth_with_space).pos + 1) <
            get_internal(depth_with_space).node->get_size()) {
          break;
        }
      }

      if (depth_with_space <= get_depth()) {
        return seastar::do_with(
          [](const internal_node_t &internal) { return internal.begin(); },
          [](const leaf_node_t &leaf) { return leaf.begin(); },
          [this, c, depth_with_space, visitor](auto &li, auto &ll) {
            for (depth_t depth = 2; depth < depth_with_space; ++depth) {
              get_internal(depth).reset();
            }
            leaf.reset();
            get_internal(depth_with_space).pos++;
            // note, cannot result in at_boundary() by construction
            return lookup_depth_range(
              c, *this, depth_with_space - 1, 0, li, ll, visitor
            );
          });
      } else {
        // end
        return seastar::now();
      }
    }

    depth_t check_split() const {
      if (!leaf.node->at_max_capacity()) {
	return 0;
      }
      for (depth_t split_from = 1; split_from < get_depth(); ++split_from) {
	if (!get_internal(split_from + 1).node->at_max_capacity())
	  return split_from;
      }
      return get_depth();
    }

    depth_t check_merge() const {
      if (!leaf.node->below_min_capacity()) {
	return 0;
      }
      for (depth_t merge_from = 1; merge_from < get_depth(); ++merge_from) {
	if (!get_internal(merge_from + 1).node->below_min_capacity())
	  return merge_from;
      }
      return get_depth();
    }
  };

  FixedKVBtree(RootBlockRef &root_block) : root_block(root_block) {}

  auto& get_root() {
    return get_phy_tree_root<self_type>(root_block->get_root());
  }

  auto& get_root() const {
    return get_phy_tree_root<self_type>(root_block->get_root());
  }

  template <typename T>
  void set_root_node(const TCachedExtentRef<T> &root_node) {
    static_assert(std::is_base_of_v<typename internal_node_t::base_t, T>);
    link_phy_tree_root_node(root_block, root_node.get());
  }

  auto get_root_node(op_context_t<node_key_t> c) const {
    return get_phy_tree_root_node<self_type>(root_block, c);
  }

  /// mkfs
  using mkfs_ret = phy_tree_root_t;
  static mkfs_ret mkfs(RootBlockRef &root_block, op_context_t<node_key_t> c) {
    assert(root_block->is_mutation_pending());
    auto root_leaf = c.cache.template alloc_new_extent<leaf_node_t>(
      c.trans,
      node_size,
      placement_hint_t::HOT,
      INIT_GENERATION);
    root_leaf->set_size(0);
    fixed_kv_node_meta_t<node_key_t> meta{min_max_t<node_key_t>::min, min_max_t<node_key_t>::max, 1};
    root_leaf->set_meta(meta);
    root_leaf->range = meta;
    get_tree_stats<self_type>(c.trans).depth = 1u;
    get_tree_stats<self_type>(c.trans).extents_num_delta++;
    link_phy_tree_root_node(root_block, root_leaf.get());
    return phy_tree_root_t{root_leaf->get_paddr(), 1u};
  }

  /**
   * lower_bound
   *
   * @param c [in] context
   * @param addr [in] ddr
   * @return least iterator >= key
   */
  iterator_fut lower_bound(
    op_context_t<node_key_t> c,
    node_key_t addr,
    mapped_space_visitor_t *visitor=nullptr,
    depth_t min_depth = 1) const
  {
    LOG_PREFIX(FixedKVBtree::lower_bound);
    return lookup(
      c,
      [addr](const internal_node_t &internal) {
        assert(internal.get_size() > 0);
        auto iter = internal.upper_bound(addr);
        assert(iter != internal.begin());
        --iter;
        return iter;
      },
      [FNAME, c, addr](const leaf_node_t &leaf) {
        auto ret = leaf.lower_bound(addr);
        SUBTRACET(
          seastore_fixedkv_tree,
          "leaf addr {}, got ret offset {}, size {}, end {}",
          c.trans,
          addr,
          ret.get_offset(),
          leaf.get_size(),
          ret == leaf.end());
        return ret;
      },
      min_depth,
      visitor
    ).si_then([FNAME, c, min_depth](auto &&ret) {
      SUBTRACET(
        seastore_fixedkv_tree,
        "ret.leaf.pos {}",
        c.trans,
        ret.leaf.pos);
      if (min_depth == 1) {
        ret.assert_valid();
      }
      return std::move(ret);
    });
  }


  /**
   * upper_bound
   *
   * @param c [in] context
   * @param addr [in] ddr
   * @return least iterator > key
   */
  iterator_fut upper_bound(
    op_context_t<node_key_t> c,
    node_key_t addr
  ) const {
    return lower_bound(
      c, addr
    ).si_then([c, addr](auto iter) {
      if (!iter.is_end() && iter.get_key() == addr) {
	return iter.next(c);
      } else {
	return iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      }
    });
  }

  /**
   * upper_bound_right
   *
   * @param c [in] context
   * @param addr [in] addr
   * @return least iterator i s.t. i.get_key() + i.get_val().len > key
   */
  iterator_fut upper_bound_right(
    op_context_t<node_key_t> c,
    node_key_t addr) const
  {
    return lower_bound(
      c, addr
    ).si_then([c, addr](auto iter) {
      if (iter.is_begin()) {
	return iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      } else {
	return iter.prev(
	  c
	).si_then([iter, addr](auto prev) {
	  if ((prev.get_key() + prev.get_val().len) > addr) {
	    return iterator_fut(
	      interruptible::ready_future_marker{},
	      prev);
	  } else {
	    return iterator_fut(
	      interruptible::ready_future_marker{},
	      iter);
	  }
	});
      }
    });
  }

  iterator_fut begin(op_context_t<node_key_t> c) const {
    return lower_bound(c, 0);
  }
  iterator_fut end(op_context_t<node_key_t> c) const {
    return upper_bound(c, min_max_t<node_key_t>::max);
  }

  template <typename child_node_t, typename node_t, bool lhc = leaf_has_children,
           typename std::enable_if<lhc, int>::type = 0>
  void check_node(
    op_context_t<node_key_t> c,
    TCachedExtentRef<node_t> node)
  {
    assert(leaf_has_children);
    for (auto i : *node) {
      CachedExtentRef child_node;
      Transaction::get_extent_ret ret;

      if constexpr (std::is_base_of_v<typename internal_node_t::base_t, child_node_t>) {
        ret = c.trans.get_extent(
          i->get_val().maybe_relative_to(node->get_paddr()),
          &child_node);
      } else {
        if (i->get_val().pladdr.is_laddr()) {
          continue;
        }
        ret = c.trans.get_extent(
          i->get_val().pladdr.get_paddr().maybe_relative_to(node->get_paddr()),
          &child_node);
      }
      if (ret == Transaction::get_extent_ret::PRESENT) {
        if (child_node->is_stable_written()) {
          assert(child_node->is_valid());
          auto cnode = child_node->template cast<child_node_t>();
          assert(cnode->has_parent_tracker());
          if (node->is_pending()) {
            auto &n = node->get_stable_for_key(i->get_key());
            assert(cnode->get_parent_node().get() == &n);
            auto pos = n.lower_bound_offset(i->get_key());
            assert(pos < n.get_node_size());
            assert(n.children[pos] == cnode.get());
          } else {
            assert(cnode->get_parent_node().get() == node.get());
            assert(node->children[i->get_offset()] == cnode.get());
          }
        } else if (child_node->is_pending()) {
          if (child_node->is_mutation_pending()) {
            auto &prior = (child_node_t &)*child_node->prior_instance;
            assert(prior.is_valid());
            assert(prior.is_parent_valid());
            if (node->is_mutation_pending()) {
              auto &n = node->get_stable_for_key(i->get_key());
              assert(prior.get_parent_node().get() == &n);
              auto pos = n.lower_bound_offset(i->get_key());
              assert(pos < n.get_node_size());
              assert(n.children[pos] == &prior);
            } else {
              assert(prior.get_parent_node().get() == node.get());
              assert(node->children[i->get_offset()] == &prior);
            }
          } else {
            auto cnode = child_node->template cast<child_node_t>();
            auto pos = node->find(i->get_key()).get_offset();
            auto child = node->children[pos];
            assert(child);
            assert(child == cnode.get());
            assert(cnode->is_parent_valid());
          }
        } else {
          ceph_assert(!child_node->is_valid());
          ceph_abort("impossible");
        }
      } else if (ret == Transaction::get_extent_ret::ABSENT) {
        ChildableCachedExtent* child = nullptr;
        if (node->is_pending()) {
          auto &n = node->get_stable_for_key(i->get_key());
          auto pos = n.lower_bound_offset(i->get_key());
          assert(pos < n.get_node_size());
          child = n.children[pos];
          if (is_valid_child_ptr(child)) {
            auto c = (child_node_t*)child;
            assert(c->has_parent_tracker());
            assert(c->get_parent_node().get() == &n);
          }
        } else {
          child = node->children[i->get_offset()];
          if (is_valid_child_ptr(child)) {
            auto c = (child_node_t*)child;
            assert(c->has_parent_tracker());
            assert(c->get_parent_node().get() == node.get());
          }
        }

        if (!is_valid_child_ptr(child)) {
          if constexpr (
            std::is_base_of_v<typename internal_node_t::base_t, child_node_t>)
          {
            assert(!c.cache.query_cache(i->get_val(), nullptr));
          } else {
            if constexpr (leaf_has_children) {
              assert(i->get_val().pladdr.is_paddr()
                ? (bool)!c.cache.query_cache(
                    i->get_val().pladdr.get_paddr(), nullptr)
                : true);
            }
          }
        }
      } else {
        ceph_abort("impossible");
      }
    }
  }

  using check_child_trackers_ret = base_iertr::future<>;
  template <bool lhc = leaf_has_children,
            typename std::enable_if<lhc, int>::type = 0>
  check_child_trackers_ret check_child_trackers(
    op_context_t<node_key_t> c) {
    mapped_space_visitor_t checker = [c, this](
      paddr_t,
      node_key_t,
      extent_len_t,
      depth_t depth,
      extent_types_t,
      iterator& iter) {
      if constexpr (!leaf_has_children) {
        if (depth == 1) {
          return seastar::now();
        }
      }
      if (depth > 1) {
        auto &node = iter.get_internal(depth).node;
        assert(node->is_valid());
        check_node<typename internal_node_t::base_t>(c, node);
      } else {
        assert(depth == 1);
        auto &node = iter.leaf.node;
        assert(node->is_valid());
        check_node<LogicalCachedExtent>(c, node);
      }
      return seastar::now();
    };

    return seastar::do_with(
      std::move(checker),
      [this, c](auto &checker) {
      return iterate_repeat(
        c,
        lower_bound(
          c,
          min_max_t<node_key_t>::min,
          &checker),
        [](auto &pos) {
          if (pos.is_end()) {
            return base_iertr::make_ready_future<
              seastar::stop_iteration>(
                seastar::stop_iteration::yes);
          }
          return base_iertr::make_ready_future<
            seastar::stop_iteration>(
              seastar::stop_iteration::no);
        },
        &checker);
    });
  }

  using iterate_repeat_ret_inner = base_iertr::future<
    seastar::stop_iteration>;
  template <typename F>
  static base_iertr::future<> iterate_repeat(
    op_context_t<node_key_t> c,
    iterator_fut &&iter_fut,
    F &&f,
    mapped_space_visitor_t *visitor=nullptr) {
    return std::move(
      iter_fut
    ).si_then([c, visitor, f=std::forward<F>(f)](auto iter) {
      return seastar::do_with(
	iter,
	std::move(f),
	[c, visitor](auto &pos, auto &f) {
	  return trans_intr::repeat(
	    [c, visitor, &f, &pos] {
	      return f(
		pos
	      ).si_then([c, visitor, &pos](auto done) {
		if (done == seastar::stop_iteration::yes) {
		  return iterate_repeat_ret_inner(
		    interruptible::ready_future_marker{},
		    seastar::stop_iteration::yes);
		} else {
		  ceph_assert(!pos.is_end());
		  return pos.next(
		    c, visitor
		  ).si_then([&pos](auto next) {
		    pos = next;
		    return iterate_repeat_ret_inner(
		      interruptible::ready_future_marker{},
		      seastar::stop_iteration::no);
		  });
		}
	      });
	    });
	});
    });
  }

  /**
   * insert
   *
   * Inserts val at laddr with iter as a hint.  If element at laddr already
   * exists returns iterator to that element unchanged and returns false.
   *
   * Invalidates all outstanding iterators for this tree on this transaction.
   *
   * @param c [in] op context
   * @param iter [in] hint, insertion constant if immediately prior to iter
   * @param laddr [in] addr at which to insert
   * @param val [in] val to insert
   * @return pair<iter, bool> where iter points to element at addr, bool true
   *         iff element at laddr did not exist.
   */
  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<std::pair<iterator, bool>>;
  insert_ret insert(
    op_context_t<node_key_t> c,
    iterator iter,
    node_key_t laddr,
    node_val_t val,
    LogicalCachedExtent* nextent
  ) {
    LOG_PREFIX(FixedKVBtree::insert);
    SUBTRACET(
      seastore_fixedkv_tree,
      "inserting laddr {} at iter {}",
      c.trans,
      laddr,
      iter.is_end() ? min_max_t<node_key_t>::max : iter.get_key());
    return seastar::do_with(
      iter,
      [this, c, laddr, val, nextent](auto &ret) {
        return find_insertion(
          c, laddr, ret
        ).si_then([this, c, laddr, val, &ret, nextent] {
          if (!ret.at_boundary() && ret.get_key() == laddr) {
            return insert_ret(
              interruptible::ready_future_marker{},
              std::make_pair(ret, false));
          } else {
            ++(get_tree_stats<self_type>(c.trans).num_inserts);
            return handle_split(
              c, ret
            ).si_then([c, laddr, val, &ret, nextent] {
              if (!ret.leaf.node->is_mutable()) {
                CachedExtentRef mut = c.cache.duplicate_for_write(
                  c.trans, ret.leaf.node
                );
                ret.leaf.node = mut->cast<leaf_node_t>();
              }
              auto iter = typename leaf_node_t::const_iterator(
                  ret.leaf.node.get(), ret.leaf.pos);
              assert(iter == ret.leaf.node->lower_bound(laddr));
              assert(iter == ret.leaf.node->end() || iter->get_key() > laddr);
              assert(laddr >= ret.leaf.node->get_meta().begin &&
                     laddr < ret.leaf.node->get_meta().end);
              ret.leaf.node->insert(iter, laddr, val, nextent);
              return insert_ret(
                interruptible::ready_future_marker{},
                std::make_pair(ret, true));
            });
          }
        });
      });
  }

  insert_ret insert(
    op_context_t<node_key_t> c,
    node_key_t laddr,
    node_val_t val,
    LogicalCachedExtent* nextent) {
    return lower_bound(
      c, laddr
    ).si_then([this, c, laddr, val, nextent](auto iter) {
      return this->insert(c, iter, laddr, val, nextent);
    });
  }

  /**
   * update
   *
   * Invalidates all outstanding iterators for this tree on this transaction.
   *
   * @param c [in] op context
   * @param iter [in] iterator to element to update, must not be end
   * @param val [in] val with which to update
   * @return iterator to newly updated element
   */
  using update_iertr = base_iertr;
  using update_ret = update_iertr::future<iterator>;
  update_ret update(
    op_context_t<node_key_t> c,
    iterator iter,
    node_val_t val,
    LogicalCachedExtent* nextent)
  {
    LOG_PREFIX(FixedKVBtree::update);
    SUBTRACET(
      seastore_fixedkv_tree,
      "update element at {}",
      c.trans,
      iter.is_end() ? min_max_t<node_key_t>::max : iter.get_key());
    if (!iter.leaf.node->is_mutable()) {
      CachedExtentRef mut = c.cache.duplicate_for_write(
        c.trans, iter.leaf.node
      );
      iter.leaf.node = mut->cast<leaf_node_t>();
    }
    ++(get_tree_stats<self_type>(c.trans).num_updates);
    iter.leaf.node->update(
      iter.leaf.node->iter_idx(iter.leaf.pos),
      val,
      nextent);
    return update_ret(
      interruptible::ready_future_marker{},
      iter);
  }


  /**
   * remove
   *
   * Invalidates all outstanding iterators for this tree on this transaction.
   *
   * @param c [in] op context
   * @param iter [in] iterator to element to remove, must not be end
   */
  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<>;
  remove_ret remove(
    op_context_t<node_key_t> c,
    iterator iter)
  {
    LOG_PREFIX(FixedKVBtree::remove);
    SUBTRACET(
      seastore_fixedkv_tree,
      "remove element at {}",
      c.trans,
      iter.is_end() ? min_max_t<node_key_t>::max : iter.get_key());
    assert(!iter.is_end());
    ++(get_tree_stats<self_type>(c.trans).num_erases);
    return seastar::do_with(
      iter,
      [this, c](auto &ret) {
        if (!ret.leaf.node->is_mutable()) {
          CachedExtentRef mut = c.cache.duplicate_for_write(
            c.trans, ret.leaf.node
          );
          ret.leaf.node = mut->cast<leaf_node_t>();
        }
        ret.leaf.node->remove(
          ret.leaf.node->iter_idx(ret.leaf.pos));

        return handle_merge(
          c, ret
        );
      });
  }
    
  /**
   * init_cached_extent
   *
   * Checks whether e is live (reachable from fixed kv tree) and drops or initializes
   * accordingly. 
   *
   * Returns if e is live.
   */
  using init_cached_extent_iertr = base_iertr;
  using init_cached_extent_ret = init_cached_extent_iertr::future<bool>;
  init_cached_extent_ret init_cached_extent(
    op_context_t<node_key_t> c,
    CachedExtentRef e)
  {
    assert(!e->is_logical());
    LOG_PREFIX(FixedKVTree::init_cached_extent);
    SUBTRACET(seastore_fixedkv_tree, "extent {}", c.trans, *e);
    if (e->get_type() == internal_node_t::TYPE) {
      auto eint = e->cast<internal_node_t>();
      return lower_bound(
        c, eint->get_node_meta().begin
      ).si_then([e, c, eint](auto iter) {
        // Note, this check is valid even if iter.is_end()
        LOG_PREFIX(FixedKVTree::init_cached_extent);
        depth_t cand_depth = eint->get_node_meta().depth;
        if (cand_depth <= iter.get_depth() &&
            &*iter.get_internal(cand_depth).node == &*eint) {
          SUBTRACET(
            seastore_fixedkv_tree,
            "extent {} is live",
            c.trans,
            *eint);
          return true;
        } else {
          SUBTRACET(
            seastore_fixedkv_tree,
            "extent {} is not live",
            c.trans,
            *eint);
          return false;
        }
      });
    } else if (e->get_type() == leaf_node_t::TYPE) {
      auto eleaf = e->cast<leaf_node_t>();
      return lower_bound(
        c, eleaf->get_node_meta().begin
      ).si_then([c, e, eleaf](auto iter) {
        // Note, this check is valid even if iter.is_end()
        LOG_PREFIX(FixedKVTree::init_cached_extent);
        if (iter.leaf.node == &*eleaf) {
          SUBTRACET(
            seastore_fixedkv_tree,
            "extent {} is live",
            c.trans,
            *eleaf);
          return true;
        } else {
          SUBTRACET(
            seastore_fixedkv_tree,
            "extent {} is not live",
            c.trans,
            *eleaf);
          return false;
        }
      });
    } else {
      SUBTRACET(
        seastore_fixedkv_tree,
        "found other extent {} type {}",
        c.trans,
        *e,
        e->get_type());
      return init_cached_extent_ret(
        interruptible::ready_future_marker{},
        true);
    }
  }

  /// get_leaf_if_live: get leaf node at laddr/addr if still live
  using get_leaf_if_live_iertr = base_iertr;
  using get_leaf_if_live_ret = get_leaf_if_live_iertr::future<CachedExtentRef>;
  get_leaf_if_live_ret get_leaf_if_live(
    op_context_t<node_key_t> c,
    paddr_t addr,
    node_key_t laddr,
    extent_len_t len)
  {
    LOG_PREFIX(FixedKVBtree::get_leaf_if_live);
    return lower_bound(
      c, laddr
    ).si_then([FNAME, c, addr, laddr, len](auto iter) {
      if (iter.leaf.node->get_paddr() == addr) {
        SUBTRACET(
          seastore_fixedkv_tree,
          "extent laddr {} addr {}~{} found: {}",
          c.trans,
          laddr,
          addr,
          len,
          *iter.leaf.node);
        return CachedExtentRef(iter.leaf.node);
      } else {
        SUBTRACET(
          seastore_fixedkv_tree,
          "extent laddr {} addr {}~{} is not live, does not match node {}",
          c.trans,
          laddr,
          addr,
          len,
          *iter.leaf.node);
        return CachedExtentRef();
      }
    });
  }


  /// get_internal_if_live: get internal node at laddr/addr if still live
  using get_internal_if_live_iertr = base_iertr;
  using get_internal_if_live_ret = get_internal_if_live_iertr::future<CachedExtentRef>;
  get_internal_if_live_ret get_internal_if_live(
    op_context_t<node_key_t> c,
    paddr_t addr,
    node_key_t laddr,
    extent_len_t len)
  {
    LOG_PREFIX(FixedKVBtree::get_internal_if_live);
    return lower_bound(
      c, laddr
    ).si_then([FNAME, c, addr, laddr, len](auto iter) {
      for (depth_t d = 2; d <= iter.get_depth(); ++d) {
        CachedExtent &node = *iter.get_internal(d).node;
        auto internal_node = node.cast<internal_node_t>();
        if (internal_node->get_paddr() == addr) {
          SUBTRACET(
            seastore_fixedkv_tree,
            "extent laddr {} addr {}~{} found: {}",
            c.trans,
            laddr,
            addr,
            len,
            *internal_node);
          assert(internal_node->get_node_meta().begin == laddr);
          return CachedExtentRef(internal_node);
        }
      }
      SUBTRACET(
        seastore_fixedkv_tree,
        "extent laddr {} addr {}~{} is not live, no matching internal node",
        c.trans,
        laddr,
        addr,
        len);
      return CachedExtentRef();
    });
  }


  /**
   * rewrite_extent
   *
   * Rewrites a fresh copy of extent into transaction and updates internal
   * references.
   */
  using rewrite_extent_iertr = base_iertr;
  using rewrite_extent_ret = rewrite_extent_iertr::future<>;
  rewrite_extent_ret rewrite_extent(
    op_context_t<node_key_t> c,
    CachedExtentRef e) {
    LOG_PREFIX(FixedKVBtree::rewrite_extent);
    assert(is_lba_backref_node(e->get_type()));
    
    auto do_rewrite = [&](auto &fixed_kv_extent) {
      auto n_fixed_kv_extent = c.cache.template alloc_new_extent<
        std::remove_reference_t<decltype(fixed_kv_extent)>
        >(
        c.trans,
        fixed_kv_extent.get_length(),
        fixed_kv_extent.get_user_hint(),
        // get target rewrite generation
        fixed_kv_extent.get_rewrite_generation());
      fixed_kv_extent.get_bptr().copy_out(
        0,
        fixed_kv_extent.get_length(),
        n_fixed_kv_extent->get_bptr().c_str());
      n_fixed_kv_extent->set_modify_time(fixed_kv_extent.get_modify_time());
      n_fixed_kv_extent->range = n_fixed_kv_extent->get_node_meta();

      if (fixed_kv_extent.get_type() == internal_node_t::TYPE ||
          leaf_node_t::do_has_children) {
        if (!fixed_kv_extent.is_pending()) {
          n_fixed_kv_extent->copy_sources.emplace(&fixed_kv_extent);
          n_fixed_kv_extent->prior_instance = &fixed_kv_extent;
        } else {
          ceph_assert(fixed_kv_extent.is_mutation_pending());
          n_fixed_kv_extent->copy_sources.emplace(
            (typename internal_node_t::base_t*
             )fixed_kv_extent.get_prior_instance().get());
          n_fixed_kv_extent->children = std::move(fixed_kv_extent.children);
          n_fixed_kv_extent->prior_instance = fixed_kv_extent.get_prior_instance();
          n_fixed_kv_extent->adjust_ptracker_for_children();
        }
      }
      
      /* This is a bit underhanded.  Any relative addrs here must necessarily
       * be record relative as we are rewriting a dirty extent.  Thus, we
       * are using resolve_relative_addrs with a (likely negative) block
       * relative offset to correct them to block-relative offsets adjusted
       * for our new transaction location.
       *
       * Upon commit, these now block relative addresses will be interpretted
       * against the real final address.
       */
      if (!n_fixed_kv_extent->get_paddr().is_absolute()) {
	// backend_type_t::SEGMENTED
	assert(n_fixed_kv_extent->get_paddr().is_record_relative());
	n_fixed_kv_extent->resolve_relative_addrs(
	  make_record_relative_paddr(0).block_relative_to(
	    n_fixed_kv_extent->get_paddr()));
      } // else: backend_type_t::RANDOM_BLOCK
      
      SUBTRACET(
        seastore_fixedkv_tree,
        "rewriting {} into {}",
        c.trans,
        fixed_kv_extent,
        *n_fixed_kv_extent);
      
      return update_internal_mapping(
        c,
        n_fixed_kv_extent->get_node_meta().depth,
        n_fixed_kv_extent->get_node_meta().begin,
        e->get_paddr(),
        n_fixed_kv_extent->get_paddr(),
        n_fixed_kv_extent
      ).si_then([c, e] {
        c.cache.retire_extent(c.trans, e);
      });
    };
    
    CachedExtentRef n_fixed_kv_extent;
    if (e->get_type() == internal_node_t::TYPE) {
      auto lint = e->cast<internal_node_t>();
      return do_rewrite(*lint);
    } else {
      assert(e->get_type() == leaf_node_t::TYPE);
      auto lleaf = e->cast<leaf_node_t>();
      return do_rewrite(*lleaf);
    }
  }

  using update_internal_mapping_iertr = base_iertr;
  using update_internal_mapping_ret = update_internal_mapping_iertr::future<>;
  update_internal_mapping_ret update_internal_mapping(
    op_context_t<node_key_t> c,
    depth_t depth,
    node_key_t laddr,
    paddr_t old_addr,
    paddr_t new_addr,
    typename internal_node_t::base_ref nextent)
  {
    LOG_PREFIX(FixedKVBtree::update_internal_mapping);
    SUBTRACET(
      seastore_fixedkv_tree,
      "updating laddr {} at depth {} from {} to {}, nextent {}",
      c.trans,
      laddr,
      depth,
      old_addr,
      new_addr,
      *nextent);

    return lower_bound(
      c, laddr, nullptr, depth + 1
    ).si_then([=, this](auto iter) {
      assert(iter.get_depth() >= depth);
      if (depth == iter.get_depth()) {
        SUBTRACET(seastore_fixedkv_tree, "update at root", c.trans);

        if (laddr != min_max_t<node_key_t>::min) {
          SUBERRORT(
            seastore_fixedkv_tree,
            "updating root laddr {} at depth {} from {} to {},"
            "laddr is not 0",
            c.trans,
            laddr,
            depth,
            old_addr,
            new_addr,
            get_root().get_location());
          ceph_assert(0 == "impossible");
        }

        if (get_root().get_location() != old_addr) {
          SUBERRORT(
            seastore_fixedkv_tree,
            "updating root laddr {} at depth {} from {} to {},"
            "root addr {} does not match",
            c.trans,
            laddr,
            depth,
            old_addr,
            new_addr,
            get_root().get_location());
          ceph_assert(0 == "impossible");
        }

        root_block = c.cache.duplicate_for_write(
          c.trans, root_block)->template cast<RootBlock>();
        get_root().set_location(new_addr);
        set_root_node(nextent);
      } else {
        auto &parent = iter.get_internal(depth + 1);
        assert(parent.node);
        assert(parent.pos < parent.node->get_size());
        auto piter = parent.node->iter_idx(parent.pos);

        if (piter->get_key() != laddr) {
          SUBERRORT(
            seastore_fixedkv_tree,
            "updating laddr {} at depth {} from {} to {},"
            "node {} pos {} val pivot addr {} does not match",
            c.trans,
            laddr,
            depth,
            old_addr,
            new_addr,
            *(parent.node),
            parent.pos,
            piter->get_key());
          ceph_assert(0 == "impossible");
        }


        if (piter->get_val() != old_addr) {
          SUBERRORT(
            seastore_fixedkv_tree,
            "updating laddr {} at depth {} from {} to {},"
            "node {} pos {} val addr {} does not match",
            c.trans,
            laddr,
            depth,
            old_addr,
            new_addr,
            *(parent.node),
            parent.pos,
            piter->get_val());
          ceph_assert(0 == "impossible");
        }

        CachedExtentRef mut = c.cache.duplicate_for_write(
          c.trans,
          parent.node
        );
        typename internal_node_t::Ref mparent = mut->cast<internal_node_t>();
        mparent->update(piter, new_addr, nextent.get());

        /* Note, iter is now invalid as we didn't udpate either the parent
         * node reference to the new mutable instance nor did we update the
         * child pointer to the new node.  Not a problem as we'll now just
         * destruct it.
         */
      }
      return seastar::now();
    });
  }


private:
  RootBlockRef root_block;

  template <typename T>
  using node_position_t = typename iterator::template node_position_t<T>;

  using get_internal_node_iertr = base_iertr;
  using get_internal_node_ret = get_internal_node_iertr::future<InternalNodeRef>;
  static get_internal_node_ret get_internal_node(
    op_context_t<node_key_t> c,
    depth_t depth,
    paddr_t offset,
    node_key_t begin,
    node_key_t end,
    typename std::optional<node_position_t<internal_node_t>> parent_pos)
  {
    LOG_PREFIX(FixedKVBtree::get_internal_node);
    SUBTRACET(
      seastore_fixedkv_tree,
      "reading internal at offset {}, depth {}, begin {}, end {}",
      c.trans,
      offset,
      depth,
      begin,
      end);
    assert(depth > 1);
    auto init_internal = [c, depth, begin, end,
                          parent_pos=std::move(parent_pos)]
                          (internal_node_t &node) {
      assert(!node.is_pending());
      assert(!node.is_linked());
      node.range = fixed_kv_node_meta_t<node_key_t>{begin, end, depth};
      if (parent_pos) {
        auto &parent = parent_pos->node;
        parent->link_child(&node, parent_pos->pos);
      } else {
        assert(node.range.is_root());
        auto root_block = c.cache.get_root_fast(c.trans);
        if (root_block->is_mutation_pending()) {
          auto &stable_root = (RootBlockRef&)*root_block->get_prior_instance();
          link_phy_tree_root_node(stable_root, &node);
        } else {
          assert(!root_block->is_pending());
          link_phy_tree_root_node(root_block, &node);
        }
      }
    };
    return c.cache.template get_absent_extent<internal_node_t>(
      c.trans,
      offset,
      node_size,
      init_internal
    ).si_then([FNAME, c, offset, init_internal, depth, begin, end](
              typename internal_node_t::Ref ret) {
      SUBTRACET(
        seastore_fixedkv_tree,
        "read internal at offset {} {}",
        c.trans,
        offset,
        *ret);
      // This can only happen during init_cached_extent
      // or when backref extent being rewritten by gc space reclaiming
      if (!ret->is_pending() && !ret->is_linked()) {
        assert(ret->is_dirty()
          || (is_backref_node(ret->get_type())
            && ret->is_clean()));
        init_internal(*ret);
      }
      auto meta = ret->get_meta();
      if (ret->get_size()) {
        ceph_assert(meta.begin <= ret->begin()->get_key());
        ceph_assert(meta.end > (ret->end() - 1)->get_key());
      }
      ceph_assert(depth == meta.depth);
      ceph_assert(begin == meta.begin);
      ceph_assert(end == meta.end);
      return get_internal_node_ret(
        interruptible::ready_future_marker{},
        ret);
    });
  }


  using get_leaf_node_iertr = base_iertr;
  using get_leaf_node_ret = get_leaf_node_iertr::future<LeafNodeRef>;
  static get_leaf_node_ret get_leaf_node(
    op_context_t<node_key_t> c,
    paddr_t offset,
    node_key_t begin,
    node_key_t end,
    typename std::optional<node_position_t<leaf_node_t>> parent_pos)
  {
    LOG_PREFIX(FixedKVBtree::get_leaf_node);
    SUBTRACET(
      seastore_fixedkv_tree,
      "reading leaf at offset {}, begin {}, end {}",
      c.trans,
      offset,
      begin,
      end);
    auto init_leaf = [c, begin, end,
                      parent_pos=std::move(parent_pos)]
                      (leaf_node_t &node) {
      assert(!node.is_pending());
      assert(!node.is_linked());
      node.range = fixed_kv_node_meta_t<node_key_t>{begin, end, 1};
      if (parent_pos) {
        auto &parent = parent_pos->node;
        parent->link_child(&node, parent_pos->pos);
      } else {
        assert(node.range.is_root());
        auto root_block = c.cache.get_root_fast(c.trans);
        if (root_block->is_mutation_pending()) {
          auto &stable_root = (RootBlockRef&)*root_block->get_prior_instance();
          link_phy_tree_root_node(stable_root, &node);
        } else {
          assert(!root_block->is_pending());
          link_phy_tree_root_node(root_block, &node);
        }
      }
    };
    return c.cache.template get_absent_extent<leaf_node_t>(
      c.trans,
      offset,
      node_size,
      init_leaf
    ).si_then([FNAME, c, offset, init_leaf, begin, end]
      (typename leaf_node_t::Ref ret) {
      SUBTRACET(
        seastore_fixedkv_tree,
        "read leaf at offset {} {}",
        c.trans,
        offset,
        *ret);
      // This can only happen during init_cached_extent
      // or when backref extent being rewritten by gc space reclaiming
      if (!ret->is_pending() && !ret->is_linked()) {
        assert(ret->is_dirty()
          || (is_backref_node(ret->get_type())
            && ret->is_clean()));
        init_leaf(*ret);
      }
      auto meta = ret->get_meta();
      if (ret->get_size()) {
        ceph_assert(meta.begin <= ret->begin()->get_key());
        ceph_assert(meta.end > (ret->end() - 1)->get_key());
      }
      ceph_assert(1 == meta.depth);
      ceph_assert(begin == meta.begin);
      ceph_assert(end == meta.end);
      return get_leaf_node_ret(
        interruptible::ready_future_marker{},
        ret);
    });
  }

  using lookup_root_iertr = base_iertr;
  using lookup_root_ret = lookup_root_iertr::future<>;
  lookup_root_ret lookup_root(
    op_context_t<node_key_t> c,
    iterator &iter,
    mapped_space_visitor_t *visitor) const {
    LOG_PREFIX(FixedKVBtree::lookup_root);
    SUBTRACET(seastore_fixedkv_tree,
      "looking up root on {}",
      c.trans,
      *root_block);
    auto [found, fut] = get_root_node(c);

    auto on_found_internal =
      [this, visitor, &iter](InternalNodeRef &root_node) {
      iter.get_internal(get_root().get_depth()).node = root_node;
      if (visitor) (*visitor)(
        root_node->get_paddr(),
        root_node->get_node_meta().begin,
        root_node->get_length(),
        get_root().get_depth(),
        internal_node_t::TYPE,
        iter);
      return lookup_root_iertr::now();
    };
    auto on_found_leaf =
      [visitor, &iter, this](LeafNodeRef root_node) {
      iter.leaf.node = root_node;
      if (visitor) (*visitor)(
        root_node->get_paddr(),
        root_node->get_node_meta().begin,
        root_node->get_length(),
        get_root().get_depth(),
        leaf_node_t::TYPE,
        iter);
      return lookup_root_iertr::now();
    };

    if (found) {
      return fut.si_then(
        [this, c, on_found_internal=std::move(on_found_internal),
        on_found_leaf=std::move(on_found_leaf)](auto root) {
        LOG_PREFIX(FixedKVBtree::lookup_root);
        ceph_assert(root);
        SUBTRACET(seastore_fixedkv_tree,
          "got root node on {}, res: {}",
          c.trans,
          *root_block,
          *root);

        if (get_root().get_depth() > 1) {
          auto root_node = root->template cast<internal_node_t>();
          return on_found_internal(root_node);
        } else {
          auto root_node = root->template cast<leaf_node_t>();
          return on_found_leaf(root_node);
        }
      });
    } else {
      if (get_root().get_depth() > 1) {
        return get_internal_node(
          c,
          get_root().get_depth(),
          get_root().get_location(),
          min_max_t<node_key_t>::min,
          min_max_t<node_key_t>::max,
          std::nullopt
        ).si_then([on_found=std::move(on_found_internal)](InternalNodeRef root_node) {
          return on_found(root_node);
        });
      } else {
        return get_leaf_node(
          c,
          get_root().get_location(),
          min_max_t<node_key_t>::min,
          min_max_t<node_key_t>::max,
          std::nullopt
        ).si_then([on_found=std::move(on_found_leaf)](LeafNodeRef root_node) {
          return on_found(root_node);
        });
      }
    }
  }

  using lookup_internal_level_iertr = base_iertr;
  using lookup_internal_level_ret = lookup_internal_level_iertr::future<>;
  template <typename F>
  static lookup_internal_level_ret lookup_internal_level(
    op_context_t<node_key_t> c,
    depth_t depth,
    iterator &iter,
    F &f,
    mapped_space_visitor_t *visitor
  ) {
    assert(depth > 1);
    auto &parent_entry = iter.get_internal(depth + 1);
    auto parent = parent_entry.node;
    auto node_iter = parent->iter_idx(parent_entry.pos);

    auto on_found = [depth, visitor, &iter, &f](InternalNodeRef node) {
      auto &entry = iter.get_internal(depth);
      entry.node = node;
      auto node_iter = f(*node);
      assert(node_iter != node->end());
      entry.pos = node_iter->get_offset();
      if (visitor)
        (*visitor)(
          node->get_paddr(),
          node->get_node_meta().begin,
          node->get_length(),
          depth,
          node->get_type(),
          iter);
      return seastar::now();
    };

    auto v = parent->template get_child<internal_node_t>(c, node_iter);
    if (v.has_child()) {
      return v.get_child_fut().safe_then(
        [on_found=std::move(on_found), node_iter, c,
        parent_entry](auto child) mutable {
        LOG_PREFIX(FixedKVBtree::lookup_internal_level);
        SUBTRACET(seastore_fixedkv_tree,
          "got child on {}, pos: {}, res: {}",
          c.trans,
          *parent_entry.node,
          parent_entry.pos,
          *child);
        auto &cnode = (typename internal_node_t::base_t &)*child;
        assert(cnode.get_node_meta().begin == node_iter.get_key());
        assert(cnode.get_node_meta().end > node_iter.get_key());
        return on_found(child->template cast<internal_node_t>());
      });
    }

    auto child_pos = v.get_child_pos();
    auto next_iter = node_iter + 1;
    auto begin = node_iter->get_key();
    auto end = next_iter == parent->end()
      ? parent->get_node_meta().end
      : next_iter->get_key();
    return get_internal_node(
      c,
      depth,
      node_iter->get_val().maybe_relative_to(parent->get_paddr()),
      begin,
      end,
      std::make_optional<node_position_t<internal_node_t>>(
        child_pos.template get_parent<internal_node_t>(),
        child_pos.get_pos())
    ).si_then([on_found=std::move(on_found)](InternalNodeRef node) {
      return on_found(node);
    });
  }

  using lookup_leaf_iertr = base_iertr;
  using lookup_leaf_ret = lookup_leaf_iertr::future<>;
  template <typename F>
  static lookup_internal_level_ret lookup_leaf(
    op_context_t<node_key_t> c,
    iterator &iter,
    F &f,
    mapped_space_visitor_t *visitor
  ) {
    auto &parent_entry = iter.get_internal(2);
    auto parent = parent_entry.node;
    assert(parent);
    auto node_iter = parent->iter_idx(parent_entry.pos);

    auto on_found = [visitor, &iter, &f](LeafNodeRef node) {
      iter.leaf.node = node;
      auto node_iter = f(*node);
      iter.leaf.pos = node_iter->get_offset();
      if (visitor)
        (*visitor)(
          node->get_paddr(),
          node->get_node_meta().begin,
          node->get_length(),
          1,
          node->get_type(),
          iter);
      return seastar::now();
    };

    auto v = parent->template get_child<leaf_node_t>(c, node_iter);
    if (v.has_child()) {
      return v.get_child_fut().safe_then(
        [on_found=std::move(on_found), node_iter, c,
        parent_entry](auto child) mutable {
        LOG_PREFIX(FixedKVBtree::lookup_leaf);
        SUBTRACET(seastore_fixedkv_tree,
          "got child on {}, pos: {}, res: {}",
          c.trans,
          *parent_entry.node,
          parent_entry.pos,
          *child);
        auto &cnode = (typename internal_node_t::base_t &)*child;
        assert(cnode.get_node_meta().begin == node_iter.get_key());
        assert(cnode.get_node_meta().end > node_iter.get_key());
        return on_found(child->template cast<leaf_node_t>());
      });
    }

    auto child_pos = v.get_child_pos();
    auto next_iter = node_iter + 1;
    auto begin = node_iter->get_key();
    auto end = next_iter == parent->end()
      ? parent->get_node_meta().end
      : next_iter->get_key();

    return get_leaf_node(
      c,
      node_iter->get_val().maybe_relative_to(parent->get_paddr()),
      begin,
      end,
      std::make_optional<node_position_t<leaf_node_t>>(
        child_pos.template get_parent<leaf_node_t>(),
        child_pos.get_pos())
    ).si_then([on_found=std::move(on_found)](LeafNodeRef node) {
      return on_found(node);
    });
  }

  /**
   * lookup_depth_range
   *
   * Performs node lookups on depths [from, to) using li and ll to
   * specific target at each level.  Note, may leave the iterator
   * at_boundary(), call handle_boundary() prior to returning out
   * lf FixedKVBtree.
   */
  using lookup_depth_range_iertr = base_iertr;
  using lookup_depth_range_ret = lookup_depth_range_iertr::future<>;
  template <typename LI, typename LL>
  static lookup_depth_range_ret lookup_depth_range(
    op_context_t<node_key_t> c, ///< [in] context
    iterator &iter, ///< [in,out] iterator to populate
    depth_t from,   ///< [in] from inclusive
    depth_t to,     ///< [in] to exclusive, (to <= from, to == from is a noop)
    LI &li,         ///< [in] internal->iterator
    LL &ll,         ///< [in] leaf->iterator
    mapped_space_visitor_t *visitor ///< [in] mapped space visitor
  ) {
    LOG_PREFIX(FixedKVBtree::lookup_depth_range);
    SUBTRACET(seastore_fixedkv_tree, "{} -> {}", c.trans, from, to);
    return seastar::do_with(
      from,
      [c, to, visitor, &iter, &li, &ll](auto &d) {
	return trans_intr::repeat(
	  [c, to, visitor, &iter, &li, &ll, &d] {
	    if (d > to) {
	      return [&] {
		if (d > 1) {
		  return lookup_internal_level(
		    c,
		    d,
		    iter,
		    li,
		    visitor);
		} else {
		  assert(d == 1);
		  return lookup_leaf(
		    c,
		    iter,
		    ll,
		    visitor);
		}
	      }().si_then([&d] {
		--d;
		return lookup_depth_range_iertr::make_ready_future<
		  seastar::stop_iteration
		  >(seastar::stop_iteration::no);
	      });
	    } else {
	      return lookup_depth_range_iertr::make_ready_future<
		seastar::stop_iteration
		>(seastar::stop_iteration::yes);
	    }
	  });
      });
  }

  using lookup_iertr = base_iertr;
  using lookup_ret = lookup_iertr::future<iterator>;
  template <typename LI, typename LL>
  lookup_ret lookup(
    op_context_t<node_key_t> c,
    LI &&lookup_internal,
    LL &&lookup_leaf,
    depth_t min_depth,
    mapped_space_visitor_t *visitor
  ) const {
    LOG_PREFIX(FixedKVBtree::lookup);
    assert(min_depth > 0);
    return seastar::do_with(
      iterator{get_root().get_depth()},
      std::forward<LI>(lookup_internal),
      std::forward<LL>(lookup_leaf),
      [FNAME, this, visitor, c, min_depth](auto &iter, auto &li, auto &ll) {
	return lookup_root(
	  c, iter, visitor
	).si_then([FNAME, this, visitor, c, &iter, &li, &ll, min_depth] {
	  if (iter.get_depth() > 1) {
	    auto &root_entry = *(iter.internal.rbegin());
	    root_entry.pos = li(*(root_entry.node)).get_offset();
	  } else {
	    auto &root_entry = iter.leaf;
	    auto riter = ll(*(root_entry.node));
	    root_entry.pos = riter->get_offset();
	  }
	  SUBTRACET(seastore_fixedkv_tree, "got root, depth {}",
            c.trans, get_root().get_depth());
	  return lookup_depth_range(
	    c,
	    iter,
	    get_root().get_depth() - 1,
            min_depth - 1,
	    li,
	    ll,
	    visitor
	  ).si_then([c, visitor, &iter, min_depth] {
            // It's only when the lookup is triggered by
            // update_internal_mapping() that min_depth is
            // NOT 1
	    if (min_depth == 1 && iter.at_boundary()) {
	      return iter.handle_boundary(c, visitor);
	    } else {
	      return lookup_iertr::now();
	    }
	  });
	}).si_then([&iter] {
	  return std::move(iter);
	});
      });
  }

  /**
   * find_insertion
   *
   * Prepare iter for insertion.  iter should begin pointing at
   * the valid insertion point (lower_bound(laddr)).
   *
   * Upon completion, iter will point at the
   * position at which laddr should be inserted.  iter may, upon completion,
   * point at the end of a leaf other than the end leaf if that's the correct
   * insertion point.
   */
  using find_insertion_iertr = base_iertr;
  using find_insertion_ret = find_insertion_iertr::future<>;
  static find_insertion_ret find_insertion(
    op_context_t<node_key_t> c,
    node_key_t laddr,
    iterator &iter)
  {
    assert(iter.is_end() || iter.get_key() >= laddr);
    if (!iter.is_end() && iter.get_key() == laddr) {
      return seastar::now();
    } else if (iter.leaf.node->get_node_meta().begin <= laddr) {
#ifndef NDEBUG
      auto p = iter;
      if (p.leaf.pos > 0) {
        --p.leaf.pos;
        assert(p.get_key() < laddr);
      }
#endif
      return seastar::now();
    } else {
      assert(iter.leaf.pos == 0);
      return iter.prev(
        c
      ).si_then([laddr, &iter](auto p) {
        boost::ignore_unused(laddr); // avoid clang warning;
        assert(p.leaf.node->get_node_meta().begin <= laddr);
        assert(p.get_key() < laddr);
        // Note, this is specifically allowed to violate the iterator
        // invariant that pos is a valid index for the node in the event
        // that the insertion point is at the end of a node.
        p.leaf.pos++;
        assert(p.at_boundary());
        iter = p;
        return seastar::now();
      });
    }
  }

  /**
   * handle_split
   *
   * Split nodes in iter as needed for insertion. First, scan iter from leaf
   * to find first non-full level.  Then, split from there towards leaf.
   *
   * Upon completion, iter will point at the newly split insertion point.  As
   * with find_insertion, iter's leaf pointer may be end without iter being
   * end.
   */
  using handle_split_iertr = base_iertr;
  using handle_split_ret = handle_split_iertr::future<>;
  handle_split_ret handle_split(
    op_context_t<node_key_t> c,
    iterator &iter)
  {
    LOG_PREFIX(FixedKVBtree::handle_split);

    depth_t split_from = iter.check_split();

    SUBTRACET(seastore_fixedkv_tree, "split_from {}, depth {}", c.trans, split_from, iter.get_depth());

    if (split_from == iter.get_depth()) {
      auto nroot = c.cache.template alloc_new_extent<internal_node_t>(
        c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
      fixed_kv_node_meta_t<node_key_t> meta{
        min_max_t<node_key_t>::min, min_max_t<node_key_t>::max, iter.get_depth() + 1};
      nroot->set_meta(meta);
      nroot->range = meta;
      nroot->journal_insert(
        nroot->begin(),
        min_max_t<node_key_t>::min,
        get_root().get_location(),
        nullptr);
      iter.internal.push_back({nroot, 0});

      get_tree_stats<self_type>(c.trans).depth = iter.get_depth();
      get_tree_stats<self_type>(c.trans).extents_num_delta++;

      root_block = c.cache.duplicate_for_write(
        c.trans, root_block)->template cast<RootBlock>();
      get_root().set_location(nroot->get_paddr());
      get_root().set_depth(iter.get_depth());
      ceph_assert(get_root().get_depth() <= MAX_FIXEDKVBTREE_DEPTH);
      set_root_node(nroot);
    }

    /* pos may be either node_position_t<leaf_node_t> or
     * node_position_t<internal_node_t> */
    auto split_level = [&](auto &parent_pos, auto &pos) {
      LOG_PREFIX(FixedKVBtree::handle_split);
      auto [left, right, pivot] = pos.node->make_split_children(c);

      auto parent_node = parent_pos.node;
      auto parent_iter = parent_pos.get_iter();

      parent_node->update(
        parent_iter,
        left->get_paddr(),
        left.get());
      parent_node->insert(
        parent_iter + 1,
        pivot,
        right->get_paddr(),
        right.get());

      SUBTRACET(
        seastore_fixedkv_tree,
        "splitted {} into left: {}, right: {}",
        c.trans,
        *pos.node,
        *left,
        *right);
      c.cache.retire_extent(c.trans, pos.node);

      get_tree_stats<self_type>(c.trans).extents_num_delta++;
      return std::make_pair(left, right);
    };

    for (; split_from > 0; --split_from) {
      auto &parent_pos = iter.get_internal(split_from + 1);
      if (!parent_pos.node->is_mutable()) {
        parent_pos.node = c.cache.duplicate_for_write(
          c.trans, parent_pos.node
        )->template cast<internal_node_t>();
      }

      if (split_from > 1) {
        auto &pos = iter.get_internal(split_from);
        SUBTRACET(
          seastore_fixedkv_tree,
          "splitting internal {} at depth {}, parent: {} at pos: {}",
          c.trans,
          *pos.node,
          split_from,
          *parent_pos.node,
          parent_pos.pos);
        auto [left, right] = split_level(parent_pos, pos);

        if (pos.pos < left->get_size()) {
          pos.node = left;
        } else {
          pos.node = right;
          pos.pos -= left->get_size();

          parent_pos.pos += 1;
        }
      } else {
        auto &pos = iter.leaf;
        SUBTRACET(
          seastore_fixedkv_tree,
          "splitting leaf {}, parent: {} at pos: {}",
          c.trans,
          *pos.node,
          *parent_pos.node,
          parent_pos.pos);
        auto [left, right] = split_level(parent_pos, pos);

        /* right->get_node_meta().begin == pivot == right->begin()->get_key()
         * Thus, if pos.pos == left->get_size(), we want iter to point to
         * left with pos.pos at the end rather than right with pos.pos = 0
         * since the insertion would be to the left of the first element
         * of right and thus necessarily less than right->get_node_meta().begin.
         */
        if (pos.pos <= left->get_size()) {
          pos.node = left;
        } else {
          pos.node = right;
          pos.pos -= left->get_size();

          parent_pos.pos += 1;
        }
      }
    }

    return seastar::now();
  }


  using handle_merge_iertr = base_iertr;
  using handle_merge_ret = handle_merge_iertr::future<>;
  handle_merge_ret handle_merge(
    op_context_t<node_key_t> c,
    iterator &iter)
  {
    LOG_PREFIX(FixedKVBtree::handle_merge);
    if (iter.get_depth() == 1 ||
        !iter.leaf.node->below_min_capacity()) {
      SUBTRACET(
        seastore_fixedkv_tree,
        "no need to merge leaf, leaf size {}, depth {}",
        c.trans,
        iter.leaf.node->get_size(),
        iter.get_depth());
      return seastar::now();
    }

    return seastar::do_with(
      depth_t{1},
      [FNAME, this, c, &iter](auto &to_merge) {
        return trans_intr::repeat(
          [FNAME, this, c, &iter, &to_merge] {
            SUBTRACET(
              seastore_fixedkv_tree,
              "merging depth {}",
              c.trans,
              to_merge);
            auto &parent_pos = iter.get_internal(to_merge + 1);
            auto merge_fut = handle_merge_iertr::now();
            if (to_merge > 1) {
              auto &pos = iter.get_internal(to_merge);
              merge_fut = merge_level(c, to_merge, parent_pos, pos);
            } else {
              auto &pos = iter.leaf;
              merge_fut = merge_level(c, to_merge, parent_pos, pos);
            }

            return merge_fut.si_then([FNAME, this, c, &iter, &to_merge] {
              ++to_merge;
              auto &pos = iter.get_internal(to_merge);
              if (to_merge == iter.get_depth()) {
                if (pos.node->get_size() == 1) {
                  SUBTRACET(seastore_fixedkv_tree, "collapsing root", c.trans);
                  c.cache.retire_extent(c.trans, pos.node);
                  assert(pos.pos == 0);
                  auto node_iter = pos.get_iter();
                  iter.internal.pop_back();
                  get_tree_stats<self_type>(c.trans).depth = iter.get_depth();
                  get_tree_stats<self_type>(c.trans).extents_num_delta--;

                  root_block = c.cache.duplicate_for_write(
                    c.trans, root_block
                  )->template cast<RootBlock>();
                  get_root().set_location(
                    node_iter->get_val().maybe_relative_to(pos.node->get_paddr()));
                  get_root().set_depth(iter.get_depth());
                  if (iter.get_depth() > 1) {
                    auto root_node = iter.get_internal(iter.get_depth()).node;
                    set_root_node(root_node);
                  } else {
                    set_root_node(iter.leaf.node);
                  }
                } else {
                  SUBTRACET(seastore_fixedkv_tree, "no need to collapse root", c.trans);
                }
                return seastar::stop_iteration::yes;
              } else if (pos.node->below_min_capacity()) {
                SUBTRACET(
                  seastore_fixedkv_tree,
                  "continuing, next node {} depth {} at min",
                  c.trans,
                  *pos.node,
                  to_merge);
                return seastar::stop_iteration::no;
              } else {
                SUBTRACET(
                  seastore_fixedkv_tree,
                  "complete, next node {} depth {} not min",
                  c.trans,
                  *pos.node,
                  to_merge);
                return seastar::stop_iteration::yes;
              }
            });
          });
      });
  }

  template <typename NodeType,
            std::enable_if_t<std::is_same_v<NodeType, leaf_node_t>, int> = 0>
  base_iertr::future<typename NodeType::Ref> get_node(
    op_context_t<node_key_t> c,
    depth_t depth,
    paddr_t addr,
    node_key_t begin,
    node_key_t end,
    typename std::optional<node_position_t<leaf_node_t>> parent_pos) {
    assert(depth == 1);
    return get_leaf_node(c, addr, begin, end, std::move(parent_pos));
  }

  template <typename NodeType,
            std::enable_if_t<std::is_same_v<NodeType, internal_node_t>, int> = 0>
  base_iertr::future<typename NodeType::Ref> get_node(
    op_context_t<node_key_t> c,
    depth_t depth,
    paddr_t addr,
    node_key_t begin,
    node_key_t end,
    typename std::optional<node_position_t<internal_node_t>> parent_pos) {
    return get_internal_node(c, depth, addr, begin, end, std::move(parent_pos));
  }

  template <typename NodeType>
  handle_merge_ret merge_level(
    op_context_t<node_key_t> c,
    depth_t depth,
    node_position_t<internal_node_t> &parent_pos,
    node_position_t<NodeType> &pos)
  {
    LOG_PREFIX(FixedKVBtree::merge_level);
    if (!parent_pos.node->is_mutable()) {
      parent_pos.node = c.cache.duplicate_for_write(
        c.trans, parent_pos.node
      )->template cast<internal_node_t>();
    }

    auto iter = parent_pos.get_iter();
    assert(iter.get_offset() < parent_pos.node->get_size());
    bool donor_is_left = ((iter.get_offset() + 1) == parent_pos.node->get_size());
    auto donor_iter = donor_is_left ? (iter - 1) : (iter + 1);
    auto next_iter = donor_iter + 1;
    auto begin = donor_iter->get_key();
    auto end = next_iter == parent_pos.node->end()
      ? parent_pos.node->get_node_meta().end
      : next_iter->get_key();
    
    SUBTRACET(seastore_fixedkv_tree, "parent: {}, node: {}", c.trans, *parent_pos.node, *pos.node);
    auto do_merge = [c, iter, donor_iter, donor_is_left, &parent_pos, &pos](
                typename NodeType::Ref donor) {
      LOG_PREFIX(FixedKVBtree::merge_level);
      auto [l, r] = donor_is_left ?
        std::make_pair(donor, pos.node) : std::make_pair(pos.node, donor);

      auto [liter, riter] = donor_is_left ?
        std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);

      if (donor->at_min_capacity()) {
        auto replacement = l->make_full_merge(c, r);

        parent_pos.node->update(
          liter,
          replacement->get_paddr(),
          replacement.get());
        parent_pos.node->remove(riter);

        pos.node = replacement;
        if (donor_is_left) {
          pos.pos += l->get_size();
          parent_pos.pos--;
        }

        SUBTRACET(seastore_fixedkv_tree, "l: {}, r: {}, replacement: {}", c.trans, *l, *r, *replacement);
        c.cache.retire_extent(c.trans, l);
        c.cache.retire_extent(c.trans, r);
        get_tree_stats<self_type>(c.trans).extents_num_delta--;
      } else {
        LOG_PREFIX(FixedKVBtree::merge_level);
        auto [replacement_l, replacement_r, pivot] =
          l->make_balanced(
            c,
            r,
            !donor_is_left);

        parent_pos.node->update(
          liter,
          replacement_l->get_paddr(),
          replacement_l.get());
        parent_pos.node->replace(
          riter,
          pivot,
          replacement_r->get_paddr(),
          replacement_r.get());

        if (donor_is_left) {
          assert(parent_pos.pos > 0);
          parent_pos.pos--;
        }

        auto orig_position = donor_is_left ?
          l->get_size() + pos.pos :
          pos.pos;
        if (orig_position < replacement_l->get_size()) {
          pos.node = replacement_l;
          pos.pos = orig_position;
        } else {
          parent_pos.pos++;
          pos.node = replacement_r;
          pos.pos = orig_position - replacement_l->get_size();
        }

        SUBTRACET(
          seastore_fixedkv_tree,
          "l: {}, r: {}, replacement_l: {}, replacement_r: {}",
          c.trans, *l, *r, *replacement_l, *replacement_r);
        c.cache.retire_extent(c.trans, l);
        c.cache.retire_extent(c.trans, r);
      }

      return seastar::now();
    };

    auto v = parent_pos.node->template get_child<NodeType>(c, donor_iter);
    if (v.has_child()) {
      return v.get_child_fut().safe_then(
        [do_merge=std::move(do_merge), &pos,
        donor_iter, donor_is_left, c, parent_pos](auto child) mutable {
        LOG_PREFIX(FixedKVBtree::merge_level);
        SUBTRACET(seastore_fixedkv_tree,
          "got child on {}, pos: {}, res: {}",
          c.trans,
          *parent_pos.node,
          donor_iter.get_offset(),
          *child);
        auto &node = (typename internal_node_t::base_t&)*child;
        assert(donor_is_left ?
          node.get_node_meta().end == pos.node->get_node_meta().begin :
          node.get_node_meta().begin == pos.node->get_node_meta().end);
        assert(node.get_node_meta().begin == donor_iter.get_key());
        assert(node.get_node_meta().end > donor_iter.get_key());
        return do_merge(child->template cast<NodeType>());
      });
    }

    auto child_pos = v.get_child_pos();
    return get_node<NodeType>(
      c,
      depth,
      donor_iter.get_val().maybe_relative_to(parent_pos.node->get_paddr()),
      begin,
      end,
      std::make_optional<node_position_t<NodeType>>(
        child_pos.template get_parent<NodeType>(),
        child_pos.get_pos())
    ).si_then([do_merge=std::move(do_merge)](typename NodeType::Ref donor) {
      return do_merge(donor);
    });
  }
};

template <typename T>
struct is_fixed_kv_tree : std::false_type {};

template <
  typename node_key_t,
  typename node_val_t,
  typename internal_node_t,
  typename leaf_node_t,
  typename pin_t,
  size_t node_size,
  bool leaf_has_children>
struct is_fixed_kv_tree<
  FixedKVBtree<
    node_key_t,
    node_val_t,
    internal_node_t,
    leaf_node_t,
    pin_t,
    node_size,
    leaf_has_children>> : std::true_type {};

template <
  typename tree_type_t,
  typename node_key_t,
  typename F,
  std::enable_if_t<is_fixed_kv_tree<tree_type_t>::value, int> = 0>
auto with_btree(
  Cache &cache,
  op_context_t<node_key_t> c,
  F &&f) {
  return cache.get_root(
    c.trans
  ).si_then([f=std::forward<F>(f)](RootBlockRef croot) mutable {
    return seastar::do_with(
      tree_type_t(croot),
      [f=std::move(f)](auto &btree) mutable {
        return f(btree);
      });
  });
}

template <
  typename tree_type_t,
  typename State,
  typename node_key_t,
  typename F,
  std::enable_if_t<is_fixed_kv_tree<tree_type_t>::value, int> = 0>
auto with_btree_state(
  Cache &cache,
  op_context_t<node_key_t> c,
  State &&init,
  F &&f) {
  return seastar::do_with(
    std::forward<State>(init),
    [&cache, c, f=std::forward<F>(f)](auto &state) mutable {
      return with_btree<tree_type_t>(
        cache,
        c,
        [&state, f=std::move(f)](auto &btree) mutable {
        return f(btree, state);
      }).si_then([&state] {
        return seastar::make_ready_future<State>(std::move(state));
      });
    });
}

template <
  typename tree_type_t,
  typename State,
  typename node_key_t,
  typename F,
  std::enable_if_t<is_fixed_kv_tree<tree_type_t>::value, int> = 0>
auto with_btree_state(
  Cache &cache,
  op_context_t<node_key_t> c,
  F &&f) {
  return crimson::os::seastore::with_btree_state<tree_type_t, State>(
    cache, c, State{}, std::forward<F>(f));
}

template <
  typename tree_type_t,
  typename Ret,
  typename node_key_t,
  typename F>
auto with_btree_ret(
  Cache &cache,
  op_context_t<node_key_t> c,
  F &&f) {
  return with_btree_state<tree_type_t, Ret>(
    cache,
    c,
    [f=std::forward<F>(f)](auto &btree, auto &ret) mutable {
      return f(
        btree
      ).si_then([&ret](auto &&_ret) {
        ret = std::move(_ret);
      });
    });
}

}

