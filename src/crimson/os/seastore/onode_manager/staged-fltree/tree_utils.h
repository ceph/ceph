// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <cstring>
#include <random>
#include <string>
#include <sstream>
#include <utility>
#include <vector>

#include <seastar/core/thread.hh>

#include "crimson/common/log.h"
#include "stages/key_layout.h"
#include "tree.h"

/**
 * tree_utils.h
 *
 * Contains shared logic for unit tests and perf tool.
 */

namespace crimson::os::seastore::onode {

/**
 * templates to work with tree utility classes:
 *
 * struct ValueItem {
 *   <public members>
 *
 *   value_size_t get_payload_size() const;
 *   static ValueItem create(std::size_t expected_size, std::size_t id);
 * };
 * std::ostream& operator<<(std::ostream& os, const ValueItem& item);
 *
 * class ValueImpl final : public Value {
 *   ...
 *
 *   using item_t = ValueItem;
 *   void initialize(Transaction& t, const item_t& item);
 *   void validate(const item_t& item);
 * };
 *
 */

template <typename CursorType>
void initialize_cursor_from_item(
    Transaction& t,
    const ghobject_t& key,
    const typename decltype(std::declval<CursorType>().value())::item_t& item,
    CursorType& cursor,
    bool insert_success) {
  ceph_assert(insert_success);
  ceph_assert(!cursor.is_end());
  ceph_assert(cursor.get_ghobj() == key);
  auto tree_value = cursor.value();
  tree_value.initialize(t, item);
}


template <typename CursorType>
void validate_cursor_from_item(
    const ghobject_t& key,
    const typename decltype(std::declval<CursorType>().value())::item_t& item,
    CursorType& cursor) {
  ceph_assert(!cursor.is_end());
  ceph_assert(cursor.get_ghobj() == key);
  auto tree_value = cursor.value();
  tree_value.validate(item);
}

template <typename ValueItem>
class Values {
 public:
  Values(size_t n) {
    for (size_t i = 1; i <= n; ++i) {
      auto item = create(i * 8);
      values.push_back(item);
    }
  }

  Values(std::vector<size_t> sizes) {
    for (auto& size : sizes) {
      auto item = create(size);
      values.push_back(item);
    }
  }

  ~Values() = default;

  ValueItem create(size_t size) {
    return ValueItem::create(size, id++);
  }

  ValueItem pick() const {
    auto index = rd() % values.size();
    return values[index];
  }

 private:
  std::size_t id = 0;
  mutable std::random_device rd;
  std::vector<ValueItem> values;
};

template <typename ValueItem>
class KVPool {
 public:
  struct kv_t {
    ghobject_t key;
    ValueItem value;
  };
  using kv_vector_t = std::vector<kv_t>;
  using kvptr_vector_t = std::vector<kv_t*>;
  using iterator_t = typename kvptr_vector_t::iterator;

  size_t size() const {
    return kvs.size();
  }

  iterator_t begin() {
    return serial_p_kvs.begin();
  }
  iterator_t end() {
    return serial_p_kvs.end();
  }
  iterator_t random_begin() {
    return random_p_kvs.begin();
  }
  iterator_t random_end() {
    return random_p_kvs.end();
  }

  void shuffle() {
    std::shuffle(random_p_kvs.begin(), random_p_kvs.end(), std::default_random_engine{});
  }

  void erase_from_random(iterator_t begin, iterator_t end) {
    random_p_kvs.erase(begin, end);
    kv_vector_t new_kvs;
    for (auto p_kv : random_p_kvs) {
      new_kvs.emplace_back(*p_kv);
    }
    std::sort(new_kvs.begin(), new_kvs.end(), [](auto& l, auto& r) {
      return l.key < r.key;
    });

    kvs.swap(new_kvs);
    serial_p_kvs.resize(kvs.size());
    random_p_kvs.resize(kvs.size());
    init();
  }

  static KVPool create_raw_range(
      const std::vector<size_t>& ns_sizes,
      const std::vector<size_t>& oid_sizes,
      const std::vector<size_t>& value_sizes,
      const std::pair<index_t, index_t>& range2,
      const std::pair<index_t, index_t>& range1,
      const std::pair<index_t, index_t>& range0) {
    ceph_assert(range2.first < range2.second);
    ceph_assert(range2.second - 1 <= MAX_SHARD);
    ceph_assert(range2.second - 1 <= MAX_CRUSH);
    ceph_assert(range1.first < range1.second);
    ceph_assert(range1.second - 1 <= 9);
    ceph_assert(range0.first < range0.second);

    kv_vector_t kvs;
    std::random_device rd;
    Values<ValueItem> values{value_sizes};
    for (index_t i = range2.first; i < range2.second; ++i) {
      for (index_t j = range1.first; j < range1.second; ++j) {
        size_t ns_size;
        size_t oid_size;
        if (j == 0) {
          // store ns0, oid0 as empty strings for test purposes
          ns_size = 0;
          oid_size = 0;
        } else {
          ns_size = ns_sizes[rd() % ns_sizes.size()];
          oid_size = oid_sizes[rd() % oid_sizes.size()];
          assert(ns_size && oid_size);
        }
        for (index_t k = range0.first; k < range0.second; ++k) {
          kvs.emplace_back(
              kv_t{make_raw_oid(i, j, k, ns_size, oid_size), values.pick()}
          );
        }
      }
    }
    return KVPool(std::move(kvs));
  }

  static KVPool create_range(
      const std::pair<index_t, index_t>& range_i,
      const std::vector<size_t>& value_sizes,
      const uint64_t block_size) {
    kv_vector_t kvs;
    std::random_device rd;
    for (index_t i = range_i.first; i < range_i.second; ++i) {
      auto value_size = value_sizes[rd() % value_sizes.size()];
      kvs.emplace_back(
          kv_t{make_oid(i), ValueItem::create(value_size, i, block_size)}
      );
    }
    return KVPool(std::move(kvs));
  }

 private:
  KVPool(kv_vector_t&& _kvs)
      : kvs(std::move(_kvs)), serial_p_kvs(kvs.size()), random_p_kvs(kvs.size()) {
    init();
  }

  void init() {
    std::transform(kvs.begin(), kvs.end(), serial_p_kvs.begin(),
                   [] (kv_t& item) { return &item; });
    std::transform(kvs.begin(), kvs.end(), random_p_kvs.begin(),
                   [] (kv_t& item) { return &item; });
    shuffle();
  }

  static ghobject_t make_raw_oid(
      index_t index2, index_t index1, index_t index0,
      size_t ns_size, size_t oid_size) {
    assert(index1 < 10);
    std::ostringstream os_ns;
    std::ostringstream os_oid;
    if (index1 == 0) {
      assert(!ns_size);
      assert(!oid_size);
    } else {
      os_ns << "ns" << index1;
      auto current_size = (size_t)os_ns.tellp();
      assert(ns_size >= current_size);
      os_ns << std::string(ns_size - current_size, '_');

      os_oid << "oid" << index1;
      current_size = (size_t)os_oid.tellp();
      assert(oid_size >= current_size);
      os_oid << std::string(oid_size - current_size, '_');
    }

    return ghobject_t(shard_id_t(index2), index2, index2,
                      os_ns.str(), os_oid.str(), index0, index0);
  }

  static ghobject_t make_oid(index_t i) {
    std::stringstream ss;
    ss << "object_" << i;
    auto ret = ghobject_t(
      hobject_t(
        sobject_t(ss.str(), CEPH_NOSNAP)));
    ret.set_shard(shard_id_t(0));
    ret.hobj.nspace = "asdf";
    return ret;
  }

  kv_vector_t kvs;
  kvptr_vector_t serial_p_kvs;
  kvptr_vector_t random_p_kvs;
};

template <bool TRACK, typename ValueImpl>
class TreeBuilder {
 public:
  using BtreeImpl = Btree<ValueImpl>;
  using BtreeCursor = typename BtreeImpl::Cursor;
  using ValueItem = typename ValueImpl::item_t;
  using iterator_t = typename KVPool<ValueItem>::iterator_t;

  TreeBuilder(KVPool<ValueItem>& kvs, NodeExtentManagerURef&& nm)
      : kvs{kvs} {
    tree.emplace(std::move(nm));
  }

  eagain_ifuture<> bootstrap(Transaction& t) {
    std::ostringstream oss;
#ifndef NDEBUG
    oss << "debug=on, ";
#else
    oss << "debug=off, ";
#endif
#ifdef UNIT_TESTS_BUILT
    oss << "UNIT_TEST_BUILT=on, ";
#else
    oss << "UNIT_TEST_BUILT=off, ";
#endif
    if constexpr (TRACK) {
      oss << "track=on, ";
    } else {
      oss << "track=off, ";
    }
    oss << *tree;
    logger().warn("TreeBuilder: {}, bootstrapping ...", oss.str());
    return tree->mkfs(t);
  }

  eagain_ifuture<BtreeCursor> insert_one(
      Transaction& t, const iterator_t& iter_rd) {
    auto p_kv = *iter_rd;
    logger().debug("[{}] insert {} -> {}",
                   iter_rd - kvs.random_begin(),
                   key_hobj_t{p_kv->key},
                   p_kv->value);
    return tree->insert(
        t, p_kv->key, {p_kv->value.get_payload_size()}
    ).si_then([&t, this, p_kv](auto ret) {
      boost::ignore_unused(this);  // avoid clang warning;
      auto success = ret.second;
      auto cursor = std::move(ret.first);
      initialize_cursor_from_item(t, p_kv->key, p_kv->value, cursor, success);
#ifndef NDEBUG
      validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
      return tree->find(t, p_kv->key
      ).si_then([cursor, p_kv](auto cursor_) mutable {
        assert(!cursor_.is_end());
        ceph_assert(cursor_.get_ghobj() == p_kv->key);
        ceph_assert(cursor_.value() == cursor.value());
        validate_cursor_from_item(p_kv->key, p_kv->value, cursor_);
        return cursor;
      });
#else
      return eagain_iertr::make_ready_future<BtreeCursor>(cursor);
#endif
    }).handle_error_interruptible(
      crimson::ct_error::value_too_large::assert_failure{"impossible path"},
      crimson::ct_error::pass_further_all{}
    );
  }

  eagain_ifuture<> insert(Transaction& t) {
    auto ref_kv_iter = seastar::make_lw_shared<iterator_t>();
    *ref_kv_iter = kvs.random_begin();
    auto cursors = seastar::make_lw_shared<std::vector<BtreeCursor>>();
    logger().warn("start inserting {} kvs ...", kvs.size());
    auto start_time = mono_clock::now();
    return trans_intr::repeat([&t, this, cursors, ref_kv_iter,
                            start_time]()
      -> eagain_ifuture<seastar::stop_iteration> {
      if (*ref_kv_iter == kvs.random_end()) {
        std::chrono::duration<double> duration = mono_clock::now() - start_time;
        logger().warn("Insert done! {}s", duration.count());
        return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
      } else {
        return insert_one(t, *ref_kv_iter
        ).si_then([cursors, ref_kv_iter] (auto cursor) {
          if constexpr (TRACK) {
            cursors->emplace_back(cursor);
          }
          ++(*ref_kv_iter);
          return seastar::stop_iteration::no;
        });
      }
    }).si_then([&t, this, cursors, ref_kv_iter] {
      if (!cursors->empty()) {
        logger().info("Verifing tracked cursors ...");
        *ref_kv_iter = kvs.random_begin();
        return seastar::do_with(
            cursors->begin(),
            [&t, this, cursors, ref_kv_iter] (auto& c_iter) {
          return trans_intr::repeat(
            [&t, this, &c_iter, cursors, ref_kv_iter] ()
            -> eagain_ifuture<seastar::stop_iteration> {
            if (*ref_kv_iter == kvs.random_end()) {
              logger().info("Verify done!");
              return seastar::make_ready_future<seastar::stop_iteration>(
                seastar::stop_iteration::yes);
            }
            assert(c_iter != cursors->end());
            auto p_kv = **ref_kv_iter;
            // validate values in tree keep intact
            return tree->find(t, p_kv->key).si_then([&c_iter, ref_kv_iter](auto cursor) {
              auto p_kv = **ref_kv_iter;
              validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
              // validate values in cursors keep intact
              validate_cursor_from_item(p_kv->key, p_kv->value, *c_iter);
              ++(*ref_kv_iter);
              ++c_iter;
              return seastar::stop_iteration::no;
            });
          });
        });
      } else {
        return eagain_iertr::now();
      }
    });
  }

  eagain_ifuture<> erase_one(
      Transaction& t, const iterator_t& iter_rd) {
    auto p_kv = *iter_rd;
    logger().debug("[{}] erase {} -> {}",
                   iter_rd - kvs.random_begin(),
                   key_hobj_t{p_kv->key},
                   p_kv->value);
    return tree->erase(t, p_kv->key
    ).si_then([&t, this, p_kv] (auto size) {
      boost::ignore_unused(t);  // avoid clang warning;
      boost::ignore_unused(this);
      boost::ignore_unused(p_kv);
      ceph_assert(size == 1);
#ifndef NDEBUG
      return tree->contains(t, p_kv->key
      ).si_then([] (bool ret) {
        ceph_assert(ret == false);
      });
#else
      return eagain_iertr::now();
#endif
    });
  }

  eagain_ifuture<> erase(Transaction& t, std::size_t erase_size) {
    assert(erase_size <= kvs.size());
    kvs.shuffle();
    auto erase_end = kvs.random_begin() + erase_size;
    auto ref_kv_iter = seastar::make_lw_shared<iterator_t>();
    auto cursors = seastar::make_lw_shared<std::map<ghobject_t, BtreeCursor>>();
    return eagain_iertr::now().si_then([&t, this, cursors, ref_kv_iter] {
      (void)this; // silence clang warning for !TRACK
      (void)t; // silence clang warning for !TRACK
      if constexpr (TRACK) {
        logger().info("Tracking cursors before erase ...");
        *ref_kv_iter = kvs.begin();
        auto start_time = mono_clock::now();
        return trans_intr::repeat(
          [&t, this, cursors, ref_kv_iter, start_time] ()
          -> eagain_ifuture<seastar::stop_iteration> {
          if (*ref_kv_iter == kvs.end()) {
            std::chrono::duration<double> duration = mono_clock::now() - start_time;
            logger().info("Track done! {}s", duration.count());
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          }
          auto p_kv = **ref_kv_iter;
          return tree->find(t, p_kv->key).si_then([cursors, ref_kv_iter](auto cursor) {
            auto p_kv = **ref_kv_iter;
            validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
            cursors->emplace(p_kv->key, cursor);
            ++(*ref_kv_iter);
            return seastar::stop_iteration::no;
          });
        });
      } else {
        return eagain_iertr::now();
      }
    }).si_then([&t, this, ref_kv_iter, erase_end] {
      *ref_kv_iter = kvs.random_begin();
      logger().warn("start erasing {}/{} kvs ...",
                    erase_end - kvs.random_begin(), kvs.size());
      auto start_time = mono_clock::now();
      return trans_intr::repeat([&t, this, ref_kv_iter,
                              start_time, erase_end] ()
        -> eagain_ifuture<seastar::stop_iteration> {
        if (*ref_kv_iter == erase_end) {
          std::chrono::duration<double> duration = mono_clock::now() - start_time;
          logger().warn("Erase done! {}s", duration.count());
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        } else {
          return erase_one(t, *ref_kv_iter
          ).si_then([ref_kv_iter] {
            ++(*ref_kv_iter);
            return seastar::stop_iteration::no;
          });
        }
      });
    }).si_then([this, cursors, ref_kv_iter, erase_end] {
      if constexpr (TRACK) {
        logger().info("Verifing tracked cursors ...");
        *ref_kv_iter = kvs.random_begin();
        while (*ref_kv_iter != erase_end) {
          auto p_kv = **ref_kv_iter;
          auto c_it = cursors->find(p_kv->key);
          ceph_assert(c_it != cursors->end());
          ceph_assert(c_it->second.is_end());
          cursors->erase(c_it);
          ++(*ref_kv_iter);
        }
      }
      kvs.erase_from_random(kvs.random_begin(), erase_end);
      if constexpr (TRACK) {
        *ref_kv_iter = kvs.begin();
        for (auto& [k, c] : *cursors) {
          assert(*ref_kv_iter != kvs.end());
          auto p_kv = **ref_kv_iter;
          validate_cursor_from_item(p_kv->key, p_kv->value, c);
          ++(*ref_kv_iter);
        }
        logger().info("Verify done!");
      }
    });
  }

  eagain_ifuture<> get_stats(Transaction& t) {
    return tree->get_stats_slow(t
    ).si_then([](auto stats) {
      logger().warn("{}", stats);
    });
  }

  eagain_ifuture<std::size_t> height(Transaction& t) {
    return tree->height(t);
  }

  void reload(NodeExtentManagerURef&& nm) {
    tree.emplace(std::move(nm));
  }

  eagain_ifuture<> validate_one(
      Transaction& t, const iterator_t& iter_seq) {
    assert(iter_seq != kvs.end());
    auto next_iter = iter_seq + 1;
    auto p_kv = *iter_seq;
    return tree->find(t, p_kv->key
    ).si_then([p_kv, &t] (auto cursor) {
      validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
      return cursor.get_next(t);
    }).si_then([next_iter, this] (auto cursor) {
      if (next_iter == kvs.end()) {
        ceph_assert(cursor.is_end());
      } else {
        auto p_kv = *next_iter;
        validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
      }
    });
  }

  eagain_ifuture<> validate(Transaction& t) {
    logger().info("Verifing inserted ...");
    return seastar::do_with(
      kvs.begin(),
      [this, &t] (auto &iter) {
      return trans_intr::repeat(
        [this, &t, &iter]() ->eagain_iertr::future<seastar::stop_iteration> {
        if (iter == kvs.end()) {
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        return validate_one(t, iter).si_then([&iter] {
          ++iter;
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::no);
        });
      });
    });
  }

 private:
  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }

  KVPool<ValueItem>& kvs;
  std::optional<BtreeImpl> tree;
};

}
