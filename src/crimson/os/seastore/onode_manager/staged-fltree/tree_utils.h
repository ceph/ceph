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
 * ValueItem template to work with tree utility classes:
 *
 * struct ValueItem {
 *   using ValueType = ConcreteValueType;
 *   <public members>
 *
 *   value_size_t get_payload_size() const;
 *   void initialize(Transaction& t, ValueType& value) const;
 *   void validate(ValueType& value) const;
 *   static ValueItem create(std::size_t expected_size, std::size_t id);
 * };
 * std::ostream& operator<<(std::ostream& os, const ValueItem& item);
 */

template <typename ValueItem>
void initialize_cursor_from_item(
    Transaction& t,
    const ghobject_t& key,
    const ValueItem& item,
    typename Btree<typename ValueItem::ValueType>::Cursor& cursor,
    bool insert_success) {
  ceph_assert(insert_success);
  ceph_assert(!cursor.is_end());
  ceph_assert(cursor.get_ghobj() == key);
  auto tree_value = cursor.value();
  item.initialize(t, tree_value);
}


template <typename ValueItem>
void validate_cursor_from_item(
    const ghobject_t& key,
    const ValueItem& item,
    typename Btree<typename ValueItem::ValueType>::Cursor& cursor) {
  ceph_assert(!cursor.is_end());
  ceph_assert(cursor.get_ghobj() == key);
  auto value = cursor.value();
  item.validate(value);
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
    std::random_shuffle(random_p_kvs.begin(), random_p_kvs.end());
  }

  static KVPool create_raw_range(
      const std::vector<size_t>& str_sizes,
      const std::vector<size_t>& value_sizes,
      const std::pair<index_t, index_t>& range2,
      const std::pair<index_t, index_t>& range1,
      const std::pair<index_t, index_t>& range0) {
    ceph_assert(range2.first < range2.second);
    ceph_assert(range2.second - 1 <= (index_t)std::numeric_limits<shard_t>::max());
    ceph_assert(range2.second - 1 <= std::numeric_limits<crush_hash_t>::max());
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
          ns_size = str_sizes[rd() % str_sizes.size()];
          oid_size = str_sizes[rd() % str_sizes.size()];
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
      const std::vector<size_t>& value_sizes) {
    kv_vector_t kvs;
    std::random_device rd;
    for (index_t i = range_i.first; i < range_i.second; ++i) {
      auto value_size = value_sizes[rd() % value_sizes.size()];
      kvs.emplace_back(
          kv_t{make_oid(i), ValueItem::create(value_size, i)}
      );
    }
    return KVPool(std::move(kvs));
  }

 private:
  KVPool(kv_vector_t&& _kvs)
      : kvs(std::move(_kvs)), serial_p_kvs(kvs.size()), random_p_kvs(kvs.size()) {
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
    ret.hobj.nspace = "asdf";
    return ret;
  }

  kv_vector_t kvs;
  kvptr_vector_t serial_p_kvs;
  kvptr_vector_t random_p_kvs;
};

template <bool TRACK, typename ValueItem>
class TreeBuilder {
 public:
  using BtreeImpl = Btree<typename ValueItem::ValueType>;
  using BtreeCursor = typename BtreeImpl::Cursor;
  using ertr = typename BtreeImpl::btree_ertr;
  template <class ValueT=void>
  using future = typename ertr::template future<ValueT>;

  TreeBuilder(KVPool<ValueItem>& kvs, NodeExtentManagerURef&& nm)
      : kvs{kvs} {
    tree.emplace(std::move(nm));
  }

  future<> bootstrap(Transaction& t) {
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

  future<> insert(Transaction& t) {
    kv_iter = kvs.random_begin();
    auto cursors = seastar::make_lw_shared<std::vector<BtreeCursor>>();
    logger().warn("start inserting {} kvs ...", kvs.size());
    auto start_time = mono_clock::now();
    return crimson::do_until([&t, this, cursors]() -> future<bool> {
      if (kv_iter == kvs.random_end()) {
        return ertr::template make_ready_future<bool>(true);
      }
      auto p_kv = *kv_iter;
      logger().debug("[{}] {} -> {}",
                     kv_iter - kvs.random_begin(),
                     key_hobj_t{p_kv->key},
                     p_kv->value);
      return tree->insert(
          t, p_kv->key, {p_kv->value.get_payload_size()}
      ).safe_then([&t, this, cursors](auto ret) {
        auto p_kv = *kv_iter;
        auto& [cursor, success] = ret;
        initialize_cursor_from_item(t, p_kv->key, p_kv->value, cursor, success);
        if constexpr (TRACK) {
          cursors->emplace_back(cursor);
        }
#ifndef NDEBUG
        validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
        return tree->find(t, p_kv->key
        ).safe_then([this, cursor](auto cursor_) mutable {
          assert(!cursor_.is_end());
          auto p_kv = *kv_iter;
          ceph_assert(cursor_.get_ghobj() == p_kv->key);
          ceph_assert(cursor_.value() == cursor.value());
          validate_cursor_from_item(p_kv->key, p_kv->value, cursor_);
          ++kv_iter;
          return ertr::template make_ready_future<bool>(false);
        });
#else
        ++kv_iter;
        return ertr::template make_ready_future<bool>(false);
#endif
      });
    }).safe_then([&t, this, start_time, cursors] {
      std::chrono::duration<double> duration = mono_clock::now() - start_time;
      logger().warn("Insert done! {}s", duration.count());
      if (!cursors->empty()) {
        logger().info("Verifing tracked cursors ...");
        kv_iter = kvs.random_begin();
        return seastar::do_with(
            cursors->begin(), [&t, this, cursors](auto& c_iter) {
          return crimson::do_until([&t, this, &c_iter, cursors]() -> future<bool> {
            if (kv_iter == kvs.random_end()) {
              logger().info("Verify done!");
              return ertr::template make_ready_future<bool>(true);
            }
            assert(c_iter != cursors->end());
            auto p_kv = *kv_iter;
            // validate values in tree keep intact
            return tree->find(t, p_kv->key).safe_then([this, &c_iter](auto cursor) {
              auto p_kv = *kv_iter;
              validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
              // validate values in cursors keep intact
              validate_cursor_from_item(p_kv->key, p_kv->value, *c_iter);
              ++kv_iter;
              ++c_iter;
              return ertr::template make_ready_future<bool>(false);
            });
          });
        });
      } else {
        return ertr::now();
      }
    });
  }

  future<> get_stats(Transaction& t) {
    return tree->get_stats_slow(t
    ).safe_then([this](auto stats) {
      logger().warn("{}", stats);
    });
  }

  void reload(NodeExtentManagerURef&& nm) {
    tree.emplace(std::move(nm));
  }

  future<> validate(Transaction& t) {
    return seastar::async([this, &t] {
      logger().info("Verifing insertion ...");
      for (auto& p_kv : kvs) {
        auto cursor = tree->find(t, p_kv->key).unsafe_get0();
        validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
      }

      logger().info("Verifing range query ...");
      auto cursor = tree->begin(t).unsafe_get0();
      for (auto& p_kv : kvs) {
        assert(!cursor.is_end());
        validate_cursor_from_item(p_kv->key, p_kv->value, cursor);
        cursor = cursor.get_next(t).unsafe_get0();
      }
      assert(cursor.is_end());

      logger().info("Verify done!");
    });
  }

 private:
  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }

  KVPool<ValueItem>& kvs;
  std::optional<BtreeImpl> tree;
  typename KVPool<ValueItem>::iterator_t kv_iter;
};

}
