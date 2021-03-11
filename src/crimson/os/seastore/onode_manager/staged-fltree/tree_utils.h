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
  struct kv_conf_t {
    index_t index2;
    index_t index1;
    index_t index0;
    size_t ns_size;
    size_t oid_size;
    ValueItem value;

    ghobject_t get_ghobj() const {
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
  };
  using kv_vector_t = std::vector<kv_conf_t>;

 public:
  using kv_t = std::pair<ghobject_t, ValueItem>;

  KVPool(const std::vector<size_t>& str_sizes,
         const std::vector<size_t>& value_sizes,
         const std::pair<index_t, index_t>& range2,
         const std::pair<index_t, index_t>& range1,
         const std::pair<index_t, index_t>& range0)
      : str_sizes{str_sizes}, values{value_sizes} {
    ceph_assert(range2.first < range2.second);
    ceph_assert(range2.second - 1 <= (index_t)std::numeric_limits<shard_t>::max());
    ceph_assert(range2.second - 1 <= std::numeric_limits<crush_hash_t>::max());
    ceph_assert(range1.first < range1.second);
    ceph_assert(range1.second - 1 <= 9);
    ceph_assert(range0.first < range0.second);
    std::random_device rd;
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
          kvs.emplace_back(kv_conf_t{i, j, k, ns_size, oid_size, values.pick()});
        }
      }
    }
    random_kvs = kvs;
    std::random_shuffle(random_kvs.begin(), random_kvs.end());
  }

  class iterator_t {
   public:
    iterator_t() = default;
    iterator_t(const iterator_t&) = default;
    iterator_t(iterator_t&&) = default;
    iterator_t& operator=(const iterator_t&) = default;
    iterator_t& operator=(iterator_t&&) = default;

    kv_t get_kv() const {
      assert(!is_end());
      auto& conf = (*p_kvs)[i];
      return std::make_pair(conf.get_ghobj(), conf.value);
    }
    bool is_end() const { return !p_kvs || i >= p_kvs->size(); }
    index_t index() const { return i; }

    iterator_t& operator++() {
      assert(!is_end());
      ++i;
      return *this;
    }

    iterator_t operator++(int) {
      iterator_t tmp = *this;
      ++*this;
      return tmp;
    }

   private:
    iterator_t(const kv_vector_t& kvs) : p_kvs{&kvs} {}

    const kv_vector_t* p_kvs = nullptr;
    index_t i = 0;
    friend class KVPool;
  };

  iterator_t begin() const {
    return iterator_t(kvs);
  }

  iterator_t random_begin() const {
    return iterator_t(random_kvs);
  }

  size_t size() const {
    return kvs.size();
  }

 private:
  std::vector<size_t> str_sizes;
  Values<ValueItem> values;
  kv_vector_t kvs;
  kv_vector_t random_kvs;
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
      if (kv_iter.is_end()) {
        return ertr::template make_ready_future<bool>(true);
      }
      auto [key, value] = kv_iter.get_kv();
      logger().debug("[{}] {} -> {}", kv_iter.index(), key_hobj_t{key}, value);
      return tree->insert(
          t, key, {value.get_payload_size()}
      ).safe_then([&t, this, cursors, key, value](auto ret) {
        auto& [cursor, success] = ret;
        initialize_cursor_from_item(t, key, value, cursor, success);
        if constexpr (TRACK) {
          cursors->emplace_back(cursor);
        }
#ifndef NDEBUG
        auto [key, value] = kv_iter.get_kv();
        validate_cursor_from_item(key, value, cursor);
        return tree->find(t, key
        ).safe_then([this, cursor](auto cursor_) mutable {
          assert(!cursor_.is_end());
          auto [key, value] = kv_iter.get_kv();
          ceph_assert(cursor_.get_ghobj() == key);
          ceph_assert(cursor_.value() == cursor.value());
          validate_cursor_from_item(key, value, cursor_);
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
            if (kv_iter.is_end()) {
              logger().info("Verify done!");
              return ertr::template make_ready_future<bool>(true);
            }
            assert(c_iter != cursors->end());
            auto [k, v] = kv_iter.get_kv();
            // validate values in tree keep intact
            return tree->find(t, k).safe_then([this, &c_iter](auto cursor) {
              auto [k, v] = kv_iter.get_kv();
              validate_cursor_from_item(k, v, cursor);
              // validate values in cursors keep intact
              validate_cursor_from_item(k, v, *c_iter);
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
      auto iter = kvs.begin();
      while (!iter.is_end()) {
        auto [k, v] = iter.get_kv();
        auto cursor = tree->find(t, k).unsafe_get0();
        validate_cursor_from_item(k, v, cursor);
        ++iter;
      }

      logger().info("Verifing range query ...");
      iter = kvs.begin();
      auto cursor = tree->begin(t).unsafe_get0();
      while (!iter.is_end()) {
        assert(!cursor.is_end());
        auto [k, v] = iter.get_kv();
        validate_cursor_from_item(k, v, cursor);
        cursor = cursor.get_next(t).unsafe_get0();
        ++iter;
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
