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

#include "crimson/common/log.h"
#include "stages/key_layout.h"
#include "tree.h"
#include "test/crimson/seastore/onode_tree/test_value.h"

/**
 * tree_utils.h
 *
 * Contains shared logic for unit tests and perf tool.
 */

namespace crimson::os::seastore::onode {

using TestBtree = Btree<TestValue>;

struct value_item_t {
  value_size_t size;
  TestValue::id_t id;
  TestValue::magic_t magic;

  TestBtree::tree_value_config_t get_config() const {
    assert(size > sizeof(value_header_t));
    return {static_cast<value_size_t>(size - sizeof(value_header_t))};
  }
};
inline std::ostream& operator<<(std::ostream& os, const value_item_t& item) {
  return os << "ValueItem(#" << item.id << ", " << item.size << "B)";
}

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

  value_item_t create(size_t _size) {
    ceph_assert(_size <= std::numeric_limits<value_size_t>::max());
    ceph_assert(_size > sizeof(value_header_t));
    value_size_t size = _size;
    auto current_id = id++;
    return value_item_t{size, current_id, (TestValue::magic_t)current_id * 137};
  }

  value_item_t pick() const {
    auto index = rd() % values.size();
    return values[index];
  }

  static void initialize_cursor(
      Transaction& t,
      TestBtree::Cursor& cursor,
      const value_item_t& item) {
    ceph_assert(!cursor.is_end());
    auto value = cursor.value();
    ceph_assert(value.get_payload_size() + sizeof(value_header_t) == item.size);
    value.set_id_replayable(t, item.id);
    value.set_tail_magic_replayable(t, item.magic);
  }

  static void validate_cursor(
      TestBtree::Cursor& cursor,
      const ghobject_t& key,
      const value_item_t& item) {
    ceph_assert(!cursor.is_end());
    ceph_assert(cursor.get_ghobj() == key);
    auto value = cursor.value();
    ceph_assert(value.get_payload_size() + sizeof(value_header_t) == item.size);
    ceph_assert(value.get_id() == item.id);
    ceph_assert(value.get_tail_magic() == item.magic);
  }

 private:
  TestValue::id_t id = 0;
  mutable std::random_device rd;
  std::vector<value_item_t> values;
};

class KVPool {
  struct kv_conf_t {
    index_t index2;
    index_t index1;
    index_t index0;
    size_t ns_size;
    size_t oid_size;
    value_item_t value;

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
  using kv_t = std::pair<ghobject_t, value_item_t>;

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
  Values values;
  kv_vector_t kvs;
  kv_vector_t random_kvs;
};

template <bool TRACK>
class TreeBuilder {
 public:
  using ertr = TestBtree::btree_ertr;
  template <class ValueT=void>
  using future = ertr::future<ValueT>;

  TreeBuilder(KVPool& kvs, NodeExtentManagerURef&& nm)
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
    auto cursors = seastar::make_lw_shared<std::vector<TestBtree::Cursor>>();
    logger().warn("start inserting {} kvs ...", kvs.size());
    auto start_time = mono_clock::now();
    return crimson::do_until([&t, this, cursors]() -> future<bool> {
      if (kv_iter.is_end()) {
        return ertr::make_ready_future<bool>(true);
      }
      auto [key, value] = kv_iter.get_kv();
      logger().debug("[{}] {} -> {}", kv_iter.index(), key_hobj_t{key}, value);
      return tree->insert(t, key, value.get_config()
      ).safe_then([&t, this, cursors, value](auto ret) {
        auto& [cursor, success] = ret;
        assert(success == true);
        Values::initialize_cursor(t, cursor, value);
        if constexpr (TRACK) {
          cursors->emplace_back(cursor);
        }
#ifndef NDEBUG
        auto [key, value] = kv_iter.get_kv();
        Values::validate_cursor(cursor, key, value);
        return tree->lower_bound(t, key
        ).safe_then([this, cursor](auto cursor_) mutable {
          auto [key, value] = kv_iter.get_kv();
          ceph_assert(cursor_.get_ghobj() == key);
          ceph_assert(cursor_.value() == cursor.value());
          Values::validate_cursor(cursor_, key, value);
          ++kv_iter;
          return ertr::make_ready_future<bool>(false);
        });
#else
        ++kv_iter;
        return ertr::make_ready_future<bool>(false);
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
              return ertr::make_ready_future<bool>(true);
            }
            assert(c_iter != cursors->end());
            auto [k, v] = kv_iter.get_kv();
            // validate values in tree keep intact
            return tree->lower_bound(t, k).safe_then([this, &c_iter](auto cursor) {
              auto [k, v] = kv_iter.get_kv();
              Values::validate_cursor(cursor, k, v);
              // validate values in cursors keep intact
              Values::validate_cursor(*c_iter, k, v);
              ++kv_iter;
              ++c_iter;
              return ertr::make_ready_future<bool>(false);
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
    logger().info("Verifing insertion ...");
    return seastar::do_with(
        kvs.begin(), [&t, this] (auto& kvs_iter) {
      return crimson::do_until([&t, this, &kvs_iter]() -> future<bool> {
        if (kvs_iter.is_end()) {
          logger().info("Verify done!");
          return ertr::make_ready_future<bool>(true);
        }
        auto [k, v] = kvs_iter.get_kv();
        return tree->lower_bound(t, k
        ).safe_then([&kvs_iter, k=k, v=v] (auto cursor) {
          Values::validate_cursor(cursor, k, v);
          ++kvs_iter;
          return ertr::make_ready_future<bool>(false);
        });
      });
    });
  }

 private:
  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }

  KVPool& kvs;
  std::optional<TestBtree> tree;
  KVPool::iterator_t kv_iter;
};

}
