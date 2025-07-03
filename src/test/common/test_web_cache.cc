#include <gmock/gmock-matchers.h>

#include <atomic>
#include <chrono>
#include <include/expected.hpp>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <system_error>
#include <thread>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/web_cache.h"
#include "gtest/gtest.h"
#include "include/ceph_assert.h"
#include "include/msgr.h"
#include "include/types.h"
#include "include/uuid.h"
#include "log/Log.h"

using namespace std::chrono_literals;

namespace webcache {

class WebCacheTest : public ::testing::Test {
 protected:
  static constexpr size_t SIEVE_EXAMPLE_CACHE_SIZE = 7;
  const std::string a_key = "testkey";
  const std::string a_value = "testvalue";
  const std::shared_ptr<std::string> a_valptr =
      std::make_shared<std::string>(a_value);

  const std::unique_ptr<CephContext> _cct;
  size_t _initialized_capacity;
  std::unique_ptr<WebCache<std::string, std::string>> _uut;

  WebCacheTest()
      : _cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        _initialized_capacity(SIEVE_EXAMPLE_CACHE_SIZE),
        _uut(std::make_unique<WebCache<std::string, std::string>>(
            SIEVE_EXAMPLE_CACHE_SIZE)) {
    _cct->_log->start();
  }
  void TearDown() override { lderr(_cct.get()) << "AFTER: " << *_uut << dendl; }

  void reset_cache(size_t capacity) {
    _uut = std::make_unique<WebCache<std::string, std::string>>(capacity);
    _initialized_capacity = capacity;
  }
  void reset_cache_system_mode(size_t capacity) {
    _uut = std::make_unique<WebCache<std::string, std::string>>(
        _cct.get(), "testing", capacity);
    _initialized_capacity = capacity;
  }

  std::vector<std::string> sieve_queue_keys() {
    std::vector<std::string> keys;
    std::transform(
        _uut->_sieve_queue.begin(), _uut->_sieve_queue.end(),
        std::back_inserter(keys),
        [](const auto& node) { return *(node->key); });
    return keys;
  }
  size_t sieve_hand() { return _uut->_sieve_hand; }
};

TEST_F(WebCacheTest, AddReturnsGreaterZeroTs) {
  auto added_ts = _uut->add(a_key, a_valptr);
  ASSERT_GT(added_ts, ceph::real_clock::zero());
}

TEST_F(WebCacheTest, MultipleAddsToEmptyReturnFirstTs) {
  const std::vector<ceph::real_time> inserts = {
      _uut->add(a_key, a_valptr),
      _uut->add(a_key, a_valptr),
      _uut->add(a_key, a_valptr),
      _uut->add(a_key, a_valptr),
  };
  ASSERT_THAT(inserts, ::testing::Each(inserts[0]));
}

TEST_F(WebCacheTest, TwoSameKeyAddsGivesSize1) {
  const auto _ = {
      _uut->add(a_key, a_valptr),
      _uut->add(a_key, a_valptr),
      _uut->add(a_key, a_valptr),
  };
  ASSERT_THAT(_uut->size(), 1);
}

TEST_F(WebCacheTest, TwoDistinctKeyAddsGiveSize2) {
  const auto _ = {
      _uut->add("a", a_valptr),
      _uut->add("b", a_valptr),
  };
  ASSERT_THAT(_uut->size(), 2);
}

TEST_F(WebCacheTest, LookupNonExistingReturnsIsNullopt) {
  ASSERT_FALSE(_uut->lookup("not-existing").has_value());
}

TEST_F(WebCacheTest, AddDoesNotOverwrite) {
  _uut->add(a_key, a_valptr);
  ASSERT_EQ(_uut->lookup(a_key).value(), a_valptr);

  auto another_valptr = std::make_shared<std::string>("another-value");
  _uut->add(a_key, another_valptr);
  ASSERT_EQ(_uut->lookup(a_key).value(), a_valptr);
}

TEST_F(WebCacheTest, LookupReturnsWhatWasAdded) {
  const std::string key = "testkey";
  const std::string value = "testvalue";
  auto addptr = std::make_shared<std::string>(value);
  _uut->add(key, addptr);

  auto maybe_val = _uut->lookup(key);
  ASSERT_TRUE(maybe_val);
  ASSERT_EQ(value, *maybe_val.value());
}

TEST_F(WebCacheTest, AddsOverCapacityDontGoOver) {
  for (size_t i = 0; i < (_initialized_capacity + 10); ++i) {
    _uut->add(std::to_string(i), a_valptr);
  }
  ASSERT_EQ(_initialized_capacity, _uut->size());
}

TEST_F(WebCacheTest, AddALotUnique) {
  reset_cache(100);
  for (size_t i = 0; i < 10000; ++i) {
    uuid_d uuid;
    uuid.generate_random();
    _uut->add(uuid.to_string(), a_valptr);
  }
}

TEST_F(WebCacheTest, CacheTakesValOwnership) {
  const std::string a_key = "testkey";
  const std::string a_value = "testvalue";
  std::shared_ptr<std::string> a_valptr =
      std::make_shared<std::string>(a_value);
  ASSERT_EQ(1, a_valptr.use_count());
  _uut->add(a_key, a_valptr);
  ASSERT_EQ(2, a_valptr.use_count());
  auto retrieved = _uut->lookup(a_key).value();
  ASSERT_EQ(3, retrieved.use_count());
  a_valptr.reset();
  ASSERT_EQ(2, retrieved.use_count());
}

TEST_F(WebCacheTest, SimpleLookupOr) {
  auto value = _uut->lookup_or(a_key, std::make_shared<std::string>("test"));
  ASSERT_EQ("test", *value);
}

TEST_F(WebCacheTest, LookupOrAddsToCache) {
  auto future = _uut->lookup_or(a_key, std::make_shared<std::string>("test"));
  ASSERT_EQ("test", *_uut->lookup(a_key).value());
}

TEST_F(WebCacheTest, ExpireEraseOne) {
  WebCache<std::string, std::string>::Node alive_node(
      a_valptr, ceph::real_clock::from_time_t(300));
  WebCache<std::string, std::string>::Node expired_node(
      a_valptr, ceph::real_clock::from_time_t(10));
  std::list<WebCache<std::string, std::string>::Node*> nodes = {
      &alive_node, &expired_node};

  lderr(_cct.get()) << "NODES:         " << &alive_node << ":" << alive_node
                    << ", " << &expired_node << ":" << expired_node << dendl;
  lderr(_cct.get()) << "BEFORE EXPIRE: " << nodes << dendl;
  EXPECT_EQ(2, nodes.size());
  const auto fake_now = ceph::real_clock::from_time_t(301);
  std::vector<WebCache<std::string, std::string>::Node*> expired;
  WebCache<std::string, std::string>::sieve_expire_erase_unmutexed(
      nodes, 1min, fake_now, expired);
  lderr(_cct.get()) << "AFTER EXPIRE:  " << nodes << dendl;
  lderr(_cct.get()) << "EXPIRED:       " << expired << dendl;
  EXPECT_EQ(1, nodes.size());
  ASSERT_EQ(1, expired.size());
  EXPECT_EQ(expired[0], &expired_node);
}

TEST_F(WebCacheTest, ExpireEraseAll) {
  WebCache<std::string, std::string>::Node expired_node_1(
      a_valptr, ceph::real_clock::from_time_t(23));
  WebCache<std::string, std::string>::Node expired_node_2(
      a_valptr, ceph::real_clock::from_time_t(42));
  std::list<WebCache<std::string, std::string>::Node*> nodes = {
      &expired_node_1, &expired_node_2};
  lderr(_cct.get()) << "NODES:         " << &expired_node_1 << ":"
                    << expired_node_1 << ", " << &expired_node_2 << ":"
                    << expired_node_2 << dendl;
  lderr(_cct.get()) << "BEFORE EXPIRE: " << nodes << dendl;
  EXPECT_EQ(2, nodes.size());
  const auto fake_now = ceph::real_clock::from_time_t(300);
  std::vector<WebCache<std::string, std::string>::Node*> expired;
  WebCache<std::string, std::string>::sieve_expire_erase_unmutexed(
      nodes, 1min, fake_now, expired);
  lderr(_cct.get()) << "AFTER EXPIRE:  " << nodes << dendl;
  lderr(_cct.get()) << "EXPIRED:       " << expired << dendl;
  EXPECT_EQ(0, nodes.size());
  ASSERT_EQ(2, expired.size());
  ASSERT_THAT(
      expired, ::testing::ElementsAre(&expired_node_1, &expired_node_2));
}

TEST_F(WebCacheTest, ExpireEraseEmpty) {
  std::list<WebCache<std::string, std::string>::Node*> nodes = {};
  const auto fake_now = ceph::real_clock::from_time_t(301);
  std::vector<WebCache<std::string, std::string>::Node*> expired;
  WebCache<std::string, std::string>::sieve_expire_erase_unmutexed(
      nodes, 1min, fake_now, expired);
  ASSERT_EQ(0, nodes.size());
  ASSERT_EQ(0, expired.size());
}

TEST_F(WebCacheTest, CacheHasSizeZeroAfterClear) {
  _uut->add("a", a_valptr);
  _uut->add("b", a_valptr);
  _uut->add("c", a_valptr);
  ASSERT_EQ(3, _uut->size());
  ASSERT_EQ(3, _uut->clear());
  ASSERT_EQ(0, _uut->size());
}

TEST_F(WebCacheTest, SieveExample) {
  // Example from https://cachemon.github.io/SIEVE-website/
  _uut->add("a", a_valptr);
  _uut->add("b", a_valptr);
  _uut->add("c", a_valptr);
  _uut->add("d", a_valptr);
  _uut->add("e", a_valptr);
  _uut->add("f", a_valptr);
  _uut->add("g", a_valptr);

  _uut->add("a", a_valptr);
  _uut->add("b", a_valptr);
  _uut->add("g", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("g", "f", "e", "d", "c", "b", "a"));
  ASSERT_EQ(sieve_hand(), 6);

  _uut->add("h", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("h", "g", "f", "e", "d", "b", "a"));
  ASSERT_EQ(sieve_hand(), 4);

  _uut->add("a", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("h", "g", "f", "e", "d", "b", "a"));
  ASSERT_EQ(sieve_hand(), 4);

  _uut->add("d", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("h", "g", "f", "e", "d", "b", "a"));
  ASSERT_EQ(sieve_hand(), 4);

  _uut->add("i", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("i", "h", "g", "f", "d", "b", "a"));
  ASSERT_EQ(sieve_hand(), 3);

  _uut->add("b", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("i", "h", "g", "f", "d", "b", "a"));
  ASSERT_EQ(sieve_hand(), 3);

  _uut->add("j", a_valptr);
  ASSERT_THAT(
      sieve_queue_keys(),
      ::testing::ElementsAre("j", "i", "h", "g", "d", "b", "a"));
  ASSERT_EQ(sieve_hand(), 3);
}

TEST_F(WebCacheTest, SieveAllVisited) {
  reset_cache(2);
  _uut->add("a", a_valptr);
  _uut->add("b", a_valptr);
  _uut->add("a", a_valptr);
  _uut->add("b", a_valptr);
  _uut->add("c", a_valptr);
}

class WebCacheConcurrencyTest : public WebCacheTest {
  void TearDown() override {
    if (_uut->perf() != nullptr) {
      JSONFormatter f(true);
      _uut->perf()->dump_formatted(&f, false, select_labeled_t::labeled);
      f.flush(std::cout);
      _uut->perf()->reset();
    }
  }
};

TEST_F(WebCacheConcurrencyTest, BasicAddSame) {
  reset_cache_system_mode(100);
  const auto num_threads =
      std::max(std::thread::hardware_concurrency() * 100, 100U);
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      for (size_t j = 0; j < 1000; ++j) {
        _uut->add(a_key, a_valptr);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  ASSERT_EQ(1, _uut->size());
  ASSERT_TRUE(_uut->lookup(a_key).has_value());
}

TEST_F(WebCacheConcurrencyTest, BasicAddUnique) {
  reset_cache_system_mode(100);
  const auto num_threads =
      std::max(std::thread::hardware_concurrency() * 100, 100U);
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      for (size_t j = 0; j < 10; ++j) {
        uuid_d uuid;
        uuid.generate_random();
        _uut->add(uuid.to_string(), a_valptr);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

// Example: Mitigate cache stampedes using std::call_once
TEST_F(WebCacheConcurrencyTest, StampedeSyncCallOnce) {
  struct CacheValue {
    std::once_flag once;
    std::string value;
  };
  webcache::WebCache<std::string, CacheValue> cache(
      _cct.get(), "test_web_cache", 100);
  std::atomic_int fetches = 0;
  const auto num_threads =
      std::max(std::thread::hardware_concurrency() * 100, 100U);
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      std::shared_ptr<CacheValue> cache_value =
          cache.lookup_or(a_key, std::make_shared<CacheValue>());
      std::call_once(cache_value->once, [cache_value, &fetches]() {
        fetches++;
        std::this_thread::sleep_for(1000ms);
        cache_value->value = "test";
      });
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  ASSERT_EQ(fetches.load(), 1);
}

// Example: Mitigate cache stampedes using a mutex per value
TEST_F(WebCacheConcurrencyTest, StampedeMutex) {
  struct CacheValue {
    std::mutex mutex;
    std::string value;
    std::function<std::string()> fn;
    CacheValue(std::function<std::string()> fn) : fn(fn) {}
    std::string get() {
      std::unique_lock<std::mutex> lock(mutex);
      if (value.empty()) {
        value = fn();
      }
      return value;
    }
  };
  webcache::WebCache<std::string, CacheValue> cache(
      _cct.get(), "test_web_cache", 100);
  std::atomic_int fetches = 0;
  const auto num_threads =
      std::max(std::thread::hardware_concurrency() * 100, 100U);
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      std::shared_ptr<CacheValue> cache_value =
          cache.lookup_or(a_key, std::make_shared<CacheValue>([&fetches]() {
                            fetches++;
                            std::this_thread::sleep_for(1000ms);
                            return "test";
                          }));
      cache_value->get();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  ASSERT_EQ(fetches.load(), 1);
}

class WebCacheRandomizedTest : public WebCacheTest {
  void TearDown() override {
    if (_uut->perf() != nullptr) {
      JSONFormatter f(true);
      _uut->perf()->dump_formatted(&f, false, select_labeled_t::labeled);
      f.flush(std::cout);
      _uut->perf()->reset();
    }
  }
};

TEST_F(WebCacheRandomizedTest, RandomCallMainOperations) {
  reset_cache_system_mode(1000);
  const size_t ops = 10000;
  const auto num_threads = std::max(std::thread::hardware_concurrency(), 8U);

  std::vector<int> base(100);
  std::iota(base.begin(), base.end(), 0);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, base.size() - 1);
  std::queue<std::string> keys;
  for (size_t i = 0; i < ops; ++i) {
    keys.emplace(std::to_string(base[dist(gen)]));
  }

  std::discrete_distribution<> op_dist({90, 1, 9});

  std::mutex mutex;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      for (size_t op_i = 0; op_i < ops / num_threads; ++op_i) {
        std::string key;
        int op = -1;
        {
          std::unique_lock<std::mutex> lock(mutex);
          key = keys.front();
          keys.pop();
          op = op_dist(gen);
        }

        switch (op) {
          case 0: {  // lookup_or
            auto value = _uut->lookup_or(key, a_valptr);
          } break;
          case 1:  // clear cache
            _uut->clear();
            break;
          case 2:  // expire
            _uut->expire_erase(1s);
            break;
          default:
            ceph_abort("should not happen");
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

}  // namespace webcache
