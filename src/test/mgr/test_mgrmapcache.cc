// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "global/global_context.h"

#include "mgr/mgr_perf_counters.h"
#include "mgr/MgrMapCache.h"
#include "include/scope_guard.h"

#include "gtest/gtest.h"

PerfCounters *perfcounter = nullptr;

using namespace std;

TEST(LFUCache, Get) {
	LFUCache<int> c{{"foo"}, 100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
}

TEST(LFUCache, Erase) {
	LFUCache<int> c{{"foo"}, 100};
	c.insert("foo", 1);
	int foo;
	if (!c.try_get("foo", &foo)) {
		FAIL();
	}
	ASSERT_EQ(foo, 1);
	c.erase("foo");
  try{
    foo = c.get("foo");
    FAIL();
  } catch (std::out_of_range& e) {
    SUCCEED();
  }
}

TEST(LFUCache, Clear) {
	LFUCache<int> c{{"osd_map", "pg_dump", "pg_stats"}, 100};
	c.insert("osd_map", 1);
	c.insert("pg_dump", 2);
	c.insert("pg_stats", 3);
	ASSERT_EQ(c.size(), 3);
	c.clear();
	ASSERT_EQ(c.size(), 0);
	try{
		c.get("osd_map");
		FAIL();
	} catch (std::out_of_range& e) {
		SUCCEED();
	}
}

TEST(LFUCache, NotEnabled) {
	LFUCache<int> c{{"foo"}, 100};
	c.insert("foo", 1);
	int foo = c.get("foo");
	ASSERT_EQ(foo, 1);
	c.set_enabled(false);
  try{
	foo = c.get("foo");
	FAIL();
  } catch (std::out_of_range& e) {
	SUCCEED();
  }
}

TEST(LFUCache, SizeLimit) {
	LFUCache<int> c{{"foo", "osd_map", "pg_dump", "pg_stats", "mon_status"}, 4, true};
	c.insert("foo", 1);
	c.insert("osd_map", 2);
	c.insert("pg_dump", 3);
	c.insert("pg_stats", 4);
	c.get("foo"); // foo hits 1
	c.get("pg_dump"); // pg_dump hits 1
	for (int i = 0; i < 100; ++i) {
		c.get("pg_stats"); // pg_stats hits 100
	}
	c.insert("mon_status", 5); // This should evict "osd_map" since it has the least hits
	ASSERT_EQ(c.size(), 4);
	int foo = c.get("foo");
	int pg_dump = c.get("pg_dump");
	int pg_stats = c.get("pg_stats");
	int mon_status = c.get("mon_status");
	try {
		c.get("osd_map"); // This should throw an exception since it was evicted
		FAIL(); // If nothing throws, this will fail
	} catch (std::out_of_range& e) {
		ASSERT_EQ(foo, 1);
		ASSERT_EQ(pg_dump, 3);
		ASSERT_EQ(pg_stats, 4);
		ASSERT_EQ(mon_status, 5);
		SUCCEED();
	}
}

TEST(LFUCache, HitRatio) {
	LFUCache<int> c{{"osd_map", "pg_dump", "pg_stats"}, 100, true};
	c.insert("osd_map", 1);
	c.insert("pg_dump", 2);
	c.insert("pg_stats", 3);
	c.get("osd_map"); // hits 1
	c.get("osd_map"); // hits 2
	c.get("osd_map"); // hits 3
	c.get("pg_dump"); // hits 4
	std::pair<uint64_t, uint64_t> hit_miss_ratio = {c.get_hits(), c.get_misses()};
	ASSERT_EQ(std::get<1>(hit_miss_ratio), 3);
	ASSERT_EQ(std::get<0>(hit_miss_ratio), 4);
}

TEST(LFUCache, InsertResNewKey) {
  LFUCache<int> c{{"foo"}, 100};
  auto res = c.insert("foo", 42);
  ASSERT_TRUE(res.inserted);
  ASSERT_FALSE(res.replaced.has_value());
  ASSERT_FALSE(res.evicted.has_value());
}

TEST(LFUCache, InsertResReplaced) {
  LFUCache<int> c{{"foo"}, 100};
  c.insert("foo", 1);
  auto res = c.insert("foo", 2);
  ASSERT_TRUE(res.inserted);
  ASSERT_TRUE(res.replaced.has_value());
  ASSERT_EQ(res.replaced.value(), 1);
  ASSERT_FALSE(res.evicted.has_value());
  // new value is live in cache
  int v;
  ASSERT_TRUE(c.try_get("foo", &v));
  ASSERT_EQ(v, 2);
}

TEST(LFUCache, InsertResEvicted) {
  LFUCache<int> c{{"foo", "osd_map", "pg_dump", "pg_stats"}, 3};
  c.insert("osd_map", 10);
  c.insert("pg_dump", 20);
  c.insert("pg_stats", 30);
  // Give pg_dump and pg_stats hits so osd_map has the clear minimum (0 hits)
  c.get("pg_dump");
  c.get("pg_stats");
  auto res = c.insert("foo", 99);
  ASSERT_TRUE(res.inserted);
  ASSERT_FALSE(res.replaced.has_value());
  ASSERT_TRUE(res.evicted.has_value());
  ASSERT_EQ(res.evicted.value(), 10);
}

TEST(LFUCache, InsertResRejectedWhenDisabled) {
  LFUCache<int> c{{"foo"}, 100};
  c.set_enabled(false);
  auto res = c.insert("foo", 1);
  ASSERT_FALSE(res.inserted);
  ASSERT_FALSE(res.replaced.has_value());
  ASSERT_FALSE(res.evicted.has_value());
  ASSERT_EQ(c.size(), 0);
}

TEST(LFUCache, InsertResRejectedForUnknownKey) {
  LFUCache<int> c{{"foo"}, 100};
  auto res = c.insert("not_an_allowed_key", 1);
  ASSERT_FALSE(res.inserted);
  ASSERT_EQ(c.size(), 0);
}

TEST(LFUCache, ZeroCapacityRejectsInsertion) {
  LFUCache<int> cache{{"foo"}, 0};
  EXPECT_FALSE(cache.insert("foo", 1).inserted);
  EXPECT_EQ(cache.size(), 0);
  EXPECT_FALSE(cache.exists("foo"));
}

TEST(LFUCache, InsertResRejectedAfterDisableDuringWait) {
  // Simulate the post-lock enabled re-check: disable the cache, then call
  // insert directly on LFUCache (bypassing the outer can_write_cache guard)
  // by re-enabling just long enough to pass can_write_cache, then disabling.
  // We approximate this by disabling between two sequential inserts and
  // verifying the second is rejected.
  LFUCache<int> c{{"foo"}, 100};
  auto res1 = c.insert("foo", 1);
  ASSERT_TRUE(res1.inserted);
  c.set_enabled(false);
  auto res2 = c.insert("foo", 2);
  ASSERT_FALSE(res2.inserted);
  // cache was cleared by set_enabled(false)
  ASSERT_EQ(c.size(), 0);
}

TEST(LFUCache, ConcurrentReads) {
  LFUCache<int> c{{"foo"}, 100, true};
  c.insert("foo", 42);

  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;

  // Launch 10 threads doing 1000 reads each
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&c, &success_count]() {
      for (int j = 0; j < 1000; ++j) {
        int val;
        if (c.try_get("foo", &val)) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(success_count, 10000);
  ASSERT_EQ(c.get_hits(), 10000);
}

TEST(LFUCache, MoveOnlyValues) {
  LFUCache<std::unique_ptr<int>> cache{{"first", "second"}, 1};
  cache.insert("first", std::make_unique<int>(1));
  auto replaced = cache.insert("first", std::make_unique<int>(2));
  ASSERT_TRUE(replaced.replaced.has_value());
  EXPECT_EQ(**replaced.replaced, 1);
  auto evicted = cache.insert("second", std::make_unique<int>(3));
  ASSERT_TRUE(evicted.evicted.has_value());
  EXPECT_EQ(**evicted.evicted, 2);
  auto copied = cache.lookup("second", [](const auto& value) {
    return std::make_unique<int>(*value);
  });
  ASSERT_TRUE(copied);
  EXPECT_EQ(**copied, 3);

  std::unique_ptr<int> extracted;
  EXPECT_TRUE(cache.extract("second", &extracted));
  EXPECT_EQ(*extracted, 3);
  EXPECT_EQ(cache.size(), 0);
  cache.insert("first", std::make_unique<int>(4));
  std::vector<std::unique_ptr<int>> drained;
  drained.push_back(std::make_unique<int>(5));
  cache.drain(drained);
  ASSERT_EQ(drained.size(), 2);
  EXPECT_EQ(*drained[0], 5);
  EXPECT_EQ(*drained[1], 4);
  EXPECT_EQ(cache.size(), 0);
}

TEST(LFUCache, EvictionPrefersStaleEntries) {
  LFUCache<int> cache{{"first", "second", "third"}, 2};
  cache.insert("first", 1);
  cache.insert("second", 2);
  for (int i = 0; i < 10; ++i) {
    cache.get("first");
  }
  EXPECT_TRUE(cache.invalidate("first"));
  EXPECT_FALSE(cache.invalidate("first"));
  EXPECT_FALSE(cache.exists("first"));
  EXPECT_EQ(cache.size(), 1);
  auto result = cache.insert("third", 3);
  ASSERT_TRUE(result.evicted);
  EXPECT_EQ(*result.evicted, 1);
  EXPECT_EQ(cache.get("second"), 2);
  EXPECT_EQ(cache.get("third"), 3);
}

// Ownership contracts are checked by the type system, including at callers.
static_assert(!std::is_copy_constructible_v<PyObjectRef>);
static_assert(!std::is_constructible_v<PyObjectRef, PyObject*>);
static_assert(!std::is_convertible_v<PyObjectRef, PyObject*>);
static_assert(!std::is_invocable_v<decltype(&MgrMapCache::insert),
                                 MgrMapCache&, std::string_view, PyObject*>);
static_assert(!std::is_invocable_v<decltype(&MgrMapCache::insert),
                                 MgrMapCache&, std::string_view, PyObjectRef&>);
static_assert(std::is_invocable_v<decltype(&MgrMapCache::insert),
                                MgrMapCache&, std::string_view, PyObjectRef>);
template <typename T>
concept CanInspectTemporary = requires(T owner) { std::move(owner).get(); };
template <typename T>
concept CanTransferLvalue = requires(T owner) { owner.into_python(); };
static_assert(!CanInspectTemporary<PyObjectRef>);
static_assert(!CanTransferLvalue<PyObjectRef>);

class PyMapCache : public ::testing::Test {
protected:
  static void SetUpTestSuite() {
    g_ceph_context = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
    Py_Initialize();
  }
  static void TearDownTestSuite() {
    Py_Finalize();
    g_ceph_context->put();
    g_ceph_context = nullptr;
  }
  PyObjectRef make_dict() {
    return PyObjectRef::adopt_new_reference(PyDict_New());
  }
};

TEST_F(PyMapCache, InsertConsumesOwnerAndGetReturnsIndependentOwner) {
  MgrMapCache cache;
  auto value = make_dict();
  PyObject* identity = value.get();
  cache.insert("pg_dump", std::move(value));
  EXPECT_FALSE(value);
  auto result = cache.get("pg_dump");
  ASSERT_TRUE(result);
  EXPECT_EQ(result.get(), identity);
  EXPECT_EQ(Py_REFCNT(result.get()), 2);
  cache.clear();
  EXPECT_EQ(Py_REFCNT(result.get()), 1);
}

TEST_F(PyMapCache, ReplacementAndEvictionReleaseReferences) {
  MgrMapCache cache(1);
  auto first = make_dict();
  auto second = make_dict();
  auto third = make_dict();
  cache.insert("osd_map", first.clone());
  cache.insert("osd_map", second.clone());
  EXPECT_EQ(Py_REFCNT(first.get()), 1);
  cache.insert("pg_dump", third.clone());
  EXPECT_EQ(Py_REFCNT(second.get()), 1);
  cache.clear();
  EXPECT_EQ(Py_REFCNT(third.get()), 1);
}

TEST_F(PyMapCache, RepeatedInvalidationRetainsOnlyOneStaleOwnerPerKey) {
  MgrMapCache cache;
  auto value = make_dict();
  cache.insert("pg_dump", value.clone());
  auto* state = PyEval_SaveThread();
  for (int i = 0; i < 10000; ++i) {
    cache.invalidate("pg_dump");
  }
  PyEval_RestoreThread(state);
  EXPECT_EQ(cache.size(), 0);
  EXPECT_FALSE(cache.exists("pg_dump"));
  EXPECT_EQ(Py_REFCNT(value.get()), 2);
  // A normal Python lookup retires the stale entry outside the cache lock.
  EXPECT_FALSE(cache.get("pg_dump"));
  EXPECT_EQ(Py_REFCNT(value.get()), 1);
}

TEST_F(PyMapCache, RepeatedInsertInvalidateAndLookupDoesNotRetainOldMaps) {
  MgrMapCache cache;
  for (int i = 0; i < 128; ++i) {
    auto value = make_dict();
    cache.insert("pg_dump", value.clone());
    auto* state = PyEval_SaveThread();
    std::thread notifier([&] { cache.invalidate("pg_dump"); });
    notifier.join();
    PyEval_RestoreThread(state);
    EXPECT_FALSE(cache.get("pg_dump"));
    EXPECT_EQ(Py_REFCNT(value.get()), 1);
  }
}

TEST_F(PyMapCache, ReplacementReclaimsStaleOwnerWithoutLookup) {
  MgrMapCache cache;
  auto old_value = make_dict();
  auto new_value = make_dict();
  cache.insert("pg_dump", old_value.clone());
  auto* state = PyEval_SaveThread();
  cache.invalidate("pg_dump");
  PyEval_RestoreThread(state);
  cache.insert("pg_dump", new_value.clone());
  EXPECT_EQ(Py_REFCNT(old_value.get()), 1);
  EXPECT_EQ(Py_REFCNT(new_value.get()), 2);
  EXPECT_TRUE(cache.exists("pg_dump"));
  EXPECT_EQ(cache.size(), 1);
}

TEST_F(PyMapCache, ClearReleasesLiveAndStaleOwnersWithoutCallerGil) {
  MgrMapCache cache;
  auto cached = make_dict();
  auto invalidated = make_dict();
  cache.insert("osd_map", cached.clone());
  cache.insert("pg_dump", invalidated.clone());
  auto* state = PyEval_SaveThread();
  cache.invalidate("pg_dump");
  cache.clear();
  PyEval_RestoreThread(state);
  EXPECT_EQ(Py_REFCNT(cached.get()), 1);
  EXPECT_EQ(Py_REFCNT(invalidated.get()), 1);
}

TEST_F(PyMapCache, DisableReleasesReferencesAndRejectedWritesConsumeOwners) {
  MgrMapCache cache;
  auto value = make_dict();
  cache.insert("pg_dump", value.clone());
  auto* state = PyEval_SaveThread();
  cache.set_enabled(false);
  PyEval_RestoreThread(state);
  EXPECT_EQ(Py_REFCNT(value.get()), 1);
  auto rejected = value.clone();
  cache.insert("pg_dump", std::move(rejected));
  EXPECT_FALSE(rejected);
  EXPECT_EQ(Py_REFCNT(value.get()), 1);
  EXPECT_EQ(cache.size(), 0);
  cache.set_enabled(true);
  cache.insert("not_cacheable", value.clone());
  EXPECT_EQ(Py_REFCNT(value.get()), 1);
  EXPECT_EQ(cache.size(), 0);
}

TEST_F(PyMapCache, SameObjectCanHaveMultipleExplicitOwners) {
  MgrMapCache cache;
  auto value = make_dict();
  cache.insert("pg_dump", value.clone());
  cache.insert("osd_map", value.clone());
  EXPECT_EQ(Py_REFCNT(value.get()), 3);
  cache.insert("pg_dump", value.clone());
  EXPECT_EQ(Py_REFCNT(value.get()), 3);
  auto* state = PyEval_SaveThread();
  cache.invalidate("pg_dump");
  cache.invalidate("osd_map");
  PyEval_RestoreThread(state);
  cache.clear();
  EXPECT_EQ(Py_REFCNT(value.get()), 1);
}

TEST_F(PyMapCache, ReturnedOwnerOutlivesCache) {
  PyObjectRef result;
  {
    MgrMapCache cache;
    cache.insert("pg_dump", make_dict());
    result = cache.get("pg_dump");
    ASSERT_TRUE(result);
  }
  EXPECT_TRUE(PyDict_Check(result.get()));
  EXPECT_EQ(Py_REFCNT(result.get()), 1);
}

TEST_F(PyMapCache, DestructionWithoutGilReleasesAllReferences) {
  auto cached = make_dict();
  auto invalidated = make_dict();
  auto cache = std::make_unique<MgrMapCache>();
  cache->insert("osd_map", cached.clone());
  cache->insert("pg_dump", invalidated.clone());
  auto* state = PyEval_SaveThread();
  cache->invalidate("pg_dump");
  cache.reset();
  PyEval_RestoreThread(state);
  EXPECT_EQ(Py_REFCNT(cached.get()), 1);
  EXPECT_EQ(Py_REFCNT(invalidated.get()), 1);
}

TEST_F(PyMapCache, InvalidationRejectsAnInFlightSnapshot) {
  for (bool existing_entry : {false, true}) {
    MgrMapCache cache;
    if (existing_entry) {
      cache.insert("osd_map", make_dict());
    }
    const auto generation = cache.generation("osd_map");
    auto snapshot = make_dict();
    auto* state = PyEval_SaveThread();
    std::thread notifier([&] { cache.invalidate("osd_map"); });
    notifier.join();
    PyEval_RestoreThread(state);
    EXPECT_FALSE(cache.insert_if_generation("osd_map", snapshot.clone(), generation));
    EXPECT_EQ(Py_REFCNT(snapshot.get()), 1);
    EXPECT_FALSE(cache.get("osd_map"));
    auto current = make_dict();
    EXPECT_TRUE(cache.insert_if_generation("osd_map", current.clone(),
                                          cache.generation("osd_map")));
    auto result = cache.get("osd_map");
    EXPECT_EQ(result.get(), current.get());
  }
}

TEST_F(PyMapCache, ClearAndDisableRejectInFlightSnapshots) {
  for (int operation = 0; operation < 3; ++operation) {
    MgrMapCache cache;
    const auto generation = cache.generation("osd_map");
    if (operation == 0) {
      cache.clear();
    } else if (operation == 1) {
      cache.set_enabled(false);
      cache.set_enabled(true);
    } else {
      cache.erase("osd_map");
    }
    EXPECT_FALSE(cache.insert_if_generation("osd_map", make_dict(), generation));
  }
}

TEST_F(PyMapCache, FinalizersRunOutsideCacheAndCallerLocks) {
  struct State {
    MgrMapCache* cache;
    std::mutex caller_lock;
    bool finalized = false;
    size_t expected_size = 0;
  };
  for (int operation = 0; operation < 5; ++operation) {
    MgrMapCache cache(1);
    State state{&cache};
    state.expected_size = operation >= 3 ? 1 : 0;
    auto value = PyObjectRef::adopt_new_reference(
      PyCapsule_New(&state, "cache_finalizer", [](PyObject* capsule) {
        auto* state = static_cast<State*>(PyCapsule_GetPointer(capsule, "cache_finalizer"));
        EXPECT_NE(PyThreadState_Get(), nullptr);
        std::lock_guard l(state->caller_lock);
        EXPECT_EQ(state->cache->size(), state->expected_size);
        state->cache->clear();
        state->finalized = true;
      }));
    ASSERT_TRUE(value);
    cache.insert("pg_dump", std::move(value));
    if (operation == 0) {
      auto* ts = PyEval_SaveThread();
      {
        std::lock_guard l(state.caller_lock);
        cache.invalidate("pg_dump");
      }
      PyEval_RestoreThread(ts);
      EXPECT_FALSE(state.finalized);
      EXPECT_FALSE(cache.get("pg_dump"));
    } else if (operation == 1) {
      cache.erase("pg_dump");
    } else if (operation == 2) {
      cache.clear();
    } else if (operation == 3) {
      cache.insert("pg_dump", make_dict());
    } else {
      cache.insert("osd_map", make_dict());
    }
    EXPECT_TRUE(state.finalized);
  }
}

TEST_F(PyMapCache, ConcurrentCacheTurnoverReleasesAllReferences) {
  MgrMapCache cache(1);
  auto first = make_dict();
  auto second = make_dict();
  constexpr int iterations = 2000;
  auto* state = PyEval_SaveThread();
  std::vector<std::thread> threads;
  for (const auto* value : {&first, &second}) {
    threads.emplace_back([&, value] {
      for (int i = 0; i < iterations; ++i) {
        auto gil = PyGILState_Ensure();
        {
          cache.insert(i % 2 ? "pg_dump" : "osd_map", value->clone());
          auto result = cache.get(i % 2 ? "pg_dump" : "osd_map");
        }
        PyGILState_Release(gil);
      }
    });
  }
  threads.emplace_back([&] {
    for (int i = 0; i < iterations; ++i) {
      cache.invalidate("pg_dump");
      cache.invalidate("osd_map");
    }
  });
  for (auto& thread : threads) {
    thread.join();
  }
  // clear() acquires the GIL after extracting the owners from the cache.
  cache.clear();
  PyEval_RestoreThread(state);
  EXPECT_EQ(Py_REFCNT(first.get()), 1);
  EXPECT_EQ(Py_REFCNT(second.get()), 1);
}

TEST_F(PyMapCache, AdoptingAndMovingDoNotIncrementPythonRefcount) {
  PyObject* value = PyDict_New();
  auto first = PyObjectRef::adopt_new_reference(value);
  EXPECT_EQ(Py_REFCNT(value), 1);
  auto second = std::move(first);
  EXPECT_FALSE(first);
  EXPECT_EQ(second.get(), value);
  EXPECT_EQ(Py_REFCNT(value), 1);
}

TEST_F(PyMapCache, ExplicitCloneCreatesOneIndependentPythonReference) {
  auto value = make_dict();
  auto duplicate = value.clone();
  EXPECT_EQ(duplicate.get(), value.get());
  EXPECT_EQ(Py_REFCNT(value.get()), 2);
  duplicate.reset();
  EXPECT_EQ(Py_REFCNT(value.get()), 1);
}

TEST_F(PyMapCache, MoveAssignmentReleasesPreviousOwner) {
  auto original = make_dict();
  auto replacement = make_dict();
  auto owner = original.clone();
  owner = std::move(replacement);
  EXPECT_FALSE(replacement);
  EXPECT_EQ(Py_REFCNT(original.get()), 1);
  EXPECT_EQ(Py_REFCNT(owner.get()), 1);
}

TEST_F(PyMapCache, TransferToPythonConsumesOwnerWithoutChangingRefcount) {
  auto value = make_dict();
  PyObject* result = std::move(value).into_python();
  EXPECT_FALSE(value);
  EXPECT_EQ(Py_REFCNT(result), 1);
  Py_DECREF(result);
}

TEST_F(PyMapCache, NullReferencesPreservePythonErrors) {
  PyErr_SetString(PyExc_RuntimeError, "test error");
  auto value = PyObjectRef::adopt_new_reference(nullptr);
  EXPECT_FALSE(value);
  auto duplicate = value.clone();
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(std::move(value).into_python(), nullptr);
  EXPECT_EQ(PyErr_Occurred(), PyExc_RuntimeError);
  PyErr_Clear();
}

TEST_F(PyMapCache, MovingOwnerWithoutGilDoesNotTouchRefcounts) {
  auto source = make_dict();
  auto* identity = source.get();
  PyObjectRef destination;
  auto* state = PyEval_SaveThread();
  destination = std::move(source);
  PyEval_RestoreThread(state);
  EXPECT_FALSE(source);
  EXPECT_EQ(destination.get(), identity);
  EXPECT_EQ(Py_REFCNT(identity), 1);
}

TEST_F(PyMapCache, CleanupPreservesAnAttachedSubinterpreter) {
  struct State {
    PyInterpreterState* finalized_in = nullptr;
  } state;
  auto* main = PyThreadState_Get();
  auto* sub = Py_NewInterpreter();
  ASSERT_NE(sub, nullptr);
  auto* origin = PyThreadState_GetInterpreter(sub);
  const auto end_subinterpreter = make_scope_guard([&] {
    PyThreadState_Swap(sub);
    Py_EndInterpreter(sub);
    PyThreadState_Swap(main);
  });
#if PY_VERSION_HEX >= 0x030d0000
  constexpr int operations = 3;
#else
  constexpr int operations = 1;
#endif
  for (int operation = 0; operation < operations; ++operation) {
    state.finalized_in = nullptr;
    auto cache = std::make_unique<MgrMapCache>();
    auto value = PyObjectRef::adopt_new_reference(
      PyCapsule_New(&state, "interpreter_finalizer", [](PyObject* capsule) {
        auto* state = static_cast<State*>(
          PyCapsule_GetPointer(capsule, "interpreter_finalizer"));
        state->finalized_in = PyThreadState_GetInterpreter(PyThreadState_Get());
      }));
    ASSERT_TRUE(value);
    cache->insert("pg_dump", std::move(value));
#if PY_VERSION_HEX >= 0x030d0000
    if (operation == 0) {
      cache->clear();
    } else if (operation == 1) {
      cache->set_enabled(false);
    } else {
      cache.reset();
    }
#else
    cache->clear_with_gil();
#endif
    EXPECT_EQ(state.finalized_in, origin);
    EXPECT_EQ(PyThreadState_Get(), sub);
  }
}

TEST_F(PyMapCache, CleanupWithoutGilWhileAnotherThreadHoldsGil) {
  for (int operation = 0; operation < 3; ++operation) {
    auto value = make_dict();
    auto cache = std::make_unique<MgrMapCache>();
    cache->insert("osd_map", value.clone());
    std::promise<void> holding_gil;
    auto* state = PyEval_SaveThread();
    std::thread holder([&] {
      auto gil = PyGILState_Ensure();
      holding_gil.set_value();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      PyGILState_Release(gil);
    });
    holding_gil.get_future().wait();
    if (operation == 0) {
      cache->clear();
    } else if (operation == 1) {
      cache->set_enabled(false);
    } else {
      cache.reset();
    }
    holder.join();
    PyEval_RestoreThread(state);
    EXPECT_EQ(Py_REFCNT(value.get()), 1);
  }
}
