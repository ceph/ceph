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
#include <thread>
#include <vector>

#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "mgr/mgr_perf_counters.h"
#include "mgr/MgrMapCache.h"

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

  for (auto& t : threads) t.join();

  ASSERT_EQ(success_count, 10000);
  ASSERT_EQ(c.get_hits(), 10000);
}

// MgrMapCache<PyObject*> refcount tests. Each test holds the GIL.
struct PyCache : public ::testing::Test {
  static void SetUpTestSuite() {
    if (!Py_IsInitialized()) {
      Py_Initialize();
    }
  }
  void SetUp() override {
    gstate = PyGILState_Ensure();
  }
  void TearDown() override {
    PyGILState_Release(gstate);
  }
private:
  PyGILState_STATE gstate{};
};

TEST_F(PyCache, InsertAndGet) {
  PyObject* o = PyList_New(0);
  {
    MgrMapCache<PyObject*> c;
    c.insert("pg_dump", o);
    ASSERT_EQ(Py_REFCNT(o), 2);
    PyObject* got = c.get("pg_dump");
    ASSERT_EQ(got, o);
    ASSERT_EQ(Py_REFCNT(o), 3);
    Py_DECREF(got);
  }
  // destructor drops the cache's ref
  ASSERT_EQ(Py_REFCNT(o), 1);
  Py_DECREF(o);
}

TEST_F(PyCache, ReplaceDropsOld) {
  PyObject* a = PyList_New(0);
  PyObject* b = PyList_New(0);
  MgrMapCache<PyObject*> c;
  c.insert("pg_dump", a);
  c.insert("pg_dump", b);
  ASSERT_EQ(Py_REFCNT(a), 1);
  ASSERT_EQ(Py_REFCNT(b), 2);
  c.clear();
  ASSERT_EQ(Py_REFCNT(b), 1);
  Py_DECREF(a);
  Py_DECREF(b);
}

TEST_F(PyCache, EvictDropsOld) {
  PyObject* a = PyList_New(0);
  PyObject* b = PyList_New(0);
  MgrMapCache<PyObject*> c{1};
  c.insert("pg_dump", a);
  c.insert("osd_map", b);
  ASSERT_EQ(c.size(), 1);
  ASSERT_EQ(Py_REFCNT(a), 1);
  ASSERT_EQ(Py_REFCNT(b), 2);
  c.clear();
  Py_DECREF(a);
  Py_DECREF(b);
}

// erase() runs without the GIL in the mgr, so it only queues the object.
// The next get() or insert() must drop the cache's ref.
TEST_F(PyCache, EraseReleasedOnGet) {
  PyObject* o = PyList_New(0);
  MgrMapCache<PyObject*> c;
  c.insert("pg_dump", o);
  ASSERT_EQ(Py_REFCNT(o), 2);

  PyThreadState* ts = PyEval_SaveThread();
  c.erase("pg_dump");
  PyEval_RestoreThread(ts);
  ASSERT_EQ(c.size(), 0);
  ASSERT_EQ(Py_REFCNT(o), 2);

  ASSERT_EQ(c.get("pg_dump"), nullptr);
  ASSERT_EQ(Py_REFCNT(o), 1);
  Py_DECREF(o);
}

TEST_F(PyCache, EraseReleasedOnInsert) {
  PyObject* a = PyList_New(0);
  PyObject* b = PyList_New(0);
  MgrMapCache<PyObject*> c;
  c.insert("pg_dump", a);

  PyThreadState* ts = PyEval_SaveThread();
  c.erase("pg_dump");
  PyEval_RestoreThread(ts);
  ASSERT_EQ(Py_REFCNT(a), 2);

  c.insert("osd_map", b);
  ASSERT_EQ(Py_REFCNT(a), 1);
  ASSERT_EQ(Py_REFCNT(b), 2);
  c.clear();
  Py_DECREF(a);
  Py_DECREF(b);
}

TEST_F(PyCache, ClearDropsCachedAndQueued) {
  PyObject* a = PyList_New(0);
  PyObject* b = PyList_New(0);
  MgrMapCache<PyObject*> c;
  c.insert("pg_dump", a);
  c.insert("osd_map", b);

  PyThreadState* ts = PyEval_SaveThread();
  c.erase("pg_dump");
  PyEval_RestoreThread(ts);

  c.clear();
  ASSERT_EQ(Py_REFCNT(a), 1);
  ASSERT_EQ(Py_REFCNT(b), 1);
  // nothing left to release twice
  c.clear();
  ASSERT_EQ(c.get("pg_dump"), nullptr);
  ASSERT_EQ(Py_REFCNT(a), 1);
  Py_DECREF(a);
  Py_DECREF(b);
}

// Reproducer for https://tracker.ceph.com/issues/81794: prometheus builds a
// new pg_dump each scrape, and every new PG map invalidates it.
TEST_F(PyCache, InsertGetEraseLoop) {
  constexpr int n = 1000;
  MgrMapCache<PyObject*> c;
  std::vector<PyObject*> objs;
  for (int i = 0; i < n; ++i) {
    PyObject* o = PyList_New(0);
    objs.push_back(o);
    c.insert("pg_dump", o);
    PyObject* got = c.get("pg_dump");
    ASSERT_EQ(got, o);
    Py_DECREF(got);

    PyThreadState* ts = PyEval_SaveThread();
    c.erase("pg_dump");
    PyEval_RestoreThread(ts);
  }
  // drain the last erase
  ASSERT_EQ(c.get("pg_dump"), nullptr);
  for (auto* o : objs) {
    ASSERT_EQ(Py_REFCNT(o), 1);
    Py_DECREF(o);
  }
}

// insert() and get() take the GIL, erase() runs without it, like the
// notify paths. Every ref the cache took must be dropped exactly once.
TEST_F(PyCache, Concurrent) {
  constexpr int iters = 2000;
  PyObject* a = PyList_New(0);
  PyObject* b = PyList_New(0);
  MgrMapCache<PyObject*> c;

  PyThreadState* ts = PyEval_SaveThread();
  std::vector<std::thread> threads;
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([&c, a, b, t] {
      for (int i = 0; i < iters; ++i) {
        PyGILState_STATE st = PyGILState_Ensure();
        c.insert(i % 2 ? "pg_dump" : "osd_map", t ? a : b);
        PyGILState_Release(st);
      }
    });
    threads.emplace_back([&c] {
      for (int i = 0; i < iters; ++i) {
        PyGILState_STATE st = PyGILState_Ensure();
        PyObject* o = c.get(i % 2 ? "pg_dump" : "osd_map");
        Py_XDECREF(o);
        PyGILState_Release(st);
      }
    });
    threads.emplace_back([&c] {
      for (int i = 0; i < iters; ++i) {
        c.erase(i % 2 ? "osd_map" : "pg_dump");
      }
    });
  }
  for (auto& th : threads) th.join();
  PyEval_RestoreThread(ts);

  c.clear();
  ASSERT_EQ(Py_REFCNT(a), 1);
  ASSERT_EQ(Py_REFCNT(b), 1);
  Py_DECREF(a);
  Py_DECREF(b);
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
