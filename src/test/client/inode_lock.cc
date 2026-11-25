// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Inode::Lock (exclusive and shared holds, recursion, the lock-free
 * sequence-count readers, readers spread over several counts) and the
 * per-CPU slots, without a cluster: writers keep two counts equal under
 * the lock, and every reader, locked or not, must see them equal.
 */

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "client/Client.h"
#include "client/Inode.h"
#include "include/current_cpu.h"

using namespace std::chrono_literals;

namespace {

struct Pair {
  // relaxed atomics: the lock-free readers read them while writers write
  std::atomic<uint64_t> a{0}, b{0};
};

void run_mixed(Inode::Lock& l, Pair& p, int writers, int readers,
               int seq_readers, std::chrono::milliseconds how_long,
               std::atomic<int>& bad, std::atomic<uint64_t>& writes)
{
  std::atomic<bool> stop{false};
  std::vector<std::thread> th;
  auto r = std::memory_order_relaxed;
  for (int w = 0; w < writers; w++) {
    th.emplace_back([&] {
      while (!stop) {
        l.lock();
        uint64_t v = p.a.load(r) + 1;
        p.a.store(v, r);
        std::this_thread::yield();
        p.b.store(v, r);
        l.unlock();
        writes++;
      }
    });
  }
  for (int i = 0; i < readers; i++) {
    th.emplace_back([&] {
      while (!stop) {
        l.lock_shared();
        if (p.a.load(r) != p.b.load(r))
          bad++;
        l.unlock_shared();
      }
    });
  }
  for (int i = 0; i < seq_readers; i++) {
    th.emplace_back([&] {
      while (!stop) {
        uint64_t a, b;
        uint32_t s;
        do {
          s = l.read_begin();
          a = p.a.load(r);
          b = p.b.load(r);
        } while ((s & 1) || l.read_retry(s));
        if (a != b)
          bad++;
      }
    });
  }
  std::this_thread::sleep_for(how_long);
  stop = true;
  for (auto& t : th)
    t.join();
}

} // anonymous namespace

TEST(InodeLock, ExclusiveSharedAndSeqReaders)
{
  Inode::Lock l;
  Pair p;
  std::atomic<int> bad{0};
  std::atomic<uint64_t> writes{0};
  run_mixed(l, p, 4, 8, 4, 2s, bad, writes);
  EXPECT_EQ(0, bad.load());
  EXPECT_GT(writes.load(), 0u);
  EXPECT_EQ(writes.load(), p.a.load());
  EXPECT_EQ(p.a.load(), p.b.load());
  EXPECT_EQ(0u, l.held_seq() & 1) << "odd sequence count with nobody inside";
}

TEST(InodeLock, ReadersSpreadOverCounts)
{
  auto old_after = Inode::Lock::spread_after.load();
  auto old_slots = Inode::Lock::spread_slots.load();
  Inode::Lock::spread_after = 0;   // spread at once
  for (unsigned slots : {1u, 3u, 16u}) {
    Inode::Lock::spread_slots = slots;
    Inode::Lock l;
    Pair p;
    std::atomic<int> bad{0};
    std::atomic<uint64_t> writes{0};
    run_mixed(l, p, 2, 16, 2, 1s, bad, writes);
    EXPECT_EQ(0, bad.load()) << slots << " slots";
    EXPECT_EQ(p.a.load(), p.b.load()) << slots << " slots";
  }
  Inode::Lock::spread_after = old_after;
  Inode::Lock::spread_slots = old_slots;
}

TEST(InodeLock, Recursion)
{
  Inode::Lock l;
  l.lock();
  l.lock();          // the owner again
  l.lock_shared();   // a shared hold inside an exclusive one
  l.unlock_shared();
  l.unlock();
  // still held: another thread cannot get it
  std::atomic<bool> got{false};
  std::thread t([&] {
    l.lock();
    got = true;
    l.unlock();
  });
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(got.load());
  l.unlock();
  t.join();
  EXPECT_TRUE(got.load());
}

TEST(InodeLock, SharedHoldersExcludeWriter)
{
  Inode::Lock l;
  l.lock_shared();
  std::atomic<bool> got{false};
  std::thread t([&] {
    l.lock();
    got = true;
    l.unlock();
  });
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(got.load()) << "a writer got in past a shared holder";
  l.unlock_shared();
  t.join();
  EXPECT_TRUE(got.load());
}

TEST(PerCpuSlots, SizesAndIndexes)
{
  for (unsigned want : {1u, 2u, 3u, 5u, 64u, 100u, 4096u}) {
    unsigned n = ceph::percpu_slots(want, 16);
    EXPECT_EQ(std::bit_ceil(want), n) << want;
  }
  unsigned dflt = ceph::percpu_slots(0, 16);
  EXPECT_GE(dflt, 1u);
  EXPECT_LE(dflt, 16u);
  EXPECT_EQ(dflt & (dflt - 1), 0u) << "not a power of two";

  struct alignas(64) Slot {
    std::atomic<uint64_t> n{0};
  };
  ceph::PerCpuSlots<Slot> s(16);
  s.resize(3);
  EXPECT_EQ(4u, s.size());
  EXPECT_EQ(3u, s.mask());
  // here() and [] stay in range from any thread
  std::vector<std::thread> th;
  for (int t = 0; t < 32; t++)
    th.emplace_back([&] {
      for (int i = 0; i < 10000; i++) {
        s.here().n++;
        s[size_t(i) * 7919].n++;
      }
    });
  for (auto& t : th)
    t.join();
  uint64_t sum = 0;
  for (auto& x : s)
    sum += x.n;
  EXPECT_EQ(32u * 10000 * 2, sum);
  unsigned cpu = ceph::cpu_index();
  (void)cpu;   // any value: it is masked by the users
}
