// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The ObjectCacher features a user that shards its cache turns on: cache
 * hits read through the read gate without the lock, members of a budget
 * group sharing their limits, and the bound on the writeback in flight.
 * Each test checks the data as well as the limits.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/ceph_mutex.h"
#include "common/snap_types.h"
#include "global/global_context.h"
#include "include/Context.h"
#include "osdc/ObjectCacher.h"
#include "osdc/WritebackHandler.h"

using namespace std::chrono_literals;

namespace {

constexpr uint64_t BLK = 4096;

// An in-memory OSD whose writes commit after a delay, one at a time, on a
// thread of its own that takes the cache's lock and shuts the cache's
// read gate around the completion, as the client's completions do.  It
// records the most writeback it ever had in flight.
class TestWriteback : public WritebackHandler {
public:
  TestWriteback(CephContext *cct, ceph::mutex& l, std::chrono::microseconds d,
                bool scattered = false)
    : lock(l), delay(d), fin(cct, "test_oc_wb", "fn_test_oc_wb"),
      scattered(scattered) {
    fin.start();
  }
  ~TestWriteback() override {
    fin.wait_for_empty();
    fin.stop();
  }
  void set_cache(ObjectCacher *c) {
    oc = c;
  }

  void read(const object_t& oid, uint64_t object_no,
            const object_locator_t& oloc, uint64_t off, uint64_t len,
            snapid_t snapid, bufferlist *pbl, uint64_t trunc_size,
            __u32 trunc_seq, int op_flags,
            const ZTracer::Trace &parent_trace, Context *onfinish) override {
    fin.queue(new LambdaContext([this, oid, off, len, pbl, onfinish](int) {
      std::this_thread::sleep_for(delay);
      std::lock_guard l(lock);
      bufferlist bl;
      auto p = store.find(oid);
      if (p != store.end() && off < p->second.length())
        bl.substr_of(p->second, off,
                     std::min<uint64_t>(len, p->second.length() - off));
      int r = bl.length();
      pbl->claim_append(bl);
      oc->gate_shut();
      onfinish->complete(r);
      oc->gate_open();
    }));
  }

  ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
                   uint64_t off, uint64_t len, const SnapContext& snapc,
                   const bufferlist &bl, ceph::real_time mtime,
                   uint64_t trunc_size, __u32 trunc_seq,
                   ceph_tid_t journal_tid, const ZTracer::Trace &parent_trace,
                   Context *oncommit) override {
    uint64_t now = inflight.fetch_add(len) + len;
    uint64_t peak = max_inflight.load();
    while (now > peak && !max_inflight.compare_exchange_weak(peak, now))
      ;
    sent.push_back(std::chrono::steady_clock::now());
    fin.queue(new LambdaContext([this, oid, off, len, bl, oncommit](int) {
      std::this_thread::sleep_for(delay);
      std::lock_guard l(lock);
      int r = fail();
      if (r == 0)
        put(oid, off, bl);
      inflight -= len;
      committed.push_back(std::chrono::steady_clock::now());
      oc->gate_shut();
      oncommit->complete(r);
      oc->gate_open();
    }));
    return ++last_tid;
  }

  using WritebackHandler::write;

  bool can_scattered_write() override {
    return scattered;
  }
  // a scattered write: several pieces of one object in one write
  ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
                   std::vector<std::pair<uint64_t, bufferlist>>& io_vec,
                   const SnapContext& snapc, ceph::real_time mtime,
                   uint64_t trunc_size, __u32 trunc_seq,
                   Context *oncommit) override {
    uint64_t len = 0;
    for (auto& [off, bl] : io_vec)
      len += bl.length();
    pieces.push_back(io_vec.size());
    inflight += len;
    sent.push_back(std::chrono::steady_clock::now());
    auto v = io_vec;
    fin.queue(new LambdaContext([this, oid, v, len, oncommit](int) {
      std::this_thread::sleep_for(delay);
      std::lock_guard l(lock);
      int r = fail();
      if (r == 0)
        for (auto& [off, bl] : v)
          put(oid, off, bl);
      inflight -= len;
      committed.push_back(std::chrono::steady_clock::now());
      oc->gate_shut();
      oncommit->complete(r);
      oc->gate_open();
    }));
    return ++last_tid;
  }
  // the pieces of each scattered write, in the order sent (under the lock)
  std::vector<size_t> pieces;

  bool may_copy_on_write(const object_t&, uint64_t, uint64_t,
                         snapid_t) override {
    return false;
  }

  // what the OSD holds of @oid at @off, BLK bytes; call with the lock held
  std::string stored(const object_t& oid, uint64_t off) {
    auto p = store.find(oid);
    if (p == store.end() || p->second.length() < off + BLK)
      return std::string();
    bufferlist b;
    b.substr_of(p->second, off, BLK);
    return b.to_str();
  }

  std::atomic<uint64_t> max_inflight{0};
  // the next fail_writes writes fail with -EIO and store nothing; failed
  // counts them
  std::atomic<int> fail_writes{0}, failed{0};
  // when each write was sent and when each completed (under the lock)
  std::vector<std::chrono::steady_clock::time_point> sent, committed;
  uint64_t in_flight() const {
    return inflight;
  }

private:
  int fail() {
    if (fail_writes.load() > 0 && fail_writes-- > 0) {
      failed++;
      return -EIO;
    }
    return 0;
  }
  void put(const object_t& oid, uint64_t off, const bufferlist& bl) {
    uint64_t len = bl.length();
    bufferlist& obj = store[oid];
    if (obj.length() < off + len)
      obj.append_zero(off + len - obj.length());
    bufferlist head, tail;
    head.substr_of(obj, 0, off);
    if (obj.length() > off + len)
      tail.substr_of(obj, off + len, obj.length() - off - len);
    bufferlist n;
    n.claim_append(head);
    n.append(bl);
    n.claim_append(tail);
    obj = std::move(n);
  }
  ceph::mutex& lock;
  std::chrono::microseconds delay;
  Finisher fin;
  ObjectCacher *oc = nullptr;
  std::map<object_t, bufferlist> store;
  std::atomic<uint64_t> inflight{0};
  std::atomic<ceph_tid_t> last_tid{0};
  const bool scattered;
};

struct Limits {
  uint64_t max_size = 64 << 20;
  uint64_t max_objects = 100000;
  uint64_t max_dirty = 32 << 20;
  uint64_t target_dirty = 16 << 20;
  double max_dirty_age = 1.0;
};

// One cache with its own lock and OSD, used the way the client uses each
// of its shards: every call that may change the cache holds the lock and
// shuts the read gate.
struct Shard {
  ceph::mutex lock = ceph::make_mutex("object_cacher_shard::lock");
  std::unique_ptr<TestWriteback> wb;
  std::unique_ptr<ObjectCacher> oc;
  ObjectCacher::ObjectSet oset{nullptr, 1, 0};
  SnapContext snapc;

  Shard(const Limits& l, std::chrono::microseconds delay, bool scattered = false) {
    wb = std::make_unique<TestWriteback>(g_ceph_context, lock, delay, scattered);
    oc = std::make_unique<ObjectCacher>(
      g_ceph_context, "object_cacher_shard", *wb, lock, nullptr, nullptr,
      l.max_size, l.max_objects, l.max_dirty, l.target_dirty,
      l.max_dirty_age, true);
    wb->set_cache(oc.get());
  }

  template <typename F>
  auto locked(F&& f) {
    std::lock_guard l(lock);
    oc->gate_shut();
    auto r = f();
    oc->gate_open();
    return r;
  }

  static ObjectExtent extent(const std::string& oid, uint64_t blk) {
    ObjectExtent ex(object_t(oid), 0, blk * BLK, BLK, 0);
    ex.oloc.pool = 1;
    ex.buffer_extents.emplace_back(0, BLK);
    return ex;
  }

  // a buffered write of block @blk of @oid, filled with @v, as the client's
  // fast write path does it: the writeback wait comes after the write
  void write(const std::string& oid, uint64_t blk, char v) {
    bufferlist bl;
    bl.append(std::string(BLK, v));
    auto *wr = oc->prepare_write(snapc, bl, ceph::real_clock::now(), 0, 0);
    wr->extents.push_back(extent(oid, blk));
    bool wait = false;
    locked([&] {
      return oc->writex(wr, &oset, nullptr, nullptr, true, &wait);
    });
    if (wait)
      locked([&] { oc->wait_for_writeback_space(BLK); return 0; });
  }

  // read block @blk of @oid into @out; through the read gate first when
  // @gate, and *hit tells whether that served it
  int read(const std::string& oid, uint64_t blk, bufferlist *out,
           bool gate, bool *hit = nullptr) {
    auto *rd = oc->prepare_read(CEPH_NOSNAP, out, 0);
    rd->extents.push_back(extent(oid, blk));
    if (hit)
      *hit = false;
    if (gate) {
      int r = oc->read_hit(rd, &oset);
      if (r >= 0) {
        if (hit)
          *hit = true;
        return r;
      }
      if (r != -EBUSY && r != -EAGAIN) {
        delete rd;
        return r;
      }
    }
    C_SaferCond cond;
    int r = locked([&] { return oc->readx(rd, &oset, &cond); });
    if (r == 0)
      r = cond.wait();
    return r;
  }

  void flush() {
    C_SaferCond cond;
    bool done = locked([&] { return oc->flush_all(&cond); });
    if (done) {
      // flush_all() did not take it
      cond.complete(0);
    }
    cond.wait();
  }

  uint64_t dirty_tx() {
    std::lock_guard l(lock);
    return oc->get_stat_dirty() + oc->get_stat_tx();
  }

  // stop the flusher and drop the cached data
  void stop() {
    flush();
    locked([&] { oc->release_set(&oset); return 0; });
    oc->stop();
  }
};

bool uniform(const bufferlist& bl, char *v)
{
  std::string s = bl.to_str();
  if (s.size() != BLK)
    return false;
  *v = s[0];
  return std::all_of(s.begin(), s.end(), [&](char c) { return c == s[0]; });
}

} // anonymous namespace

/*
 * Readers serve cache hits through the read gate while a writer rewrites
 * the same blocks and the flusher writes them back.  A reader must never
 * see a block half old and half new, and the gate must serve reads (how
 * many depends on how often the writer holds the lock: a slow build, with
 * a sanitizer or debug checks, falls back to the locked path more).
 */
TEST(ObjectCacherShard, ReadGateServesWholeBlocks)
{
  Limits l;
  Shard s(l, 50us);
  s.oc->enable_read_gate();
  s.oc->start();

  const std::string oid = "gate";
  constexpr uint64_t NBLK = 64;
  std::vector<char> last(NBLK, 'a');
  for (uint64_t b = 0; b < NBLK; b++)
    s.write(oid, b, 'a');

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> hits{0}, reads{0}, torn{0};
  std::vector<std::thread> readers;
  for (int t = 0; t < 4; t++) {
    readers.emplace_back([&, t] {
      std::mt19937 rng(t + 1);
      while (!stop) {
        bufferlist bl;
        bool hit;
        int r = s.read(oid, rng() % NBLK, &bl, true, &hit);
        char v;
        if (r != int(BLK) || !uniform(bl, &v) || v < 'a' || v > 'z')
          torn++;
        reads++;
        if (hit)
          hits++;
      }
    });
  }
  std::mt19937 rng(42);
  auto end = std::chrono::steady_clock::now() + 2s;
  for (uint64_t i = 0; std::chrono::steady_clock::now() < end; i++) {
    uint64_t b = rng() % NBLK;
    last[b] = 'a' + i % 26;
    s.write(oid, b, last[b]);
  }
  stop = true;
  for (auto& t : readers)
    t.join();

  EXPECT_EQ(0u, torn.load());
  EXPECT_GT(reads.load(), 0u);
  EXPECT_GT(hits.load(), 0u) << hits << " of " << reads;

  // the cache, and after a flush the OSD, hold the last write of each block
  for (uint64_t b = 0; b < NBLK; b++) {
    bufferlist bl;
    ASSERT_EQ(int(BLK), s.read(oid, b, &bl, false));
    EXPECT_EQ(std::string(BLK, last[b]), bl.to_str()) << "block " << b;
  }
  s.flush();
  {
    std::lock_guard g(s.lock);
    for (uint64_t b = 0; b < NBLK; b++)
      EXPECT_EQ(std::string(BLK, last[b]), s.wb->stored(object_t(oid), b * BLK))
        << "block " << b;
  }
  s.stop();
}

/*
 * The read gate is off unless the user turns it on: read_hit() then
 * serves nothing, and readx() works as before (librbd's case).
 */
TEST(ObjectCacherShard, ReadGateOffByDefault)
{
  Limits l;
  Shard s(l, 0us);
  s.oc->start();
  s.write("off", 0, 'x');
  bufferlist bl;
  auto *rd = s.oc->prepare_read(CEPH_NOSNAP, &bl, 0);
  rd->extents.push_back(Shard::extent("off", 0));
  int r = s.oc->read_hit(rd, &s.oset);
  EXPECT_TRUE(r == -EBUSY || r == -EAGAIN) << r;
  delete rd;
  bool hit;
  bufferlist bl2;
  ASSERT_EQ(int(BLK), s.read("off", 0, &bl2, true, &hit));
  EXPECT_FALSE(hit);
  EXPECT_EQ(std::string(BLK, 'x'), bl2.to_str());
  s.stop();
}

/*
 * Members of a budget group share max_dirty: one member alone may use
 * more than its share (max_dirty / members), and all of them together
 * stay near max_dirty.  Writers of every member then finish (none is left
 * asleep: room another member frees wakes them), and the OSD ends up with
 * the last write of every block.
 */
TEST(ObjectCacherShard, BudgetGroupSharesLimits)
{
  Limits l;
  l.max_size = 8 << 20;
  l.max_objects = 4000;
  l.max_dirty = 1 << 20;
  l.target_dirty = 512 << 10;
  constexpr int N = 4;
  // the group outlives its members
  std::vector<ObjectCacher*> group;
  std::vector<std::unique_ptr<Shard>> shards;
  for (int i = 0; i < N; i++) {
    shards.push_back(std::make_unique<Shard>(l, 100us));
    group.push_back(shards.back()->oc.get());
  }
  for (auto& s : shards) {
    s->oc->set_budget_group(&group);
    s->oc->start();
  }

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> peak0{0}, peak_all{0};
  std::thread sampler([&] {
    while (!stop) {
      // all members at one instant: hold every lock (nothing else takes
      // more than one of them)
      uint64_t all = 0, d0 = 0;
      for (int i = 0; i < N; i++)
        shards[i]->lock.lock();
      for (int i = 0; i < N; i++) {
        auto& oc = *shards[i]->oc;
        uint64_t d = oc.get_stat_dirty() + oc.get_stat_tx();
        if (i == 0)
          d0 = d;
        all += d;
      }
      for (int i = N - 1; i >= 0; i--)
        shards[i]->lock.unlock();
      peak0 = std::max(peak0.load(), d0);
      peak_all = std::max(peak_all.load(), all);
      std::this_thread::sleep_for(200us);
    }
  });

  // one member alone: it borrows the others' shares
  for (uint64_t b = 0; b < 1024; b++)
    shards[0]->write("alone", b, 'a' + b % 26);
  // alone, it is held to the limit for the whole group, give or take the
  // write it waits after
  EXPECT_LE(peak_all.exchange(0), l.max_dirty + 2 * BLK);

  // all of them at once, rewriting blocks of a few objects each
  std::vector<std::map<std::pair<int, uint64_t>, char>> last(N);
  std::vector<std::thread> writers;
  for (int i = 0; i < N; i++) {
    writers.emplace_back([&, i] {
      std::mt19937 rng(i + 7);
      for (int k = 0; k < 2000; k++) {
        int o = rng() % 4;
        uint64_t b = rng() % 256;
        char v = 'a' + rng() % 26;
        shards[i]->write("shard" + std::to_string(i) + "." + std::to_string(o),
                         b, v);
        last[i][{o, b}] = v;
      }
    });
  }
  for (auto& t : writers)
    t.join();
  stop = true;
  sampler.join();

  EXPECT_GT(peak0.load(), l.max_dirty / N * 2)
    << "a member alone never used more than its share";
  // together: a member within its share is never held back for the others,
  // so while one is at the group limit the others may still fill their
  // shares; and each writer may be a write over, the one it waits after
  uint64_t share = l.max_dirty / N;
  EXPECT_LE(peak_all.load(), l.max_dirty + (N - 1) * share + 2 * N * BLK);

  for (int i = 0; i < N; i++) {
    shards[i]->flush();
    std::lock_guard g(shards[i]->lock);
    for (auto& [ob, v] : last[i]) {
      object_t oid("shard" + std::to_string(i) + "." + std::to_string(ob.first));
      EXPECT_EQ(std::string(BLK, v), shards[i]->wb->stored(oid, ob.second * BLK))
        << oid << " block " << ob.second;
    }
  }
  for (auto& s : shards)
    s->stop();
}

/*
 * With set_max_writeback() the flusher keeps at most that much writeback
 * in flight (plus the BufferHead it sent last), however far the writers
 * are ahead; without it the flusher sends all it has to.  A flush the
 * user asks for is not held back by the bound.
 */
TEST(ObjectCacherShard, MaxWritebackBoundsInflight)
{
  Limits l;
  l.max_dirty = 4 << 20;
  l.target_dirty = 1 << 20;
  constexpr uint64_t BOUND = 64 << 10;
  // every block in an object of its own, so a BufferHead is one block
  auto run = [&](uint64_t bound) {
    Shard s(l, 1000us);
    s.oc->set_max_writeback(bound);
    s.oc->start();
    for (uint64_t b = 0; b < 1536; b++)
      s.write("wb." + std::to_string(b), 0, 'a' + b % 26);
    // what the flusher sent; the flush below is not held back
    uint64_t peak = s.wb->max_inflight;
    s.flush();
    {
      std::lock_guard g(s.lock);
      for (uint64_t b = 0; b < 1536; b++)
        EXPECT_EQ(std::string(BLK, 'a' + b % 26),
                  s.wb->stored(object_t("wb." + std::to_string(b)), 0))
          << "block " << b;
    }
    s.stop();
    return peak;
  };
  uint64_t bounded = run(BOUND);
  EXPECT_LE(bounded, BOUND + BLK);
  uint64_t unbounded = run(0);
  EXPECT_GT(unbounded, BOUND + BLK);
}

/*
 * Writers of the members of a budget group, far more than the dirty limit
 * holds, with only one throttled writer woken at a time: each wake-up
 * passes on what room is left, so every writer finishes, and the OSD gets
 * the last write of every block.
 */
TEST(ObjectCacherShard, WokenWritersOneAtATime)
{
  Limits l;
  l.max_size = 8 << 20;
  l.max_objects = 4000;
  l.max_dirty = 256 << 10;
  l.target_dirty = 128 << 10;
  constexpr int N = 4, T = 32;
  std::vector<ObjectCacher*> group;
  std::vector<std::unique_ptr<Shard>> shards;
  for (int i = 0; i < N; i++) {
    shards.push_back(std::make_unique<Shard>(l, 200us));
    group.push_back(shards.back()->oc.get());
  }
  for (auto& s : shards) {
    s->oc->set_budget_group(&group);
    s->oc->set_max_woken_writers(1);
    s->oc->start();
  }
  std::vector<std::thread> writers;
  for (int t = 0; t < T; t++) {
    writers.emplace_back([&, t] {
      for (uint64_t b = 0; b < 64; b++)
        shards[t % N]->write("w" + std::to_string(t), b, 'a' + (t + b) % 26);
    });
  }
  for (auto& w : writers)
    w.join();
  for (auto& s : shards)
    s->flush();
  for (int t = 0; t < T; t++) {
    auto& s = *shards[t % N];
    std::lock_guard g(s.lock);
    for (uint64_t b = 0; b < 64; b++)
      EXPECT_EQ(std::string(BLK, 'a' + (t + b) % 26),
                s.wb->stored(object_t("w" + std::to_string(t)), b * BLK))
        << "writer " << t << " block " << b;
  }
  for (auto& s : shards)
    s->stop();
}

/*
 * With set_flush_min_age(), the flusher that only brings the cache down
 * to its dirty target leaves data younger than that age in the cache, and
 * sends it once it is older.
 */
TEST(ObjectCacherShard, FlushMinAgeLeavesYoungData)
{
  Limits l;
  l.max_dirty = 8 << 20;
  l.target_dirty = 256 << 10;
  l.max_dirty_age = 30;   // the age flush stays out of it
  Shard s(l, 0us);
  s.oc->set_flush_min_age(ceph::make_timespan(2.0));
  s.oc->start();
  for (uint64_t b = 0; b < 256; b++)   // 1 MiB: over the target
    s.write("age." + std::to_string(b), 0, 'a' + b % 26);
  std::this_thread::sleep_for(1s);
  EXPECT_EQ(0u, s.wb->max_inflight.load()) << "young data was sent";
  EXPECT_GT(s.dirty_tx(), l.target_dirty);
  // once older than the minimum age, it goes down to the target
  auto end = std::chrono::steady_clock::now() + 10s;
  while (s.dirty_tx() > l.target_dirty && std::chrono::steady_clock::now() < end)
    std::this_thread::sleep_for(100ms);
  EXPECT_LE(s.dirty_tx(), l.target_dirty);
  EXPECT_GT(s.wb->max_inflight.load(), 0u);
  s.stop();
}

/*
 * With scattered writeback, a flusher a little over the dirty target
 * sends only about as much as it is over (a few BufferHeads of an
 * object) by default, and all of the object's dirty data in one write
 * with set_flush_whole_objects(); the data is right either way.
 */
TEST(ObjectCacherShard, FlushWholeObjects)
{
  constexpr int NOBJ = 4, NBLK = 16;   // 16 blocks apart in each object
  for (bool whole : {false, true}) {
    Limits l;
    l.max_dirty = 8 << 20;
    l.max_dirty_age = 30;   // the age flush stays out of it
    l.target_dirty = NOBJ * NBLK * BLK - 2 * BLK;   // over it by 2 blocks
    Shard s(l, 0us, true);
    s.oc->set_flush_whole_objects(whole);
    s.oc->start();
    for (int o = 0; o < NOBJ; o++)
      for (int b = 0; b < NBLK; b++)
        s.write("whole." + std::to_string(o), b * 2, 'a' + (o + b) % 26);
    // the flusher looks once a second
    auto end = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < end) {
      {
        std::lock_guard g(s.lock);
        if (!s.wb->pieces.empty())
          break;
      }
      std::this_thread::sleep_for(50ms);
    }
    {
      std::lock_guard g(s.lock);
      ASSERT_FALSE(s.wb->pieces.empty()) << "nothing flushed";
      if (whole)
        EXPECT_EQ(size_t(NBLK), s.wb->pieces[0]) << "not the whole object";
      else
        EXPECT_LE(s.wb->pieces[0], size_t(3)) << "more than it was over";
    }
    s.flush();
    {
      std::lock_guard g(s.lock);
      for (int o = 0; o < NOBJ; o++)
        for (int b = 0; b < NBLK; b++)
          EXPECT_EQ(std::string(BLK, 'a' + (o + b) % 26),
                    s.wb->stored(object_t("whole." + std::to_string(o)), b * 2 * BLK))
            << "object " << o << " block " << b;
    }
    s.stop();
  }
}

/*
 * Writes to the OSD fail for a while.  The cache marks their data dirty
 * again and sends it later; meanwhile writers held back by the dirty limit
 * and the writeback bound must still be let through (the failed writes are
 * out of flight), and once the OSD takes writes again every block reaches
 * it with its last value.  Both with one write per BufferHead and with the
 * whole dirty data of an object in one scattered write.
 */
TEST(ObjectCacherShard, WriteErrorsAreRetried)
{
  for (bool whole : {false, true}) {
    SCOPED_TRACE(whole ? "whole objects" : "per BufferHead");
    Limits l;
    l.max_dirty = 512 << 10;
    l.target_dirty = 64 << 10;
    Shard s(l, 200us, whole);
    s.oc->set_max_writeback(64 << 10);
    s.oc->set_flush_whole_objects(whole);
    s.wb->fail_writes = 20;
    s.oc->start();

    constexpr int T = 4, NB = 256;
    std::vector<std::thread> writers;
    std::atomic<int> done{0};
    for (int t = 0; t < T; t++) {
      writers.emplace_back([&, t] {
        for (int b = 0; b < NB; b++)
          s.write("err." + std::to_string(t) + "." + std::to_string(b / 16),
                  b % 16, 'a' + (t + b) % 26);
        done++;
      });
    }
    auto end = std::chrono::steady_clock::now() + 60s;
    while (done < T && std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(50ms);
    if (done < T) {
      ADD_FAILURE() << T - done << " writers stuck";
      std::abort();
    }
    for (auto& w : writers)
      w.join();
    EXPECT_GT(s.wb->failed.load(), 0) << "no write failed";

    // a flush can come back with the error of a write that failed: flush
    // until nothing is dirty or in flight
    for (int i = 0; i < 50 && s.dirty_tx(); i++)
      s.flush();
    EXPECT_EQ(0u, s.dirty_tx());
    {
      std::lock_guard g(s.lock);
      EXPECT_EQ(0u, s.wb->in_flight());
      EXPECT_EQ(0u, s.oc->get_stat_tx());
      for (int t = 0; t < T; t++)
        for (int b = 0; b < NB; b++)
          EXPECT_EQ(std::string(BLK, 'a' + (t + b) % 26),
                    s.wb->stored(object_t("err." + std::to_string(t) + "." +
                                          std::to_string(b / 16)),
                                 (b % 16) * BLK))
            << "writer " << t << " block " << b;
    }
    s.stop();
  }
}

/*
 * Two members of a budget group share a writeback bound of four blocks.
 * B has three blocks in flight on a slow OSD, A two of its own: A's
 * flusher, with a third block to send, sleeps on the group's bound.  When
 * B's second write completes there is room, and A must send its block
 * then: woken by B (kick_flusher()), not by its own one-second timeout.
 */
TEST(ObjectCacherShard, GroupMemberFlusherWokenWhenRoomFrees)
{
  Limits l;
  l.target_dirty = 0;   // the flusher sends whatever is dirty
  std::vector<ObjectCacher*> group;
  Shard a(l, 2500ms), b(l, 700ms);
  group = {a.oc.get(), b.oc.get()};
  for (Shard *s : {&a, &b}) {
    s->oc->set_budget_group(&group);
    s->oc->set_max_writeback(4 * BLK);
    s->oc->start();
  }
  auto sent = [](Shard& s) {
    std::lock_guard g(s.lock);
    return s.wb->sent.size();
  };
  auto wait_sent = [&](Shard& s, size_t n, std::chrono::seconds t) {
    auto end = std::chrono::steady_clock::now() + t;
    while (sent(s) < n && std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(5ms);
    return sent(s) >= n;
  };
  for (int i = 0; i < 3; i++)
    b.write("kick.b" + std::to_string(i), 0, 'b');
  ASSERT_TRUE(wait_sent(b, 3, 5s));
  for (int i = 0; i < 3; i++)
    a.write("kick.a" + std::to_string(i), 0, 'a');
  ASSERT_TRUE(wait_sent(a, 2, 5s));
  ASSERT_TRUE(wait_sent(a, 3, 5s));
  {
    std::lock_guard ga(a.lock), gb(b.lock);
    ASSERT_GE(b.wb->committed.size(), 2u);
    auto room = b.wb->committed[1];   // the group under its bound from here
    auto third = a.wb->sent[2];
    EXPECT_GE(third, room) << "sent while the group was at its bound";
    EXPECT_LT(third - room, 450ms)
      << "A's flusher slept on after B's writeback ended: "
      << std::chrono::duration_cast<std::chrono::milliseconds>(third - room).count()
      << " ms";
  }
  a.flush();
  b.flush();
  {
    std::lock_guard ga(a.lock), gb(b.lock);
    for (int i = 0; i < 3; i++) {
      EXPECT_EQ(std::string(BLK, 'a'), a.wb->stored(object_t("kick.a" + std::to_string(i)), 0));
      EXPECT_EQ(std::string(BLK, 'b'), b.wb->stored(object_t("kick.b" + std::to_string(i)), 0));
    }
  }
  a.stop();
  b.stop();
}
