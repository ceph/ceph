// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Many threads write buffered data while the data pool runs out of room
 * (a pool quota here, so the cluster is left as it was): every writer gets
 * ENOSPC or EDQUOT and returns, none hangs on the dirty limit or on the
 * writeback in flight, and once the quota is gone writes succeed again.
 * The buffered writes go through the client's fast path when it applies,
 * which must hand a full pool over to the slow path.
 */

#include <errno.h>

#include <atomic>
#include <functional>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <fmt/format.h>

#include "common/Cond.h"
#include "test/client/TestClient.h"

using namespace std::chrono_literals;

namespace {

int mon_command(MonClient *mc, const std::string& cmd, std::string *outs = nullptr)
{
  bufferlist outbl;
  std::string s;
  C_SaferCond c;
  mc->start_mon_command(std::vector<std::string>{cmd}, bufferlist{}, &outbl, &s, &c);
  int r = c.wait();
  if (outs)
    *outs = s;
  return r;
}

} // anonymous namespace

TEST_F(TestClient, ThreadsWriteIntoPoolQuota)
{
  const std::vector<int64_t> &data_pools = client->mdsmap->get_data_pools();
  ASSERT_EQ(1u, data_pools.size());
  std::string pool = objecter->with_osdmap([&](const OSDMap& m) {
    return m.get_pool_name(data_pools[0]);
  });
  ASSERT_FALSE(pool.empty());
  auto set_quota = [&](const std::string& bytes) {
    std::string outs;
    int r = mon_command(mc, fmt::format(
      R"({{"prefix": "osd pool set-quota", "pool": "{}", "field": "max_bytes", "val": "{}"}})",
      pool, bytes), &outs);
    EXPECT_EQ(0, r) << outs;
  };
  // the quota goes, whatever happens below
  struct Unquota {
    std::function<void()> f;
    ~Unquota() { f(); }
  } unquota{[&] { set_quota("0"); }};

  Inode *root = client->get_root();
  ASSERT_NE(nullptr, root);
  constexpr int T = 8;
  constexpr int64_t CHUNK = 64 << 10;
  const std::string data(CHUNK, 'q');
  std::vector<Inode*> ins(T, nullptr);
  std::vector<Fh*> fhs(T, nullptr);
  std::vector<std::string> names(T);
  for (int t = 0; t < T; t++) {
    struct ceph_statx stx;
    names[t] = fmt::format("threads_full.{}.{}", getpid(), t);
    ASSERT_EQ(0, client->ll_createx(root, names[t].c_str(), 0644,
                                    O_RDWR | O_CREAT | O_TRUNC,
                                    &ins[t], &fhs[t], &stx, 0, 0, myperm));
  }

  // a clean file, written and synced while there is room
  std::string cname = fmt::format("threads_full.{}.clean", getpid());
  Inode *cin = nullptr;
  Fh *cfh = nullptr;
  {
    struct ceph_statx stx;
    ASSERT_EQ(0, client->ll_createx(root, cname.c_str(), 0644, O_RDWR | O_CREAT | O_TRUNC,
                                    &cin, &cfh, &stx, 0, 0, myperm));
    ASSERT_EQ(CHUNK, client->ll_write(cfh, 0, CHUNK, data.data()));
    ASSERT_EQ(0, client->ll_fsync(cfh, false));
  }

  std::atomic<bool> stop{false};
  std::atomic<int> finished{0}, full{0}, other{0};
  std::atomic<int64_t> written{0};
  std::vector<std::thread> th;
  for (int t = 0; t < T; t++) {
    th.emplace_back([&, t] {
      int64_t off = 0;
      for (int i = 0; !stop; i++) {
        int r = client->ll_write(fhs[t], off, CHUNK, data.data());
        if (r >= 0) {
          off += r;
          written += r;
          if (i % 16 == 15)
            r = client->ll_fsync(fhs[t], false);
        }
        if (r == -ENOSPC || r == -EDQUOT) {
          full++;
          break;
        }
        if (r < 0) {
          other++;
          break;
        }
      }
      finished++;
    });
  }
  // let them write a while, then take the room away; the mon sets the
  // pool's full flag once the stats it gets show the pool over its quota
  std::this_thread::sleep_for(2s);
  set_quota("1");
  auto end = std::chrono::steady_clock::now() + 180s;
  while (finished < T && std::chrono::steady_clock::now() < end)
    std::this_thread::sleep_for(100ms);
  stop = true;
  // a writer still in a call after the deadline is stuck
  end = std::chrono::steady_clock::now() + 60s;
  while (finished < T && std::chrono::steady_clock::now() < end)
    std::this_thread::sleep_for(100ms);
  if (finished < T) {
    ADD_FAILURE() << T - finished << " writers stuck with the pool full";
    std::abort();
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(T, full.load()) << "writers that never saw the pool full";
  EXPECT_EQ(0, other.load()) << "writers that failed otherwise";
  EXPECT_GT(written.load(), 0);

  // with the pool still full, a buffered write itself fails at once: it
  // must not go into the cache (the fast path) to fail only at fsync
  for (int t = 0; t < T; t++) {
    int r = client->ll_write(fhs[t], 0, CHUNK, data.data());
    EXPECT_TRUE(r == -ENOSPC || r == -EDQUOT)
      << "a write to file " << t << " in a full pool returned " << r;
  }
  // the same for a file that has the caps to buffer writes, no dirty data
  // and no error (made, written and synced before the pool filled, then
  // left alone): only the full pool can turn this in-place write away
  {
    int r = client->ll_write(cfh, 0, CHUNK, data.data());
    EXPECT_TRUE(r == -ENOSPC || r == -EDQUOT)
      << "an in-place write to a clean file in a full pool returned " << r;
  }

  // room again: a new write and its fsync succeed, once the flag is gone
  set_quota("0");
  bool ok = false;
  end = std::chrono::steady_clock::now() + 120s;
  while (!ok && std::chrono::steady_clock::now() < end) {
    int r = client->ll_write(fhs[0], 0, CHUNK, data.data());
    if (r == CHUNK)
      r = client->ll_fsync(fhs[0], false);
    ok = (r == 0);
    if (!ok)
      std::this_thread::sleep_for(1s);
  }
  EXPECT_TRUE(ok) << "writes still fail after the quota went";

  for (int t = 0; t < T; t++) {
    client->ll_release(fhs[t]);
    client->ll_put(ins[t]);
    client->ll_unlink(root, names[t].c_str(), myperm);
  }
  client->ll_release(cfh);
  client->ll_put(cin);
  client->ll_unlink(root, cname.c_str(), myperm);
  client->ll_put(root);
}
