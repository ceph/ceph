// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Many threads of one mount, as in ceph-fuse, nfs-ganesha or Samba: cached
 * reads, buffered writes, getattr, lookup, open/close and file locks run
 * at the same time, on files of their own and on shared ones, while a
 * second client looks at the same files.  Every test checks what was
 * written against what is read back, from this mount and from the other.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "common/ceph_json.h"
#if defined(__linux__)
#include "client/FSCrypt.h"
#endif

#ifndef O_DIRECT
#define O_DIRECT 0   // none on Windows: those writers buffer too
#endif

using namespace std::chrono_literals;

namespace {

constexpr int64_t BLK = 4096;

// CEPH_TEST_THREADS_SCALE multiplies the rounds and the run times of the
// tests below, CEPH_TEST_THREADS_WIDE the threads: 1 (the default) for a
// quick run, more for a stress run
int env_factor(const char *name)
{
  const char *e = getenv(name);
  int v = e ? atoi(e) : 1;
  return v > 0 ? v : 1;
}
int scale()
{
  static const int s = env_factor("CEPH_TEST_THREADS_SCALE");
  return s;
}
int wide()
{
  static const int w = env_factor("CEPH_TEST_THREADS_WIDE");
  return w;
}

struct Mount {
  ceph_mount_info *cmount = nullptr;
  Inode *root = nullptr;
  UserPerm *perms = nullptr;

  // @conf: options to set before mounting
  explicit Mount(const std::map<std::string, std::string>& conf = {}) {
    EXPECT_EQ(0, ceph_create(&cmount, nullptr));
    EXPECT_EQ(0, ceph_conf_read_file(cmount, nullptr));
    EXPECT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
    for (auto& [k, v] : conf)
      EXPECT_EQ(0, ceph_conf_set(cmount, k.c_str(), v.c_str())) << k;
    EXPECT_EQ(0, ceph_mount(cmount, "/"));
    EXPECT_EQ(0, ceph_ll_lookup_root(cmount, &root));
    perms = ceph_mount_perms(cmount);
  }
  ~Mount() {
    ceph_ll_put(cmount, root);
    ceph_shutdown(cmount);
  }

  Inode *lookup(Inode *dir, const std::string& name, int *r = nullptr) {
    Inode *in = nullptr;
    struct ceph_statx stx;
    int ret = ceph_ll_lookup(cmount, dir, name.c_str(), &in, &stx,
                             CEPH_STATX_INO, 0, perms);
    if (r)
      *r = ret;
    return ret == 0 ? in : nullptr;
  }
  Inode *mkdir(Inode *dir, const std::string& name) {
    Inode *in = nullptr;
    struct ceph_statx stx;
    EXPECT_EQ(0, ceph_ll_mkdir(cmount, dir, name.c_str(), 0755, &in, &stx,
                               0, 0, perms));
    return in;
  }
  // create @name in @dir, @size bytes of zeroes, and leave it open
  Fh *create(Inode *dir, const std::string& name, int64_t size,
             Inode **inp) {
    Fh *fh = nullptr;
    struct ceph_statx stx;
    EXPECT_EQ(0, ceph_ll_create(cmount, dir, name.c_str(), 0644,
                                O_RDWR | O_CREAT | O_TRUNC, inp, &fh, &stx,
                                0, 0, perms));
    std::string zero(1 << 20, '\0');
    for (int64_t off = 0; off < size; off += zero.size()) {
      int64_t n = std::min<int64_t>(zero.size(), size - off);
      EXPECT_EQ(n, ceph_ll_write(cmount, fh, off, n, zero.data()));
    }
    return fh;
  }
  Fh *open(Inode *in, int flags) {
    Fh *fh = nullptr;
    EXPECT_EQ(0, ceph_ll_open(cmount, in, flags, &fh, perms));
    return fh;
  }
  std::string read(Fh *fh, int64_t off, int64_t len) {
    std::string buf(len, '\0');
    int r = ceph_ll_read(cmount, fh, off, len, buf.data());
    if (r < 0)
      return "error " + std::to_string(r);
    buf.resize(r);
    return buf;
  }
};

std::string unique_name(const char *what)
{
  return std::string(what) + "." + std::to_string(getpid()) + "." +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

std::string block(char v, int64_t len = BLK)
{
  return std::string(len, v);
}

bool uniform(const std::string& s)
{
  return !s.empty() &&
         std::all_of(s.begin(), s.end(), [&](char c) { return c == s[0]; });
}

// the contents of /@dname/@name as another client sees it, opening it
// afresh (close-to-open)
std::string read_via(Mount& m, const std::string& dname, const std::string& name)
{
  Inode *dir = m.lookup(m.root, dname);
  if (!dir)
    return "no dir";
  Inode *in = m.lookup(dir, name);
  if (!in) {
    ceph_ll_put(m.cmount, dir);
    return "no file";
  }
  struct ceph_statx stx;
  ceph_ll_getattr(m.cmount, in, &stx, CEPH_STATX_SIZE, 0, m.perms);
  Fh *fh = m.open(in, O_RDONLY);
  std::string r = m.read(fh, 0, stx.stx_size);
  ceph_ll_close(m.cmount, fh);
  ceph_ll_put(m.cmount, in);
  ceph_ll_put(m.cmount, dir);
  return r;
}

// each 4K block of @s is one byte repeated (a short last block too)
int torn_blocks(const std::string& s)
{
  int bad = 0;
  for (size_t off = 0; off < s.size(); off += BLK)
    if (!uniform(s.substr(off, BLK)))
      bad++;
  return bad;
}

} // anonymous namespace

/*
 * Each thread rewrites random 4K and 64K pieces of a file of its own,
 * reading pieces back between writes; the file's contents, on this mount
 * and on another client, are then exactly what the thread wrote.
 */
TEST(LibCephFSThreads, PrivateFiles)
{
  Mount a;
  std::string dname = unique_name("thr_priv");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);

  const int T = 16 * wide();
  constexpr int64_t SIZE = 1 << 20;
  std::vector<std::string> model(T, std::string(SIZE, '\0'));
  std::vector<Inode*> ins(T);
  std::vector<Fh*> fhs(T);
  for (int t = 0; t < T; t++)
    fhs[t] = a.create(dir, "f" + std::to_string(t), SIZE, &ins[t]);

  std::atomic<int> bad{0};
  std::vector<std::thread> th;
  for (int t = 0; t < T; t++) {
    th.emplace_back([&, t] {
      std::mt19937 rng(t + 1);
      for (int i = 0; i < 2000 * scale(); i++) {
        int64_t len = (rng() % 4) ? BLK : 16 * BLK;
        int64_t off = (rng() % (SIZE / len)) * len;
        std::string data = block('a' + (t + i) % 26, len);
        if (ceph_ll_write(a.cmount, fhs[t], off, len, data.data()) != len)
          bad++;
        model[t].replace(off, len, data);
        int64_t roff = (rng() % (SIZE / BLK)) * BLK;
        if (a.read(fhs[t], roff, BLK) != model[t].substr(roff, BLK))
          bad++;
      }
    });
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, bad.load());

  for (int t = 0; t < T; t++) {
    EXPECT_EQ(model[t], a.read(fhs[t], 0, SIZE)) << "file " << t;
    ASSERT_EQ(0, ceph_ll_close(a.cmount, fhs[t]));
  }

  Mount b;
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  for (int t = 0; t < T; t++) {
    Inode *in = b.lookup(bdir, "f" + std::to_string(t));
    ASSERT_NE(nullptr, in);
    Fh *fh = b.open(in, O_RDONLY);
    EXPECT_EQ(model[t], b.read(fh, 0, SIZE)) << "file " << t << " on mount b";
    ceph_ll_close(b.cmount, fh);
    ceph_ll_put(b.cmount, in);
  }
  ceph_ll_put(b.cmount, bdir);
  for (int t = 0; t < T; t++)
    ceph_ll_put(a.cmount, ins[t]);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Writers rewrite whole 4K blocks of one shared file, each its own blocks,
 * while readers of the same mount read random blocks: a reader never sees
 * a block that mixes two writes.  A second client, which opens and reads
 * the file now and then and so takes the buffered-write caps away from the
 * writers, sees whole blocks too, and in the end exactly the last writes.
 */
static void shared_file_with_another_client(
  const std::map<std::string, std::string>& conf)
{
  Mount a(conf);
  std::string dname = unique_name("thr_shared");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int W = 16 * wide(), R = 4 * wide();
  constexpr int64_t NBLK = 1024;
  Inode *in = nullptr;
  Fh *wfh = a.create(dir, "shared", NBLK * BLK, &in);
  std::vector<char> last(NBLK, '\0');

  std::atomic<bool> stop{false};
  std::atomic<int> bad_write{0}, torn{0}, torn_b{0};
  std::atomic<uint64_t> b_reads{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 11);
      for (int i = 0; i < 3000 * scale(); i++) {
        int64_t b = (rng() % (NBLK / W)) * W + w;   // blocks b % W == w
        char v = 'a' + (i + w) % 26;
        std::string data = block(v);
        if (ceph_ll_write(a.cmount, wfh, b * BLK, BLK, data.data()) != BLK)
          bad_write++;
        last[b] = v;
      }
    });
  }
  for (int r = 0; r < R; r++) {
    th.emplace_back([&, r] {
      std::mt19937 rng(r + 101);
      Fh *fh = a.open(in, O_RDONLY);
      while (!stop) {
        int64_t b = rng() % NBLK;
        std::string s = a.read(fh, b * BLK, BLK);
        if (s.size() != size_t(BLK) || !uniform(s))
          torn++;
      }
      ceph_ll_close(a.cmount, fh);
    });
  }
  std::thread other([&] {
    Mount b;
    Inode *bdir = b.lookup(b.root, dname);
    Inode *bin = bdir ? b.lookup(bdir, "shared") : nullptr;
    if (!bin) {
      torn_b++;
      stop = true;
      return;
    }
    std::mt19937 rng(7);
    while (!stop) {
      Fh *fh = b.open(bin, O_RDONLY);
      for (int k = 0; k < 16; k++) {
        int64_t blk = rng() % NBLK;
        std::string s = b.read(fh, blk * BLK, BLK);
        if (s.size() != size_t(BLK) || !uniform(s))
          torn_b++;
        b_reads++;
      }
      ceph_ll_close(b.cmount, fh);
      std::this_thread::sleep_for(20ms);
    }
    ceph_ll_put(b.cmount, bin);
    ceph_ll_put(b.cmount, bdir);
  });
  for (int w = 0; w < W; w++)
    th[w].join();
  stop = true;
  for (size_t i = W; i < th.size(); i++)
    th[i].join();
  other.join();

  EXPECT_EQ(0, bad_write.load());
  EXPECT_EQ(0, torn.load());
  EXPECT_EQ(0, torn_b.load());
  EXPECT_GT(b_reads.load(), 0u);

  ASSERT_EQ(0, ceph_ll_fsync(a.cmount, wfh, 0));
  ASSERT_EQ(0, ceph_ll_close(a.cmount, wfh));
  Mount b;
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  Inode *bin = b.lookup(bdir, "shared");
  ASSERT_NE(nullptr, bin);
  Fh *fh = b.open(bin, O_RDONLY);
  std::string all = b.read(fh, 0, NBLK * BLK);
  ASSERT_EQ(size_t(NBLK * BLK), all.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), all.substr(blk * BLK, BLK)) << "block " << blk;
  ceph_ll_close(b.cmount, fh);
  ceph_ll_put(b.cmount, bin);
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

TEST(LibCephFSThreads, SharedFileWithAnotherClient)
{
  shared_file_with_another_client({});
}

/*
 * The same with the writes of the shared file done two at a time
 * (client_write_combine_max): most writers find their write left for the
 * next holder of the file's lock.
 */
TEST(LibCephFSThreads, SharedFileSmallWriteBatches)
{
  shared_file_with_another_client({{"client_write_combine_max", "2"}});
}

/*
 * Many threads rewrite the SAME blocks of one file, 4K and 8K at a time,
 * each with its own byte, while other threads of the mount read them: the
 * queued writes of the file are done together by whichever thread holds
 * its lock, yet no read ever sees a block with two writers' bytes, every
 * block ends up with the byte of one writer, the size is right, and this
 * mount and another client read the same file.  With batches of 1, 2 and
 * 32 (the default) and 4096 queued writes.
 */
TEST(LibCephFSThreads, WriteCombineSameBlocks)
{
  for (const char *cmax : {"1", "2", "32", "4096"}) {
    SCOPED_TRACE(std::string("client_write_combine_max ") + cmax);
    Mount a({{"client_write_combine_max", cmax}});
    std::string dname = unique_name("thr_samebl");
    Inode *dir = a.mkdir(a.root, dname);
    ASSERT_NE(nullptr, dir);
    constexpr int NB = 32;
    Inode *in = nullptr;
    Fh *cfh = a.create(dir, "f", NB * BLK, &in);
    ASSERT_NE(nullptr, cfh);
    ceph_ll_close(a.cmount, cfh);
    const int W = 16 * wide(), R = 4;
    std::atomic<bool> stop{false};
    std::atomic<int> torn{0}, err{0}, alien{0};
    std::atomic<int64_t> writes{0}, reads{0};
    auto valid = [&](char c) { return c == 0 || (c >= 'A' && c < 'A' + W); };
    std::vector<std::thread> th;
    for (int w = 0; w < W; w++) {
      th.emplace_back([&, w] {
        Fh *fh = a.open(in, O_RDWR);
        if (!fh) { err++; return; }
        std::mt19937 rng(w + 101);
        const std::string d = block('A' + w, 2 * BLK);
        while (!stop) {
          int nb = 1 + rng() % 2;
          int64_t off = int64_t(rng() % (NB - nb + 1)) * BLK;
          if (ceph_ll_write(a.cmount, fh, off, nb * BLK, d.data()) != nb * BLK)
            err++;
          else
            writes++;
        }
        ceph_ll_close(a.cmount, fh);
      });
    }
    for (int r = 0; r < R; r++) {
      th.emplace_back([&, r] {
        Fh *fh = a.open(in, O_RDONLY);
        if (!fh) { err++; return; }
        std::mt19937 rng(r + 7);
        while (!stop) {
          int64_t off = int64_t(rng() % NB) * BLK;
          std::string s = a.read(fh, off, BLK);
          reads++;
          if (s.size() != size_t(BLK)) { err++; continue; }
          if (!uniform(s)) torn++;
          if (!valid(s[0])) alien++;
        }
        ceph_ll_close(a.cmount, fh);
      });
    }
    std::this_thread::sleep_for(3s * scale());
    stop = true;
    for (auto& x : th)
      x.join();
    EXPECT_EQ(0, err.load());
    EXPECT_EQ(0, torn.load()) << "reads that saw two writers' bytes in a block";
    EXPECT_EQ(0, alien.load());
    EXPECT_GT(writes.load(), 0);
    EXPECT_GT(reads.load(), 0);

    struct ceph_statx stx;
    ASSERT_EQ(0, ceph_ll_getattr(a.cmount, in, &stx, CEPH_STATX_SIZE, 0, a.perms));
    EXPECT_EQ(uint64_t(NB * BLK), stx.stx_size);
    std::string mine = read_via(a, dname, "f");
    ASSERT_EQ(size_t(NB * BLK), mine.size());
    EXPECT_EQ(0, torn_blocks(mine));
    for (int b = 0; b < NB; b++)
      EXPECT_TRUE(valid(mine[b * BLK])) << "block " << b;
    // the other client takes the caps away: it gets what was written back
    Mount b;
    std::string theirs = read_via(b, dname, "f");
    EXPECT_TRUE(mine == theirs) << "the other client reads other data";
    ceph_ll_put(a.cmount, in);
    ceph_ll_put(a.cmount, dir);
  }
}

/*
 * getattr runs without a lock while a writer appends to the file and
 * rewrites it: the size, mtime and change attribute it returns never go
 * back, and at the end match what the writer did.
 */
TEST(LibCephFSThreads, GetattrWhileWriting)
{
  Mount a;
  std::string dname = unique_name("thr_getattr");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "grow", BLK, &in);

  std::atomic<bool> stop{false};
  std::atomic<int> back{0}, err{0};
  std::vector<std::thread> th;
  for (int t = 0; t < 8 * wide(); t++) {
    th.emplace_back([&] {
      uint64_t size = 0, ver = 0;
      int64_t mtime = 0;
      while (!stop) {
        struct ceph_statx stx;
        if (ceph_ll_getattr(a.cmount, in, &stx,
                            CEPH_STATX_SIZE | CEPH_STATX_MTIME | CEPH_STATX_VERSION,
                            0, a.perms) != 0) {
          err++;
          continue;
        }
        int64_t m = int64_t(stx.stx_mtime.tv_sec) * 1000000000 + stx.stx_mtime.tv_nsec;
        if (stx.stx_size < size || m < mtime || stx.stx_version < ver)
          back++;
        size = stx.stx_size;
        mtime = m;
        ver = stx.stx_version;
      }
    });
  }
  int64_t size = BLK;
  std::mt19937 rng(3);
  std::string data = block('z');
  for (int i = 0; i < 4000 * scale(); i++) {
    if (i % 4 == 0) {
      ASSERT_EQ(BLK, ceph_ll_write(a.cmount, fh, size, BLK, data.data()));
      size += BLK;
    } else {
      int64_t off = (rng() % (size / BLK)) * BLK;
      ASSERT_EQ(BLK, ceph_ll_write(a.cmount, fh, off, BLK, data.data()));
    }
  }
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, back.load());
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_getattr(a.cmount, in, &stx, CEPH_STATX_SIZE, 0, a.perms));
  EXPECT_EQ(uint64_t(size), stx.stx_size);
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Threads look names up, getattr them and open and close them, cached and
 * not cached, while another client creates and removes a name in the same
 * directory: once the other client's create (or unlink) has returned, a
 * lookup on this mount finds the name (or ENOENT) at once, however hot the
 * cached answer was.
 */
TEST(LibCephFSThreads, LookupOpenCloseAgainstAnotherClient)
{
  Mount a;
  std::string dname = unique_name("thr_lookup");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int NF = 64;
  for (int i = 0; i < NF; i++) {
    Inode *in = nullptr;
    Fh *fh = a.create(dir, "f" + std::to_string(i), BLK * (i + 1), &in);
    ceph_ll_close(a.cmount, fh);
    ceph_ll_put(a.cmount, in);
  }

  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};
  std::vector<std::thread> th;
  for (int t = 0; t < 8 * wide(); t++) {
    th.emplace_back([&, t] {
      std::mt19937 rng(t + 5);
      while (!stop) {
        int i = rng() % NF;
        int r;
        Inode *in = a.lookup(dir, "f" + std::to_string(i), &r);
        if (!in) {
          bad++;
          continue;
        }
        struct ceph_statx stx;
        if (ceph_ll_getattr(a.cmount, in, &stx, CEPH_STATX_SIZE, 0, a.perms) ||
            stx.stx_size != uint64_t(BLK * (i + 1)))
          bad++;
        Fh *fh = a.open(in, O_RDONLY);
        if (fh)
          ceph_ll_close(a.cmount, fh);
        else
          bad++;
        ceph_ll_put(a.cmount, in);
        // a name that is never there, and the one the other client flips:
        // keep their answers cached and hot
        if (a.lookup(dir, "never", &r) || r != -ENOENT)
          bad++;
        Inode *f = a.lookup(dir, "flip", &r);
        if (f)
          ceph_ll_put(a.cmount, f);
        else if (r != -ENOENT)
          bad++;
      }
    });
  }

  Mount b;
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  int missed_create = 0, missed_unlink = 0;
  for (int i = 0; i < 200 * scale(); i++) {
    Inode *bin = nullptr;
    Fh *bfh = nullptr;
    struct ceph_statx stx;
    ASSERT_EQ(0, ceph_ll_create(b.cmount, bdir, "flip", 0644,
                                O_RDWR | O_CREAT | O_EXCL, &bin, &bfh, &stx,
                                0, 0, b.perms));
    ceph_ll_close(b.cmount, bfh);
    ceph_ll_put(b.cmount, bin);
    int r;
    Inode *in = a.lookup(dir, "flip", &r);
    if (in)
      ceph_ll_put(a.cmount, in);
    else
      missed_create++;
    ASSERT_EQ(0, ceph_ll_unlink(b.cmount, bdir, "flip", b.perms));
    in = a.lookup(dir, "flip", &r);
    if (in) {
      missed_unlink++;
      ceph_ll_put(a.cmount, in);
    } else if (r != -ENOENT) {
      missed_unlink++;
    }
  }
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, bad.load());
  EXPECT_EQ(0, missed_create);
  EXPECT_EQ(0, missed_unlink);
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Threads of one mount, each a lock owner of its own, take and drop a
 * write lock on the same range of one file: at most one holds it at a
 * time, and every one of them gets it.
 */
TEST(LibCephFSThreads, FileLocksOneMount)
{
  Mount a;
  std::string dname = unique_name("thr_lock");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "lock", BLK, &in);

  const int T = 8 * wide(), N = 100 * scale();
  std::atomic<int> holder{-1}, overlap{0}, err{0};
  std::vector<int> got(T, 0);
  std::vector<std::thread> th;
  for (int t = 0; t < T; t++) {
    th.emplace_back([&, t] {
      for (int i = 0; i < N; i++) {
        struct flock fl = {};
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = BLK;
        if (ceph_ll_setlk(a.cmount, fh, &fl, 1000 + t, 1) != 0) {
          err++;
          continue;
        }
        int none = -1;
        if (!holder.compare_exchange_strong(none, t))
          overlap++;
        got[t]++;
        std::this_thread::sleep_for(100us);
        holder = -1;
        fl.l_type = F_UNLCK;
        if (ceph_ll_setlk(a.cmount, fh, &fl, 1000 + t, 0) != 0)
          err++;
      }
    });
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, overlap.load());
  for (int t = 0; t < T; t++)
    EXPECT_EQ(N, got[t]) << "owner " << t;
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Writers rewrite whole blocks of a file while another thread of the
 * mount truncates it to half its size and back, and readers read it: no
 * block is ever half one write and half another or half zeroes, and once
 * the last truncate is done the file has its size and whole blocks, on
 * this mount and on another client.
 */
TEST(LibCephFSThreads, TruncateWhileWriting)
{
  Mount a;
  std::string dname = unique_name("thr_trunc");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int64_t NBLK = 256, SIZE = NBLK * BLK;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", SIZE, &in);

  std::atomic<bool> stop{false};
  std::atomic<int> err{0}, torn{0};
  std::vector<std::thread> th;
  for (int w = 0; w < 8 * wide(); w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 1);
      while (!stop) {
        std::string d = block('a' + rng() % 26);
        if (ceph_ll_write(a.cmount, fh, (rng() % NBLK) * BLK, BLK, d.data()) != BLK)
          err++;
      }
    });
  }
  for (int r = 0; r < 2; r++) {
    th.emplace_back([&, r] {
      std::mt19937 rng(r + 50);
      Fh *rfh = a.open(in, O_RDONLY);
      while (!stop) {
        std::string s = a.read(rfh, (rng() % NBLK) * BLK, BLK);
        if (!s.empty() && !uniform(s))
          torn++;
      }
      ceph_ll_close(a.cmount, rfh);
    });
  }
  for (int i = 0; i < 200 * scale(); i++) {
    struct ceph_statx stx = {};
    stx.stx_size = (i % 2) ? SIZE : SIZE / 2;
    ASSERT_EQ(0, ceph_ll_setattr(a.cmount, in, &stx, CEPH_SETATTR_SIZE, a.perms));
    std::this_thread::sleep_for(1ms);
  }
  stop = true;
  for (auto& x : th)
    x.join();
  struct ceph_statx stx = {};
  stx.stx_size = SIZE / 2;
  ASSERT_EQ(0, ceph_ll_setattr(a.cmount, in, &stx, CEPH_SETATTR_SIZE, a.perms));
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, torn.load());
  std::string all = a.read(fh, 0, SIZE);
  EXPECT_EQ(size_t(SIZE / 2), all.size());
  EXPECT_EQ(0, torn_blocks(all));
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Mount b;
  std::string ball = read_via(b, dname, "f");
  EXPECT_EQ(size_t(SIZE / 2), ball.size());
  EXPECT_EQ(0, torn_blocks(ball));
  EXPECT_EQ(all, ball);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Threads write their own blocks of a shared file and fsync, while
 * another client reads the file now and then: after the last fsync, with
 * the file still open for writing here, the other client reads every
 * block as last written.
 */
TEST(LibCephFSThreads, FsyncThenAnotherClientReads)
{
  Mount a;
  std::string dname = unique_name("thr_fsync");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int W = 8 * wide();
  constexpr int64_t NBLK = 256;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::vector<char> last(NBLK, '\0');
  std::atomic<int> err{0}, torn_b{0};
  std::atomic<bool> stop{false};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 3);
      for (int i = 0; i < 20 * scale(); i++) {
        for (int k = 0; k < 8; k++) {
          int64_t b = (rng() % (NBLK / W)) * W + w;
          char v = 'a' + (i + k + w) % 26;
          std::string d = block(v);
          if (ceph_ll_write(a.cmount, fh, b * BLK, BLK, d.data()) != BLK)
            err++;
          last[b] = v;
        }
        if (ceph_ll_fsync(a.cmount, fh, 0) != 0)
          err++;
      }
    });
  }
  std::thread other([&] {
    Mount b;
    while (!stop) {
      std::string s = read_via(b, dname, "f");
      if (s.size() != size_t(NBLK * BLK) || torn_blocks(s))
        torn_b++;
      std::this_thread::sleep_for(50ms);
    }
  });
  for (auto& x : th)
    x.join();
  stop = true;
  other.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, torn_b.load());
  Mount b;
  std::string s = read_via(b, dname, "f");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Many writers through a cache far smaller than their data: they wait
 * for writeback room, the flusher is held to a small writeback in flight
 * and wakes one writer at a time, and reads miss the cache.  Every writer
 * finishes, and the data is what they wrote.
 */
static void small_cache_many_writers(const std::string& shards)
{
  Mount a({{"client_oc_size", "4194304"},
           {"client_oc_max_dirty", "1048576"},
           {"client_oc_target_dirty", "524288"},
           {"client_oc_max_writeback", "262144"},
           {"client_oc_max_woken_writers", "1"},
           {"client_oc_shards", shards}});
  std::string dname = unique_name("thr_small");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int T = 16 * wide();
  constexpr int64_t SIZE = 2 << 20;
  std::vector<std::string> model(T, std::string(SIZE, '\0'));
  std::vector<Inode*> ins(T);
  std::vector<Fh*> fhs(T);
  for (int t = 0; t < T; t++)
    fhs[t] = a.create(dir, "f" + std::to_string(t), SIZE, &ins[t]);
  std::atomic<int> bad{0};
  std::vector<std::thread> th;
  for (int t = 0; t < T; t++) {
    th.emplace_back([&, t] {
      std::mt19937 rng(t + 9);
      for (int i = 0; i < 300 * scale(); i++) {
        int64_t len = (rng() % 2) ? BLK : 16 * BLK;
        int64_t off = (rng() % (SIZE / len)) * len;
        std::string d = block('a' + (t + i) % 26, len);
        if (ceph_ll_write(a.cmount, fhs[t], off, len, d.data()) != len)
          bad++;
        model[t].replace(off, len, d);
        int64_t roff = (rng() % (SIZE / BLK)) * BLK;
        if (a.read(fhs[t], roff, BLK) != model[t].substr(roff, BLK))
          bad++;
      }
    });
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, bad.load());
  for (int t = 0; t < T; t++) {
    ASSERT_EQ(0, ceph_ll_close(a.cmount, fhs[t]));
    ceph_ll_put(a.cmount, ins[t]);
  }
  Mount b;
  for (int t = 0; t < T; t++)
    EXPECT_EQ(model[t], read_via(b, dname, "f" + std::to_string(t))) << "file " << t;
  ceph_ll_put(a.cmount, dir);
}

TEST(LibCephFSThreads, SmallCacheManyWriters)
{
  small_cache_many_writers("8");
}

TEST(LibCephFSThreads, SmallCacheManyWritersOneShard)
{
  small_cache_many_writers("1");
}

/*
 * The lock and cache tuning options change while threads write and read
 * a shared file: nothing is lost or torn.
 */
TEST(LibCephFSThreads, RuntimeTuningUnderLoad)
{
  Mount a;
  std::string dname = unique_name("thr_tune");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int W = 8 * wide();
  constexpr int64_t NBLK = 512;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::vector<char> last(NBLK, '\0');
  std::atomic<bool> stop{false};
  std::atomic<int> err{0}, torn{0}, conf_err{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 21);
      while (!stop) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        char v = 'a' + rng() % 26;
        std::string d = block(v);
        if (ceph_ll_write(a.cmount, fh, b * BLK, BLK, d.data()) != BLK)
          err++;
        last[b] = v;
      }
    });
  }
  for (int r = 0; r < 4; r++) {
    th.emplace_back([&, r] {
      std::mt19937 rng(r + 77);
      Fh *rfh = a.open(in, O_RDONLY);
      while (!stop) {
        std::string s = a.read(rfh, (rng() % NBLK) * BLK, BLK);
        if (s.size() != size_t(BLK) || !uniform(s))
          torn++;
        struct ceph_statx stx;
        if (ceph_ll_getattr(a.cmount, in, &stx, CEPH_STATX_SIZE, 0, a.perms) ||
            stx.stx_size != uint64_t(NBLK * BLK))
          err++;
      }
      ceph_ll_close(a.cmount, rfh);
    });
  }
  const std::vector<std::pair<const char*, std::vector<const char*>>> knobs = {
    {"client_oc_max_writeback", {"0", "65536", "1048576"}},
    {"client_oc_flush_batch", {"0", "1", "32"}},
    {"client_oc_flush_min_age", {"0", "0.01"}},
    {"client_oc_max_woken_writers", {"1", "32"}},
    {"client_oc_read_gate_retries", {"0", "4"}},
    {"client_write_combine_max", {"1", "2", "32"}},
    {"client_lock_max_spinners", {"0", "2", "8"}},
    {"client_lock_spin_tries", {"0", "100"}},
    {"client_inode_lock_reader_slots", {"1", "16"}},
    {"client_inode_lock_spread_after", {"0", "64"}},
  };
  auto end = std::chrono::steady_clock::now() + 4s * scale();
  for (int i = 0; std::chrono::steady_clock::now() < end; i++) {
    auto& [k, vals] = knobs[i % knobs.size()];
    if (ceph_conf_set(a.cmount, k, vals[(i / knobs.size()) % vals.size()]) != 0)
      conf_err++;
    std::this_thread::sleep_for(10ms);
  }
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, torn.load());
  EXPECT_EQ(0, conf_err.load());
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Mount b;
  std::string s = read_via(b, dname, "f");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Threads of one mount create and remove names in a directory while other
 * threads of it look them up, getattr, open, close and drop them: lookups
 * find a name or ENOENT, and nothing else fails.
 */
TEST(LibCephFSThreads, LookupPutWhileUnlinking)
{
  Mount a;
  std::string dname = unique_name("thr_unlink");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int NAMES = 32;
  std::atomic<bool> stop{false};
  std::atomic<int> bad{0}, found{0}, enoent{0};
  std::vector<std::thread> th;
  for (int m = 0; m < 4; m++) {
    th.emplace_back([&, m] {
      std::mt19937 rng(m + 31);
      while (!stop) {
        std::string n = "n" + std::to_string(rng() % NAMES);
        if (rng() % 2) {
          Inode *in = nullptr;
          Fh *fh = nullptr;
          struct ceph_statx stx;
          int r = ceph_ll_create(a.cmount, dir, n.c_str(), 0644,
                                 O_RDWR | O_CREAT | O_EXCL, &in, &fh, &stx,
                                 0, 0, a.perms);
          if (r == 0) {
            std::string d = block('c');
            if (ceph_ll_write(a.cmount, fh, 0, BLK, d.data()) != BLK)
              bad++;
            ceph_ll_close(a.cmount, fh);
            ceph_ll_put(a.cmount, in);
          } else if (r != -EEXIST) {
            bad++;
          }
        } else {
          int r = ceph_ll_unlink(a.cmount, dir, n.c_str(), a.perms);
          if (r != 0 && r != -ENOENT)
            bad++;
        }
      }
    });
  }
  for (int l = 0; l < 8 * wide(); l++) {
    th.emplace_back([&, l] {
      std::mt19937 rng(l + 61);
      while (!stop) {
        int r;
        Inode *in = a.lookup(dir, "n" + std::to_string(rng() % NAMES), &r);
        if (!in) {
          if (r == -ENOENT)
            enoent++;
          else
            bad++;
          continue;
        }
        found++;
        struct ceph_statx stx;
        if (ceph_ll_getattr(a.cmount, in, &stx, CEPH_STATX_SIZE, 0, a.perms))
          bad++;
        Fh *fh = nullptr;
        if (ceph_ll_open(a.cmount, in, O_RDONLY, &fh, a.perms) == 0) {
          std::string s = a.read(fh, 0, BLK);
          if (!s.empty() && !uniform(s))
            bad++;
          ceph_ll_close(a.cmount, fh);
        }
        ceph_ll_put(a.cmount, in);
      }
    });
  }
  std::this_thread::sleep_for(4s * scale());
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, bad.load());
  EXPECT_GT(found.load(), 0);
  EXPECT_GT(enoent.load(), 0);
  for (int i = 0; i < NAMES; i++) {
    std::string n = "n" + std::to_string(i);
    int r = ceph_ll_unlink(a.cmount, dir, n.c_str(), a.perms);
    EXPECT_TRUE(r == 0 || r == -ENOENT) << n << " " << r;
    a.lookup(dir, n, &r);
    EXPECT_EQ(-ENOENT, r) << n;
  }
  ceph_ll_put(a.cmount, dir);
}

/*
 * Close-to-open between two clients while threads of one of them keep
 * opening and closing the file read-only: what one client wrote and
 * closed, the other reads when it opens the file next.
 */
TEST(LibCephFSThreads, CloseToOpenAgainstAnotherClient)
{
  Mount a, b;
  std::string dname = unique_name("thr_cto");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", BLK, &in);
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  Inode *bin = b.lookup(bdir, "f");
  ASSERT_NE(nullptr, bin);

  std::atomic<bool> stop{false};
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int t = 0; t < 4; t++) {
    th.emplace_back([&] {
      while (!stop) {
        Fh *f = nullptr;
        if (ceph_ll_open(a.cmount, in, O_RDONLY, &f, a.perms) != 0) {
          err++;
          continue;
        }
        std::string s = a.read(f, 0, BLK);
        if (s.size() != size_t(BLK) || !uniform(s))
          err++;
        ceph_ll_close(a.cmount, f);
      }
    });
  }
  int miss_a = 0, miss_b = 0;
  for (int i = 0; i < 100 * scale(); i++) {
    std::string vb = block('a' + i % 26);
    Fh *f = b.open(bin, O_WRONLY);
    ASSERT_EQ(BLK, ceph_ll_write(b.cmount, f, 0, BLK, vb.data()));
    ASSERT_EQ(0, ceph_ll_close(b.cmount, f));
    f = a.open(in, O_RDONLY);
    if (a.read(f, 0, BLK) != vb)
      miss_a++;
    ceph_ll_close(a.cmount, f);

    std::string va = block('A' + i % 26);
    f = a.open(in, O_WRONLY);
    ASSERT_EQ(BLK, ceph_ll_write(a.cmount, f, 0, BLK, va.data()));
    ASSERT_EQ(0, ceph_ll_close(a.cmount, f));
    f = b.open(bin, O_RDONLY);
    if (b.read(f, 0, BLK) != va)
      miss_b++;
    ceph_ll_close(b.cmount, f);
  }
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, miss_a);
  EXPECT_EQ(0, miss_b);
  ceph_ll_put(b.cmount, bin);
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Buffered writers, O_DIRECT writers and readv() readers of one file at
 * once, each writer on blocks of its own: readv() never returns a torn
 * block, and another client ends up with the last write of every block.
 */
TEST(LibCephFSThreads, BufferedDirectAndReadv)
{
  Mount a;
  std::string dname = unique_name("thr_direct");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int W = 6;   // 0..3 buffered, 4..5 O_DIRECT
  constexpr int64_t NBLK = 384;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::vector<char> last(NBLK, '\0');
  std::atomic<bool> stop{false};
  std::atomic<int> err{0}, torn{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 41);
      Fh *f = w < 4 ? fh : a.open(in, O_RDWR | O_DIRECT);
      for (int i = 0; i < 1000 * scale(); i++) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        char v = 'a' + (i + w) % 26;
        std::string d = block(v);
        if (ceph_ll_write(a.cmount, f, b * BLK, BLK, d.data()) != BLK)
          err++;
        last[b] = v;
      }
      if (f != fh)
        ceph_ll_close(a.cmount, f);
    });
  }
  for (int r = 0; r < 4; r++) {
    th.emplace_back([&, r] {
      std::mt19937 rng(r + 91);
      Fh *f = a.open(in, O_RDONLY);
      char x[BLK / 2], y[BLK / 2];
      struct iovec iov[2] = {{x, sizeof(x)}, {y, sizeof(y)}};
      while (!stop) {
        int64_t n = ceph_ll_readv(a.cmount, f, iov, 2, (rng() % NBLK) * BLK);
        if (n != BLK || !uniform(std::string(x, sizeof(x)) + std::string(y, sizeof(y))))
          torn++;
      }
      ceph_ll_close(a.cmount, f);
    });
  }
  for (int w = 0; w < W; w++)
    th[w].join();
  stop = true;
  for (size_t i = W; i < th.size(); i++)
    th[i].join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, torn.load());
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Mount b;
  std::string s = read_via(b, dname, "f");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Snapshots of a directory taken while threads rewrite blocks of a file
 * in it: each snapshot of the file has its size and whole blocks, and the
 * head ends up with the last writes.
 */
TEST(LibCephFSThreads, SnapshotsWhileWriting)
{
  Mount a;
  std::string dname = unique_name("thr_snap");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int W = 8 * wide(), NSNAP = 10 * scale();
  constexpr int64_t NBLK = 128;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::vector<char> last(NBLK, '\0');
  std::atomic<bool> stop{false};
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 13);
      while (!stop) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        char v = 'a' + rng() % 26;
        std::string d = block(v);
        if (ceph_ll_write(a.cmount, fh, b * BLK, BLK, d.data()) != BLK)
          err++;
        last[b] = v;
      }
    });
  }
  std::string path = "/" + dname;
  for (int i = 0; i < NSNAP; i++) {
    std::this_thread::sleep_for(100ms);
    ASSERT_EQ(0, ceph_mksnap(a.cmount, path.c_str(), ("s" + std::to_string(i)).c_str(),
                             0755, nullptr, 0));
  }
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Mount b;
  for (int i = 0; i < NSNAP; i++) {
    std::string snap = path + "/.snap/s" + std::to_string(i) + "/f";
    int fd = ceph_open(b.cmount, snap.c_str(), O_RDONLY, 0);
    ASSERT_GE(fd, 0) << snap;
    std::string s(NBLK * BLK, '\0');
    EXPECT_EQ(NBLK * BLK, ceph_read(b.cmount, fd, s.data(), s.size(), 0)) << snap;
    EXPECT_EQ(0, torn_blocks(s)) << snap;
    ceph_close(b.cmount, fd);
  }
  std::string s = read_via(b, dname, "f");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  for (int i = 0; i < NSNAP; i++)
    EXPECT_EQ(0, ceph_rmsnap(a.cmount, path.c_str(), ("s" + std::to_string(i)).c_str()));
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Each client option of the lock split, one value at a time, set before
 * mounting (client_oc_shards and client_percpu_slots only take effect at
 * mount): the mount runs a mixed load of many threads (writers and readers
 * of a shared file, getattr, writers of files of their own, lookups and
 * open/close of cached names), and another client reads back what was
 * written.
 */
struct OptionCase {
  const char *name;
  const char *value;
};

class LibCephFSClientOption : public ::testing::TestWithParam<OptionCase> {};

TEST_P(LibCephFSClientOption, MixedLoad)
{
  auto [name, value] = GetParam();
  Mount a({{name, value}});
  char got[64] = {};
  ASSERT_EQ(0, ceph_conf_get(a.cmount, name, got, sizeof(got)));
  EXPECT_DOUBLE_EQ(std::stod(value), std::stod(got)) << name;

  std::string dname = unique_name("thr_opt");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int W = 6, P = 4, NF = 8;
  constexpr int64_t NBLK = 256, PSIZE = 1 << 20;
  Inode *sin = nullptr;
  Fh *sfh = a.create(dir, "shared", NBLK * BLK, &sin);
  std::vector<char> last(NBLK, '\0');
  std::vector<std::string> model(P, std::string(PSIZE, '\0'));
  std::vector<Inode*> pins(P);
  std::vector<Fh*> pfhs(P);
  for (int p = 0; p < P; p++)
    pfhs[p] = a.create(dir, "p" + std::to_string(p), PSIZE, &pins[p]);
  for (int f = 0; f < NF; f++) {
    Inode *in = nullptr;
    ceph_ll_close(a.cmount, a.create(dir, "c" + std::to_string(f), BLK, &in));
    ceph_ll_put(a.cmount, in);
  }

  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};
  std::vector<std::thread> workers, others;
  for (int w = 0; w < W; w++) {
    workers.emplace_back([&, w] {
      std::mt19937 rng(w + 1);
      for (int i = 0; i < 300 * scale(); i++) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        char v = 'a' + (i + w) % 26;
        std::string d = block(v);
        if (ceph_ll_write(a.cmount, sfh, b * BLK, BLK, d.data()) != BLK)
          bad++;
        last[b] = v;
      }
    });
  }
  for (int p = 0; p < P; p++) {
    workers.emplace_back([&, p] {
      std::mt19937 rng(p + 100);
      for (int i = 0; i < 200 * scale(); i++) {
        int64_t len = (rng() % 2) ? BLK : 16 * BLK;
        int64_t off = (rng() % (PSIZE / len)) * len;
        std::string d = block('A' + (i + p) % 26, len);
        if (ceph_ll_write(a.cmount, pfhs[p], off, len, d.data()) != len)
          bad++;
        model[p].replace(off, len, d);
        int64_t roff = (rng() % (PSIZE / BLK)) * BLK;
        if (a.read(pfhs[p], roff, BLK) != model[p].substr(roff, BLK))
          bad++;
      }
    });
  }
  for (int r = 0; r < 2; r++) {
    others.emplace_back([&, r] {
      std::mt19937 rng(r + 200);
      Fh *f = a.open(sin, O_RDONLY);
      while (!stop) {
        std::string s = a.read(f, (rng() % NBLK) * BLK, BLK);
        if (s.size() != size_t(BLK) || !uniform(s))
          bad++;
        struct ceph_statx stx;
        if (ceph_ll_getattr(a.cmount, sin, &stx, CEPH_STATX_SIZE, 0, a.perms) ||
            stx.stx_size != uint64_t(NBLK * BLK))
          bad++;
      }
      ceph_ll_close(a.cmount, f);
    });
  }
  for (int l = 0; l < 2; l++) {
    others.emplace_back([&, l] {
      std::mt19937 rng(l + 300);
      while (!stop) {
        int r;
        Inode *in = a.lookup(dir, "c" + std::to_string(rng() % NF), &r);
        if (!in) {
          bad++;
          continue;
        }
        Fh *f = nullptr;
        if (ceph_ll_open(a.cmount, in, O_RDONLY, &f, a.perms) == 0)
          ceph_ll_close(a.cmount, f);
        else
          bad++;
        ceph_ll_put(a.cmount, in);
        if (a.lookup(dir, "missing", &r) || r != -ENOENT)
          bad++;
      }
    });
  }
  for (auto& x : workers)
    x.join();
  stop = true;
  for (auto& x : others)
    x.join();
  EXPECT_EQ(0, bad.load());
  for (int p = 0; p < P; p++)
    ASSERT_EQ(0, ceph_ll_close(a.cmount, pfhs[p]));
  ASSERT_EQ(0, ceph_ll_close(a.cmount, sfh));

  Mount b;
  std::string s = read_via(b, dname, "shared");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  for (int p = 0; p < P; p++) {
    EXPECT_EQ(model[p], read_via(b, dname, "p" + std::to_string(p))) << "file " << p;
    ceph_ll_put(a.cmount, pins[p]);
  }
  ceph_ll_put(a.cmount, sin);
  ceph_ll_put(a.cmount, dir);
}

INSTANTIATE_TEST_SUITE_P(
  AllValues, LibCephFSClientOption,
  ::testing::Values(
    OptionCase{"client_oc_shards", "1"},
    OptionCase{"client_oc_shards", "2"},
    OptionCase{"client_oc_shards", "8"},
    OptionCase{"client_oc_shards", "256"},
    OptionCase{"client_percpu_slots", "0"},
    OptionCase{"client_percpu_slots", "1"},
    OptionCase{"client_percpu_slots", "3"},
    OptionCase{"client_percpu_slots", "4096"},
    OptionCase{"client_oc_max_writeback", "0"},
    OptionCase{"client_oc_max_writeback", "4096"},
    OptionCase{"client_oc_max_writeback", "65536"},
    OptionCase{"client_oc_max_writeback", "1073741824"},
    OptionCase{"client_oc_flush_batch", "0"},
    OptionCase{"client_oc_flush_batch", "1"},
    OptionCase{"client_oc_flush_batch", "1024"},
    OptionCase{"client_oc_flush_min_age", "0"},
    OptionCase{"client_oc_flush_min_age", "0.5"},
    OptionCase{"client_oc_flush_min_age", "-1"},
    OptionCase{"client_oc_max_woken_writers", "1"},
    OptionCase{"client_oc_max_woken_writers", "32"},
    OptionCase{"client_oc_max_woken_writers", "65536"},
    OptionCase{"client_oc_read_gate_retries", "0"},
    OptionCase{"client_oc_read_gate_retries", "1"},
    OptionCase{"client_oc_read_gate_retries", "1000"},
    OptionCase{"client_lock_max_spinners", "0"},
    OptionCase{"client_lock_max_spinners", "1"},
    OptionCase{"client_lock_max_spinners", "1024"},
    OptionCase{"client_lock_spin_tries", "0"},
    OptionCase{"client_lock_spin_tries", "1"},
    OptionCase{"client_lock_spin_tries", "1000000"},
    OptionCase{"client_inode_lock_reader_slots", "1"},
    OptionCase{"client_inode_lock_reader_slots", "3"},
    OptionCase{"client_inode_lock_reader_slots", "1024"},
    OptionCase{"client_inode_lock_spread_after", "0"},
    OptionCase{"client_inode_lock_spread_after", "1"},
    OptionCase{"client_inode_lock_spread_after", "1000000"},
    OptionCase{"client_write_combine_max", "1"},
    OptionCase{"client_write_combine_max", "2"},
    OptionCase{"client_write_combine_max", "4096"}),
  [](const ::testing::TestParamInfo<OptionCase>& i) {
    std::string n = std::string(i.param.name) + "_" + i.param.value;
    for (auto& c : n)
      if (!isalnum(c))
        c = '_';
    return n;
  });

/*
 * The options refuse values out of their range.
 */
TEST(LibCephFSClientOptions, OutOfRange)
{
  ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  const std::vector<OptionCase> bad = {
    {"client_oc_shards", "0"},
    {"client_oc_shards", "257"},
    {"client_percpu_slots", "4097"},
    {"client_oc_max_woken_writers", "0"},
    {"client_oc_max_woken_writers", "65537"},
    {"client_oc_read_gate_retries", "1001"},
    {"client_lock_max_spinners", "-1"},
    {"client_lock_max_spinners", "1025"},
    {"client_lock_spin_tries", "-1"},
    {"client_lock_spin_tries", "1000001"},
    {"client_inode_lock_reader_slots", "0"},
    {"client_inode_lock_reader_slots", "1025"},
    {"client_inode_lock_spread_after", "1000001"},
    {"client_write_combine_max", "0"},
    {"client_write_combine_max", "4097"},
  };
  for (auto& [name, value] : bad)
    EXPECT_NE(0, ceph_conf_set(cmount, name, value)) << name << "=" << value;
  ceph_release(cmount);
}

namespace {

// run an MDS command (tell), e.g. {"prefix": "respawn"}; 0 on success
int mds_command(Mount& m, const std::string& json, const char *spec = "*")
{
  const char *cmd[] = {json.c_str()};
  char *outbuf = nullptr, *outs = nullptr;
  size_t outbuflen = 0, outslen = 0;
  int r = ceph_mds_command(m.cmount, spec, cmd, 1, "", 0,
                           &outbuf, &outbuflen, &outs, &outslen);
  ceph_buffer_free(outbuf);
  ceph_buffer_free(outs);
  return r;
}

// send a command to every active rank ("*" also reaches the standbys,
// which do not take client commands such as "client evict")
int mds_command_ranks(Mount& m, const std::string& json)
{
  int n = 0;
  for (int rank = 0; ; rank++) {
    int r = mds_command(m, json, std::to_string(rank).c_str());
    if (r == -ENOENT || r == -EINVAL)
      break;
    if (r < 0)
      return r;
    n++;
  }
  return n > 0 ? 0 : -ENOENT;
}

uint64_t ino_of(Mount& m, Inode *in)
{
  struct ceph_statx stx;
  if (ceph_ll_getattr(m.cmount, in, &stx, CEPH_STATX_INO, 0, m.perms))
    return 0;
  return stx.stx_ino;
}

} // anonymous namespace

/*
 * Threads rename names in a directory back and forth, and over a name that
 * must always exist, while threads of the same mount look the names up
 * (cached answers, ENOENT included) and list the directory: a lookup finds
 * a name's own file or ENOENT, the name renamed over is always found, and
 * in the end each file is under exactly one of its names, on this mount
 * and on another client.
 */
TEST(LibCephFSThreads, LookupRenameReaddir)
{
  Mount a;
  std::string dname = unique_name("thr_rename");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int NF = 16;
  std::vector<uint64_t> ino(NF);
  for (int i = 0; i < NF; i++) {
    Inode *in = nullptr;
    ceph_ll_close(a.cmount, a.create(dir, "a" + std::to_string(i), BLK, &in));
    ino[i] = ino_of(a, in);
    ceph_ll_put(a.cmount, in);
  }
  {
    Inode *in = nullptr;
    ceph_ll_close(a.cmount, a.create(dir, "victim", BLK, &in));
    ceph_ll_put(a.cmount, in);
  }
  std::atomic<bool> stop{false};
  std::atomic<int> bad{0}, renames{0}, found{0};
  // what went wrong, for the failure message
  std::atomic<int> bad_rename{0}, bad_victim_make{0}, bad_ino{0},
    bad_lookup_err{0}, victim_missing{0}, bad_readdir{0};
  std::vector<std::thread> th;
  for (int r = 0; r < 4; r++) {
    th.emplace_back([&, r] {
      std::mt19937 rng(r + 3);
      while (!stop) {
        std::string i = std::to_string(rng() % NF);
        int e = ceph_ll_rename(a.cmount, dir, ("a" + i).c_str(), dir,
                               ("b" + i).c_str(), a.perms);
        if (e == -ENOENT)
          e = ceph_ll_rename(a.cmount, dir, ("b" + i).c_str(), dir,
                             ("a" + i).c_str(), a.perms);
        if (e == 0)
          renames++;
        else if (e != -ENOENT) {
          bad++;
          bad_rename++;
        }
      }
    });
  }
  // replace "victim" over and over: it never goes missing
  th.emplace_back([&] {
    for (int k = 0; !stop; k++) {
      std::string x = "x" + std::to_string(k);
      Inode *in = nullptr;
      Fh *fh = nullptr;
      struct ceph_statx stx;
      if (ceph_ll_create(a.cmount, dir, x.c_str(), 0644, O_RDWR | O_CREAT | O_EXCL,
                         &in, &fh, &stx, 0, 0, a.perms)) {
        bad++;
        bad_victim_make++;
        continue;
      }
      ceph_ll_close(a.cmount, fh);
      ceph_ll_put(a.cmount, in);
      if (ceph_ll_rename(a.cmount, dir, x.c_str(), dir, "victim", a.perms)) {
        bad++;
        bad_victim_make++;
      }
    }
  });
  for (int l = 0; l < 8 * wide(); l++) {
    th.emplace_back([&, l] {
      std::mt19937 rng(l + 17);
      while (!stop) {
        int i = rng() % NF;
        int r;
        Inode *in = a.lookup(dir, (rng() % 2 ? "a" : "b") + std::to_string(i), &r);
        if (in) {
          found++;
          if (ino_of(a, in) != ino[i]) {
            bad++;
            bad_ino++;
          }
          ceph_ll_put(a.cmount, in);
        } else if (r != -ENOENT) {
          bad++;
          bad_lookup_err++;
        }
        in = a.lookup(dir, "victim", &r);
        if (in) {
          ceph_ll_put(a.cmount, in);
        } else {
          bad++;
          victim_missing++;
        }
      }
    });
  }
  th.emplace_back([&] {
    while (!stop) {
      struct ceph_dir_result *dirp = nullptr;
      if (ceph_ll_opendir(a.cmount, dir, &dirp, a.perms)) {
        bad++;
        continue;
      }
      struct dirent *de;
      while ((de = ceph_readdir(a.cmount, dirp)) != nullptr) {
        std::string n = de->d_name;
        if (n != "." && n != ".." && n != "victim" &&
            !(n.size() > 1 && (n[0] == 'a' || n[0] == 'b' || n[0] == 'x') &&
              std::all_of(n.begin() + 1, n.end(), ::isdigit))) {
          bad++;
          bad_readdir++;
        }
      }
      ceph_ll_releasedir(a.cmount, dirp);
    }
  });
  std::this_thread::sleep_for(4s * scale());
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, bad.load())
    << "renames " << bad_rename << ", making victim " << bad_victim_make
    << ", wrong ino " << bad_ino << ", lookup errors " << bad_lookup_err
    << ", victim missing " << victim_missing << ", readdir " << bad_readdir;
  EXPECT_GT(renames.load(), 0);
  EXPECT_GT(found.load(), 0);
  Mount b;
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  for (int i = 0; i < NF; i++) {
    for (Mount *m : {&a, &b}) {
      Inode *d = m == &a ? dir : bdir;
      int ra, rb;
      Inode *ia = m->lookup(d, "a" + std::to_string(i), &ra);
      Inode *ib = m->lookup(d, "b" + std::to_string(i), &rb);
      EXPECT_TRUE((ia != nullptr) != (ib != nullptr))
        << "file " << i << (m == &a ? " on a" : " on b")
        << ": a" << i << " " << (ia ? std::to_string(ino_of(*m, ia)) : std::to_string(ra))
        << ", b" << i << " " << (ib ? std::to_string(ino_of(*m, ib)) : std::to_string(rb))
        << ", ino " << ino[i];
      Inode *in = ia ? ia : ib;
      if (in) {
        EXPECT_EQ(ino[i], ino_of(*m, in)) << "file " << i;
      }
      if (ia)
        ceph_ll_put(m->cmount, ia);
      if (ib)
        ceph_ll_put(m->cmount, ib);
    }
  }
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, dir);
}

/*
 * A rename keeps the source dentry its walk found, and walking the target
 * can drop client_lock (a lookup sent to the MDS): another rename of the
 * same mount can move the source meanwhile, and link the name again to a
 * new dentry.  client_inject_rename_walk_delay holds that window open, so
 * each case below happens on every run: whatever the other requests did,
 * the renaming mount's cache ends up with the names the MDS has, the ones
 * another client sees.
 */
namespace {

std::set<std::string> list_names(Mount& m, Inode *dir)
{
  std::set<std::string> names;
  struct ceph_dir_result *dirp = nullptr;
  if (ceph_ll_opendir(m.cmount, dir, &dirp, m.perms))
    return {"<opendir failed>"};
  struct dirent *de;
  while ((de = ceph_readdir(m.cmount, dirp)) != nullptr) {
    std::string n = de->d_name;
    if (n != "." && n != "..")
      names.insert(n);
  }
  ceph_ll_releasedir(m.cmount, dirp);
  return names;
}

// name -> ino as @m looks the names up (0: ENOENT, -1: another error)
std::map<std::string, int64_t> look(Mount& m, Inode *dir,
                                    const std::vector<std::string>& names)
{
  std::map<std::string, int64_t> r;
  for (auto& n : names) {
    int e = 0;
    Inode *in = m.lookup(dir, n, &e);
    if (in) {
      r[n] = ino_of(m, in);
      ceph_ll_put(m.cmount, in);
    } else {
      r[n] = e == -ENOENT ? 0 : -1;
    }
  }
  return r;
}

std::string str(const std::map<std::string, int64_t>& m)
{
  std::string s;
  for (auto& [k, v] : m)
    s += k + "=" + std::to_string(v) + " ";
  return s;
}

std::string str(const std::set<std::string>& m)
{
  std::string s;
  for (auto& k : m)
    s += k + " ";
  return s;
}

} // anonymous namespace

TEST(LibCephFSThreads, RenameStaleSource)
{
  Mount a;
  std::string top = unique_name("thr_stale_rename");
  Inode *topdir = a.mkdir(a.root, top);
  ASSERT_NE(nullptr, topdir);

  // the rename "a" -> "b" holds what its walks found for 2 s after the
  // source walk and again after the target walk; the mount meanwhile:
  enum Case {
    RELINKED,         // moves "a" away and back: a new dentry for "a"
    RELINKED_OVER,    // the same, and the rename replaces a file "b"
    MOVED_AWAY,       // moves "a" away: the rename fails
    TARGET_IS_SOURCE, // moves "a" to "b" before the target walk, so the
                      // walk finds the renamed file at "b", and back after
  };
  for (Case c : {RELINKED, RELINKED_OVER, MOVED_AWAY, TARGET_IS_SOURCE}) {
    SCOPED_TRACE("case " + std::to_string(c));
    std::string dname = "d" + std::to_string(c);
    Inode *dir = a.mkdir(topdir, dname);
    ASSERT_NE(nullptr, dir);
    Inode *in = nullptr;
    ceph_ll_close(a.cmount, a.create(dir, "a", BLK, &in));
    ASSERT_NE(nullptr, in);
    int64_t ino = ino_of(a, in);
    ceph_ll_put(a.cmount, in);
    int64_t old_b = 0;
    if (c == RELINKED_OVER) {
      ceph_ll_close(a.cmount, a.create(dir, "b", BLK, &in));
      ASSERT_NE(nullptr, in);
      old_b = ino_of(a, in);
      ceph_ll_put(a.cmount, in);
    }
    // cache every name, and the dir as complete
    list_names(a, dir);
    look(a, dir, {"a", "b", "x"});

    ASSERT_EQ(0, ceph_conf_set(a.cmount, "client_inject_rename_walk_delay", "2000"));
    int rc = 1;
    auto t0 = std::chrono::steady_clock::now();
    std::thread t([&] {
      rc = ceph_ll_rename(a.cmount, dir, "a", dir, "b", a.perms);
    });
    // in the first window (0 - 2 s)
    std::this_thread::sleep_until(t0 + 500ms);
    ASSERT_EQ(0, ceph_conf_set(a.cmount, "client_inject_rename_walk_delay", "0"));
    switch (c) {
    case RELINKED:
    case RELINKED_OVER:
      EXPECT_EQ(0, ceph_ll_rename(a.cmount, dir, "a", dir, "x", a.perms));
      EXPECT_EQ(0, ceph_ll_rename(a.cmount, dir, "x", dir, "a", a.perms));
      break;
    case MOVED_AWAY:
      EXPECT_EQ(0, ceph_ll_rename(a.cmount, dir, "a", dir, "x", a.perms));
      break;
    case TARGET_IS_SOURCE:
      EXPECT_EQ(0, ceph_ll_rename(a.cmount, dir, "a", dir, "b", a.perms));
      // in the second window (2 - 4 s)
      std::this_thread::sleep_until(t0 + 2800ms);
      EXPECT_EQ(0, ceph_ll_rename(a.cmount, dir, "b", dir, "a", a.perms));
      break;
    }
    t.join();
    EXPECT_GE(std::chrono::steady_clock::now() - t0, 4s)
      << "the rename did not wait in both windows";

    std::map<std::string, int64_t> want;
    std::set<std::string> want_names;
    if (c == MOVED_AWAY) {
      EXPECT_EQ(-ENOENT, rc);
      want = {{"a", 0}, {"b", 0}, {"x", ino}};
      want_names = {"x"};
    } else {
      EXPECT_EQ(0, rc);
      want = {{"a", 0}, {"b", ino}, {"x", 0}};
      want_names = {"b"};
    }
    Mount b;
    Inode *btop = b.lookup(b.root, top);
    ASSERT_NE(nullptr, btop);
    Inode *bdir = b.lookup(btop, dname);
    ASSERT_NE(nullptr, bdir);
    // the other client, from the MDS
    EXPECT_EQ(str(want), str(look(b, bdir, {"a", "b", "x"}))) << "on b";
    EXPECT_EQ(str(want_names), str(list_names(b, bdir))) << "on b";
    // the renaming mount, from its cache
    EXPECT_EQ(str(want), str(look(a, dir, {"a", "b", "x"}))) << "on a";
    EXPECT_EQ(str(want_names), str(list_names(a, dir))) << "on a";
    if (old_b) {
      for (auto& [n, i] : look(a, dir, {"a", "b", "x"}))
        EXPECT_NE(old_b, i) << n << " still names the file renamed over";
    }
    ceph_ll_put(b.cmount, bdir);
    ceph_ll_put(b.cmount, btop);
    ceph_ll_put(a.cmount, dir);
  }
  ceph_ll_put(a.cmount, topdir);
}

/*
 * A rename from one hard link of a file to another does nothing (the MDS
 * replies without a trace): both names stay, in the cache of the mount
 * that renamed as well as on the MDS.
 */
TEST(LibCephFSThreads, RenameOverOwnHardLink)
{
  Mount a;
  std::string dname = unique_name("thr_rename_link");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  Inode *in = nullptr;
  ceph_ll_close(a.cmount, a.create(dir, "a", BLK, &in));
  ASSERT_NE(nullptr, in);
  int64_t ino = ino_of(a, in);
  ASSERT_EQ(0, ceph_ll_link(a.cmount, in, dir, "b", a.perms));
  ceph_ll_put(a.cmount, in);
  list_names(a, dir);
  look(a, dir, {"a", "b"});

  EXPECT_EQ(0, ceph_ll_rename(a.cmount, dir, "a", dir, "b", a.perms));
  std::map<std::string, int64_t> want = {{"a", ino}, {"b", ino}};
  std::set<std::string> want_names = {"a", "b"};
  Mount b;
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  EXPECT_EQ(str(want), str(look(b, bdir, {"a", "b"}))) << "on b";
  EXPECT_EQ(str(want_names), str(list_names(b, bdir))) << "on b";
  EXPECT_EQ(str(want), str(look(a, dir, {"a", "b"}))) << "on a";
  EXPECT_EQ(str(want_names), str(list_names(a, dir))) << "on a";
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Nonblocking reads of a shared file (ceph_ll_nonblocking_readv_writev()),
 * cached and not, while threads rewrite its blocks: every read completes,
 * with a whole block.
 */
TEST(LibCephFSThreads, NonblockingReads)
{
  Mount a;
  std::string dname = unique_name("thr_nbread");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int W = 4;
  constexpr int64_t NBLK = 512;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::atomic<bool> stop{false};
  std::atomic<int> err{0}, torn{0};
  std::atomic<uint64_t> reads{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 5);
      while (!stop) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        std::string d = block('a' + rng() % 26);
        if (ceph_ll_write(a.cmount, fh, b * BLK, BLK, d.data()) != BLK)
          err++;
      }
    });
  }
  struct Op {
    std::mutex m;
    std::condition_variable c;
    bool done = false;
  };
  for (int r = 0; r < 4 * wide(); r++) {
    th.emplace_back([&, r] {
      std::mt19937 rng(r + 55);
      Fh *rfh = a.open(in, O_RDONLY);
      char buf[BLK];
      while (!stop) {
        Op op;
        struct iovec iov = {buf, sizeof(buf)};
        struct ceph_ll_io_info io = {};
        io.callback = [](struct ceph_ll_io_info *i) {
          auto o = static_cast<Op*>(i->priv);
          std::lock_guard l(o->m);
          o->done = true;
          o->c.notify_one();
        };
        io.priv = &op;
        io.fh = rfh;
        io.iov = &iov;
        io.iovcnt = 1;
        io.off = (rng() % NBLK) * BLK;
        io.write = false;
        int64_t r0 = ceph_ll_nonblocking_readv_writev(a.cmount, &io);
        if (r0 < 0) {
          err++;
          continue;
        }
        std::unique_lock l(op.m);
        if (!op.c.wait_for(l, 60s, [&] { return op.done; })) {
          err++;   // never completed
          break;
        }
        if (io.result != BLK || !uniform(std::string(buf, BLK)))
          torn++;
        reads++;
      }
      ceph_ll_close(a.cmount, rfh);
    });
  }
  std::this_thread::sleep_for(3s * scale());
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, torn.load());
  EXPECT_GT(reads.load(), 0u);
  ceph_ll_close(a.cmount, fh);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Writers and readers of a mount keep going while the MDS respawns (and
 * replays, and the client reconnects): no write is lost, and another
 * client reads every block as last written.
 */
TEST(LibCephFSThreads, MdsRespawnDuringIO)
{
  Mount a;
  std::string dname = unique_name("thr_respawn");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int W = 8 * wide();
  constexpr int64_t NBLK = 256;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  ASSERT_EQ(0, ceph_ll_fsync(a.cmount, fh, 0));
  std::vector<char> last(NBLK, '\0');
  std::atomic<bool> stop{false};
  std::atomic<int> err{0}, torn{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 7);
      while (!stop) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        char v = 'a' + rng() % 26;
        std::string d = block(v);
        if (ceph_ll_write(a.cmount, fh, b * BLK, BLK, d.data()) != BLK)
          err++;
        last[b] = v;
        std::this_thread::sleep_for(1ms);
      }
    });
  }
  th.emplace_back([&] {
    std::mt19937 rng(77);
    Fh *rfh = a.open(in, O_RDONLY);
    while (!stop) {
      std::string s = a.read(rfh, (rng() % NBLK) * BLK, BLK);
      if (s.size() != size_t(BLK) || !uniform(s))
        torn++;
      struct ceph_statx stx;
      ceph_ll_getattr(a.cmount, in, &stx, CEPH_STATX_SIZE, 0, a.perms);
    }
    ceph_ll_close(a.cmount, rfh);
  });
  std::this_thread::sleep_for(2s);
  for (int i = 0; i < scale(); i++) {
    EXPECT_EQ(0, mds_command(a, "{\"prefix\": \"respawn\"}"));
    // the MDS is back once a metadata op goes through again
    std::this_thread::sleep_for(5s);
    struct ceph_statx stx;
    Inode *probe = nullptr;
    EXPECT_EQ(0, ceph_ll_lookup(a.cmount, a.root, dname.c_str(), &probe, &stx,
                                CEPH_STATX_INO, 0, a.perms));
    if (probe)
      ceph_ll_put(a.cmount, probe);
  }
  std::this_thread::sleep_for(2s);
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, torn.load());
  ASSERT_EQ(0, ceph_ll_fsync(a.cmount, fh, 0));
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Mount b;
  std::string s = read_via(b, dname, "f");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * A client is evicted while its threads write, read and getattr through
 * the lock-free paths: every call returns (with an error once the
 * eviction is seen), none hangs, and the mount can be torn down.
 */
TEST(LibCephFSThreads, EvictedWhileBusy)
{
  Mount a;
  std::string dname = unique_name("thr_evict");
  Inode *adir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, adir);
  {
    Inode *in = nullptr;
    ceph_ll_close(a.cmount, a.create(adir, "f", 256 * BLK, &in));
    ceph_ll_put(a.cmount, in);
  }
  auto c = std::make_unique<Mount>(std::map<std::string, std::string>{
    {"client_mount_timeout", "30"}});
  Inode *cdir = c->lookup(c->root, dname);
  ASSERT_NE(nullptr, cdir);
  Inode *cin = c->lookup(cdir, "f");
  ASSERT_NE(nullptr, cin);
  Fh *cfh = c->open(cin, O_RDWR);
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0}, calls{0};
  std::vector<std::thread> th;
  std::atomic<int> finished{0};
  for (int t = 0; t < 6; t++) {
    th.emplace_back([&, t] {
      std::mt19937 rng(t + 1);
      std::string d = block('e');
      while (!stop) {
        int64_t off = (rng() % 256) * BLK;
        int r;
        switch (t % 3) {
        case 0: r = ceph_ll_write(c->cmount, cfh, off, BLK, d.data()); break;
        case 1: r = c->read(cfh, off, BLK).size() == size_t(BLK) ? 0 : -1; break;
        default: {
          struct ceph_statx stx;
          r = ceph_ll_getattr(c->cmount, cin, &stx, CEPH_STATX_SIZE, 0, c->perms);
        }
        }
        if (r < 0)
          errors++;
        calls++;
        if (t % 3 == 0 && rng() % 64 == 0 && ceph_ll_fsync(c->cmount, cfh, 0) < 0)
          errors++;
      }
      finished++;
    });
  }
  std::this_thread::sleep_for(1s);
  uint64_t id = ceph_get_instance_id(c->cmount);
  EXPECT_EQ(0, mds_command_ranks(a, "{\"prefix\": \"client evict\", \"filters\": [\"id=" +
                                    std::to_string(id) + "\"]}"));
  // the evicted client finds out, at the latest, at its next MDS or OSD
  // request: give it time, then every thread must be done with its call
  std::this_thread::sleep_for(10s);
  stop = true;
  // a thread stuck in a call never ends: fail rather than hang the suite
  auto deadline = std::chrono::steady_clock::now() + 60s;
  while (finished < int(th.size()) && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(100ms);
  if (finished < int(th.size())) {
    ADD_FAILURE() << int(th.size()) - finished << " calls hang after the eviction";
    std::abort();
  }
  for (auto& x : th)
    x.join();
  EXPECT_GT(calls.load(), 0);
  EXPECT_GT(errors.load(), 0) << "the eviction was never seen";
  ceph_ll_close(c->cmount, cfh);
  ceph_ll_put(c->cmount, cin);
  ceph_ll_put(c->cmount, cdir);
  c.reset();
  // the other client is unaffected
  Inode *in = a.lookup(adir, "f");
  ASSERT_NE(nullptr, in);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, adir);
}

/*
 * The MDS keeps at most 100 caps per client, and recalls the rest, while
 * threads of a mount look up, getattr, open and read 1000 files and write
 * a shared one: nothing fails, and the data is right.
 */
TEST(LibCephFSThreads, CapRecallStorm)
{
  Mount a;
  ASSERT_EQ(0, mds_command_ranks(a, "{\"prefix\": \"config set\", \"var\": "
                              "\"mds_max_caps_per_client\", \"val\": [\"100\"]}"));
  std::string dname = unique_name("thr_recall");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int NF = 1000;
  for (int i = 0; i < NF; i++) {
    Inode *in = nullptr;
    Fh *fh = a.create(dir, "c" + std::to_string(i), 0, &in);
    std::string d = block('a' + i % 26);
    ceph_ll_write(a.cmount, fh, 0, BLK, d.data());
    ceph_ll_close(a.cmount, fh);
    ceph_ll_put(a.cmount, in);
  }
  constexpr int64_t NBLK = 256;
  const int W = 4 * wide();
  Inode *sin = nullptr;
  Fh *sfh = a.create(dir, "shared", NBLK * BLK, &sin);
  std::vector<char> last(NBLK, '\0');
  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 9);
      while (!stop) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        char v = 'a' + rng() % 26;
        std::string d = block(v);
        if (ceph_ll_write(a.cmount, sfh, b * BLK, BLK, d.data()) != BLK)
          bad++;
        last[b] = v;
      }
    });
  }
  for (int l = 0; l < 8 * wide(); l++) {
    th.emplace_back([&, l] {
      std::mt19937 rng(l + 99);
      while (!stop) {
        int i = rng() % NF;
        Inode *in = a.lookup(dir, "c" + std::to_string(i));
        if (!in) {
          bad++;
          continue;
        }
        Fh *fh = nullptr;
        if (ceph_ll_open(a.cmount, in, O_RDONLY, &fh, a.perms)) {
          bad++;
        } else {
          if (a.read(fh, 0, BLK) != block('a' + i % 26))
            bad++;
          ceph_ll_close(a.cmount, fh);
        }
        ceph_ll_put(a.cmount, in);
      }
    });
  }
  std::this_thread::sleep_for(8s * scale());
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, mds_command_ranks(a, "{\"prefix\": \"config set\", \"var\": "
                              "\"mds_max_caps_per_client\", \"val\": [\"1048576\"]}"));
  EXPECT_EQ(0, bad.load());
  ASSERT_EQ(0, ceph_ll_close(a.cmount, sfh));
  Mount b;
  std::string s = read_via(b, dname, "shared");
  ASSERT_EQ(size_t(NBLK * BLK), s.size());
  for (int64_t blk = 0; blk < NBLK; blk++)
    EXPECT_EQ(block(last[blk]), s.substr(blk * BLK, BLK)) << "block " << blk;
  ceph_ll_put(a.cmount, sin);
  ceph_ll_put(a.cmount, dir);
}

/*
 * A directory with a byte quota: threads rewrite their files in place
 * (allowed however full the quota) while others extend theirs until the
 * quota stops them with EDQUOT; the rewritten data is right.
 */
TEST(LibCephFSThreads, QuotaInPlaceAndExtend)
{
  Mount a;
  std::string dname = unique_name("thr_quota");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const std::string q = std::to_string(4 << 20);
  ASSERT_EQ(0, ceph_ll_setxattr(a.cmount, dir, "ceph.quota.max_bytes", q.data(),
                                q.size(), 0, a.perms));
  constexpr int P = 4;
  constexpr int64_t SIZE = 512 << 10;   // 2 MiB in place
  std::vector<std::string> model(P, std::string(SIZE, '\0'));
  std::vector<Inode*> ins(P);
  std::vector<Fh*> fhs(P);
  for (int p = 0; p < P; p++)
    fhs[p] = a.create(dir, "p" + std::to_string(p), SIZE, &ins[p]);
  for (int p = 0; p < P; p++)
    ASSERT_EQ(0, ceph_ll_fsync(a.cmount, fhs[p], 0));
  std::atomic<int> bad{0}, dquot{0};
  std::atomic<int64_t> extended{0};
  std::vector<std::thread> th;
  for (int p = 0; p < P; p++) {
    th.emplace_back([&, p] {
      std::mt19937 rng(p + 1);
      for (int i = 0; i < 500 * scale(); i++) {
        int64_t off = (rng() % (SIZE / BLK)) * BLK;
        std::string d = block('a' + (p + i) % 26);
        if (ceph_ll_write(a.cmount, fhs[p], off, BLK, d.data()) != BLK)
          bad++;
        model[p].replace(off, BLK, d);
      }
    });
  }
  for (int e = 0; e < 2; e++) {
    th.emplace_back([&, e] {
      Inode *in = nullptr;
      Fh *fh = a.create(dir, "e" + std::to_string(e), 0, &in);
      std::string d = block('z', 64 * BLK);
      for (int64_t off = 0; off < (64 << 20); off += d.size()) {
        int r = ceph_ll_write(a.cmount, fh, off, d.size(), d.data());
        if (r == -EDQUOT) {
          dquot++;
          break;
        }
        if (r < 0) {
          bad++;
          break;
        }
        extended += r;
        if (off % (1 << 20) == 0)
          ceph_ll_fsync(a.cmount, fh, 0);   // let the quota see the size
      }
      ceph_ll_close(a.cmount, fh);
      ceph_ll_put(a.cmount, in);
    });
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, bad.load());
  EXPECT_EQ(2, dquot.load()) << "the quota stopped " << dquot << " of 2";
  EXPECT_LT(extended.load(), int64_t(32) << 20);
  for (int p = 0; p < P; p++) {
    ASSERT_EQ(0, ceph_ll_close(a.cmount, fhs[p]));
    ceph_ll_put(a.cmount, ins[p]);
  }
  Mount b;
  for (int p = 0; p < P; p++)
    EXPECT_EQ(model[p], read_via(b, dname, "p" + std::to_string(p))) << "file " << p;
  ceph_ll_put(a.cmount, dir);
}

/*
 * Threads of two clients, each a lock owner of its own, take a write lock
 * on a counter block, read it, add one and write it back: with locks and
 * the caps keeping the clients coherent, no increment is lost.
 */
TEST(LibCephFSThreads, CrossClientLockedCounter)
{
  Mount a, b;
  std::string dname = unique_name("thr_counter");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  Inode *in = nullptr;
  Fh *init = a.create(dir, "counter", BLK, &in);
  ASSERT_EQ(0, ceph_ll_close(a.cmount, init));
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  Inode *bin = b.lookup(bdir, "counter");
  ASSERT_NE(nullptr, bin);
  const int T = 4 * wide(), K = 25 * scale();
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int c = 0; c < 2; c++) {
    Mount& m = c ? b : a;
    Inode *fin = c ? bin : in;
    for (int t = 0; t < T; t++) {
      th.emplace_back([&, fin, c, t] {
        Fh *fh = nullptr;
        if (ceph_ll_open(m.cmount, fin, O_RDWR, &fh, m.perms)) {
          err++;
          return;
        }
        uint64_t owner = 1000 * (c + 1) + t;
        for (int k = 0; k < K; k++) {
          struct flock fl = {};
          fl.l_type = F_WRLCK;
          fl.l_whence = SEEK_SET;
          fl.l_len = BLK;
          if (ceph_ll_setlk(m.cmount, fh, &fl, owner, 1)) {
            err++;
            continue;
          }
          std::string s = m.read(fh, 0, 32);
          long v = atol(s.c_str());
          std::string n = std::to_string(v + 1);
          n.resize(32, '\0');
          if (ceph_ll_write(m.cmount, fh, 0, n.size(), n.data()) != int(n.size()))
            err++;
          if (ceph_ll_fsync(m.cmount, fh, 0))
            err++;
          fl.l_type = F_UNLCK;
          if (ceph_ll_setlk(m.cmount, fh, &fl, owner, 0))
            err++;
        }
        ceph_ll_close(m.cmount, fh);
      });
    }
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  Mount c;
  std::string s = read_via(c, dname, "counter");
  EXPECT_EQ(2L * T * K, atol(s.c_str()));
  ceph_ll_put(b.cmount, bin);
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * A client holds a read delegation while its threads open, read and close
 * the file through the fast path; another client opens it for writing,
 * which recalls the delegation: the holder returns it, the writer's open
 * goes through, and once the writer closed, new opens of the holder read
 * what it wrote.
 */
TEST(LibCephFSThreads, DelegationRecallWhileOpenClose)
{
  Mount a, b;
  ASSERT_EQ(0, ceph_set_deleg_timeout(a.cmount, ceph_get_cap_return_timeout(a.cmount) - 1));
  std::string dname = unique_name("thr_deleg");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  Inode *in = nullptr;
  Fh *init = a.create(dir, "f", BLK, &in);
  ASSERT_EQ(0, ceph_ll_close(a.cmount, init));
  Inode *bdir = b.lookup(b.root, dname);
  ASSERT_NE(nullptr, bdir);
  Inode *bin = b.lookup(bdir, "f");
  ASSERT_NE(nullptr, bin);
  std::atomic<bool> stop{false};
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int t = 0; t < 4; t++) {
    th.emplace_back([&] {
      while (!stop) {
        Fh *f = nullptr;
        if (ceph_ll_open(a.cmount, in, O_RDONLY, &f, a.perms)) {
          err++;
          continue;
        }
        std::string s = a.read(f, 0, BLK);
        if (s.size() != size_t(BLK) || !uniform(s))
          err++;
        ceph_ll_close(a.cmount, f);
      }
    });
  }
  int missed = 0;
  for (int round = 0; round < 5 * scale(); round++) {
    Fh *dfh = a.open(in, O_RDONLY);
    std::atomic_bool recalled{false};
    int r, tries = 0;
    do {
      r = ceph_ll_delegation(a.cmount, dfh, CEPH_DELEGATION_RD,
                             [](Fh *, void *p) {
                               static_cast<std::atomic_bool*>(p)->store(true);
                             }, &recalled);
      if (r == -EAGAIN)
        std::this_thread::sleep_for(10ms);
    } while (r == -EAGAIN && tries++ < 1000);
    ASSERT_EQ(0, r);
    std::thread giver([&] {
      for (int i = 0; i < 60000 && !recalled; i++)
        std::this_thread::sleep_for(1ms);
      ceph_ll_delegation(a.cmount, dfh, CEPH_DELEGATION_NONE, nullptr, nullptr);
    });
    std::string v = block('a' + round % 26);
    Fh *wf = b.open(bin, O_WRONLY);
    ASSERT_NE(nullptr, wf);
    ASSERT_EQ(BLK, ceph_ll_write(b.cmount, wf, 0, BLK, v.data()));
    ASSERT_EQ(0, ceph_ll_close(b.cmount, wf));
    giver.join();
    EXPECT_TRUE(recalled.load()) << "round " << round;
    ceph_ll_close(a.cmount, dfh);
    Fh *rf = a.open(in, O_RDONLY);
    if (a.read(rf, 0, BLK) != v)
      missed++;
    ceph_ll_close(a.cmount, rf);
  }
  stop = true;
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, missed);
  ceph_ll_put(b.cmount, bin);
  ceph_ll_put(b.cmount, bdir);
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

/*
 * Two clients write their halves of one file in LazyIO mode from several
 * threads each, propagate and synchronize: both then read the whole file
 * as written.
 */
TEST(LibCephFSThreads, LazyIOTwoClients)
{
  Mount a, b;
  std::string dname = unique_name("thr_lazy");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int64_t NBLK = 256;
  {
    Inode *in = nullptr;
    ASSERT_EQ(0, ceph_ll_close(a.cmount, a.create(dir, "f", NBLK * BLK, &in)));
    ceph_ll_put(a.cmount, in);
  }
  std::string path = "/" + dname + "/f";
  int fa = ceph_open(a.cmount, path.c_str(), O_RDWR, 0);
  int fb = ceph_open(b.cmount, path.c_str(), O_RDWR, 0);
  ASSERT_GE(fa, 0);
  ASSERT_GE(fb, 0);
  ASSERT_EQ(0, ceph_lazyio(a.cmount, fa, 1));
  ASSERT_EQ(0, ceph_lazyio(b.cmount, fb, 1));
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int c = 0; c < 2; c++) {
    for (int t = 0; t < 4; t++) {
      th.emplace_back([&, c, t] {
        auto cm = c ? b.cmount : a.cmount;
        int fd = c ? fb : fa;
        for (int64_t blk = c * NBLK / 2 + t; blk < (c + 1) * NBLK / 2; blk += 4) {
          std::string d = block(c ? 'B' : 'A');
          if (ceph_write(cm, fd, d.data(), BLK, blk * BLK) != BLK)
            err++;
        }
      });
    }
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(0, ceph_lazyio_propagate(a.cmount, fa, 0, 0));
  EXPECT_EQ(0, ceph_lazyio_propagate(b.cmount, fb, 0, 0));
  EXPECT_EQ(0, ceph_lazyio_synchronize(a.cmount, fa, 0, 0));
  EXPECT_EQ(0, ceph_lazyio_synchronize(b.cmount, fb, 0, 0));
  std::string expect = std::string(NBLK / 2 * BLK, 'A') + std::string(NBLK / 2 * BLK, 'B');
  for (auto [cm, fd] : {std::pair{a.cmount, fa}, std::pair{b.cmount, fb}}) {
    std::string s(NBLK * BLK, '\0');
    EXPECT_EQ(NBLK * BLK, ceph_read(cm, fd, s.data(), s.size(), 0));
    EXPECT_EQ(expect, s);
  }
  ceph_close(a.cmount, fa);
  ceph_close(b.cmount, fb);
  ceph_ll_put(a.cmount, dir);
}

/*
 * One file written at once by buffered writers in place (the fast path),
 * O_APPEND writers (the slow path, which extends the file) and an O_SYNC
 * writer: the appended blocks are whole and counted, the size is right,
 * and another client reads the same.
 */
TEST(LibCephFSThreads, AppendSyncAndInPlace)
{
  Mount a;
  std::string dname = unique_name("thr_append");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  constexpr int64_t NBLK = 128;
  const int K = 100 * scale();
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int w = 0; w < 4; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 1);
      for (int i = 0; i < K * 4; i++) {
        int64_t b = (rng() % (NBLK / 4)) * 4 + w;
        std::string d = block('a' + rng() % 26);
        if (ceph_ll_write(a.cmount, fh, b * BLK, BLK, d.data()) != BLK)
          err++;
      }
    });
  }
  th.emplace_back([&] {
    Fh *sf = a.open(in, O_WRONLY | O_SYNC);
    std::mt19937 rng(42);
    for (int i = 0; i < K; i++) {
      std::string d = block('s');
      if (ceph_ll_write(a.cmount, sf, (rng() % NBLK) * BLK, BLK, d.data()) != BLK)
        err++;
    }
    ceph_ll_close(a.cmount, sf);
  });
  for (int p = 0; p < 2; p++) {
    th.emplace_back([&, p] {
      Fh *af = a.open(in, O_WRONLY | O_APPEND);
      std::string d = block(p ? 'Q' : 'P');
      // offset -1: at the end of the file (O_APPEND only applies to
      // writes at the file position)
      for (int i = 0; i < K; i++)
        if (ceph_ll_write(a.cmount, af, -1, BLK, d.data()) != BLK)
          err++;
      ceph_ll_close(a.cmount, af);
    });
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));
  Mount b;
  for (Mount *m : {&a, &b}) {
    std::string s = read_via(*m, dname, "f");
    ASSERT_EQ(size_t((NBLK + 2 * K) * BLK), s.size()) << (m == &a ? "a" : "b");
    EXPECT_EQ(0, torn_blocks(s));
    int np = 0, nq = 0;
    for (int64_t blk = NBLK; blk < NBLK + 2 * K; blk++) {
      char c = s[blk * BLK];
      np += c == 'P';
      nq += c == 'Q';
    }
    EXPECT_EQ(K, np);
    EXPECT_EQ(K, nq);
  }
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}

#if defined(__linux__)
/*
 * Threads of a mount rewrite pieces of their own blocks of one encrypted
 * file (fscrypt: the writes that do not cover whole blocks read, modify
 * and write), through a small cache that makes writers wait for writeback
 * room: nothing hangs, and this mount and another one with the key read
 * back what was written.
 */
TEST(LibCephFSThreads, FscryptSharedFile)
{
  const std::map<std::string, std::string> small = {
    {"client_oc_max_dirty", "1048576"}, {"client_oc_target_dirty", "524288"}};
  Mount a(small);
  char key[32];
  for (size_t i = 0; i < sizeof(key); i++)
    key[i] = char(i * 7 + 3);
  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  ASSERT_EQ(0, ceph_add_fscrypt_key(a.cmount, key, sizeof(key), keyid, getuid()));
  std::string dname = unique_name("thr_fscrypt");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  struct fscrypt_policy_v2 policy;
  memset(&policy, 0, sizeof(policy));
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy.master_key_identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);
  ASSERT_EQ(0, ceph_ll_set_fscrypt_policy_v2(a.cmount, dir, &policy));

  const int W = 4 * wide();
  constexpr int64_t NBLK = 128;
  Inode *in = nullptr;
  Fh *fh = a.create(dir, "f", NBLK * BLK, &in);
  std::string model(NBLK * BLK, '\0');
  std::mutex mm;   // the model; each thread changes only its own blocks
  std::atomic<int> err{0};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++) {
    th.emplace_back([&, w] {
      std::mt19937 rng(w + 1);
      for (int i = 0; i < 200 * scale(); i++) {
        int64_t b = (rng() % (NBLK / W)) * W + w;
        int64_t piece = 512 * (rng() % 8);
        int64_t len = 512 * (1 + rng() % 4);
        if (piece + len > BLK)
          len = BLK - piece;
        int64_t off = b * BLK + piece;
        std::string d(len, 'a' + (w + i) % 26);
        if (ceph_ll_write(a.cmount, fh, off, len, d.data()) != len)
          err++;
        std::lock_guard l(mm);
        model.replace(off, len, d);
      }
    });
  }
  for (auto& x : th)
    x.join();
  EXPECT_EQ(0, err.load());
  EXPECT_EQ(model, a.read(fh, 0, NBLK * BLK));
  ASSERT_EQ(0, ceph_ll_close(a.cmount, fh));

  Mount b;
  char keyid2[FSCRYPT_KEY_IDENTIFIER_SIZE];
  ASSERT_EQ(0, ceph_add_fscrypt_key(b.cmount, key, sizeof(key), keyid2, getuid()));
  EXPECT_EQ(model, read_via(b, dname, "f"));
  ceph_ll_put(a.cmount, in);
  ceph_ll_put(a.cmount, dir);
}
#endif

/*
 * The reads and writes of many threads are counted in the per-CPU slots
 * and added up into the client's perf counters: after the client folds
 * them (on its tick), rdops and wrops went up by exactly what was done.
 */
TEST(LibCephFSThreads, PerfCountersCountEveryOp)
{
  Mount a;
  std::string dname = unique_name("thr_perf");
  Inode *dir = a.mkdir(a.root, dname);
  ASSERT_NE(nullptr, dir);
  const int T = 8 * wide(), K = 500 * scale();
  std::vector<Inode*> ins(T);
  std::vector<Fh*> fhs(T);
  for (int t = 0; t < T; t++)
    fhs[t] = a.create(dir, "f" + std::to_string(t), 16 * BLK, &ins[t]);
  auto counters = [&](int64_t *rd, int64_t *wr) {
    char *dump = nullptr;
    int len = ceph_get_perf_counters(a.cmount, &dump);
    ASSERT_GT(len, 0);
    JSONParser jp;
    ASSERT_TRUE(jp.parse(dump, len));
    JSONObj *jo = jp.find_obj("client");
    ASSERT_NE(nullptr, jo);
    JSONDecoder::decode_json("rdops", *rd, jo);
    JSONDecoder::decode_json("wrops", *wr, jo);
    free(dump);
  };
  // let the setup writes be folded in first
  std::this_thread::sleep_for(3s);
  int64_t rd0, wr0, rd1, wr1;
  counters(&rd0, &wr0);
  std::vector<std::thread> th;
  for (int t = 0; t < T; t++) {
    th.emplace_back([&, t] {
      std::string d = block('p');
      char buf[BLK];
      for (int i = 0; i < K; i++) {
        ceph_ll_write(a.cmount, fhs[t], (i % 16) * BLK, BLK, d.data());
        ceph_ll_read(a.cmount, fhs[t], (i % 16) * BLK, BLK, buf);
      }
    });
  }
  for (auto& x : th)
    x.join();
  std::this_thread::sleep_for(3s);   // a tick or two
  counters(&rd1, &wr1);
  EXPECT_EQ(int64_t(T) * K, rd1 - rd0);
  EXPECT_EQ(int64_t(T) * K, wr1 - wr0);
  for (int t = 0; t < T; t++) {
    ceph_ll_close(a.cmount, fhs[t]);
    ceph_ll_put(a.cmount, ins[t]);
  }
  ceph_ll_put(a.cmount, dir);
}
