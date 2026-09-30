/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 */

/*
 * cephfs-mdsbench - Client-side MDS metadata stress / benchmark
 *
 * Drives tunable lookup/stat, open/close, create/unlink, setattr, mkdir/rmdir,
 * and readdir pressure against CephFS via libcephfs. Intended for regression
 * of MDS journaler/throttle stalls and for metadata performance work.
 *
 * Default mix and scale target typical software-build style metadata load
 * (stat-heavy Run mix; nested Dir/bucket trees). Client mix is stat-heavy;
 * MDS wire traffic is mostly req_lookup, with create/setattr/unlink feeding
 * journal (evadd) + purge_queue + objecter writes.
 * Tree layout: workdir/CL{id}_MDSBENCH/DirN/bucketM/file — each client id
 * only performs I/O under its own CL{id}_MDSBENCH (avoids clashes with other
 * workload generators).
 *
 * Init CAPS storm: mass create/write-fill/close during tree populate can flood
 * MDS CLIENT_CAPS; the measured Run mix is a separate phase. Use --cap-storm /
 * --prep-only with many --client-id processes to drive that producer path.
 *
 * Standalone build sketch:
 *   g++ --std=c++20 -D_FILE_OFFSET_BITS=64 -O3 -o cephfs-mdsbench \
 *       cephfs-mdsbench.cc -lcephfs -lpthread -lboost_program_options
 */

#include <cephfs/libcephfs.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/program_options.hpp>

#include "common/JSONFormatter.h"
#include "include/ceph_assert.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::chrono::duration_cast;
using std::chrono::nanoseconds;
using std::chrono::steady_clock;
namespace po = boost::program_options;

enum class OpType {
  Stat,
  OpenClose,
  Create,
  Unlink,
  Chmod,
  Access,
  Mkdir,
  Rmdir,
  Readdir,
  WriteSmall,
  COUNT
};

static const char*
op_name(OpType op)
{
  switch (op) {
    // clang-format off
  case OpType::Stat: return "stat";
  case OpType::OpenClose: return "open_close";
  case OpType::Create: return "create";
  case OpType::Unlink: return "unlink";
  case OpType::Chmod: return "chmod";
  case OpType::Access: return "access";
  case OpType::Mkdir: return "mkdir";
  case OpType::Rmdir: return "rmdir";
  case OpType::Readdir: return "readdir";
  case OpType::WriteSmall: return "write_small";
  default: return "unknown";
    // clang-format on
  }
}

struct MixWeights {
  // Relative weights; normalized at runtime.
  double w[(size_t)OpType::COUNT]{};

  static MixWeights
  balanced()
  {
    // Software-build style mix: open_close ≈ mean(open, close); write_file →
    // write_small; rmdir rare on clients (keep tiny / zero).
    MixWeights m;
    m.w[(size_t)OpType::Stat] = 65;
    m.w[(size_t)OpType::OpenClose] = 12;
    m.w[(size_t)OpType::WriteSmall] = 6;
    m.w[(size_t)OpType::Access] = 6;
    m.w[(size_t)OpType::Chmod] = 5;
    m.w[(size_t)OpType::Unlink] = 3;
    m.w[(size_t)OpType::Readdir] = 2;
    m.w[(size_t)OpType::Mkdir] = 1;
    m.w[(size_t)OpType::Create] = 1;
    m.w[(size_t)OpType::Rmdir] = 0;
    return m;
  }

  static MixWeights
  journal_heavy()
  {
    // Shift toward MDS-visible mutating ops (req_create/unlink/setattr/mkdir/rmdir
    // and journal evadd / purge_queue pressure).
    MixWeights m;
    m.w[(size_t)OpType::Stat] = 15;
    m.w[(size_t)OpType::OpenClose] = 8;
    m.w[(size_t)OpType::Create] = 20;
    m.w[(size_t)OpType::Unlink] = 18;
    m.w[(size_t)OpType::Chmod] = 10;
    m.w[(size_t)OpType::Access] = 2;
    m.w[(size_t)OpType::Mkdir] = 8;
    m.w[(size_t)OpType::Rmdir] = 8;
    m.w[(size_t)OpType::Readdir] = 3;
    m.w[(size_t)OpType::WriteSmall] = 8;
    return m;
  }

  static MixWeights
  lookup_heavy()
  {
    // Emphasize client stat/access + readdir (MDS req_lookup-heavy traffic).
    MixWeights m;
    m.w[(size_t)OpType::Stat] = 70;
    m.w[(size_t)OpType::OpenClose] = 8;
    m.w[(size_t)OpType::Access] = 8;
    m.w[(size_t)OpType::Readdir] = 8;
    m.w[(size_t)OpType::Chmod] = 2;
    m.w[(size_t)OpType::Create] = 1;
    m.w[(size_t)OpType::Unlink] = 1;
    m.w[(size_t)OpType::Mkdir] = 0;
    m.w[(size_t)OpType::Rmdir] = 0;
    m.w[(size_t)OpType::WriteSmall] = 2;
    return m;
  }
};

struct BenchConfig {
  // Typical software-build style metadata load aims around ~12k aggregate
  // ops/s with tens of client workers. Default: one thread per client id.
  int threads =
      24; // derived: client_id>=0 ? 1 : clients (kept for JSON/reporting)
  double ops_per_sec = 12000;
  int duration_sec = 60;
  int warmup_sec = 5;
  uint64_t seed = 1;

  // Fixed nested tree (not derived from --clients / --ops-per-sec):
  //   workdir/CL{id}_MDSBENCH/Dir{0..49}/bucket{0..25}/f{0..99}
  // ≈ 1300 dirs / 130k files per client. Hidden CLI overrides for experiments.
  int clients = 24; // CL0_MDSBENCH .. CL{clients-1}_MDSBENCH (+ one thread each)
  int client_id = -1; // if >= 0, this process only uses CL{client_id}_MDSBENCH
  int top_dirs = 50;
  int buckets_per_dir = 26;
  int files_per_bucket = 100;
  int scratch_slots = 64; // transient create/unlink/mkdir/rmdir names per client

  string mode = "balanced"; // balanced | journal-heavy | lookup-heavy
  MixWeights mix = MixWeights::balanced();

  string workdir = "/mdsbench";
  string ceph_conf;
  string userid = "admin";
  string keyring;
  string filesystem;
  string mount_root = "/";
  int uid = -1;
  int gid = -1;

  string json_path;
  bool cleanup = true;
  bool keep_tree = false;
  int write_bytes = 4096;
  bool show_progress = false;
  int progress_interval = 10; // minimum % between progress line updates

  // Init / precreate (tree populate — source of Init CAPS storm):
  // create → optional write-fill → close. Dirty caps flush as CLIENT_CAPS.
  int init_file_bytes = 16384;   // default fill size (16 KiB)
  int init_fill_percent = 46;    // share of files that get write-fill (~60k/130k)
  int init_threads = 1;          // parallel create/fill workers within this process
  bool cap_storm = false;        // preset: heavy Init fill + prep-only

  // Multi-process coordination (CephFS file barrier under workdir).
  bool barrier = false; // wait until clients 0..N-1 have signaled ready
  bool no_barrier = false; // disable auto-barrier when --client-id is set
  bool prep_only = false; // precreate (+ optional ready signal), then exit
  bool skip_precreate = false; // assume CL{id}_MDSBENCH already exists
  int barrier_timeout_sec = 0; // 0 = wait forever
};

struct LatencyHist {
  // Fixed reservoir of samples for percentile estimates (per op type).
  static constexpr size_t kCap = 100000;
  vector<uint64_t> samples_ns;
  std::mutex mu;
  uint64_t count = 0;
  uint64_t sum_ns = 0;

  void
  record(uint64_t ns)
  {
    std::lock_guard lock(mu);
    ++count;
    sum_ns += ns;
    if (samples_ns.size() < kCap) {
      samples_ns.push_back(ns);
    } else {
      // Reservoir: replace with decreasing probability.
      std::mt19937_64 eng(ns ^ count);
      std::uniform_int_distribution<uint64_t> dist(0, count - 1);
      uint64_t j = dist(eng);
      if (j < kCap) {
        samples_ns[j] = ns;
      }
    }
  }

  double
  percentile(double p)
  {
    std::lock_guard lock(mu);
    if (samples_ns.empty()) {
      return 0;
    }
    vector<uint64_t> copy = samples_ns;
    size_t idx = (size_t)std::clamp(
        p * (copy.size() - 1), 0.0, (double)(copy.size() - 1));
    std::nth_element(copy.begin(), copy.begin() + idx, copy.end());
    return copy[idx] / 1000.0; // usec
  }

  double
  mean_usec()
  {
    std::lock_guard lock(mu);
    if (count == 0) {
      return 0;
    }
    return (sum_ns / (double)count) / 1000.0;
  }
};

struct ThreadStats {
  std::atomic<uint64_t> ops{0};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> op_counts[(size_t)OpType::COUNT]{};
};

struct RateLimiter {
  // Shared token-bucket across threads.
  std::mutex mu;
  double tokens;
  double rate;
  steady_clock::time_point last;

  explicit RateLimiter(double r) :
    tokens(r), rate(r), last(steady_clock::now())
  {}

  void
  acquire(int n = 1)
  {
    if (rate <= 0) {
      return; // unlimited
    }
    for (;;) {
      std::unique_lock lock(mu);
      auto now = steady_clock::now();
      double elapsed = duration_cast<nanoseconds>(now - last).count() / 1e9;
      last = now;
      tokens = std::min(rate * 2.0, tokens + elapsed * rate);
      if (tokens >= n) {
        tokens -= n;
        return;
      }
      double need = (n - tokens) / rate;
      lock.unlock();
      std::this_thread::sleep_for(
          std::chrono::duration<double>(std::min(need, 0.01)));
    }
  }
};

struct PathIndex {
  vector<string> dirs; // dirs under this client root (for readdir/scratch)
  vector<string> files;
  string root; // .../CL{id}_MDSBENCH
  int client_id = -1;
};

static string
join_path(const string& a, const string& b)
{
  if (a.empty() || a == "/") {
    return "/" + b;
  }
  if (a.back() == '/') {
    return a + b;
  }
  return a + "/" + b;
}

static string
client_root(const BenchConfig& cfg, int id)
{
  return join_path(cfg.workdir, "CL" + std::to_string(id) + "_MDSBENCH");
}

static void
build_client_paths(const BenchConfig& cfg, int id, PathIndex& idx)
{
  // Nested Dir/bucket/file layout under CL{N}_MDSBENCH (namespaced to avoid
  // colliding with other workload generators on the same mount).
  idx.dirs.clear();
  idx.files.clear();
  idx.client_id = id;
  idx.root = client_root(cfg, id);
  idx.dirs.push_back(idx.root);

  for (int d = 0; d < cfg.top_dirs; ++d) {
    string dir = join_path(idx.root, "Dir" + std::to_string(d));
    idx.dirs.push_back(dir);
    for (int b = 0; b < cfg.buckets_per_dir; ++b) {
      string bucket = join_path(dir, "bucket" + std::to_string(b));
      idx.dirs.push_back(bucket);
      for (int f = 0; f < cfg.files_per_bucket; ++f) {
        idx.files.push_back(join_path(bucket, "f" + std::to_string(f)));
      }
    }
  }
}

static void
build_all_client_paths(const BenchConfig& cfg, vector<PathIndex>& by_client)
{
  by_client.clear();
  if (cfg.client_id >= 0) {
    by_client.resize(1);
    build_client_paths(cfg, cfg.client_id, by_client[0]);
    return;
  }
  by_client.resize(cfg.clients);
  for (int c = 0; c < cfg.clients; ++c) {
    build_client_paths(cfg, c, by_client[c]);
  }
}

static int
setup_mount(
    struct ceph_mount_info** cmount,
    const BenchConfig& config,
    std::ostream& out)
{
  if (int rc = ceph_create(
          cmount, config.userid.empty() ? nullptr : config.userid.c_str());
      rc < 0) {
    out << "ceph_create failed: " << strerror(-rc) << endl;
    return rc;
  }
  auto fail = [&](int rc) {
    ceph_shutdown(*cmount);
    *cmount = nullptr;
    return rc;
  };

  if (!config.ceph_conf.empty()) {
    if (int rc = ceph_conf_read_file(*cmount, config.ceph_conf.c_str());
        rc < 0) {
      out << "conf_read_file failed: " << strerror(-rc) << endl;
      return fail(rc);
    }
  } else if (int rc = ceph_conf_read_file(*cmount, nullptr); rc < 0) {
    out << "default conf_read_file failed: " << strerror(-rc) << endl;
    return fail(rc);
  }

  if (!config.keyring.empty()) {
    if (int rc = ceph_conf_set(*cmount, "keyring", config.keyring.c_str());
        rc < 0) {
      out << "set keyring failed: " << strerror(-rc) << endl;
      return fail(rc);
    }
  }
  if (!config.filesystem.empty()) {
    if (int rc = ceph_conf_set(
            *cmount, "client_mds_namespace", config.filesystem.c_str());
        rc < 0) {
      out << "set filesystem failed: " << strerror(-rc) << endl;
      return fail(rc);
    }
  }
  // libcephfs skips global_init, so nothing registers an assert context or
  // installs fatal SIGABRT handlers. Without this, --debug_*=N/M gather-only
  // rings are never dump_recent()'d on ceph_assert. Disable fatal handlers so
  // __ceph_assert_fail dumps the ring itself (handlers would use g_ceph_context,
  // which is also unset for library clients).
  if (int rc = ceph_conf_set(*cmount, "fatal_signal_handlers", "false");
      rc < 0) {
    out << "set fatal_signal_handlers failed: " << strerror(-rc) << endl;
    return fail(rc);
  }
  ceph::register_assert_context(ceph_get_mount_context(*cmount));
  if (int rc = ceph_init(*cmount); rc < 0) {
    out << "ceph_init failed: " << strerror(-rc) << endl;
    return fail(rc);
  }
  if (config.uid != -1 || config.gid != -1) {
    UserPerm* perms = ceph_userperm_new(config.uid, config.gid, 0, nullptr);
    if (!perms) {
      return fail(-ENOMEM);
    }
    int rc = ceph_mount_perms_set(*cmount, perms);
    ceph_userperm_destroy(perms);
    if (rc != 0) {
      out << "mount_perms_set failed: " << strerror(-rc) << endl;
      return fail(rc);
    }
  }
  if (int rc = ceph_mount(*cmount, config.mount_root.c_str()); rc < 0) {
    out << "ceph_mount failed: " << strerror(-rc) << endl;
    return fail(rc);
  }
  return 0;
}

static int
ensure_dir(struct ceph_mount_info* cmount, const string& path)
{
  struct ceph_statx stx;
  if (ceph_statx(cmount, path.c_str(), &stx, 0, 0) == 0) {
    return 0;
  }
  // Create each path component (skip empty / root).
  string cur;
  for (size_t i = 0; i < path.size(); ++i) {
    cur.push_back(path[i]);
    if ((i + 1 == path.size() || path[i + 1] == '/') && cur != "/") {
      int rc = ceph_mkdir(cmount, cur.c_str(), 0755);
      if (rc < 0 && rc != -EEXIST) {
        return rc;
      }
    }
  }
  return 0;
}

static int
init_one_file(
    struct ceph_mount_info* cmount,
    const string& path,
    const vector<char>& fill_buf,
    bool do_fill)
{
  int fd = ceph_open(cmount, path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    if (fd == -EEXIST) {
      return 0;
    }
    return fd;
  }
  if (do_fill && !fill_buf.empty()) {
    // Write-fill like generic_init_file (dirty Fx/Fw caps → CLIENT_CAPS on close).
    uint64_t off = 0;
    while (off < fill_buf.size()) {
      int n = ceph_write(cmount, fd, fill_buf.data() + off,
                         fill_buf.size() - off, off);
      if (n < 0) {
        ceph_close(cmount, fd);
        return n;
      }
      if (n == 0) {
        break;
      }
      off += n;
    }
  }
  return ceph_close(cmount, fd);
}

static int
precreate_tree(
    struct ceph_mount_info* cmount,
    const BenchConfig& cfg,
    const vector<PathIndex>& by_client,
    std::ostream& out)
{
  uint64_t total_dirs = 0;
  uint64_t total_files = 0;
  for (const auto& idx : by_client) {
    total_dirs += idx.dirs.size();
    total_files += idx.files.size();
  }
  out << "Precreating under " << cfg.workdir << " (" << by_client.size()
      << " clients, " << total_dirs << " dirs, " << total_files << " files"
      << ", fill=" << cfg.init_fill_percent << "% @" << cfg.init_file_bytes
      << "B, init_threads=" << cfg.init_threads << ")..." << endl;
  if (int rc = ensure_dir(cmount, cfg.workdir); rc < 0) {
    out << "ensure workdir failed: " << strerror(-rc) << endl;
    return rc;
  }

  vector<char> fill_buf;
  if (cfg.init_file_bytes > 0 && cfg.init_fill_percent > 0) {
    fill_buf.assign(cfg.init_file_bytes, 'I');
  }

  // Collect all file paths first; dirs must exist before parallel create.
  vector<string> all_files;
  all_files.reserve(total_files);
  for (const auto& idx : by_client) {
    if (!cfg.show_progress) {
      out << "  client " << idx.client_id << " -> " << idx.root << endl;
    }
    for (const auto& d : idx.dirs) {
      int rc = ceph_mkdir(cmount, d.c_str(), 0755);
      if (rc < 0 && rc != -EEXIST) {
        out << "mkdir " << d << " failed: " << strerror(-rc) << endl;
        return rc;
      }
    }
    all_files.insert(all_files.end(), idx.files.begin(), idx.files.end());
  }

  const int nthreads = std::max(1, cfg.init_threads);
  std::atomic<uint64_t> created{0};
  std::atomic<uint64_t> filled{0};
  std::atomic<int> first_err{0};
  std::atomic<int> last_interval{-1};
  auto t0 = steady_clock::now();

  auto worker_fn = [&](int tid) {
    for (size_t i = tid; i < all_files.size(); i += nthreads) {
      if (first_err.load(std::memory_order_relaxed) != 0) {
        return;
      }
      // Deterministic fill subset (files that get data during Init).
      bool do_fill = !fill_buf.empty() &&
                     ((int)(i % 100) < cfg.init_fill_percent);
      int rc = init_one_file(cmount, all_files[i], fill_buf, do_fill);
      if (rc < 0) {
        int expected = 0;
        if (first_err.compare_exchange_strong(expected, rc)) {
          cerr << "init create/fill " << all_files[i]
               << " failed: " << strerror(-rc) << endl;
        }
        return;
      }
      uint64_t c = created.fetch_add(1, std::memory_order_relaxed) + 1;
      if (do_fill) {
        filled.fetch_add(1, std::memory_order_relaxed);
      }
      if (cfg.show_progress && total_files > 0) {
        double pct = 100.0 * (double)c / (double)total_files;
        if (pct > 100.0) {
          pct = 100.0;
        }
        int cur = (int)(pct / cfg.progress_interval);
        int prev = last_interval.load(std::memory_order_relaxed);
        if (cur > prev &&
            last_interval.compare_exchange_strong(prev, cur)) {
          double elapsed =
              duration_cast<nanoseconds>(steady_clock::now() - t0).count() /
              1e9;
          double creates_per_sec = elapsed > 0 ? c / elapsed : 0;
          cout << "\r[" << std::fixed << std::setprecision(0) << pct << "%]"
               << "[init=" << (uint64_t)creates_per_sec << " files/s]"
               << "[" << c << "/" << total_files << " files]"
               << "[filled=" << filled.load(std::memory_order_relaxed) << "]"
               << std::flush;
        }
      }
    }
  };

  if (nthreads == 1) {
    worker_fn(0);
  } else {
    vector<std::thread> pool;
    for (int t = 0; t < nthreads; ++t) {
      pool.emplace_back(worker_fn, t);
    }
    for (auto& t : pool) {
      t.join();
    }
  }

  if (cfg.show_progress) {
    cout << "\r" << string(80, ' ') << "\r" << std::flush;
  }
  if (int err = first_err.load(); err != 0) {
    return err;
  }
  out << "Precreate done (" << created.load() << " files, " << filled.load()
      << " write-filled)." << endl;
  return 0;
}

static OpType
pick_op(const MixWeights& mix, std::mt19937_64& rng)
{
  double total = 0;
  for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
    total += mix.w[i];
  }
  std::uniform_real_distribution<double> dist(0.0, total);
  double r = dist(rng);
  double acc = 0;
  for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
    acc += mix.w[i];
    if (r <= acc) {
      return (OpType)i;
    }
  }
  return OpType::Stat;
}

static int
do_op(
    struct ceph_mount_info* cmount,
    OpType op,
    const BenchConfig& cfg,
    const PathIndex& idx,
    std::mt19937_64& rng,
    vector<char>& wbuf)
{
  if (idx.files.empty() || idx.dirs.empty()) {
    return -ENOENT;
  }
  std::uniform_int_distribution<size_t> file_dist(0, idx.files.size() - 1);
  std::uniform_int_distribution<size_t> dir_dist(0, idx.dirs.size() - 1);
  std::uniform_int_distribution<int> slot_dist(0, cfg.scratch_slots - 1);

  const string& file = idx.files[file_dist(rng)];
  const string& dir = idx.dirs[dir_dist(rng)];
  // Scratch names stay under this client's tree only.
  string scratch_file = join_path(dir, "s_" + std::to_string(slot_dist(rng)));
  string scratch_dir = join_path(dir, "sd_" + std::to_string(slot_dist(rng)));

  switch (op) {
  case OpType::Stat: {
    struct ceph_statx stx;
    return ceph_statx(cmount, file.c_str(), &stx, CEPH_STATX_MODE, 0);
  }
  case OpType::OpenClose: {
    int fd = ceph_open(cmount, file.c_str(), O_RDONLY, 0);
    if (fd < 0) {
      return fd;
    }
    return ceph_close(cmount, fd);
  }
  case OpType::Create: {
    int fd = ceph_open(
        cmount, scratch_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
      return fd;
    }
    return ceph_close(cmount, fd);
  }
  case OpType::Unlink:
    return ceph_unlink(cmount, scratch_file.c_str());
  case OpType::Chmod:
    return ceph_chmod(cmount, file.c_str(), 0644);
  case OpType::Access: {
    // Approximate access(2) via statx of mode bits.
    struct ceph_statx stx;
    return ceph_statx(cmount, file.c_str(), &stx, CEPH_STATX_MODE, 0);
  }
  case OpType::Mkdir:
    return ceph_mkdir(cmount, scratch_dir.c_str(), 0755);
  case OpType::Rmdir:
    return ceph_rmdir(cmount, scratch_dir.c_str());
  case OpType::Readdir: {
    struct ceph_dir_result* dirp = nullptr;
    int rc = ceph_opendir(cmount, dir.c_str(), &dirp);
    if (rc < 0) {
      return rc;
    }
    while (ceph_readdir(cmount, dirp) != nullptr) {
    }
    return ceph_closedir(cmount, dirp);
  }
  case OpType::WriteSmall: {
    int fd = ceph_open(cmount, file.c_str(), O_WRONLY, 0);
    if (fd < 0) {
      return fd;
    }
    int n = std::min(cfg.write_bytes, (int)wbuf.size());
    int wr = ceph_write(cmount, fd, wbuf.data(), n, 0);
    int cl = ceph_close(cmount, fd);
    return wr < 0 ? wr : cl;
  }
  default:
    return -EINVAL;
  }
}

static void
progress_reporter(
    const vector<ThreadStats>& stats,
    const BenchConfig& config,
    std::atomic<bool>& stop_signal,
    steady_clock::time_point start_time)
{
  if (!config.show_progress) {
    return;
  }

  int last_interval = -1;
  uint64_t last_ops = 0;
  auto last_sample = start_time;

  while (!stop_signal.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    uint64_t total_ops = 0;
    uint64_t total_err = 0;
    for (const auto& s : stats) {
      total_ops += s.ops.load(std::memory_order_relaxed);
      total_err += s.errors.load(std::memory_order_relaxed);
    }

    auto now = steady_clock::now();
    double elapsed_sec =
        duration_cast<std::chrono::milliseconds>(now - start_time).count() /
        1000.0;
    if (elapsed_sec <= 0) {
      continue;
    }

    double progress_pct = (elapsed_sec / config.duration_sec) * 100.0;
    if (progress_pct > 100.0) {
      progress_pct = 100.0;
    }
    double eta_sec = config.duration_sec - elapsed_sec;
    if (eta_sec < 0) {
      eta_sec = 0;
    }

    int current_interval = (int)(progress_pct / config.progress_interval);
    if (current_interval > last_interval) {
      last_interval = current_interval;

      double sample_sec =
          duration_cast<std::chrono::milliseconds>(now - last_sample).count() /
          1000.0;
      double current_iops = 0;
      if (sample_sec > 0) {
        current_iops = (double)(total_ops - last_ops) / sample_sec;
      }
      double avg_iops = (double)total_ops / elapsed_sec;
      last_ops = total_ops;
      last_sample = now;

      int eta_m = (int)eta_sec / 60;
      int eta_s = (int)eta_sec % 60;

      cout << "\r[" << std::fixed << std::setprecision(0) << progress_pct
           << "%]"
           << "[cur=" << (uint64_t)current_iops << " IOPS]"
           << "[avg=" << (uint64_t)avg_iops << " IOPS]"
           << "[errs=" << total_err << "]"
           << "[eta " << std::setfill('0') << std::setw(2) << eta_m
           << "m:" << std::setw(2) << eta_s << "s]" << std::setfill(' ')
           << std::flush;
    }
  }
  cout << "\r" << string(80, ' ') << "\r" << std::flush;
}

static void
worker(
    int client_id,
    struct ceph_mount_info* cmount,
    const BenchConfig& cfg,
    const PathIndex& idx,
    RateLimiter& limiter,
    std::atomic<bool>& stop,
    std::atomic<bool>& counting,
    ThreadStats& stats,
    LatencyHist* lats)
{
  std::mt19937_64 rng(cfg.seed + 10007ULL * (uint64_t)client_id);
  vector<char> wbuf(std::max(cfg.write_bytes, 1), 'M');

  while (!stop.load(std::memory_order_relaxed)) {
    limiter.acquire(1);
    OpType op = pick_op(cfg.mix, rng);
    auto t0 = steady_clock::now();
    int rc = do_op(cmount, op, cfg, idx, rng, wbuf);
    auto t1 = steady_clock::now();
    // Expected misses on unlink/rmdir of empty scratch slots are fine.
    bool soft_err = (rc == -ENOENT || rc == -EEXIST);
    if (rc < 0 && !soft_err) {
      stats.errors.fetch_add(1, std::memory_order_relaxed);
    }
    if (counting.load(std::memory_order_relaxed)) {
      stats.ops.fetch_add(1, std::memory_order_relaxed);
      stats.op_counts[(size_t)op].fetch_add(1, std::memory_order_relaxed);
      uint64_t ns = duration_cast<nanoseconds>(t1 - t0).count();
      lats[(size_t)op].record(ns);
    }
  }
}

static bool
apply_mode(BenchConfig& cfg)
{
  if (cfg.mode == "custom") {
    return true; // mix already set via --mix
  }
  if (cfg.mode == "balanced") {
    cfg.mix = MixWeights::balanced();
  } else if (cfg.mode == "journal-heavy") {
    cfg.mix = MixWeights::journal_heavy();
  } else if (cfg.mode == "lookup-heavy") {
    cfg.mix = MixWeights::lookup_heavy();
  } else {
    return false;
  }
  return true;
}

static int
parse_mix_override(const string& s, MixWeights& mix)
{
  // Format: stat=35,open_close=25,... (unlisted stay 0)
  for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
    mix.w[i] = 0;
  }
  std::stringstream ss(s);
  string tok;
  while (std::getline(ss, tok, ',')) {
    auto eq = tok.find('=');
    if (eq == string::npos) {
      return -EINVAL;
    }
    string name = tok.substr(0, eq);
    double val = std::stod(tok.substr(eq + 1));
    bool found = false;
    for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
      if (name == op_name((OpType)i)) {
        mix.w[i] = val;
        found = true;
        break;
      }
    }
    if (!found) {
      return -EINVAL;
    }
  }
  return 0;
}

static string
barrier_dir(const BenchConfig& cfg)
{
  return join_path(cfg.workdir, ".mdsbench_barrier");
}

static string
barrier_ready_path(const BenchConfig& cfg, int id)
{
  return join_path(barrier_dir(cfg), "ready." + std::to_string(id));
}

// Drop this client's ready marker so a prior crashed run cannot leave a
// stale signal. Safe if the file does not exist.
static void
barrier_clear_own(
    struct ceph_mount_info* cmount,
    const BenchConfig& cfg,
    int id,
    std::ostream& out)
{
  string dir = barrier_dir(cfg);
  (void)ensure_dir(cmount, dir);
  string path = barrier_ready_path(cfg, id);
  int rc = ceph_unlink(cmount, path.c_str());
  if (rc == 0) {
    out << "Barrier: cleared stale " << path << endl;
  }
}

static int
barrier_signal_ready(
    struct ceph_mount_info* cmount,
    const BenchConfig& cfg,
    int id,
    std::ostream& out)
{
  string dir = barrier_dir(cfg);
  if (int rc = ensure_dir(cmount, dir); rc < 0) {
    out << "barrier mkdir failed: " << strerror(-rc) << endl;
    return rc;
  }
  string path = barrier_ready_path(cfg, id);
  int fd = ceph_open(cmount, path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    out << "barrier ready create failed (" << path << "): " << strerror(-fd)
        << endl;
    return fd;
  }
  string payload = "client " + std::to_string(id) + " ready\n";
  (void)ceph_write(cmount, fd, payload.data(), payload.size(), 0);
  ceph_close(cmount, fd);
  out << "Barrier: signaled ready as client " << id << " (" << path << ")"
      << endl;
  return 0;
}

// Wait until ready.0 .. ready.{expect-1} exist. Once a peer is seen, do not
// re-stat it on later polls.
static int
barrier_wait(
    struct ceph_mount_info* cmount,
    const BenchConfig& cfg,
    int expect,
    std::ostream& out)
{
  out << "Barrier: waiting for " << expect << " clients under "
      << barrier_dir(cfg) << endl;
  auto t0 = steady_clock::now();
  vector<char> seen(expect, 0);
  int ready = 0;
  for (;;) {
    bool progressed = false;
    for (int i = 0; i < expect; ++i) {
      if (seen[i]) {
        continue;
      }
      struct ceph_statx stx;
      string path = barrier_ready_path(cfg, i);
      if (ceph_statx(cmount, path.c_str(), &stx, 0, 0) == 0) {
        seen[i] = 1;
        ++ready;
        progressed = true;
      }
    }
    if (progressed) {
      out << "Barrier: " << ready << "/" << expect << " ready" << endl;
    }
    if (ready >= expect) {
      out << "Barrier: all clients ready; starting together" << endl;
      return 0;
    }
    if (cfg.barrier_timeout_sec > 0) {
      double elapsed =
          duration_cast<nanoseconds>(steady_clock::now() - t0).count() / 1e9;
      if (elapsed >= cfg.barrier_timeout_sec) {
        out << "Barrier: timed out after " << cfg.barrier_timeout_sec
            << "s with " << ready << "/" << expect << " ready" << endl;
        return -ETIMEDOUT;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

static void
barrier_cleanup(
    struct ceph_mount_info* cmount,
    const BenchConfig& cfg,
    int expect)
{
  for (int i = 0; i < expect; ++i) {
    (void)ceph_unlink(cmount, barrier_ready_path(cfg, i).c_str());
  }
  (void)ceph_rmdir(cmount, barrier_dir(cfg).c_str());
}

static int
run_bench(BenchConfig& cfg)
{
  if (!apply_mode(cfg)) {
    cerr << "Unknown mode: " << cfg.mode
         << " (expected balanced|journal-heavy|lookup-heavy)" << endl;
    return 1;
  }

  // One worker thread per client id; each only touches CL{id}_MDSBENCH.
  if (cfg.client_id >= 0) {
    cfg.threads = 1;
  } else {
    cfg.threads = cfg.clients;
  }

  // Multi-process: auto-barrier unless --no-barrier (single-process in-tree
  // threads already start together after a shared precreate).
  bool use_barrier = cfg.barrier || (cfg.client_id >= 0 && !cfg.no_barrier);
  if (use_barrier && cfg.client_id < 0) {
    cerr << "Error: --barrier requires --client-id (multi-process mode)\n";
    return 1;
  }
  if (cfg.client_id >= 0 && cfg.client_id >= cfg.clients) {
    cerr << "Error: --client-id must be in [0, --clients)\n";
    return 1;
  }

  vector<PathIndex> by_client;
  build_all_client_paths(cfg, by_client);

  struct ceph_mount_info* cmount = nullptr;
  if (int rc = setup_mount(&cmount, cfg, cerr); rc < 0) {
    return 1;
  }

  // Clear our ready marker before seeding so a crashed prior run cannot
  // leave ready.{id} and let peers race ahead.
  if (use_barrier) {
    barrier_clear_own(cmount, cfg, cfg.client_id, cout);
  }

  if (!cfg.skip_precreate) {
    if (int rc = precreate_tree(cmount, cfg, by_client, cout); rc < 0) {
      ceph_unmount(cmount);
      ceph_shutdown(cmount);
      return 1;
    }
  } else {
    cout << "Skipping precreate (--skip-precreate); using existing trees"
         << endl;
  }

  if (use_barrier) {
    if (int rc = barrier_signal_ready(cmount, cfg, cfg.client_id, cout);
        rc < 0) {
      ceph_unmount(cmount);
      ceph_shutdown(cmount);
      return 1;
    }
    if (cfg.prep_only) {
      cout << "Prep-only: ready signaled; exiting before barrier wait/bench"
           << endl;
      ceph_unmount(cmount);
      ceph_shutdown(cmount);
      return 0;
    }
    if (int rc = barrier_wait(cmount, cfg, cfg.clients, cout); rc < 0) {
      ceph_unmount(cmount);
      ceph_shutdown(cmount);
      return 1;
    }
    // Leave ready.* markers in place until cleanup so late waiters cannot
    // miss a peer that already passed the barrier.
  } else if (cfg.prep_only) {
    cout << "Prep-only: precreate done; exiting" << endl;
    ceph_unmount(cmount);
    ceph_shutdown(cmount);
    return 0;
  } else if (cfg.client_id >= 0) {
    cout << "Warning: --client-id without barrier; this process may start "
            "benchmarking while other clients are still seeding. Pass "
            "--barrier (default when --client-id is set) or --no-barrier to "
            "silence."
         << endl;
  }

  RateLimiter limiter(cfg.ops_per_sec);
  std::atomic<bool> stop{false};
  std::atomic<bool> counting{false};
  vector<ThreadStats> tstats(by_client.size());
  LatencyHist lats[(size_t)OpType::COUNT];
  vector<std::thread> threads;

  cout << "Starting " << by_client.size() << " clients"
       << " target=" << cfg.ops_per_sec << " ops/s"
       << " mode=" << cfg.mode << " warmup=" << cfg.warmup_sec << "s"
       << " duration=" << cfg.duration_sec << "s" << endl;
  for (const auto& idx : by_client) {
    cout << "  client " << idx.client_id << " confined to " << idx.root << endl;
  }

  for (size_t i = 0; i < by_client.size(); ++i) {
    threads.emplace_back(
        worker, by_client[i].client_id, cmount, std::cref(cfg),
        std::cref(by_client[i]), std::ref(limiter), std::ref(stop),
        std::ref(counting), std::ref(tstats[i]), lats);
  }

  if (cfg.warmup_sec > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(cfg.warmup_sec));
  }
  counting.store(true, std::memory_order_relaxed);
  auto t_start = steady_clock::now();

  std::atomic<bool> progress_stop{false};
  std::thread progress_thread;
  if (cfg.show_progress) {
    progress_thread = std::thread(
        progress_reporter, std::cref(tstats), std::cref(cfg),
        std::ref(progress_stop), t_start);
  }

  std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_sec));
  auto t_end = steady_clock::now();
  counting.store(false, std::memory_order_relaxed);

  if (cfg.show_progress) {
    progress_stop.store(true, std::memory_order_relaxed);
    if (progress_thread.joinable()) {
      progress_thread.join();
    }
  }

  stop.store(true, std::memory_order_relaxed);
  for (auto& t : threads) {
    t.join();
  }

  double elapsed = duration_cast<nanoseconds>(t_end - t_start).count() / 1e9;
  uint64_t total_ops = 0;
  uint64_t total_err = 0;
  uint64_t per_op[(size_t)OpType::COUNT]{};
  for (auto& s : tstats) {
    total_ops += s.ops.load();
    total_err += s.errors.load();
    for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
      per_op[i] += s.op_counts[i].load();
    }
  }
  double ops_s = elapsed > 0 ? total_ops / elapsed : 0;

  uint64_t total_dirs = 0;
  uint64_t total_files = 0;
  for (const auto& idx : by_client) {
    total_dirs += idx.dirs.size();
    total_files += idx.files.size();
  }

  cout << "\n*** Results ***" << endl;
  cout << "elapsed_sec: " << std::fixed << std::setprecision(3) << elapsed
       << endl;
  cout << "ops: " << total_ops << endl;
  cout << "ops_per_sec: " << std::setprecision(1) << ops_s << endl;
  cout << "errors: " << total_err << endl;
  cout << "\nPer-op (count, mean_us, p50_us, p99_us):" << endl;
  for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
    if (per_op[i] == 0 && cfg.mix.w[i] == 0) {
      continue;
    }
    cout << "  " << std::setw(12) << op_name((OpType)i) << ": " << per_op[i]
         << "  mean=" << std::setprecision(1) << lats[i].mean_usec()
         << "  p50=" << lats[i].percentile(0.50)
         << "  p99=" << lats[i].percentile(0.99) << endl;
  }

  if (!cfg.json_path.empty()) {
    ceph::JSONFormatter f(true);
    f.open_object_section("mdsbench");
    f.open_object_section("configuration");
    f.dump_string("mode", cfg.mode);
    f.dump_int("clients", (int)by_client.size());
    if (cfg.client_id >= 0) {
      f.dump_int("client_id", cfg.client_id);
    }
    f.dump_float("ops_per_sec_target", cfg.ops_per_sec);
    f.dump_int("duration_sec", cfg.duration_sec);
    f.dump_int("warmup_sec", cfg.warmup_sec);
    f.dump_unsigned("seed", cfg.seed);
    f.dump_int("top_dirs", cfg.top_dirs);
    f.dump_int("buckets_per_dir", cfg.buckets_per_dir);
    f.dump_int("files_per_bucket", cfg.files_per_bucket);
    f.dump_int("init_file_bytes", cfg.init_file_bytes);
    f.dump_int("init_fill_percent", cfg.init_fill_percent);
    f.dump_int("init_threads", cfg.init_threads);
    f.dump_bool("cap_storm", cfg.cap_storm);
    f.dump_unsigned("dirs", total_dirs);
    f.dump_unsigned("files", total_files);
    f.dump_string("workdir", cfg.workdir);
    f.open_array_section("client_roots");
    for (const auto& idx : by_client) {
      f.open_object_section("client");
      f.dump_int("id", idx.client_id);
      f.dump_string("root", idx.root);
      f.close_section();
    }
    f.close_section();
    f.open_object_section("mix");
    for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
      f.dump_float(op_name((OpType)i), cfg.mix.w[i]);
    }
    f.close_section();
    f.close_section(); // configuration
    f.open_object_section("summary");
    f.dump_float("elapsed_sec", elapsed);
    f.dump_unsigned("ops", total_ops);
    f.dump_float("ops_per_sec", ops_s);
    f.dump_unsigned("errors", total_err);
    f.close_section();
    f.open_object_section("ops");
    for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
      f.open_object_section(op_name((OpType)i));
      f.dump_unsigned("count", per_op[i]);
      f.dump_float("mean_usec", lats[i].mean_usec());
      f.dump_float("p50_usec", lats[i].percentile(0.50));
      f.dump_float("p90_usec", lats[i].percentile(0.90));
      f.dump_float("p99_usec", lats[i].percentile(0.99));
      f.close_section();
    }
    f.close_section();
    f.close_section(); // mdsbench

    if (cfg.json_path == "-") {
      f.flush(cout);
      cout << endl;
    } else {
      std::ofstream ofs(cfg.json_path);
      if (!ofs) {
        cerr << "Failed to write JSON to " << cfg.json_path << endl;
      } else {
        f.flush(ofs);
        cout << "\nJSON written to " << cfg.json_path << endl;
      }
    }
  }

  if (cfg.cleanup && !cfg.keep_tree) {
    cout << "Cleanup: removing per-client trees (best-effort)..." << endl;
    for (const auto& idx : by_client) {
      for (const auto& file : idx.files) {
        (void)ceph_unlink(cmount, file.c_str());
      }
      for (auto it = idx.dirs.rbegin(); it != idx.dirs.rend(); ++it) {
        (void)ceph_rmdir(cmount, it->c_str());
      }
    }
    if (cfg.client_id >= 0) {
      barrier_cleanup(cmount, cfg, cfg.clients);
    }
    (void)ceph_rmdir(cmount, cfg.workdir.c_str());
  } else if (cfg.client_id >= 0 && cfg.client_id == 0) {
    // Drop barrier markers after a successful multi-process run so a retry
    // does not immediately see a stale all-ready set.
    barrier_cleanup(cmount, cfg, cfg.clients);
  }

  ceph_unmount(cmount);
  ceph_shutdown(cmount);
  return 0;
}

int
main(int argc, char** argv)
{
  BenchConfig cfg;
  string mix_override;
  bool no_cleanup = false;

  // clang-format off
  po::options_description desc("cephfs-mdsbench options");
  desc.add_options()
    ("help,h", "Show help")
    ("conf,c", po::value<string>(&cfg.ceph_conf), "Ceph config file")
    ("id,i", po::value<string>(&cfg.userid)->default_value("admin"), "Client id")
    ("keyring,k", po::value<string>(&cfg.keyring), "Keyring path")
    ("filesystem,fs", po::value<string>(&cfg.filesystem), "CephFS name")
    ("root-path", po::value<string>(&cfg.mount_root)->default_value("/"), "Mount root")
    ("uid", po::value<int>(&cfg.uid)->default_value(-1), "UID")
    ("gid", po::value<int>(&cfg.gid)->default_value(-1), "GID")
    ("workdir", po::value<string>(&cfg.workdir)->default_value("/mdsbench"),
     "Work directory in CephFS")
    ("mode", po::value<string>(&cfg.mode)->default_value("balanced"),
     "Op mix: balanced | journal-heavy | lookup-heavy")
    ("mix", po::value<string>(&mix_override),
     "Override mix weights, e.g. stat=35,open_close=25,create=8,...")
    ("threads", po::value<int>(&cfg.threads)->default_value(24),
     "Deprecated alias for --clients (ignored if --client-id is set)")
    ("ops-per-sec", po::value<double>(&cfg.ops_per_sec)->default_value(12000),
     "Target aggregate op rate (0 = unlimited)")
    ("duration", po::value<int>(&cfg.duration_sec)->default_value(60),
     "Measured duration seconds")
    ("warmup", po::value<int>(&cfg.warmup_sec)->default_value(5), "Warmup seconds")
    ("seed", po::value<uint64_t>(&cfg.seed)->default_value(1), "RNG seed")
    ("clients", po::value<int>(&cfg.clients)->default_value(24),
     "Number of client ids / CL{id}_MDSBENCH roots (one thread each)")
    ("client-id", po::value<int>(&cfg.client_id)->default_value(-1),
     "If >= 0, only use CL{id}_MDSBENCH (for multi-process launches)")
    ("scratch-slots", po::value<int>(&cfg.scratch_slots)->default_value(64),
     "Transient name slots per client for create/unlink/mkdir/rmdir")
    ("write-bytes", po::value<int>(&cfg.write_bytes)->default_value(4096),
     "Bytes for write_small ops")
    ("json", po::value<string>(&cfg.json_path), "Write JSON summary (- for stdout)")
    ("no-cleanup", po::bool_switch(&no_cleanup), "Leave workdir tree in place")
    ("keep-tree", po::bool_switch(&cfg.keep_tree), "Alias of --no-cleanup")
    ("progress", po::bool_switch(&cfg.show_progress),
     "Show live progress (% done, IOPS, ETA)")
    ("progress-interval", po::value<int>(&cfg.progress_interval)->default_value(10),
     "Progress update interval in percent (1-100)")
    ("barrier", po::bool_switch(&cfg.barrier),
     "After precreate, wait for ready.0..ready.N-1 under workdir/.mdsbench_barrier")
    ("no-barrier", po::bool_switch(&cfg.no_barrier),
     "Do not auto-barrier when --client-id is set")
    ("barrier-timeout", po::value<int>(&cfg.barrier_timeout_sec)->default_value(0),
     "Barrier wait timeout seconds (0 = forever)")
    ("prep-only", po::bool_switch(&cfg.prep_only),
     "Only precreate (and signal ready if barrier); do not run the benchmark")
    ("skip-precreate", po::bool_switch(&cfg.skip_precreate),
     "Skip tree seeding; use existing CL{id}_MDSBENCH")
    ("init-file-bytes", po::value<int>(&cfg.init_file_bytes)->default_value(16384),
     "Bytes to write-fill during Init (default 16KiB; 0 = create only)")
    ("init-fill-percent", po::value<int>(&cfg.init_fill_percent)->default_value(46),
     "Percent of files to write-fill in Init (~60k/130k with data)")
    ("init-threads", po::value<int>(&cfg.init_threads)->default_value(1),
     "Parallel create/fill workers in this process during Init")
    ("cap-storm", po::bool_switch(&cfg.cap_storm),
     "Reproduce Init CLIENT_CAPS storm: heavy write-fill + --prep-only")
    ;

  // Fixed nested tree; kept as hidden overrides for experiments only.
  po::options_description hidden("Hidden tree overrides");
  hidden.add_options()
    ("top-dirs", po::value<int>(&cfg.top_dirs)->default_value(50),
     "Dir* count under each CL{id}_MDSBENCH")
    ("buckets-per-dir", po::value<int>(&cfg.buckets_per_dir)->default_value(26),
     "bucket* directories under each Dir*")
    ("files-per-bucket", po::value<int>(&cfg.files_per_bucket)->default_value(100),
     "Files created in each bucket directory")
    ;

  po::options_description all_opts;
  all_opts.add(desc).add(hidden);
  // clang-format on

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, all_opts), vm);
    po::notify(vm);
  } catch (const std::exception& e) {
    cerr << "Option error: " << e.what() << endl;
    return 1;
  }

  if (vm.count("help")) {
    // clang-format off
  cout << "Usage: cephfs-mdsbench [options]\n\n"
         << "MDS metadata stress/benchmark via libcephfs.\n"
         << "Targets software-build style metadata pressure for MDS regression.\n\n"
         << "Tree shape is fixed (not tied to --clients / --ops-per-sec):\n"
         << "  CL{id}_MDSBENCH/Dir{0..49}/bucket{0..25}/f{0..99}\n"
         << "  (~1300 dirs / 130k files per client)\n\n"
         << "Init CAPS storm (tree populate create/write-fill/close):\n"
         << "  Storm is during Init, not the measured Run mix.\n"
         << "  Launch many processes together; each seeds only its CL{id}:\n"
         << "    cephfs-mdsbench --cap-storm --clients 24 --client-id $i --progress\n"
         << "  Equivalent knobs: --prep-only --init-file-bytes 16384 \\\n"
         << "    --init-fill-percent 46 --init-threads 8\n\n"
         << "Single process Run mix (threads = --clients):\n"
         << "  cephfs-mdsbench --clients 24 --ops-per-sec 12000 ...\n\n"
         << "Multi-process Run (auto start barrier after Init):\n"
         << "  cephfs-mdsbench --clients 24 --client-id $i --ops-per-sec 500 ...\n\n"
         << desc << endl;
    // clang-format on
    return 0;
  }

  cfg.cleanup = !no_cleanup && !cfg.keep_tree;
  // Prefer --clients; allow legacy --threads to set client count when clients
  // was left at default but threads was changed.
  if (vm["clients"].defaulted() && !vm["threads"].defaulted()) {
    cfg.clients = cfg.threads;
  }
  if (cfg.cap_storm) {
    // Mass create/write-fill/close during Init; measured Run mix is not the
    // CLIENT_CAPS storm source.
    cfg.prep_only = true;
    cfg.keep_tree = true;
    cfg.cleanup = false;
    if (vm["init-file-bytes"].defaulted()) {
      cfg.init_file_bytes = 16384;
    }
    if (vm["init-fill-percent"].defaulted()) {
      cfg.init_fill_percent = 46;
    }
    if (vm["init-threads"].defaulted()) {
      cfg.init_threads = 8;
    }
    if (!cfg.show_progress) {
      cfg.show_progress = true;
    }
  }
  if (cfg.duration_sec < 1) {
    cerr << "duration must be >= 1" << endl;
    return 1;
  }
  if (cfg.init_fill_percent < 0 || cfg.init_fill_percent > 100) {
    cerr << "Error: init-fill-percent must be between 0 and 100\n";
    return 1;
  }
  if (cfg.init_file_bytes < 0) {
    cerr << "Error: init-file-bytes must be >= 0\n";
    return 1;
  }
  if (cfg.init_threads < 1) {
    cerr << "Error: init-threads must be >= 1\n";
    return 1;
  }
  if (cfg.clients < 1 || cfg.top_dirs < 1 || cfg.buckets_per_dir < 1 ||
      cfg.files_per_bucket < 0) {
    cerr << "invalid tree parameters" << endl;
    return 1;
  }
  if (cfg.client_id < -1) {
    cerr << "--client-id must be >= 0 (or -1 for all clients)" << endl;
    return 1;
  }
  if (cfg.progress_interval < 1 || cfg.progress_interval > 100) {
    cerr << "Error: progress-interval must be between 1 and 100\n";
    return 1;
  }
  if (cfg.barrier && cfg.no_barrier) {
    cerr << "Error: --barrier and --no-barrier are mutually exclusive\n";
    return 1;
  }
  if (cfg.prep_only && cfg.skip_precreate && !cfg.barrier &&
      !(cfg.client_id >= 0 && !cfg.no_barrier)) {
    cerr << "Error: --prep-only with --skip-precreate needs a barrier to "
            "signal\n";
    return 1;
  }
  if (!mix_override.empty()) {
    if (parse_mix_override(mix_override, cfg.mix) < 0) {
      cerr << "Invalid --mix (use op=weight,... ; ops: ";
      for (size_t i = 0; i < (size_t)OpType::COUNT; ++i) {
        cerr << op_name((OpType)i)
             << (i + 1 < (size_t)OpType::COUNT ? "," : "");
      }
      cerr << ")" << endl;
      return 1;
    }
    cfg.mode = "custom";
  }

  return run_bench(cfg);
}
