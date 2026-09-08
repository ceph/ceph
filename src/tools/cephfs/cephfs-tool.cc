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
 * cephfs-bench - Standalone tool to interact with cephfs
 * Can be built outside Ceph: g++ --std=c++20 -D_FILE_OFFSET_BITS=64 -O3 -o cephfs-tool cephfs-tool.cc -lcephfs -lpthread -lboost_program_options
 */

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <random>
#include <semaphore>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/JSONFormatter.h"
#include "common/ceph_context.h"
#include "common/cmdparse.h"
#include "common/perf_counters_collection.h"

// Standard Public Ceph API
#include <cephfs/libcephfs.h>
#include <fcntl.h>
#include <sys/statvfs.h>
#include <unistd.h>

// Boost Program Options
#include <boost/program_options.hpp>

#include "cls/journal/cls_journal_types.h"

#ifdef CEPH_LOCKSTAT
#include "common/lockstat.h"
#endif    

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
namespace po = boost::program_options;

// Parse human-readable sizes (e.g. "4MB", "1GiB"). Accepts both SI (1000-based)
// and binary (1024-based) suffixes for use in CLI options and JSON output.
uint64_t parse_size(const string& val) {
  if (val.empty()) {
    return 0;
  }
  size_t end_pos = 0;
  uint64_t num = 0;
  try {
    num = std::stoull(val, &end_pos);
  } catch (...) {
    return 0;
  }

  if (end_pos < val.length()) {
    string suffix_str = val.substr(end_pos);
    for (auto& c : suffix_str)
      c = std::toupper(c);

    // Check for "iB" suffix (e.g. KiB, MiB, GiB, TiB)
    bool is_binary =
        (suffix_str.length() >= 3 && suffix_str.substr(1, 2) == "IB");
    // Check for "B" suffix (e.g. KB, MB, GB, TB)
    bool is_si =
        (suffix_str.length() >= 2 && suffix_str[1] == 'B' && !is_binary);

    char suffix = suffix_str[0];
    uint64_t multiplier = 1;
    if (is_si) {
      switch (suffix) {
      case 'K':
        multiplier = 1000ULL;
        break;
      case 'M':
        multiplier = 1000ULL * 1000;
        break;
      case 'G':
        multiplier = 1000ULL * 1000 * 1000;
        break;
      case 'T':
        multiplier = 1000ULL * 1000 * 1000 * 1000;
        break;
      }
    } else {
      // Binary or single letter (default to binary)
      switch (suffix) {
      case 'K':
        multiplier = 1024ULL;
        break;
      case 'M':
        multiplier = 1024ULL * 1024;
        break;
      case 'G':
        multiplier = 1024ULL * 1024 * 1024;
        break;
      case 'T':
        multiplier = 1024ULL * 1024 * 1024 * 1024;
        break;
      }
    }
    num *= multiplier;
  }
  return num;
}

struct RandomHelper {
  // Thread-local engine to prevent locking contention and re-seeding overhead
  static std::mt19937& get_engine() {
    static thread_local std::mt19937 engine(std::random_device{}());
    return engine;
  }

  static void fill_buffer(std::span<std::byte> buf) {
    auto& gen = get_engine();
    std::uniform_int_distribution<> dis(0, 255);
    for (size_t i = 0; i < buf.size(); ++i) {
      buf[i] = static_cast<std::byte>(dis(gen));
    }
  }

  // Generates a random hex suffix (e.g., for unique directories)
  static std::string generate_hex_suffix() {
    auto& gen = get_engine();
    std::uniform_int_distribution<> dis(0, 0xFFFF);
    std::stringstream ss;
    ss << std::hex << dis(gen);
    return ss.str();
  }
};

// Configuration for the benchmark
struct BenchConfig {
  int num_threads;
  int iterations;
  int num_files;
  uint64_t file_size;
  uint64_t block_size;
  uint64_t fsync_every_bytes;
  bool per_thread_mount;
  string prefix;
  string mount_root;
  bool cleanup;
  string dir_prefix;
  string ceph_conf;
  string userid;
  string keyring;
  string filesystem;
  string subdir;
  int uid;
  int gid;
  string json_path;          // --json: write structured benchmark results to a file
  int duration;              // --duration: cap each read/write phase at N seconds (0 = all files)
  string perf_dump_path;     // --perf-dump: dump libcephfs perf counters after the benchmark
  string client_oc;          // --client-oc: override object cacher enable (0|1)
  string client_oc_size;     // --client-oc-size: override object cacher size limit
  int msgr_workers;          // --msgr-workers: override ms_async_op_threads (0 = ceph.conf default)
  bool show_progress;        // --progress: show live bandwidth, IOPS, and ETA during phases
  int progress_interval;     // --progress-interval: minimum % between progress line updates
  bool async_io;             // --async-io: use ceph_ll_nonblocking_readv_writev instead of sync I/O
  bool async_write_fsync;    // --async-write-fsync: carry fsync with each async write (Ganesha-style)
  bool shared_file;          // --shared-file: use a single file for all threads
  int queue_depth;           // --queue-depth: max outstanding async I/Os per worker thread
#ifdef CEPH_LOCKSTAT
  string lockstat_dump_path; // --lockstat-dump: base path for per-phase lock contention dumps
  uint64_t lockstat_threshold_ns; // --lockstat-threshold: only record lock holds >= N ns (0 = all)
#endif
};

struct ThreadStats {
  // Atomics so progress_reporter can aggregate while workers update concurrently.
  std::atomic<uint64_t> bytes_transferred{0};
  std::atomic<uint64_t> ops{0}; // read/write calls
  std::atomic<uint64_t> files{0}; // files successfully opened/closed
  std::atomic<int> errors{0};
};

// Structure to track async I/O operations
struct AsyncIOContext {
  struct ceph_ll_io_info io_info;
  std::vector<char> buffer;
  struct iovec iov;  // Must persist for lifetime of async operation
  Fh* fh;
  uint64_t offset;
  std::atomic<bool> completed;
  int64_t result;
  std::atomic<bool>* stop_signal_ptr;
  ThreadStats* stats_ptr;
  std::counting_semaphore<>* semaphore_ptr;
  std::atomic<int>* active_callbacks;
};

// Callback for async I/O completion
static void async_io_callback(struct ceph_ll_io_info* cb_info) {
  AsyncIOContext* ctx = static_cast<AsyncIOContext*>(cb_info->priv);
  ctx->result = cb_info->result;
  
  if (cb_info->result > 0) {
    ctx->stats_ptr->bytes_transferred += cb_info->result;
    ctx->stats_ptr->ops++;
  } else if (cb_info->result < 0) {
    ctx->stats_ptr->errors++;
    *(ctx->stop_signal_ptr) = true;
  }

  // Mark the slot free before releasing the semaphore so a waiter always
  // finds completed==true after acquire().
  ctx->completed.store(true, std::memory_order_release);
  if (ctx->semaphore_ptr) {
    ctx->semaphore_ptr->release();
  }
  if (ctx->active_callbacks) {
    ctx->active_callbacks->fetch_sub(1, std::memory_order_release);
  }
}

// --- Setup Helper (Updated to use stream for output) ---
int setup_mount(struct ceph_mount_info **cmount, const BenchConfig& config, std::ostream& out_stream) {
  if (int rc = ceph_create(cmount, config.userid.empty() ? NULL : config.userid.c_str()); rc < 0) {
    out_stream << "Failed to create ceph instance: " << strerror(-rc) << endl;
    return rc;
  }

  auto cleanup_on_fail = [&](int rc) {
    ceph_shutdown(*cmount);
    *cmount = nullptr;
    return rc;
  };

  // 1. Read Config File (sets defaults)
  if (!config.ceph_conf.empty()) {
    if (int rc = ceph_conf_read_file(*cmount, config.ceph_conf.c_str()); rc < 0) {
      out_stream << "Failed to read ceph config file '" << config.ceph_conf << "': " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  } else {
    if (int rc = ceph_conf_read_file(*cmount, NULL); rc < 0) { // Search default locations
      out_stream << "Failed to read default ceph config: " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  }

  // 1b. Apply client_oc override (must precede ceph_init to affect ObjectCacher)
  if (!config.client_oc.empty()) {
    if (int rc = ceph_conf_set(*cmount, "client_oc", config.client_oc.c_str()); rc < 0) {
      out_stream << "Failed to set client_oc option: " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  }

  // 1c. Apply ms_async_op_threads override (messenger I/O threads, not bench workers)
  if (config.msgr_workers > 0) {
    string workers_str = std::to_string(config.msgr_workers);
    if (int rc = ceph_conf_set(*cmount, "ms_async_op_threads", workers_str.c_str()); rc < 0) {
      out_stream << "Failed to set ms_async_op_threads option: " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  }

  // 1d. Apply client_oc_size override
  if (!config.client_oc_size.empty()) {
    uint64_t oc_size = parse_size(config.client_oc_size);
    string oc_size_str = std::to_string(oc_size);
    if (int rc = ceph_conf_set(*cmount, "client_oc_size", oc_size_str.c_str());
        rc < 0) {
      out_stream << "Failed to set client_oc_size option: " << strerror(-rc)
                 << endl;
      return cleanup_on_fail(rc);
    }
  }

  // 2. Apply Keyring Override (if provided)
  if (!config.keyring.empty()) {
    if (int rc = ceph_conf_set(*cmount, "keyring", config.keyring.c_str()); rc < 0) {
      out_stream << "Failed to set keyring option: " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  }

  // 3. Apply Filesystem Selection (if provided)
  if (!config.filesystem.empty()) {
    if (int rc = ceph_conf_set(*cmount, "client_mds_namespace", config.filesystem.c_str()); rc < 0) {
      out_stream << "Failed to set filesystem (client_mds_namespace): " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  }

  if (int rc = ceph_init(*cmount); rc < 0) {
    out_stream << "Failed to initialize ceph client: " << strerror(-rc) << endl;
    return cleanup_on_fail(rc);
  }

  // 4. Apply UID/GID Permissions (if provided)
  if (config.uid != -1 || config.gid != -1) {
    UserPerm *perms = ceph_userperm_new(config.uid, config.gid, 0, NULL);
    if (!perms) {
      out_stream << "Failed to allocate user permissions struct." << endl;
      return cleanup_on_fail(-ENOMEM);
    }

    int rc = ceph_mount_perms_set(*cmount, perms);
    ceph_userperm_destroy(perms); // Cleanup perms object after setting

    if (rc != 0) {
      out_stream << "Failed to set mount permissions (uid=" << config.uid
                 << ", gid=" << config.gid << "): " << strerror(-rc) << endl;
      return cleanup_on_fail(rc);
    }
  }

  // 5. Mount
  if (int rc = ceph_mount(*cmount, config.mount_root.c_str()); rc < 0) {
    out_stream << "Failed to mount at '" << config.mount_root << "': " << strerror(-rc) << endl;
    return cleanup_on_fail(rc);
  }

  return 0;
}

// Background reporter for --progress: aggregates per-thread stats and prints a
// single updating line with bandwidth, IOPS, and ETA. Progress is measured by
// elapsed time when --duration is set, otherwise by files completed.
void
progress_reporter(
    const string& phase,
    const vector<ThreadStats>& stats,
    const BenchConfig& config,
    std::atomic<bool>& stop_signal,
    steady_clock::time_point start_time)
{
  if (!config.show_progress)
    return;

  char type_char = (phase == "write" ? 'w' : 'r');
  uint64_t total_files_to_do = config.num_files;
  int last_interval = -1;

  while (!stop_signal) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    uint64_t total_bytes = 0;
    uint64_t total_ops = 0;
    uint64_t total_files_done = 0;

    for (const auto& s : stats) {
      total_bytes += s.bytes_transferred.load();
      total_ops += s.ops.load();
      total_files_done += s.files.load();
    }

    auto now = steady_clock::now();
    double elapsed_sec = duration_cast<milliseconds>(now - start_time).count() /
                         1000.0;
    if (elapsed_sec <= 0)
      continue;

    double progress_pct = 0;
    double eta_sec = 0;

    if (config.duration > 0) {
      progress_pct = (elapsed_sec / config.duration) * 100.0;
      if (progress_pct > 0) {
        eta_sec = (config.duration - elapsed_sec);
      }
    } else if (total_files_to_do > 0) {
      progress_pct = ((double)total_files_done / total_files_to_do) * 100.0;
      if (total_files_done > 0) {
        double files_per_sec = total_files_done / elapsed_sec;
        eta_sec = (total_files_to_do - total_files_done) / files_per_sec;
      }
    }

    if (progress_pct > 100.0)
      progress_pct = 100.0;
    if (eta_sec < 0)
      eta_sec = 0;

    int current_interval = (int)(progress_pct / config.progress_interval);
    if (current_interval > last_interval) {
      last_interval = current_interval;

      double mbps = (double)total_bytes / 1024.0 / 1024.0 / elapsed_sec;
      double iops = (double)total_ops / elapsed_sec;

      int eta_m = (int)eta_sec / 60;
      int eta_s = (int)eta_sec % 60;

      std::cout << "\r[" << std::fixed << std::setprecision(0) << progress_pct
                << "%]"
                << "[" << type_char << "=" << std::setprecision(1) << mbps
                << "MiB/s]"
                << "[" << type_char << "=" << (uint64_t)iops << " IOPS]"
                << "[eta " << std::setfill('0') << std::setw(2) << eta_m
                << "m:" << std::setw(2) << eta_s << "s]" << std::flush;
    }
  }
  // Clear the progress line
  std::cout << "\r" << std::string(80, ' ') << "\r" << std::flush;
}

// Worker function for Write phase
void
bench_write_worker(
    int thread_id,
    int files_to_write,
    BenchConfig config,
    struct ceph_mount_info* shared_cmount,
    ThreadStats& stats,
    std::atomic<bool>& stop_signal,
    std::stringstream& ss,
    steady_clock::time_point phase_start_time)
{

  struct ceph_mount_info *cmount = shared_cmount;
  // Name threads for top(1), perf, and lockstat attribution.
  ceph_pthread_setname(("wr-worker-" + std::to_string(thread_id)).c_str());
  auto duration_limit = std::chrono::seconds(config.duration);

  if (config.per_thread_mount) {
    if (int rc = setup_mount(&cmount, config, ss); rc < 0) {
      ss << "Thread " << thread_id << " mount failed: " << strerror(-rc) << std::endl;
      stats.errors++;
      stop_signal = true; // Signal other threads to stop
      return;
    }
  }

  auto buffer = std::vector<char>(config.block_size);
  RandomHelper::fill_buffer(std::as_writable_bytes(std::span(buffer)));

  for (int i = 0;; ++i) {
    if (stop_signal) {
      break; // Check if we should stop
    }

    // Check duration limit first if specified
    if (config.duration > 0) {
      if ((steady_clock::now() - phase_start_time) >= duration_limit) {
        break; // Duration limit reached
      }
    } else if (i >= files_to_write) {
      // If no duration, stop when we've written all target files
      break;
    }

    // In --duration mode, cycle through the per-thread file set until time expires.
    int file_idx = i % files_to_write;
    string fname = config.subdir + "/" + config.prefix +
                   std::to_string(thread_id) + "_" + std::to_string(file_idx);

    // O_CREAT ensures we measure creation overhead
    int fd = ceph_open(cmount, fname.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
      ss << "Thread " << thread_id << " open failed " << fname << ": " << strerror(-fd) << std::endl;
      stats.errors++;
      stop_signal = true;
      break;
    }

    uint64_t written = 0;
    uint64_t last_sync = 0;
    bool write_error = false;

    while (written < config.file_size) {
      if (stop_signal) {
        break;
      }

      if (config.duration > 0 &&
          (steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }

      uint64_t to_write = std::min(config.block_size, config.file_size - written);
      if (int rc = ceph_write(cmount, fd, buffer.data(), to_write, -1); rc < 0) {
        ss << "Thread " << thread_id << " write error: " << strerror(-rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        write_error = true;
        break;
      } else {
        written += rc;
        stats.bytes_transferred += rc;
        stats.ops++;
      }

      if (config.fsync_every_bytes > 0 && (written - last_sync) >= config.fsync_every_bytes) {
        if (int rc = ceph_fsync(cmount, fd, 0); rc < 0) {
          ss << "Thread " << thread_id << " fsync error: " << strerror(-rc) << std::endl;
          stats.errors++;
          stop_signal = true;
          write_error = true;
          break;
        }
        last_sync = written;
      }
    }

    if (!write_error && !stop_signal) {
      if (int rc = ceph_close(cmount, fd); rc < 0) {
        ss << "Thread " << thread_id << " close error " << fname << ": " << strerror(-rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        break;
      }
      // Count each unique file once even when duration mode reuses filenames.
      if (stats.files < (uint64_t)files_to_write) {
        stats.files++;
      }
    } else {
      // Attempt close on error, ignore result
      ceph_close(cmount, fd);
      break;
    }
  }

  if (config.per_thread_mount) {
    if (int rc = ceph_unmount(cmount); rc < 0) {
      ss << "Thread " << thread_id << " unmount failed: " << strerror(-rc) << std::endl;
      // Not critical enough to stop others, but log it
    }
    ceph_shutdown(cmount);
  }
}

// Async write worker: submits I/O via ceph_ll_nonblocking_readv_writev with up to
// queue_depth outstanding operations per thread, bounded by a counting semaphore.
void
bench_async_write_worker(
    int thread_id,
    int files_to_write,
    BenchConfig config,
    struct ceph_mount_info* cmount,
    ThreadStats& stats,
    std::atomic<bool>& stop_signal,
    std::stringstream& ss,
    steady_clock::time_point phase_start_time)
{
  ceph_pthread_setname(("wr-worker-" + std::to_string(thread_id)).c_str());
  auto duration_limit = std::chrono::seconds(config.duration);

  // Semaphore for queue depth management
  std::counting_semaphore<> queue_semaphore(config.queue_depth);
  std::atomic<int> active_callbacks{0};

  // Allocate queue of async contexts
  std::vector<std::unique_ptr<AsyncIOContext>> io_queue;
  for (int i = 0; i < config.queue_depth; ++i) {
    auto ctx = std::make_unique<AsyncIOContext>();
    ctx->buffer.resize(config.block_size);
    RandomHelper::fill_buffer(std::as_writable_bytes(std::span(ctx->buffer)));
    ctx->io_info.callback = async_io_callback;
    ctx->io_info.priv = ctx.get();
    ctx->io_info.write = true;
    ctx->io_info.fsync = config.async_write_fsync;
    ctx->io_info.syncdataonly = false;
    ctx->completed = true;
    ctx->stop_signal_ptr = &stop_signal;
    ctx->stats_ptr = &stats;
    ctx->semaphore_ptr = &queue_semaphore;
    ctx->active_callbacks = &active_callbacks;
    io_queue.push_back(std::move(ctx));
  }

  for (int file_iter = 0;; ++file_iter) {
    if (stop_signal) {
      break;
    }

    if (config.duration > 0) {
      if ((steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }
    } else if (file_iter >= files_to_write) {
      break;
    }

    int file_idx = config.shared_file ? 0 : (file_iter % files_to_write);
    string fname = config.subdir + "/" + config.prefix;
    if (config.shared_file) {
      fname += "shared";
    } else {
      fname += std::to_string(thread_id) + "_" + std::to_string(file_idx);
    }

    // Get just the filename without path
    size_t last_slash = fname.find_last_of('/');
    string filename = (last_slash != string::npos) ? fname.substr(last_slash + 1) : fname;
    
    // Get parent directory inode
    Inode* parent_inode = nullptr;
    struct ceph_statx parent_stx;
    UserPerm* perms = ceph_userperm_new(config.uid, config.gid, 0, nullptr);
    
    int rc = ceph_ll_walk(cmount, config.subdir.c_str(), &parent_inode, &parent_stx,
                          CEPH_STATX_INO, 0, perms);
    
    if (rc < 0) {
      ss << "Thread " << thread_id << " walk parent dir failed " << config.subdir << ": " << strerror(-rc) << std::endl;
      stats.errors++;
      stop_signal = true;
      ceph_userperm_destroy(perms);
      break;
    }

    // Create and open file using low-level API
    Inode* inode = nullptr;
    Fh* fh = nullptr;
    struct ceph_statx stx;
    
    rc = ceph_ll_create(cmount, parent_inode, filename.c_str(), 0644,
                        O_WRONLY | O_CREAT | O_TRUNC, &inode, &fh, &stx,
                        CEPH_STATX_INO, 0, perms);
    ceph_userperm_destroy(perms);
    
    if (rc < 0) {
      ss << "Thread " << thread_id << " ll_create failed " << fname << ": " << strerror(-rc) << std::endl;
      stats.errors++;
      stop_signal = true;
      if (parent_inode) ceph_ll_forget(cmount, parent_inode, 1);
      break;
    }
    
    // Release parent inode reference
    if (parent_inode) {
      ceph_ll_forget(cmount, parent_inode, 1);
    }

    uint64_t written = 0;
    bool write_error = false;

    while (written < config.file_size) {
      if (stop_signal) {
        break;
      }

      if (config.duration > 0 &&
          (steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }

      // Wait for available slot in queue (blocks efficiently)
      queue_semaphore.acquire();

      if (stop_signal) {
        queue_semaphore.release();
        break;
      }

      // Find available slot in queue
      AsyncIOContext* available_ctx = nullptr;
      for (auto& ctx : io_queue) {
        if (ctx->completed.load(std::memory_order_acquire)) {
          available_ctx = ctx.get();
          break;
        }
      }

      if (!available_ctx) {
        // Should not happen if semaphore logic is correct
        ss << "Thread " << thread_id << " internal error: no available context after semaphore acquire" << std::endl;
        queue_semaphore.release();
        break;
      }

      // Setup I/O operation
      uint64_t to_write = std::min(config.block_size, config.file_size - written);
      available_ctx->fh = fh;
      available_ctx->offset = written;
      available_ctx->completed.store(false, std::memory_order_release);
      available_ctx->result = 0;
      
      // Setup iovec in context (must persist for async operation)
      available_ctx->iov.iov_base = available_ctx->buffer.data();
      available_ctx->iov.iov_len = to_write;
      
      available_ctx->io_info.fh = fh;
      available_ctx->io_info.iov = &available_ctx->iov;
      available_ctx->io_info.iovcnt = 1;
      available_ctx->io_info.off = written;
      available_ctx->io_info.result = 0;

      // Submit async I/O
      active_callbacks.fetch_add(1, std::memory_order_relaxed);
      int64_t submit_rc = ceph_ll_nonblocking_readv_writev(cmount, &available_ctx->io_info);
      if (submit_rc < 0) {
        active_callbacks.fetch_sub(1, std::memory_order_relaxed);
        ss << "Thread " << thread_id << " async write submit error: " << strerror(-submit_rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        write_error = true;
        available_ctx->completed.store(true, std::memory_order_release);
        queue_semaphore.release();
        break;
      }

      written += to_write;
    }

    // Wait for all outstanding I/Os to complete for this file
    for (auto& ctx : io_queue) {
      while (!ctx->completed.load(std::memory_order_acquire) &&
             ctx->fh == fh && !stop_signal) {
        std::this_thread::yield();
      }
    }

    // Flush buffered data before close so files survive the post-write remount.
    // Skip when each async write already carried fsync (Ganesha-style path).
    if (!write_error && !stop_signal && !config.async_write_fsync) {
      if (int fsync_rc = ceph_ll_fsync(cmount, fh, 0); fsync_rc < 0) {
        ss << "Thread " << thread_id << " fsync error " << fname << ": "
           << strerror(-fsync_rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        write_error = true;
      }
    }

    if (!write_error && !stop_signal) {
      if (int close_rc = ceph_ll_close(cmount, fh); close_rc < 0) {
        ss << "Thread " << thread_id << " close error " << fname << ": " << strerror(-close_rc) << std::endl;
        stats.errors++;
        stop_signal = true;
      } else {
        if (stats.files < (uint64_t)files_to_write) {
          stats.files++;
        }
      }
    } else {
      ceph_ll_close(cmount, fh);
    }

    if (inode) {
      ceph_ll_forget(cmount, inode, 1);
    }

    if (write_error) {
      break;
    }
  }

  // Wait for ALL outstanding I/Os to complete before exiting thread.
  // completed may be set before the finisher callback returns.
  for (auto& ctx : io_queue) {
    while (!ctx->completed.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  while (active_callbacks.load(std::memory_order_acquire) > 0) {
    std::this_thread::yield();
  }
}

// Worker function for Read phase
void
bench_read_worker(
    int thread_id,
    int files_to_read,
    BenchConfig config,
    struct ceph_mount_info* shared_cmount,
    ThreadStats& stats,
    std::atomic<bool>& stop_signal,
    std::stringstream& ss,
    steady_clock::time_point phase_start_time)
{

  struct ceph_mount_info *cmount = shared_cmount;
  ceph_pthread_setname(("rd-worker-" + std::to_string(thread_id)).c_str());
  auto duration_limit = std::chrono::seconds(config.duration);

  if (config.per_thread_mount) {
    if (int rc = setup_mount(&cmount, config, ss); rc < 0) {
      stats.errors++;
      stop_signal = true;
      return;
    }
  }

  std::vector<char> buffer(config.block_size);

  for (int i = 0;; ++i) {
    if (stop_signal) {
      break;
    }

    if (config.duration > 0) {
      if ((steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }
    } else if (i >= files_to_read) {
      break;
    }

    // In --duration mode, cycle through the per-thread file set until time expires.
    int file_idx = i % files_to_read;
    string fname = config.subdir + "/" + config.prefix +
                   std::to_string(thread_id) + "_" + std::to_string(file_idx);

    int fd = ceph_open(cmount, fname.c_str(), O_RDONLY, 0);
    if (fd < 0) {
      ss << "Thread " << thread_id << " open failed " << fname << ": " << strerror(-fd) << std::endl;
      stats.errors++;
      stop_signal = true;
      break;
    }

    uint64_t total_read = 0;
    bool read_error = false;
    while (total_read < config.file_size) {
      if (stop_signal) {
        break;
      }

      if (config.duration > 0 &&
          (steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }

      int rc = ceph_read(cmount, fd, buffer.data(), config.block_size, -1);
      if (rc < 0) {
        ss << "Thread " << thread_id << " read error: " << strerror(-rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        read_error = true;
        break;
      }
      if (rc == 0) {
        break; // EOF
      }

      total_read += rc;
      stats.bytes_transferred += rc;
      stats.ops++;
    }

    if (!read_error && !stop_signal) {
      if (int rc = ceph_close(cmount, fd); rc < 0) {
        ss << "Thread " << thread_id << " close error " << fname << ": "
           << strerror(-rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        break;
      }
      if (stats.files < (uint64_t)files_to_read) {
        stats.files++;
      }
    } else {
      ceph_close(cmount, fd);
      break;
    }
  }

  if (config.per_thread_mount) {
    if (int rc = ceph_unmount(cmount); rc < 0) {
      ss << "Thread " << thread_id << " unmount failed: " << strerror(-rc) << std::endl;
    }
    ceph_shutdown(cmount);
  }
}

// Async read worker: same queue-depth model as bench_async_write_worker. Inodes are
// retained until all async ops (including readahead) complete to avoid use-after-free.
void
bench_async_read_worker(
    int thread_id,
    int files_to_read,
    BenchConfig config,
    struct ceph_mount_info* cmount,
    ThreadStats& stats,
    std::atomic<bool>& stop_signal,
    std::stringstream& ss,
    steady_clock::time_point phase_start_time)
{
  ceph_pthread_setname(("rd-worker-" + std::to_string(thread_id)).c_str());
  auto duration_limit = std::chrono::seconds(config.duration);

  // Semaphore for queue depth management
  std::counting_semaphore<> queue_semaphore(config.queue_depth);
  std::atomic<int> active_callbacks{0};

  // Allocate queue of async contexts
  std::vector<std::unique_ptr<AsyncIOContext>> io_queue;
  for (int i = 0; i < config.queue_depth; ++i) {
    auto ctx = std::make_unique<AsyncIOContext>();
    ctx->buffer.resize(config.block_size);
    ctx->io_info.callback = async_io_callback;
    ctx->io_info.priv = ctx.get();
    ctx->io_info.write = false;
    ctx->io_info.fsync = false;
    ctx->completed = true;
    ctx->stop_signal_ptr = &stop_signal;
    ctx->stats_ptr = &stats;
    ctx->semaphore_ptr = &queue_semaphore;
    ctx->active_callbacks = &active_callbacks;
    io_queue.push_back(std::move(ctx));
  }

  // Store file inodes to release at the end - keeps them alive during readahead
  std::vector<Inode*> inodes_to_release;

  for (int file_iter = 0;; ++file_iter) {
    if (stop_signal) {
      break;
    }

    if (config.duration > 0) {
      if ((steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }
    } else if (file_iter >= files_to_read) {
      break;
    }

    int file_idx = file_iter % files_to_read;
    string fname = config.subdir + "/" + config.prefix +
                   std::to_string(thread_id) + "_" + std::to_string(file_idx);

    // Walk the full path (same resolution model as sync ceph_open) rather than
    // parent walk + ll_lookup, which can miss entries after remount.
    Inode* inode = nullptr;
    struct ceph_statx stx;
    UserPerm* perms = ceph_userperm_new(config.uid, config.gid, 0, nullptr);

    int rc = ceph_ll_walk(cmount, fname.c_str(), &inode, &stx,
                          CEPH_STATX_INO, 0, perms);

    if (rc < 0) {
      ss << "Thread " << thread_id << " walk failed " << fname << ": "
         << strerror(-rc) << std::endl;
      stats.errors++;
      stop_signal = true;
      ceph_userperm_destroy(perms);
      break;
    }

    // Open file for reading
    Fh* fh = nullptr;
    rc = ceph_ll_open(cmount, inode, O_RDONLY, &fh, perms);
    ceph_userperm_destroy(perms);
    
    if (rc < 0) {
      ss << "Thread " << thread_id << " ll_open failed " << fname << ": " << strerror(-rc) << std::endl;
      stats.errors++;
      stop_signal = true;
      if (inode) ceph_ll_forget(cmount, inode, 1);
      break;
    }

    uint64_t total_read = 0;
    bool read_error = false;

    while (total_read < config.file_size) {
      if (stop_signal) {
        break;
      }

      if (config.duration > 0 &&
          (steady_clock::now() - phase_start_time) >= duration_limit) {
        break;
      }

      // Wait for available slot in queue (blocks efficiently)
      queue_semaphore.acquire();

      if (stop_signal) {
        queue_semaphore.release();
        break;
      }

      // Find available slot in queue
      AsyncIOContext* available_ctx = nullptr;
      for (auto& ctx : io_queue) {
        if (ctx->completed.load(std::memory_order_acquire)) {
          available_ctx = ctx.get();
          break;
        }
      }

      if (!available_ctx) {
        // Should not happen if semaphore logic is correct
        ss << "Thread " << thread_id << " internal error: no available context after semaphore acquire" << std::endl;
        queue_semaphore.release();
        break;
      }

      // Setup I/O operation
      uint64_t to_read = std::min(config.block_size, config.file_size - total_read);
      available_ctx->fh = fh;
      available_ctx->offset = total_read;
      available_ctx->completed.store(false, std::memory_order_release);
      available_ctx->result = 0;
      
      // Setup iovec in context (must persist for async operation)
      available_ctx->iov.iov_base = available_ctx->buffer.data();
      available_ctx->iov.iov_len = to_read;
      
      available_ctx->io_info.fh = fh;
      available_ctx->io_info.iov = &available_ctx->iov;
      available_ctx->io_info.iovcnt = 1;
      available_ctx->io_info.off = total_read;
      available_ctx->io_info.result = 0;

      // Submit async I/O
      active_callbacks.fetch_add(1, std::memory_order_relaxed);
      int64_t submit_rc = ceph_ll_nonblocking_readv_writev(cmount, &available_ctx->io_info);
      if (submit_rc < 0) {
        active_callbacks.fetch_sub(1, std::memory_order_relaxed);
        ss << "Thread " << thread_id << " async read submit error: " << strerror(-submit_rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        read_error = true;
        available_ctx->completed.store(true, std::memory_order_release);
        queue_semaphore.release();
        break;
      }

      total_read += to_read;
    }

    // Wait for all outstanding I/Os to complete for this file
    for (auto& ctx : io_queue) {
      while (!ctx->completed.load(std::memory_order_acquire) &&
             ctx->fh == fh && !stop_signal) {
        std::this_thread::yield();
      }
    }

    // Flush all pending operations (including readahead) before closing
    // This ensures no callbacks will try to access the inode after we release it
    if (!read_error && !stop_signal) {
      if (int fsync_rc = ceph_ll_fsync(cmount, fh, 0); fsync_rc < 0) {
        ss << "Thread " << thread_id << " fsync error " << fname << ": " << strerror(-fsync_rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        read_error = true;
      }
    }

    if (!read_error && !stop_signal) {
      if (int close_rc = ceph_ll_close(cmount, fh); close_rc < 0) {
        ss << "Thread " << thread_id << " close error " << fname << ": " << strerror(-close_rc) << std::endl;
        stats.errors++;
        stop_signal = true;
      } else {
        if (stats.files < (uint64_t)files_to_read) {
          stats.files++;
        }
      }
    } else {
      ceph_ll_close(cmount, fh);
    }

    // Store inode for later cleanup - don't forget it yet as readahead may still be active
    // We'll release all inodes at the end of the thread after all I/O completes
    inodes_to_release.push_back(inode);
    inode = nullptr;  // Don't release in this iteration

    if (inode) {
      ceph_ll_forget(cmount, inode, 1);
    }

    if (read_error) {
      break;
    }
  }

  // Wait for ALL outstanding I/Os to complete before exiting thread.
  // completed may be set before the finisher callback returns.
  for (auto& ctx : io_queue) {
    while (!ctx->completed.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  while (active_callbacks.load(std::memory_order_acquire) > 0) {
    std::this_thread::yield();
  }

  // Now it's safe to release all inode references
  // All async operations (including readahead) have completed
  for (auto* inode_ptr : inodes_to_release) {
    if (inode_ptr) {
      ceph_ll_forget(cmount, inode_ptr, 1);
    }
  }
}

// Dump libcephfs client perf counters (JSON) via ceph_get_perf_counters. Called
// once at benchmark end while the mount is still active (--perf-dump).
void
execute_perf_dump(struct ceph_mount_info* cmount, const string& output_path)
{
  if (output_path.empty()) {
    return;
  }

  char* perf_dump = nullptr;
  int rc = ceph_get_perf_counters(cmount, &perf_dump);
  if (rc < 0) {
    cerr << "Error: Failed to get performance counters: " << strerror(-rc)
         << endl;
    return;
  }

  if (perf_dump) {
    std::ofstream ofs(output_path);
    if (!ofs.is_open()) {
      cerr << "Error: Could not open " << output_path
           << " for writing performance counters." << endl;
      free(perf_dump);
      return;
    }
    ofs << perf_dump;
    free(perf_dump);
    cout << "Performance counters dumped to " << output_path << endl;
  }
}

#ifdef CEPH_LOCKSTAT
// Write accumulated lock contention data to a per-iteration, per-phase file
// (e.g. base_iter1_write.json) when --lockstat-dump is enabled.
void dump_lockstat_data(const string& base_path, int iteration, const string& phase) {
  if (base_path.empty()) {
    return;
  }

  // Create filename with iteration and phase: base_iter1_write.json
  string output_path = base_path;
  size_t dot_pos = output_path.find_last_of('.');
  string extension = "";
  if (dot_pos != string::npos) {
    extension = output_path.substr(dot_pos);
    output_path = output_path.substr(0, dot_pos);
  }
  output_path += "_iter" + std::to_string(iteration) + "_" + phase + extension;

  std::ofstream ofs(output_path);
  if (!ofs.is_open()) {
    cerr << "Error: Could not open " << output_path << " for writing lockstat data." << endl;
    return;
  }

  ceph::JSONFormatter formatter(true);
  ceph::lockstat_detail::LockStatEntry::dump_formatted(&formatter);
  formatter.flush(ofs);
  ofs.close();

  cout << "Lockstat data dumped to " << output_path << endl;
}
#endif

// Worker function for Cleanup (Unlink) phase
void bench_cleanup_worker(int thread_id,
                          int files_to_clean,
                          BenchConfig config,
                          struct ceph_mount_info *shared_cmount,
                          std::atomic<bool>& stop_signal,
                          std::stringstream& ss) {

  struct ceph_mount_info *cmount = shared_cmount;

  if (config.per_thread_mount) {
    if (int rc = setup_mount(&cmount, config, ss); rc < 0) {
      ss << "Thread " << thread_id << " cleanup mount failed: " << strerror(-rc) << std::endl;
      stop_signal = true;
      return;
    }
  }

  for (int i = 0; i < files_to_clean; ++i) {
    // If stop signal is raised (e.g. fatal error elsewhere), stop processing
    if (stop_signal) break;

    string fname = config.subdir + "/" + config.prefix + std::to_string(thread_id) + "_" + std::to_string(i);

    if (int rc = ceph_unlink(cmount, fname.c_str()); rc < 0) {
      // Ignore ENOENT (file already gone), but report others
      if (rc != -ENOENT) {
        ss << "Thread " << thread_id << " unlink error " << fname << ": " << strerror(-rc) << std::endl;
        // don't stop cleanup
      }
    }
  }

  if (config.per_thread_mount) {
    if (int rc = ceph_unmount(cmount); rc < 0) {
      ss << "Thread " << thread_id << " unmount failed: " << strerror(-rc) << std::endl;
    }
    ceph_shutdown(cmount);
  }
}

void
print_statistics(
    const string& type,
    const vector<double>& rates,
    const string& unit,
    ceph::Formatter* f = nullptr)
{
  if (rates.empty()) {
    return;
  }
  double sum = std::accumulate(rates.begin(), rates.end(), 0.0);
  double mean = sum / rates.size();
  double sq_sum = std::inner_product(rates.begin(), rates.end(), rates.begin(), 0.0);
  double stdev = std::sqrt(sq_sum / rates.size() - mean * mean);
  double min_val = *std::min_element(rates.begin(), rates.end());
  double max_val = *std::max_element(rates.begin(), rates.end());

  cout << "\n" << type << " Statistics (" << rates.size() << " runs):" << std::endl;
  cout << "  Mean:    " << mean << " " << unit << std::endl;
  cout << "  Std Dev: " << stdev << " " << unit << std::endl;
  cout << "  Min:     " << min_val << " " << unit << std::endl;
  cout << "  Max:     " << max_val << " " << unit << std::endl;

  // Mirror summary stats into the --json output when a formatter is provided.
  if (f) {
    f->open_object_section(type);
    f->dump_int("runs", rates.size());
    f->dump_float("mean", mean);
    f->dump_float("stddev", stdev);
    f->dump_float("min", min_val);
    f->dump_float("max", max_val);
    f->dump_string("unit", unit);
    f->close_section();
  }
}

static void dump_statvfs_json(ceph::Formatter* f, const struct statvfs& st)
{
  f->dump_unsigned("block_size", st.f_bsize);
  f->dump_unsigned("total_blocks", st.f_blocks);
  f->dump_unsigned("free_blocks", st.f_bfree);
  f->dump_unsigned("files", st.f_files);
}

static int fetch_statvfs(struct ceph_mount_info* cmount,
                         const BenchConfig& config,
                         struct statvfs* stbuf)
{
  if (config.async_io) {
    Inode* root = nullptr;
    int rc = ceph_ll_lookup_root(cmount, &root);
    if (rc < 0) {
      return rc;
    }
    return ceph_ll_statfs(cmount, root, stbuf);
  }
  return ceph_statfs(cmount, config.mount_root.c_str(), stbuf);
}

static void report_fs_stats(const char* phase,
                            struct ceph_mount_info* cmount,
                            const BenchConfig& config,
                            ceph::Formatter* json_formatter)
{
  struct statvfs stbuf = {};
  int rc = fetch_statvfs(cmount, config, &stbuf);
  if (rc < 0) {
    cerr << "Failed to get filesystem stats (" << phase << "): "
         << strerror(-rc) << std::endl;
    return;
  }

  cout << "\nFilesystem statistics (" << phase << " iterations):" << std::endl;
  cout << "  Total blocks:      " << stbuf.f_blocks << std::endl;
  cout << "  Free blocks:       " << stbuf.f_bfree << std::endl;
  cout << "  Files:             " << stbuf.f_files << std::endl;

  if (json_formatter) {
    json_formatter->open_object_section(phase);
    dump_statvfs_json(json_formatter, stbuf);
    json_formatter->close_section();
  }
}

// Helper to check for errors and print them
bool check_and_report_errors(const std::atomic<bool>& stop_signal,
                             const std::vector<std::stringstream>& outputs) {
  bool has_output = false;
  for (const auto& ss : outputs) {
      if (ss.rdbuf()->in_avail() > 0) has_output = true;
  }

  if (stop_signal || has_output) {
    if (stop_signal) {
        cerr << "\n*** ERRORS ENCOUNTERED ***" << endl;
    } else {
        cerr << "\n*** WARNINGS/LOGS ***" << endl;
    }
    for (const auto& ss : outputs) {
      string msg = ss.str();
      if (!msg.empty()) {
        cerr << msg;
      }
    }
    return stop_signal; // Return true if it was a critical stop
  }
  return false;
}

int do_bench(BenchConfig& config) {
  if (config.block_size > std::numeric_limits<int>::max()) {
    cerr << "Error: block-size cannot exceed 2GB due to API limitations." << endl;
    return 1;
  }

#ifdef CEPH_LOCKSTAT
  // Start lockstat collection if enabled
  if (!config.lockstat_dump_path.empty()) {
    auto threshold_ns = std::chrono::nanoseconds(config.lockstat_threshold_ns);
    auto threshold = std::chrono::duration_cast<ceph::lockstat_detail::lockstat_clock::duration>(threshold_ns);
    int rc = ceph::lockstat_detail::LockStatEntry::start(threshold);
    if (rc != 0) {
      cerr << "Warning: Failed to start lockstat collection: " << strerror(rc) << endl;
    } else {
      cout << "Lockstat collection started with threshold " << config.lockstat_threshold_ns << " ns" << endl;
    }
  }
#endif

  // Create Main Mount
  struct ceph_mount_info *shared_cmount = NULL;
  if (int rc = setup_mount(&shared_cmount, config, cerr); rc < 0) {
    cerr << "Failed to create/mount global handle. (Is ceph.conf valid?): " << strerror(-rc) << std::endl;
    return 1;
  }

  // Setup bench directory
  config.subdir = config.dir_prefix + RandomHelper::generate_hex_suffix();

  // Build the structured result document when --json is set; flushed at the end.
  std::unique_ptr<ceph::JSONFormatter> json_formatter;
  if (!config.json_path.empty()) {
    json_formatter = std::make_unique<ceph::JSONFormatter>(true);
    json_formatter->open_object_section("benchmark");
    json_formatter->open_object_section("configuration");
    json_formatter->dump_int("threads", config.num_threads);
    json_formatter->dump_int("iterations", config.iterations);
    json_formatter->dump_int("files", config.num_files);
    json_formatter->dump_unsigned("file_size", config.file_size);
    json_formatter->dump_unsigned("block_size", config.block_size);
    json_formatter->dump_string(
        "filesystem",
        config.filesystem.empty() ? "(default)" : config.filesystem);
    json_formatter->dump_string("root", config.mount_root);
    json_formatter->dump_string("subdirectory", config.subdir);
    json_formatter->dump_int("uid", config.uid);
    json_formatter->dump_int("gid", config.gid);
    if (!config.client_oc.empty()) {
      char buf[128];
      int rc = ceph_conf_get(shared_cmount, "client_oc", buf, sizeof(buf));
      if (rc >= 0) {
        json_formatter->dump_string("client_oc", buf);
      }
    }
    if (!config.client_oc_size.empty()) {
      char buf[128];
      int rc = ceph_conf_get(shared_cmount, "client_oc_size", buf, sizeof(buf));
      if (rc >= 0) {
        json_formatter->dump_string("client_oc_size", buf);
      }
    }
    if (config.msgr_workers > 0) {
      char buf[128];
      int rc = ceph_conf_get(shared_cmount, "ms_async_op_threads", buf, sizeof(buf));
      if (rc >= 0) {
        json_formatter->dump_string("ms_async_op_threads", buf);
      }
    }
    json_formatter->dump_bool("async_io", config.async_io);
    if (config.async_io) {
      json_formatter->dump_int("queue_depth", config.queue_depth);
      json_formatter->dump_bool("async_write_fsync", config.async_write_fsync);
      json_formatter->dump_bool("shared_file", config.shared_file);
    }
    json_formatter->close_section(); // configuration
    json_formatter->open_array_section("iterations");
  }

  cout << "Benchmark Configuration:" << std::endl;
  cout << "  Threads: " << config.num_threads << " | Iterations: " << config.iterations << std::endl;
  cout << "  Files: " << config.num_files << " | Size: " << config.file_size
       << " bytes" << std::endl;
  cout << "  Filesystem: " << (config.filesystem.empty() ? "(default)" : config.filesystem) << std::endl;
  cout << "  Root: " << config.mount_root << std::endl;
  cout << "  Subdirectory: " << config.subdir << std::endl;
  cout << "  UID: " << config.uid << std::endl;
  cout << "  GID: " << config.gid << std::endl;
  if (!config.client_oc.empty()) {
    char buf[128];
    int rc = ceph_conf_get(shared_cmount, "client_oc", buf, sizeof(buf));
    if (rc >= 0) {
      cout << "  client_oc: " << buf << std::endl;
    }
  }
  if (!config.client_oc_size.empty()) {
    char buf[128];
    int rc = ceph_conf_get(shared_cmount, "client_oc_size", buf, sizeof(buf));
    if (rc >= 0) {
      cout << "  client_oc_size: " << buf << std::endl;
    }
  }
  if (config.msgr_workers > 0) {
    char buf[128];
    int rc = ceph_conf_get(shared_cmount, "ms_async_op_threads", buf, sizeof(buf));
    if (rc >= 0) {
      cout << "  ms_async_op_threads: " << buf << std::endl;
    }
  }
  cout << "  Async I/O: " << (config.async_io ? "enabled" : "disabled") << std::endl;
  if (config.async_io) {
    cout << "  Queue Depth: " << config.queue_depth << std::endl;
    cout << "  Async write+fsync: "
         << (config.async_write_fsync ? "enabled" : "disabled") << std::endl;
    cout << "  Shared file (all threads): "
         << (config.shared_file ? "yes" : "no") << std::endl;
  }

  if (int rc = ceph_mkdir(shared_cmount, config.subdir.c_str(), 0755); rc < 0) {
    cerr << "Failed to create bench directory '" << config.subdir << "': " << strerror(-rc) << std::endl;
    return 1;
  }

  int files_per_thread = config.num_files / config.num_threads;
  int remainder = config.num_files % config.num_threads;

  if (json_formatter) {
    json_formatter->open_object_section("filesystem_stats");
  }
  report_fs_stats("before", shared_cmount, config, json_formatter.get());

  std::vector<double> write_mbps;
  std::vector<double> write_fps;
  std::vector<double> read_mbps;
  std::vector<double> read_fps;

  std::atomic<bool> stop_signal{false};
  std::vector<ThreadStats> write_stats(config.num_threads);

  for (int iter = 1; iter <= config.iterations; ++iter) {
    stop_signal = false;

    cout << "\n--- Iteration " << iter << " of " << config.iterations << " ---" << std::endl;

    if (json_formatter) {
      json_formatter->open_object_section("iteration");
      json_formatter->dump_int("number", iter);
    }

    // --- WRITE PHASE ---
    cout << "Starting Write Phase..." << std::endl;

#ifdef CEPH_LOCKSTAT
    // Reset lockstat data before write phase
    if (!config.lockstat_dump_path.empty()) {
      ceph::lockstat_detail::LockStatEntry::reset_data();
    }
#endif
    std::vector<std::thread> threads;
    for (auto& s : write_stats) {
      s.bytes_transferred = 0;
      s.ops = 0;
      s.files = 0;
      s.errors = 0;
    }
    auto thread_outputs = std::vector<std::stringstream>(config.num_threads);

    auto start_time = steady_clock::now();

    std::atomic<bool> write_progress_stop{false};
    std::thread write_progress_thread;
    if (config.show_progress) {
      write_progress_thread = std::thread(
          progress_reporter, "write", std::ref(write_stats), std::ref(config),
          std::ref(write_progress_stop), start_time);
    }

    for (int i = 0; i < config.num_threads; ++i) {
      int f_count = files_per_thread + (i < remainder ? 1 : 0);
      struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
      if (config.async_io) {
        // Async path uses the low-level ll API with a per-thread I/O queue.
        threads.emplace_back(
            bench_async_write_worker, i, f_count, config, worker_mount,
            std::ref(write_stats[i]), std::ref(stop_signal),
            std::ref(thread_outputs[i]), start_time);
      } else {
        threads.emplace_back(
            bench_write_worker, i, f_count, config, worker_mount,
            std::ref(write_stats[i]), std::ref(stop_signal),
            std::ref(thread_outputs[i]), start_time);
      }
    }
    for (auto& t : threads) {
      t.join();
    }

    if (config.show_progress) {
      write_progress_stop = true;
      if (write_progress_thread.joinable()) {
        write_progress_thread.join();
      }
    }

    if (check_and_report_errors(stop_signal, thread_outputs)) {
      if (int rc = ceph_unmount(shared_cmount); rc < 0) {
        cerr << "Unmount error: " << strerror(-rc) << endl;
      }
      ceph_shutdown(shared_cmount);
      return 1;
    }

    auto end_time = steady_clock::now();

    uint64_t total_write_bytes = 0;
    uint64_t total_files = 0;
    for (const auto& s : write_stats) {
      total_write_bytes += s.bytes_transferred;
      total_files += s.files;
    }

    // Without --duration, require every file to be written before continuing.
    if (config.duration == 0 && total_files != (uint64_t)config.num_files) {
      cerr << "Write phase completed only " << total_files << " of "
           << config.num_files << " files" << endl;
      if (int rc = ceph_unmount(shared_cmount); rc < 0) {
        cerr << "Unmount error: " << strerror(-rc) << endl;
      }
      ceph_shutdown(shared_cmount);
      return 1;
    }

    // Ensure all buffered writes are on the MDS before remounting for read.
    if (int rc = ceph_sync_fs(shared_cmount); rc < 0) {
      cerr << "ceph_sync_fs after write failed: " << strerror(-rc) << endl;
      ceph_shutdown(shared_cmount);
      return 1;
    }

    double elapsed_sec = duration_cast<milliseconds>(end_time - start_time).count() / 1000.0;

    double w_rate = (double)total_write_bytes / 1024.0 / 1024.0 / elapsed_sec;
    double w_fps = (double)total_files / elapsed_sec;

    write_mbps.push_back(w_rate);
    write_fps.push_back(w_fps);

    cout << "  Write: ";
    if (config.file_size > 0) {
      cout << w_rate << " MiB/s, ";
    }
    cout << w_fps << " files/s (" << elapsed_sec << "s)" << std::endl;

    if (json_formatter) {
      json_formatter->open_object_section("write");
      if (config.file_size > 0) {
        json_formatter->dump_float("throughput_mbps", w_rate);
      }
      json_formatter->dump_float("files_per_sec", w_fps);
      json_formatter->dump_float("duration_sec", elapsed_sec);
      json_formatter->close_section(); // write
    }

#ifdef CEPH_LOCKSTAT
    // Dump lockstat data after write phase
    if (!config.lockstat_dump_path.empty()) {
      dump_lockstat_data(config.lockstat_dump_path, iter, "write");
    }
#endif

    // --- REMOUNT / CACHE CLEAR ---
    // Drop the client page cache so the read phase measures cold reads from OSDs.
    if (!config.per_thread_mount) {
      if (int rc = ceph_unmount(shared_cmount); rc < 0) {
        cerr << "Unmount failed during cache clear: " << strerror(-rc) << endl;
        return 1;
      }
      ceph_shutdown(shared_cmount);
      if (int rc = setup_mount(&shared_cmount, config, cerr); rc < 0) {
        cerr << "Failed to create/mount global handle. (Is ceph.conf valid?): " << strerror(-rc) << std::endl;
        return 1;
      }
    }

    // --- READ PHASE ---
    cout << "Starting Read Phase..." << std::endl;

#ifdef CEPH_LOCKSTAT
    // Reset lockstat data before read phase
    if (!config.lockstat_dump_path.empty()) {
      ceph::lockstat_detail::LockStatEntry::reset_data();
    }
#endif

    threads.clear();
    std::vector<ThreadStats> read_stats(config.num_threads);
    thread_outputs = std::vector<std::stringstream>(config.num_threads);

    start_time = steady_clock::now();

    std::atomic<bool> read_progress_stop{false};
    std::thread read_progress_thread;
    if (config.show_progress) {
      read_progress_thread = std::thread(
          progress_reporter, "read", std::ref(read_stats), std::ref(config),
          std::ref(read_progress_stop), start_time);
    }

    for (int i = 0; i < config.num_threads; ++i) {
      struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
      int files_to_read = (int)write_stats[i].files.load();
      if (config.async_io) {
        threads.emplace_back(
            bench_async_read_worker, i, files_to_read, config,
            worker_mount, std::ref(read_stats[i]), std::ref(stop_signal),
            std::ref(thread_outputs[i]), start_time);
      } else {
        threads.emplace_back(
            bench_read_worker, i, files_to_read, config,
            worker_mount, std::ref(read_stats[i]), std::ref(stop_signal),
            std::ref(thread_outputs[i]), start_time);
      }
    }
    for (auto& t : threads) {
      t.join();
    }

    if (config.show_progress) {
      read_progress_stop = true;
      if (read_progress_thread.joinable()) {
        read_progress_thread.join();
      }
    }

    if (check_and_report_errors(stop_signal, thread_outputs)) {
      if (int rc = ceph_unmount(shared_cmount); rc < 0) {
        cerr << "Unmount error: " << strerror(-rc) << endl;
      }
      ceph_shutdown(shared_cmount);
      return 1;
    }

    end_time = steady_clock::now();

    uint64_t total_read_bytes = 0;
    total_files = 0;
    for (const auto& s : read_stats) {
      total_read_bytes += s.bytes_transferred;
      total_files += s.files;
    }

    elapsed_sec = duration_cast<milliseconds>(end_time - start_time).count() / 1000.0;

    double r_rate = (double)total_read_bytes / 1024.0 / 1024.0 / elapsed_sec;
    double r_fps = (double)total_files / elapsed_sec;

    read_mbps.push_back(r_rate);
    read_fps.push_back(r_fps);

    cout << "  Read:  ";
    if (config.file_size > 0) {
      cout << r_rate << " MiB/s, ";
    }
    cout << r_fps << " files/s (" << elapsed_sec << "s)" << std::endl;

    if (json_formatter) {
      json_formatter->open_object_section("read");
      if (config.file_size > 0) {
        json_formatter->dump_float("throughput_mbps", r_rate);
      }
      json_formatter->dump_float("files_per_sec", r_fps);
      json_formatter->dump_float("duration_sec", elapsed_sec);
      json_formatter->close_section(); // read
      json_formatter->close_section(); // iteration
    }

#ifdef CEPH_LOCKSTAT
    // Dump lockstat data after read phase
    if (!config.lockstat_dump_path.empty()) {
      dump_lockstat_data(config.lockstat_dump_path, iter, "read");
    }
#endif

    // Cleanup for next iteration
    if (iter < config.iterations) {
      cout << "Cleaning up for next iteration..." << std::endl;
      threads.clear();
      thread_outputs = std::vector<std::stringstream>(config.num_threads);
      stop_signal = false;

      for (int i = 0; i < config.num_threads; ++i) {
        struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
        threads.emplace_back(
            bench_cleanup_worker, i, (int)write_stats[i].files.load(), config,
            worker_mount, std::ref(stop_signal), std::ref(thread_outputs[i]));
      }
      for (auto& t : threads) {
        t.join();
      }
      // Report errors
      if (check_and_report_errors(stop_signal, thread_outputs)) {
          return 1;
      }
    }
  }

  report_fs_stats("after", shared_cmount, config, json_formatter.get());
  if (json_formatter) {
    json_formatter->close_section(); // filesystem_stats
  }

  cout << std::endl << std::endl << "*** Final Report ***" << std::endl;

  if (json_formatter) {
    json_formatter->close_section(); // iterations
    json_formatter->open_object_section("summary");
  }

  // Statistics Output
  if (config.file_size > 0) {
    print_statistics(
        "Write Throughput", write_mbps, "MiB/s", json_formatter.get());
    print_statistics(
        "Read Throughput", read_mbps, "MiB/s", json_formatter.get());
  }
  print_statistics("File Creates", write_fps, "files/s", json_formatter.get());
  print_statistics(
      "File Reads (Opens)", read_fps, "files/s", json_formatter.get());

  if (json_formatter) {
    json_formatter->close_section(); // summary
    json_formatter->close_section(); // benchmark
    if (config.json_path == "-") {
      // Output to stdout
      json_formatter->flush(cout);
      cout << std::endl;
    } else {
      // Output to file
      std::ofstream ofs(config.json_path);
      if (ofs.is_open()) {
        json_formatter->flush(ofs);
        cout << "\nResults saved to " << config.json_path << std::endl;
      } else {
        cerr << "\nError: Could not open " << config.json_path << " for writing."
             << std::endl;
      }
    }
  }

  if (config.cleanup) {
    cout << "\nCleaning up..." << std::endl;
    std::vector<std::thread> threads;
    auto thread_outputs = std::vector<std::stringstream>(config.num_threads);
    stop_signal = false;

    // Note: This cleanup uses the number of files written in the LAST iteration.
    // If the tool is intended to cleanup all files ever written across all iterations,
    // this logic might need refinement if iterations write different number of files.
    // However, since we use the same filename pattern per iteration, cleaning the
    // max number of files written in any iteration or just the last one is typical.
    // Here we use write_stats from the last iteration.
    for (int i = 0; i < config.num_threads; ++i) {
      struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
      threads.emplace_back(
          bench_cleanup_worker, i, (int)write_stats[i].files.load(), config,
          worker_mount, std::ref(stop_signal), std::ref(thread_outputs[i]));
    }
    for (auto& t : threads) {
      t.join();
    }

    // Check for warnings/errors but proceed to rmdir
    check_and_report_errors(stop_signal, thread_outputs);

    if (int rc = ceph_rmdir(shared_cmount, config.subdir.c_str()); rc < 0) {
      cerr << "Warning: Failed to cleanup (rmdir) " << config.subdir << ": " << strerror(-rc) << endl;
    }
  }

  if (!config.perf_dump_path.empty()) {
    execute_perf_dump(shared_cmount, config.perf_dump_path);
  }

  if (int rc = ceph_unmount(shared_cmount); rc < 0) {
    cerr << "Final unmount failed: " << strerror(-rc) << endl;
  }


  ceph_shutdown(shared_cmount);
  return 0;
}

int main(int argc, char **argv) {
  BenchConfig config;
  string size_str, block_size_str, fsync_str;
  bool no_cleanup = false;
  string subcommand;

  // Group 1: General Options
  po::options_description general("General Options");
  general.add_options()
    ("help,h", "Produce help message")
    ("conf,c", po::value<string>(&config.ceph_conf), "Ceph config file path")
    ("id,i", po::value<string>(&config.userid)->default_value("admin"), "Client ID")
    ("keyring,k", po::value<string>(&config.keyring), "Path to keyring file")
    ("filesystem,fs", po::value<string>(&config.filesystem), "CephFS filesystem name to mount")
    ("uid", po::value<int>(&config.uid)->default_value(-1), "User ID to mount as")
    ("gid", po::value<int>(&config.gid)->default_value(-1), "Group ID to mount as")
    ("client-oc", po::value<string>(&config.client_oc), "Set the 'client_oc' option (0|1)")
    ("client-oc-size", po::value<string>(&config.client_oc_size), "Set the 'client_oc_size' option")
    ("msgr-workers", po::value<int>(&config.msgr_workers)->default_value(0), "Set the 'ms_async_op_threads' option (1-24, 0 = use default)");

  // Group 2: Benchmark Options
  po::options_description bench("Benchmark Options (used with 'bench' command)");
  bench.add_options()
    ("threads", po::value<int>(&config.num_threads)->default_value(1), "Number of threads")
    ("iterations", po::value<int>(&config.iterations)->default_value(1), "Number of iterations")
    ("files", po::value<int>(&config.num_files)->default_value(100), "Total number of files")
    ("size", po::value<string>(&size_str)->default_value("4MB"), "File size (e.g. 4MB, 0 for creates only)")
    ("block-size", po::value<string>(&block_size_str)->default_value("4MB"), "IO block size (e.g. 1MB)")
    ("fsync-every", po::value<string>(&fsync_str)->default_value("0"), "Call fsync every N bytes")
    ("prefix", po::value<string>(&config.prefix)->default_value("benchmark_"), "Filename prefix")
    ("dir-prefix", po::value<string>(&config.dir_prefix)->default_value("bench_run_"), "Directory prefix")
    ("root-path", po::value<string>(&config.mount_root)->default_value("/"), "Root path in CephFS")
    ("per-thread-mount", po::bool_switch(&config.per_thread_mount), "Use separate mount per thread")
    ("no-cleanup", po::bool_switch(&no_cleanup), "Disable cleanup of files")
    ("json", po::value<string>(&config.json_path), "Output results to a JSON file (use '-' for stdout)")
    ("duration", po::value<int>(&config.duration)->default_value(0), "Limit each phase to N seconds (0 = no limit)")
    ("perf-dump", po::value<string>(&config.perf_dump_path), "File to dump performance counters to")
    ("progress", po::bool_switch(&config.show_progress), "Show progress and current bandwidth during benchmark")
    ("progress-interval", po::value<int>(&config.progress_interval)->default_value(10), "Progress update interval in percent (1-100)")
    ("async-io", po::bool_switch(&config.async_io), "Use asynchronous I/O with ceph_ll_nonblocking_readv_writev")
    ("async-write-fsync", po::bool_switch(&config.async_write_fsync), "With async-io, fsync each write via io_info.fsync (Ganesha-style; needs async-io)")
    ("shared-file", po::bool_switch(&config.shared_file), "With async-io, all threads write to one shared file (stress cap flush waiters)")
    ("queue-depth", po::value<int>(&config.queue_depth)->default_value(16), "Queue depth for async I/O (number of outstanding I/Os per thread)")
#ifdef CEPH_LOCKSTAT
    ("lockstat-dump", po::value<string>(&config.lockstat_dump_path), "Base path for lockstat output files (iteration and phase will be appended)")
    ("lockstat-threshold", po::value<uint64_t>(&config.lockstat_threshold_ns)->default_value(0), "Lockstat threshold in nanoseconds (0 = collect all)")
#endif
    ;

  // Hidden positional option for the sub-command
  po::options_description hidden("Hidden options");
  hidden.add_options()
    ("subcommand", po::value<string>(&subcommand), "Command to execute");

  // Visible options for help output
  po::options_description visible("Allowed options");
  visible.add(general).add(bench);

  // All options for parsing
  po::options_description all("");
  all.add(visible).add(hidden);

  // Positional mapping
  po::positional_options_description p;
  p.add("subcommand", 1); // Only take the first positional arg as command

  po::variables_map vm;
  try {
    po::store(po::command_line_parser(argc, argv).options(all).positional(p).run(), vm);
    po::notify(vm);
  } catch(const std::exception& e) {
    cerr << "Error parsing options: " << e.what() << endl;
    return 1;
  }

  if (vm.count("help")) {
    cout << "Usage: cephfs-bench [general-options] <command> [command-options]\n\n";
    cout << "Commands:\n";
    cout << "  bench      Run IO benchmark\n\n";
    cout << visible << "\n";
    return 0;
  }

  if (subcommand == "help" || subcommand.empty()) {
    cerr << "Error: No command specified.\n";
    cerr << "Usage: cephfs-bench [options] <command>\n";
    cerr << "Try --help for more information.\n";
    return 1;
  }

  if (subcommand == "bench") {
    // Inside do_bench or main, after parsing options:
    config.cleanup = !no_cleanup;
    config.file_size = parse_size(size_str);
    config.block_size = parse_size(block_size_str);
    config.fsync_every_bytes = parse_size(fsync_str);

    if (config.progress_interval < 1 || config.progress_interval > 100) {
      cerr << "Error: progress-interval must be between 1 and 100\n";
      return 1;
    }

    if (config.msgr_workers != 0 && (config.msgr_workers < 1 || config.msgr_workers > 24)) {
      cerr << "Error: msgr-workers must be between 1 and 24\n";
      return 1;
    }

    if (config.queue_depth < 1 || config.queue_depth > 1024) {
      cerr << "Error: queue-depth must be between 1 and 1024\n";
      return 1;
    }

    if (config.async_io && config.per_thread_mount) {
      cerr << "Error: async-io is not compatible with per-thread-mount\n";
      return 1;
    }

    if (config.async_write_fsync && !config.async_io) {
      cerr << "Error: async-write-fsync requires --async-io\n";
      return 1;
    }

    if (config.shared_file && !config.async_io) {
      cerr << "Error: shared-file requires --async-io\n";
      return 1;
    }

    if (config.shared_file && config.num_files > 1) {
      cerr << "Warning: shared-file mode uses a single file; --files is ignored\n";
      config.num_files = 1;
    }

    return do_bench(config);
  } else {
    cerr << "Unknown command: " << subcommand << "\n";
    return 1;
  }
}
