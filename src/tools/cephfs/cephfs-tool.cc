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
#include <iostream>
#include <map>
#include <numeric>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Standard Public Ceph API
#include <cephfs/libcephfs.h>
#include <fcntl.h>
#include <unistd.h>

// Boost Program Options
#include <boost/program_options.hpp>

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
namespace po = boost::program_options;

// --- Helper: Parse sizes like "4MB", "1G" ---
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
    char suffix = std::toupper(val[end_pos]);
    switch (suffix) {
      case 'K':
        num *= 1024;
        break;
      case 'M':
        num *= 1024ULL * 1024;
        break;
      case 'G':
        num *= 1024ULL * 1024 * 1024;
        break;
      case 'T':
        num *= 1024ULL * 1024 * 1024 * 1024;
        break;
    }
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
};

struct ThreadStats {
  uint64_t bytes_transferred = 0;
  uint64_t ops = 0;    // read/write calls
  uint64_t files = 0;  // files successfully opened/closed
  int errors = 0;
};

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

// Worker function for Write phase
void bench_write_worker(int thread_id,
                        int files_to_write,
                        BenchConfig config,
                        struct ceph_mount_info *shared_cmount,
                        ThreadStats &stats,
                        std::atomic<bool>& stop_signal,
                        std::stringstream& ss) {

  struct ceph_mount_info *cmount = shared_cmount;

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
  for (int i = 0; i < files_to_write; ++i) {
    if (stop_signal) {
      break; // Check if we should stop
    }

    string fname = config.subdir + "/" + config.prefix + std::to_string(thread_id) + "_" + std::to_string(i);

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
      stats.files++;
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

// Worker function for Read phase
void bench_read_worker(int thread_id,
                       int files_to_read,
                       BenchConfig config,
                       struct ceph_mount_info *shared_cmount,
                       ThreadStats &stats,
                       std::atomic<bool>& stop_signal,
                       std::stringstream& ss) {

  struct ceph_mount_info *cmount = shared_cmount;

  if (config.per_thread_mount) {
    if (int rc = setup_mount(&cmount, config, ss); rc < 0) {
      stats.errors++;
      stop_signal = true;
      return;
    }
  }

  std::vector<char> buffer(config.block_size);

  for (int i = 0; i < files_to_read; ++i) {
    if (stop_signal) {
      break;
    }

    string fname = config.subdir + "/" + config.prefix + std::to_string(thread_id) + "_" + std::to_string(i);

    int fd = ceph_open(cmount, fname.c_str(), O_RDONLY, 0);
    if (fd < 0) {
      ss << "Thread " << thread_id << " open failed " << fname << ": " << strerror(-fd) << std::endl;
      stats.errors++;
      stop_signal = true;
      break;
    }

    uint64_t total_read = 0;
    while (total_read < config.file_size) {
      if (stop_signal) {
        break;
      }

      int rc = ceph_read(cmount, fd, buffer.data(), config.block_size, -1);
      if (rc < 0) {
        ss << "Thread " << thread_id << " read error: " << strerror(-rc) << std::endl;
        stats.errors++;
        stop_signal = true;
        break;
      }
      if (rc == 0) {
        break; // EOF
      }

      total_read += rc;
      stats.bytes_transferred += rc;
      stats.ops++;
    }

    if (int rc = ceph_close(cmount, fd); rc < 0) {
      ss << "Thread " << thread_id << " close error " << fname << ": " << strerror(-rc) << std::endl;
      stats.errors++;
      stop_signal = true;
      break;
    }
    stats.files++;
  }

  if (config.per_thread_mount) {
    if (int rc = ceph_unmount(cmount); rc < 0) {
      ss << "Thread " << thread_id << " unmount failed: " << strerror(-rc) << std::endl;
    }
    ceph_shutdown(cmount);
  }
}

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

void print_statistics(const string& type, const vector<double>& rates, const string& unit) {
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

  // Create Main Mount
  struct ceph_mount_info *shared_cmount = NULL;
  if (int rc = setup_mount(&shared_cmount, config, cerr); rc < 0) {
    cerr << "Failed to create/mount global handle. (Is ceph.conf valid?): " << strerror(-rc) << std::endl;
    return 1;
  }

  // Setup bench directory
  config.subdir = config.dir_prefix + RandomHelper::generate_hex_suffix();

  cout << "Benchmark Configuration:" << std::endl;
  cout << "  Threads: " << config.num_threads << " | Iterations: " << config.iterations << std::endl;
  cout << "  Files: " << config.num_files << " | Size: " << config.file_size << std::endl;
  cout << "  Filesystem: " << (config.filesystem.empty() ? "(default)" : config.filesystem) << std::endl;
  cout << "  Root: " << config.mount_root << std::endl;
  cout << "  Subdirectory: " << config.subdir << std::endl;
  cout << "  UID: " << config.uid << std::endl;
  cout << "  GID: " << config.gid << std::endl;

  if (int rc = ceph_mkdir(shared_cmount, config.subdir.c_str(), 0755); rc < 0) {
    cerr << "Failed to create bench directory '" << config.subdir << "': " << strerror(-rc) << std::endl;
    return 1;
  }

  int files_per_thread = config.num_files / config.num_threads;
  int remainder = config.num_files % config.num_threads;

  std::vector<double> write_mbps;
  std::vector<double> write_fps;
  std::vector<double> read_mbps;
  std::vector<double> read_fps;

  std::atomic<bool> stop_signal{false};

  for (int iter = 1; iter <= config.iterations; ++iter) {
    cout << "\n--- Iteration " << iter << " of " << config.iterations << " ---" << std::endl;

    // --- WRITE PHASE ---
    cout << "Starting Write Phase..." << std::endl;
    std::vector<std::thread> threads;
    std::vector<ThreadStats> write_stats(config.num_threads);
    auto thread_outputs = std::vector<std::stringstream>(config.num_threads);

    auto start_time = steady_clock::now();

    for (int i = 0; i < config.num_threads; ++i) {
      int f_count = files_per_thread + (i < remainder ? 1 : 0);
      struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
      threads.emplace_back(bench_write_worker, i, f_count, config, worker_mount,
                           std::ref(write_stats[i]), std::ref(stop_signal), std::ref(thread_outputs[i]));
    }
    for (auto& t : threads) {
      t.join();
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

    double elapsed_sec = duration_cast<milliseconds>(end_time - start_time).count() / 1000.0;
    double w_rate = (double)total_write_bytes / 1024.0 / 1024.0 / elapsed_sec;
    double w_fps = (double)total_files / elapsed_sec;

    write_mbps.push_back(w_rate);
    write_fps.push_back(w_fps);

    cout << "  Write: ";
    if (config.file_size > 0) {
      cout << w_rate << " MB/s, ";
    }
    cout << w_fps << " files/s (" << elapsed_sec << "s)" << std::endl;

    // --- REMOUNT / CACHE CLEAR ---
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
    threads.clear();
    std::vector<ThreadStats> read_stats(config.num_threads);
    thread_outputs = std::vector<std::stringstream>(config.num_threads);

    start_time = steady_clock::now();

    for (int i = 0; i < config.num_threads; ++i) {
      int f_count = files_per_thread + (i < remainder ? 1 : 0);
      struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
      threads.emplace_back(bench_read_worker, i, f_count, config, worker_mount,
                           std::ref(read_stats[i]), std::ref(stop_signal), std::ref(thread_outputs[i]));
    }
    for (auto& t : threads) {
      t.join();
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
      cout << r_rate << " MB/s, ";
    }
    cout << r_fps << " files/s (" << elapsed_sec << "s)" << std::endl;

    // Cleanup for next iteration
    if (iter < config.iterations) {
      cout << "Cleaning up for next iteration..." << std::endl;
      threads.clear();
      thread_outputs = std::vector<std::stringstream>(config.num_threads);
      stop_signal = false;

      for (int i = 0; i < config.num_threads; ++i) {
        int f_count = files_per_thread + (i < remainder ? 1 : 0);
        struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
        threads.emplace_back(bench_cleanup_worker, i, f_count, config, worker_mount,
                             std::ref(stop_signal), std::ref(thread_outputs[i]));
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

  cout << std::endl << std::endl << "*** Final Report ***" << std::endl;

  // Statistics Output
  if (config.file_size > 0) {
    print_statistics("Write Throughput", write_mbps, "MB/s");
    print_statistics("Read Throughput", read_mbps, "MB/s");
  }
  print_statistics("File Creates", write_fps, "files/s");
  print_statistics("File Reads (Opens)", read_fps, "files/s");

  if (config.cleanup) {
    cout << "\nCleaning up..." << std::endl;
    std::vector<std::thread> threads;
    auto thread_outputs = std::vector<std::stringstream>(config.num_threads);
    stop_signal = false;

    for (int i = 0; i < config.num_threads; ++i) {
      int f_count = files_per_thread + (i < remainder ? 1 : 0);
      struct ceph_mount_info *worker_mount = config.per_thread_mount ? NULL : shared_cmount;
      threads.emplace_back(bench_cleanup_worker, i, f_count, config, worker_mount,
                           std::ref(stop_signal), std::ref(thread_outputs[i]));
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
    ("gid", po::value<int>(&config.gid)->default_value(-1), "Group ID to mount as");

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
    ("no-cleanup", po::bool_switch(&no_cleanup), "Disable cleanup of files");

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
    return do_bench(config);
  } else {
    cerr << "Unknown command: " << subcommand << "\n";
    return 1;
  }
}
