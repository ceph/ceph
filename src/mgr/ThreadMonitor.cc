// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 */

#include "ThreadMonitor.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr[ThreadMonitor] " << __func__ << " "

void ThreadMonitor::start_monitoring() {
  if (running.exchange(true)) {
    return;
  }

  dout(20) << "Starting monitoring thread." << dendl;
  monitor_thread = std::make_unique<std::thread>(&ThreadMonitor::monitoring_loop, this);
}

void ThreadMonitor::stop_monitoring() {
  if (!running.exchange(false)) {
    return;
  }

  if (monitor_thread && monitor_thread->joinable()) {
    monitor_thread->join();
    dout(20) << "Monitoring thread stopped." << dendl;
  }
}

void ThreadMonitor::register_thread(const pid_t thread_id,
  const pid_t serve_thread_id,
  const std::string& thread_name,
  const PyModuleRef py_module_) {
  std::lock_guard<std::mutex> lock(monitored_threads_mutex);

  if (monitored_threads.count(thread_id)) {
    dout(20) << "Attempted to register already known thread (TID: " << thread_id << ")." << dendl;
    return;
  }

  MonitoredThreadInfo info;
  info.name = thread_name;
  info.py_module = py_module_;
  info.serve_thread_id = serve_thread_id;
  info.last_snapshot.timestamp = ceph::mono_clock::now();
  info.last_serve_snapshot.timestamp = ceph::mono_clock::now();
  monitored_threads[thread_id] = info;
  dout(0) << "Registered thread: '" << thread_name << "' (TID: " << thread_id << ") module: "
    << (py_module_ ? py_module_->get_name() : "unknown") << dendl;
}

void ThreadMonitor::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed) {
  if (changed.count("mgr_module_monitor_interval")) {
    int interval = m_cct->_conf.get_val<int64_t>("mgr_module_monitor_interval");
    monitoring_interval = std::chrono::seconds(interval);
    dout(20) << "Updated monitoring interval to " << interval << " seconds." << dendl;
    if (interval == 0) {
      stop_monitoring();
    } else if (!running) {
      start_monitoring();
    }
  }
}

void ThreadMonitor::monitoring_loop() {
  if (m_clock_ticks_per_sec <= 0 || m_page_size <= 0) {
    derr << "Failed to retrieve system configuration "
         << "(clock ticks per second or page size). Monitoring will not start." << dendl;
    running = false;
    return;
  }

  while (running) {
    // Phase 1: under lock — update RSS counters and copy all state needed for CPU computation.
    long long process_rss_pages = 0;
    if (!read_process_statm(process_rss_pages)) {
      derr << "Failed to read process memory info from /proc/self/statm." << dendl;
      continue;
    }

    std::vector<ThreadEntry> entries;
    {
      std::lock_guard<std::mutex> lock(monitored_threads_mutex);
      for (auto& [tid, info] : monitored_threads) {
        if (info.py_module && info.py_module->perfcounter) {
          long long rss_bytes = process_rss_pages * m_page_size;
          long long rss_change = rss_bytes - info.last_snapshot.rss_pages * m_page_size;
          info.py_module->perfcounter->set(info.py_module->l_pym_mem_rss_current, rss_bytes);
          info.py_module->perfcounter->set(info.py_module->l_pym_mem_rss_change, rss_change);
          info.last_snapshot.rss_pages = process_rss_pages;
          dout(20) << "Module '" << info.name << "' (TID: " << tid << "): "
                   << "Memory RSS: " << rss_bytes << " bytes"
                   << ", Change: " << rss_change << " bytes" << dendl;
        }
        entries.push_back({tid, info.serve_thread_id, info.name,
                           info.py_module, info.last_snapshot, info.last_serve_snapshot});
      }
    }

    // Phase 2: no lock — do slow /proc reads and CPU calculations using copied snapshots.
    std::vector<ThreadResult> results;
    results.reserve(entries.size());
    for (const auto& e : entries) {
      results.push_back(process_thread_stats(e));
    }

    // Phase 3: under lock — write results back; remove dead threads.
    {
      std::lock_guard<std::mutex> lock(monitored_threads_mutex);
      for (const auto& r : results) {
        auto it = monitored_threads.find(r.tid);
        if (it == monitored_threads.end()) {
          continue; // deregistered between phase 1 and 3
        }
        MonitoredThreadInfo& info = it->second;
        if (!r.main_ok) {
          dout(0) << "Removing dead thread '" << info.name << "' (TID: " << r.tid << ")" << dendl;
          monitored_threads.erase(it);
          continue;
        }
        info.last_snapshot.utime = r.new_utime;
        info.last_snapshot.stime = r.new_stime;
        info.last_snapshot.timestamp = r.new_ts;
        if (info.py_module && info.py_module->perfcounter) {
          info.py_module->perfcounter->set(info.py_module->l_pym_cpu_usage,
                                            static_cast<uint64_t>(r.cpu_pct));
        }
        if (info.serve_thread_id) {
          if (!r.serve_ok) {
            info.serve_thread_id = 0;
          } else {
            info.last_serve_snapshot.utime = r.new_serve_utime;
            info.last_serve_snapshot.stime = r.new_serve_stime;
            info.last_serve_snapshot.timestamp = r.new_serve_ts;
            if (info.py_module && info.py_module->perfcounter) {
              info.py_module->perfcounter->set(info.py_module->l_pym_serve_cpu_usage,
                                               static_cast<uint64_t>(r.serve_cpu_pct));
            }
          }
        }
      }
    }

    std::this_thread::sleep_for(monitoring_interval);
  }
}

ThreadMonitor::ThreadResult ThreadMonitor::process_thread_stats(const ThreadEntry& e) {
  ThreadResult r;
  r.tid = e.tid;
  long long utime, stime;
  r.new_ts = ceph::mono_clock::now();
  r.main_ok = read_thread_stat(e.tid, utime, stime);
  if (!r.main_ok) {
    dout(20) << "Thread '" << e.name << "' (TID: " << e.tid << ") may have exited." << dendl;
    return r;
  }
  double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
                     r.new_ts - e.last_snapshot.timestamp).count();
  r.cpu_pct = calculate_cpu_percentage(utime - e.last_snapshot.utime,
                                        stime - e.last_snapshot.stime, elapsed);
  r.new_utime = utime;
  r.new_stime = stime;
  dout(20) << "Module '" << e.name << "' (TID: " << e.tid << "): "
           << "CPU: " << std::fixed << std::setprecision(2) << r.cpu_pct << "%" << dendl;

  if (!e.serve_tid) {
    return r;
  }
  r.new_serve_ts = ceph::mono_clock::now();
  r.serve_ok = read_thread_stat(e.serve_tid, utime, stime);
  if (!r.serve_ok) {
    dout(20) << "Serve thread (TID: " << e.serve_tid << ") may have exited." << dendl;
    return r;
  }
  double serve_elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
                           r.new_serve_ts - e.last_serve_snapshot.timestamp).count();
  r.serve_cpu_pct = calculate_cpu_percentage(utime - e.last_serve_snapshot.utime,
                                              stime - e.last_serve_snapshot.stime, serve_elapsed);
  r.new_serve_utime = utime;
  r.new_serve_stime = stime;
  dout(20) << "Serve thread (TID: " << e.serve_tid << "): "
           << "CPU: " << std::fixed << std::setprecision(2) << r.serve_cpu_pct << "%" << dendl;
  return r;
}

bool ThreadMonitor::read_thread_stat(pid_t tid, long long& utime, long long& stime) {
  std::string stat_path = "/proc/self/task/" + std::to_string(tid) + "/stat";
  std::ifstream stat_file(stat_path);
  if (!stat_file.is_open()) {
    dout(20) << __func__ << "Could not open " << stat_path << dendl;
    return false;
  }
  std::string line;
  std::getline(stat_file, line);
  size_t start = line.find('(');
  size_t end = line.rfind(')');
  if (start == std::string::npos || end == std::string::npos) {
    dout(20) << __func__ << "Malformed stat file for TID " << tid << dendl;
    return false;
  }

  std::string remainder = line.substr(end + 2); // +2 to skip ") "
  std::stringstream ss(remainder);
  std::string val;

  // skip 11 fields before utime/stime: 
  // state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt
  for (int i = 0; i < 11; ++i) {
    ss >> val;
  }
  ss >> utime >> stime;
  return true;
}

bool ThreadMonitor::read_process_statm(long long& rss_pages) {
  std::string statm_path = "/proc/self/statm";
  std::ifstream statm_file(statm_path);

  if (!statm_file.is_open()) {
    dout(20) << __func__ << "Could not open " << statm_path << dendl;
    return false;
  }

  long long vsize_pages;
  statm_file >> vsize_pages >> rss_pages;
  return true;
}

double ThreadMonitor::calculate_cpu_percentage(long long utime_diff, long long stime_diff, 
                                              double elapsed_seconds) {
  if (elapsed_seconds <= 0) {
    return 0.0;
  }
  long long total_jiffies = utime_diff + stime_diff;
  return (static_cast<double>(total_jiffies) / (m_clock_ticks_per_sec * elapsed_seconds)) * 100.0;
}