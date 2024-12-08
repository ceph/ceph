#include "ThreadMonitor.h"
#include "common/debug.h"
#include <unistd.h>
#include <sys/syscall.h>

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
}

void ThreadMonitor::deregister_thread(pid_t thread_id) {
  std::lock_guard<std::mutex> lock(monitored_threads_mutex);

  // Find and remove the thread from the map.
  auto it = monitored_threads.find(thread_id);
  if (it != monitored_threads.end()) {
    dout(20) << "Deregistering thread: '" << it->second.name << "' (TID: " << thread_id << ")" << dendl;
    monitored_threads.erase(it);
  } else {
    dout(20) << "Attempted to deregister unknown thread (TID: " << thread_id << ")." << dendl;
  }
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
    dout(5) << "Failed to retrieve system configuration"
            << "(clock ticks per second or page size). Monitoring will not start." << dendl;
    running = false;
    return;
  }

  while (running) {
    std::vector<pid_t> threads_to_remove;
    std::vector<std::pair<pid_t, pid_t>> serve_threads_to_remove;
    // Read process-level memory ONCE per cycle
    long long process_rss_pages = 0;
    bool process_memory_ok = read_process_statm(process_rss_pages);
    if (!process_memory_ok) {
      dout(5) << "Failed to read process memory info from /proc/self/statm." << dendl;
      process_rss_pages = 0;
    }
    
    std::lock_guard<std::mutex> lock(monitored_threads_mutex);
    for (auto& pair : monitored_threads) {
      pid_t tid = pair.first;
      MonitoredThreadInfo& Info_thread = pair.second;

      if (tid && !process_main_thread(tid)) {
        threads_to_remove.push_back(tid);
        continue;
      }
      if (Info_thread.serve_thread_id && !process_serve_thread(tid, Info_thread.serve_thread_id)) {
        serve_threads_to_remove.emplace_back(tid, Info_thread.serve_thread_id);
      }

      Info_thread.py_module->perfcounter->set(Info_thread.py_module->l_pym_mem_rss_current, process_rss_pages * m_page_size);
      long long rss_change = process_rss_pages * m_page_size - Info_thread.last_snapshot.rss_pages * m_page_size;
      Info_thread.py_module->perfcounter->set(Info_thread.py_module->l_pym_mem_rss_change, rss_change);
      Info_thread.last_snapshot.rss_pages = process_rss_pages;
      Info_thread.last_snapshot.timestamp = ceph::mono_clock::now();

      dout(20) << "Module '" << Info_thread.name << "' (TID: " << tid << "): "
                << "Memory RSS: " << process_rss_pages * m_page_size << " bytes"
                << ", Change: " << rss_change << " bytes" << dendl;
    }

    // --- Remove Exited Threads ---
    for (pid_t tid_to_remove : threads_to_remove) {
      deregister_thread(tid_to_remove);
    }
  
    for (const auto& [monitored_tid, serve_tid] : serve_threads_to_remove) {
      auto it = monitored_threads.find(monitored_tid);
      if (it != monitored_threads.end()) {
        it->second.serve_thread_id = 0; // Clear the Serve thread association
      }
    }

    std::this_thread::sleep_for(monitoring_interval);
  }
}

bool ThreadMonitor::process_main_thread(pid_t monitored_tid) {
  auto it = monitored_threads.find(monitored_tid);
  if (it == monitored_threads.end()) {
    dout(20) << "Monitored thread (TID: " << monitored_tid << ") not found." << dendl;
    return false;
  }
  MonitoredThreadInfo& info = it->second;

  long long current_utime, current_stime;
  auto current_timestamp = ceph::mono_clock::now();

  bool stat_ok = read_thread_stat(monitored_tid, current_utime, current_stime);
  if (!stat_ok) {
    dout(20) << "Thread (TID: " << monitored_tid << ") stats could not be read. It may have exited." << dendl;
    return false;
  }

  double elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                              current_timestamp - info.last_snapshot.timestamp).count();

  double cpu_percent = calculate_cpu_percentage(current_utime - info.last_snapshot.utime,
     current_stime - info.last_snapshot.stime, elapsed_seconds);

  dout(20) << "Module '" << info.name << "' (TID: " << monitored_tid << "): "
            << "CPU: " << std::fixed << std::setprecision(2) << cpu_percent << "%" << dendl;
  if (info.py_module->perfcounter) {
    auto & perfcounter = info.py_module->perfcounter;
    perfcounter->set(info.py_module->l_pym_cpu_usage, static_cast<uint64_t>(cpu_percent));
  }

  // Update the last snapshot for the next iteration's delta calculation.
  info.last_snapshot.utime = current_utime;
  info.last_snapshot.stime = current_stime;
  info.last_snapshot.timestamp = current_timestamp;
  return true;
}

bool ThreadMonitor::process_serve_thread(pid_t monitored_tid, pid_t serve_tid) {
  auto it = monitored_threads.find(monitored_tid);
  if (it == monitored_threads.end()) {
    dout(20) << "Monitored thread (TID: " << monitored_tid << ") not found." << dendl;
    return false;
  }
  MonitoredThreadInfo& info = it->second;
  if (info.serve_thread_id == 0 || info.serve_thread_id != serve_tid) {
    dout(20) << "No associated Serve thread for monitored thread (TID: " << monitored_tid << ")." << dendl;
    return false;
  }
  long long current_utime, current_stime;
  auto current_timestamp = ceph::mono_clock::now();
  bool stat_ok = read_thread_stat(serve_tid, current_utime, current_stime);
  if (!stat_ok) {
    dout(20) << "Serve thread (TID: " << serve_tid << ") stats could not be read. It may have exited." << dendl;
    return false;
  }
  double elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                              current_timestamp - info.last_serve_snapshot.timestamp).count();
  double cpu_percent = calculate_cpu_percentage(current_utime - info.last_serve_snapshot.utime,
     current_stime - info.last_serve_snapshot.stime, elapsed_seconds);
  dout(20) << "Serve Thread (TID: " << serve_tid << "): "
            << "CPU: " << std::fixed << std::setprecision(2) << cpu_percent << "%" << dendl;
  if (info.py_module->perfcounter) {
    auto & perfcounter = info.py_module->perfcounter;
    perfcounter->set(info.py_module->l_pym_serve_cpu_usage, static_cast<uint64_t>(cpu_percent));
  }
  // Update the last snapshot for the next iteration's delta calculation.
  info.last_serve_snapshot.utime = current_utime;
  info.last_serve_snapshot.stime = current_stime;
  info.last_serve_snapshot.timestamp = current_timestamp; 
  return true;
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
  size_t end = line.find(')', start);
  if (start == std::string::npos || end == std::string::npos) {
    dout(20) << __func__ << "Malformed stat file for TID " << tid << dendl;
    return false;
  }

  std::string remainder = line.substr(end + 2); // +2 to skip ") "
  std::stringstream ss(remainder);
  std::string val;
  for (int i = 0; i < 11; ++i) {  // Changed from 11 to 12
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
  if (elapsed_seconds <= 0) return 0.0;
  long long total_jiffies = utime_diff + stime_diff;
  return (static_cast<double>(total_jiffies) / (m_clock_ticks_per_sec * elapsed_seconds)) * 100.0;
}