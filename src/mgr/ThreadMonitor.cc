#include "ThreadMonitor.h"
#include "common/debug.h"
#include <unistd.h>
#include <sys/syscall.h>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr[ThreadMonitor] " << __func__ << " "

ThreadMonitor::ThreadMonitor() : running(false) {
  dout(20) << "Initializing." << dendl;
  m_clock_ticks_per_sec = sysconf(_SC_CLK_TCK);
  m_page_size = sysconf(_SC_PAGESIZE);
}

ThreadMonitor::~ThreadMonitor() {
  stop_monitoring();
}

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

void ThreadMonitor::register_thread(pid_t thread_id,
  const std::string& thread_name,
  PyModuleRef py_module_) {

  std::lock_guard<std::mutex> lock(monitored_threads_mutex);

  if (monitored_threads.count(thread_id)) {
    dout(20) << "ThreadMonitor: Attempted to register already known thread (TID: " << thread_id << ")." << dendl;
    return;
  }

  MonitoredThreadInfo info;
  info.name = thread_name;
  info.py_module = py_module_;

  if (!read_thread_stat(thread_id, info.last_snapshot.utime, info.last_snapshot.stime) ||
      !read_thread_statm(thread_id, info.last_snapshot.rss_pages)) {
    dout(20) << "ThreadMonitor: Initial stats for thread (TID: " << thread_id
             << ") could not be read. It might not exist or be accessible." 
             << dendl;
  }
  info.last_snapshot.timestamp = ceph::mono_clock::now();

  // Add the new thread to the map of monitored threads.
  monitored_threads[thread_id] = info;
  dout(20) << "ThreadMonitor: Registered new thread: '" << thread_name << "' (TID: " << thread_id << ")" << dendl;
}

void ThreadMonitor::deregister_thread(pid_t thread_id) {
  std::lock_guard<std::mutex> lock(monitored_threads_mutex);

  // Find and remove the thread from the map.
  auto it = monitored_threads.find(thread_id);
  if (it != monitored_threads.end()) {
    dout(20) << "ThreadMonitor: Deregistering thread: '" << it->second.name << "' (TID: " << thread_id << ")" << dendl;
    monitored_threads.erase(it);
  } else {
    dout(20) << "ThreadMonitor: Attempted to deregister unknown thread (TID: " << thread_id << ")." << dendl;
  }
}

void ThreadMonitor::monitoring_loop() {
  dout(20) << "ThreadMonitor: Monitoring loop started." << dendl;
  
  if (m_clock_ticks_per_sec <= 0 || m_page_size <= 0) {
    dout(5) << "ThreadMonitor: Failed to retrieve system configuration"
            << "(clock ticks per second or page size). Monitoring will not start." << dendl;
    running = false;
    return;
  }

  // Main monitoring loop, runs as long as 'running' is true.
  while (running) {
    // Create a copy of the map of monitored threads to iterate over.
    // This allows us to release the mutex quickly and avoid blocking
    // register/deregister calls for the duration of the monitoring loop.
    std::map<pid_t, MonitoredThreadInfo> current_monitored_threads_copy;
    {
        std::lock_guard<std::mutex> lock(monitored_threads_mutex);
        current_monitored_threads_copy = monitored_threads;
    }

    // Vector to store TIDs of threads that are no longer active and need removal.
    std::vector<pid_t> threads_to_remove;

    // Iterate through the copied list of threads.
    for (auto& pair : current_monitored_threads_copy) {
      pid_t tid = pair.first;
      MonitoredThreadInfo& info_in_copy = pair.second;

      long long current_utime, current_stime, current_rss_pages;
      auto current_timestamp = ceph::mono_clock::now();

      // Read current statistics from /proc filesystem.
      bool stat_ok = read_thread_stat(tid, current_utime, current_stime);
      bool statm_ok = read_thread_statm(tid, current_rss_pages);

      if (!stat_ok || !statm_ok) {
        // If /proc files are unreadable, the thread likely exited.
        // Mark it for removal from monitoring.
        dout(20) << "ThreadMonitor: Thread (TID: " << tid << ") stats could not be read. It may have exited." << dendl;
        threads_to_remove.push_back(tid);
        continue;
      }

      // --- Calculate Performance Metrics ---

      // Calculate CPU time difference (user + system)
      long long diff_utime = current_utime - info_in_copy.last_snapshot.utime;
      long long diff_stime = current_stime - info_in_copy.last_snapshot.stime;
      long long total_cpu_jiffies = diff_utime + diff_stime;

      // Calculate elapsed real time between snapshots
      double elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                                  current_timestamp - info_in_copy.last_snapshot.timestamp).count();

      double cpu_percent = 0.0;
      if (elapsed_seconds > 0) {
        // CPU percentage = (CPU jiffies used / total jiffies in interval) * 100
        // For a single thread, this represents its share of a single CPU core.
        cpu_percent = (static_cast<double>(total_cpu_jiffies) / (m_clock_ticks_per_sec * elapsed_seconds)) * 100.0;
      }

      // Calculate Resident Set Size (RSS) change in bytes
      long long rss_diff_bytes = (current_rss_pages - info_in_copy.last_snapshot.rss_pages) * m_page_size;

      // --- Report Metrics ---
      dout(20) << "[ThreadMonitor] Module '" << info_in_copy.name << "' (TID: " << tid << "): "
                << "CPU: " << std::fixed << std::setprecision(2) << cpu_percent << "% | "
                << "RSS Change: " << rss_diff_bytes / (1024.0 * 1024.0) << " MB | " // Report change in MB
                << "Current RSS: " << (static_cast<long long>(current_rss_pages) * m_page_size) / (1024.0 * 1024.0) << " MB" // Report current RSS in MB
                << dendl;
      if (info_in_copy.py_module->perfcounter) {
        auto & perfcounter = info_in_copy.py_module->perfcounter;
        perfcounter->set(info_in_copy.py_module->l_pym_cpu_usage, static_cast<uint64_t>(cpu_percent));
        perfcounter->set(info_in_copy.py_module->l_pym_mem_rss_change, static_cast<uint64_t>(rss_diff_bytes));
        perfcounter->set(info_in_copy.py_module->l_pym_mem_rss_current,
                      static_cast<uint64_t>(current_rss_pages * m_page_size));
      }

      // --- Update the Original Monitored Threads Map ---
      // Re-acquire the mutex to update the original map with the latest snapshot.
      std::lock_guard<std::mutex> lock(monitored_threads_mutex);
      auto original_it = monitored_threads.find(tid);
      if (original_it != monitored_threads.end()) {
        // Update the last snapshot for the next iteration's delta calculation.
        original_it->second.last_snapshot.utime = current_utime;
        original_it->second.last_snapshot.stime = current_stime;
        original_it->second.last_snapshot.rss_pages = current_rss_pages;
        original_it->second.last_snapshot.timestamp = current_timestamp;
      }
          // If original_it is not found, it means the thread was deregistered
          // by another thread between our copy and this update. That's fine.
      }

      // --- Remove Exited Threads ---
      // Iterate through threads marked for removal and deregister them.
      for (pid_t tid_to_remove : threads_to_remove) {
          deregister_thread(tid_to_remove); // This call acquires its own mutex.
      }

      // --- Wait for Next Interval ---
      // Pause the monitoring thread for the defined interval.
      // This is the core of the periodic monitoring.
      std::this_thread::sleep_for(monitoring_interval);
  }
}

bool ThreadMonitor::read_thread_stat(pid_t tid, long long& utime, long long& stime) {
  std::string stat_path = "/proc/self/task/" + std::to_string(tid) + "/stat";
  std::ifstream stat_file(stat_path);

  if (!stat_file.is_open()) {
    return false;
  }

  std::string line;
  std::getline(stat_file, line);
  std::stringstream ss(line);

  std::string val;
  for (int i = 0; i < 13; ++i) {
    ss >> val;
  }
  ss >> utime >> stime;

  return true;
}

bool ThreadMonitor::read_thread_statm(pid_t tid, long long& rss_pages) {
  std::string statm_path = "/proc/self/task/" + std::to_string(tid) + "/statm";
  std::ifstream statm_file(statm_path);

  if (!statm_file.is_open()) {
    return false;
  }
  long long temp_size;
  statm_file >> temp_size >> rss_pages;
  return true;
}