#pragma once
#include "PyModule.h"
#include <map>
#include <memory>
#include <string>
#include <sys/types.h>
#include <thread>

class ThreadMonitor {
public:
  ThreadMonitor();
  ~ThreadMonitor();

  void start_monitoring();
  void stop_monitoring();
  void register_thread(pid_t thread_id, const std::string& thread_name, PyModuleRef py_module_);
  void deregister_thread(pid_t thread_id);

private:

  struct ThreadSnapshot {
    long long utime = 0;
    long long stime = 0;
    long long rss_pages = 0;
    ceph::mono_clock::time_point timestamp;
  };

  // Information about a thread that is currently being monitored
  struct MonitoredThreadInfo {
    std::string name;
    PyModuleRef py_module;
    ThreadSnapshot last_snapshot;
  };

  // Map to store information about all currently monitored threads.
  // Key: Linux TID (pid_t), Value: MonitoredThreadInfo struct
  std::map<pid_t, MonitoredThreadInfo> monitored_threads;

  std::mutex monitored_threads_mutex;
  std::atomic<bool> running;
  std::unique_ptr<std::thread> monitor_thread;
  ceph::mono_clock::duration monitoring_interval = std::chrono::seconds(2);
  long m_clock_ticks_per_sec = 0;
  long m_page_size = 0; 

  void monitoring_loop();
  bool read_thread_stat(pid_t tid, long long& utime, long long& stime);
  bool read_thread_statm(pid_t tid, long long& rss_pages);
  long get_clock_ticks_per_sec() const;
  long get_page_size() const;
};