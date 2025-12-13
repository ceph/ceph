#pragma once
#include "common/config_obs.h"
#include "PyModule.h"
#include <map>
#include <memory>
#include <string>
#include <sys/types.h>
#include <thread>

class ThreadMonitor : public md_config_obs_t {
public:
  ThreadMonitor(CephContext *cct) : m_cct(cct), running(false) {
    m_cct->_conf.add_observer(this);
    m_clock_ticks_per_sec = sysconf(_SC_CLK_TCK);
    m_page_size = sysconf(_SC_PAGESIZE);
  }
  ~ThreadMonitor() {
    m_cct->_conf.remove_observer(this);
    stop_monitoring();
  }

  void start_monitoring();
  void stop_monitoring();
  void register_thread(const pid_t thread_id, const pid_t serve_thread_id, const std::string& name, const PyModuleRef py_module);
  void deregister_thread(pid_t thread_id);

protected:
  std::vector<std::string> get_tracked_keys() const noexcept override {
    return std::vector<std::string>{"mgr_module_monitor_interval"};
  }
  void handle_conf_change(const ConfigProxy& conf,
    const std::set<std::string> &changed) override;
private:
  CephContext *m_cct;
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
    pid_t serve_thread_id = 0; // TID of the thread runner
    ThreadSnapshot last_serve_snapshot; // Last snapshot of the thread runner
  };

  std::map<pid_t, MonitoredThreadInfo> monitored_threads;

  std::mutex monitored_threads_mutex;
  std::atomic<bool> running;
  std::unique_ptr<std::thread> monitor_thread;
  ceph::mono_clock::duration monitoring_interval = std::chrono::seconds(2);
  long m_clock_ticks_per_sec = 0;
  long m_page_size = 0; 

  void monitoring_loop();
  bool read_thread_stat(pid_t tid, long long& utime, long long& stime);
  bool read_process_statm(long long& rss_pages);
  long get_clock_ticks_per_sec() const;
  long get_page_size() const;
  double calculate_cpu_percentage(long long utime_diff, long long stime_diff, double elapsed_seconds);
  bool process_main_thread(pid_t monitored_tid);
  bool process_serve_thread(pid_t monitored_tid, pid_t serve_tid);
};