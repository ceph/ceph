#pragma once

#include "common/admin_socket_client.h"
#include <map>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/json/object.hpp>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

struct pstat {
  unsigned long utime;
  unsigned long stime;
  unsigned long minflt;
  unsigned long majflt;
  unsigned long start_time;
  int num_threads;
  unsigned long vm_size;
  int resident_size;
};

class DaemonMetricCollector {
public:
  void main();
  std::string get_metrics();

private:
  std::map<std::string, AdminSocketClient> clients;
  std::string metrics;
  std::mutex metrics_mutex;
  void update_sockets();
  void request_loop(boost::asio::steady_timer &timer);

  void dump_asok_metrics();
  void dump_asok_metric(std::stringstream &ss, boost::json::object perf_info,
                        boost::json::value perf_values, std::string name,
                        std::string labels);
  std::pair<std::string, std::string>
  get_labels_and_metric_name(std::string daemon_name, std::string metric_name);
  std::string get_process_metrics(std::vector<std::pair<std::string, int>> daemon_pids);
  std::string asok_request(AdminSocketClient &asok, std::string command);
};

DaemonMetricCollector &collector_instance();
