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

static const int STATM_SIZE = 0;
static const int STATM_RESIDENT = 1;
static const int STAT_UTIME = 13;
static const int STAT_STIME = 14;
static const int STAT_START_TIME = 21;
static const int UPTIME_SYSTEM = 0;

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
