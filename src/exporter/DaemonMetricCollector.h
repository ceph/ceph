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

class DaemonMetricCollector {
public:
  void main();
  void set_sock_dir(std::string sock_path);
  std::string get_metrics();
  std::string SOCKETDIR = "/var/run/ceph/";

private:
  std::map<std::string, AdminSocketClient> clients;
  std::string metrics;
  std::mutex metrics_mutex;
  int stats_period;     // time to wait before sending requests again
  void update_sockets();
  void request_loop(boost::asio::steady_timer &timer);

  void dump_asok_metrics();
  void dump_asok_metric(std::stringstream &ss, boost::json::object perf_info,
                        boost::json::value perf_values, std::string name,
                        std::string labels);
  std::pair<std::string, std::string>
  get_labels_and_metric_name(std::string daemon_name, std::string metric_name);
  std::string asok_request(AdminSocketClient &asok, std::string command);
};

DaemonMetricCollector &collector_instance();
