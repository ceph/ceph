#include "DaemonMetricCollector.h"
#include "common/admin_socket_client.h"
#include "common/perf_counters.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "include/common_fwd.h"

#include <boost/json/src.hpp>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <utility>

using json_object = boost::json::object;
using json_value = boost::json::value;
using json_array = boost::json::array;

void DaemonMetricCollector::request_loop(boost::asio::steady_timer &timer) {
  timer.async_wait([&](const boost::system::error_code &e) {
    std::cerr << e << std::endl;
    const auto start = std::chrono::system_clock::now();
    update_sockets();
    dump_asok_metrics();
    const std::chrono::duration<double> end = std::chrono::system_clock::now() - start;
    timer.expires_from_now(std::chrono::seconds(std::max(stats_period, (int)std::round(end.count()))));
    request_loop(timer);
  });
}

void DaemonMetricCollector::set_sock_dir() {
  SOCKETDIR = g_conf().get_val<std::string>("sock_dir");
}

void DaemonMetricCollector::main() {
  stats_period = g_conf().get_val<int64_t>("exporter_stats_period");;
  boost::asio::io_service io;
  boost::asio::steady_timer timer{io, std::chrono::seconds(stats_period)};
  request_loop(timer);
  io.run();
}

std::string DaemonMetricCollector::get_metrics() {
  const std::lock_guard<std::mutex> lock(metrics_mutex);
  return metrics;
}

template <class T>
void add_metric(std::stringstream &ss, T value, std::string name,
                std::string description, std::string mtype,
                std::string labels) {
  ss << "# HELP " << name << " " << description << "\n";
  ss << "# TYPE " << name << " " << mtype << "\n";
  ss << name << "{" << labels << "} " << value << "\n";
}

void add_double_or_int_metric(std::stringstream &ss, json_value value,
                              std::string name, std::string description,
                              std::string mtype, std::string labels) {
  if (value.is_int64()) {
    int64_t v = value.as_int64();
    add_metric(ss, v, name, description, mtype, labels);
  } else if (value.is_double()) {
    double v = value.as_double();
    add_metric(ss, v, name, description, mtype, labels);
  }
}

std::string boost_string_to_std(boost::json::string js) {
  std::string res(js.data());
  return res;
}

std::string quote(std::string value) { return "\"" + value + "\""; }

bool is_hyphen(char ch) { return ch == '-'; }

void DaemonMetricCollector::dump_asok_metrics() {
  std::stringstream ss;
  for (auto &[daemon_name, sock_client] : clients) {
    std::string perf_dump_response = asok_request(sock_client, "perf dump");
    if (perf_dump_response.size() == 0) {
      continue;
    }
    std::string perf_schema_response = asok_request(sock_client, "perf schema");
    if (perf_schema_response.size() == 0) {
      continue;
    }
    json_object dump = boost::json::parse(perf_dump_response).as_object();
    json_object schema = boost::json::parse(perf_schema_response).as_object();
    for (auto &perf : schema) {
      std::string perf_group = perf.key().to_string();
      json_object perf_group_object = perf.value().as_object();
      for (auto &perf_counter : perf_group_object) {
        std::string perf_name = perf_counter.key().to_string();
        json_object perf_info = perf_counter.value().as_object();
        int prio_limit = g_conf().get_val<int64_t>("exporter_prio_limit");
        // std::cout << prio_limit << std::endl;
        if (perf_info["priority"].as_int64() <
            prio_limit) {
          continue;
        }
        std::string name = "ceph_" + perf_group + "_" + perf_name;
        std::replace_if(name.begin(), name.end(), is_hyphen, '_');

        // FIXME: test this, based on mgr_module perfpath_to_path_labels
        auto labels_and_name = get_labels_and_metric_name(daemon_name, name);
        std::string labels = labels_and_name.first;
        name = labels_and_name.second;

        json_value perf_values = dump[perf_group].as_object()[perf_name];
        dump_asok_metric(ss, perf_info, perf_values, name, labels);
      }
    }
  }
  const std::lock_guard<std::mutex> lock(metrics_mutex);
  metrics = ss.str();
}

std::string DaemonMetricCollector::asok_request(AdminSocketClient &asok,
                                                std::string command) {
  std::string request("{\"prefix\": \"" + command + "\"}");
  std::string response;
  std::string err = asok.do_request(request, &response);
  if (err.length() > 0) {
    return "";
  }
  return response;
}

std::pair<std::string, std::string>
DaemonMetricCollector::get_labels_and_metric_name(std::string daemon_name,
                                                  std::string metric_name) {
  std::string labels, new_metric_name;
  new_metric_name = metric_name;
  if (daemon_name.find("rgw") != std::string::npos) {
    std::string tmp = daemon_name.substr(16, std::string::npos);
    std::string::size_type pos = tmp.find('.');
    labels = std::string("instance_id=") + quote("rgw." + tmp.substr(0, pos));
  } else {
    labels = "ceph_daemon=" + quote(daemon_name);
    if (daemon_name.find("rbd-mirror") != std::string::npos) {
      std::regex re("^rbd_mirror_image_([^/]+)/(?:(?:([^/]+)/"
                    ")?)(.*)\\.(replay(?:_bytes|_latency)?)$");
      std::smatch match;
      if (std::regex_search(daemon_name, match, re) == true) {
        new_metric_name = "ceph_rbd_mirror_image_" + match.str(4);
        labels += ",pool=" + quote(match.str(1));
        labels += ",namespace=" + quote(match.str(2));
        labels += ",image=" + quote(match.str(3));
      }
    }
  }
  return {labels, new_metric_name};
}

/*
perf_values can be either a int/double or a json_object. Since
   json_value is a wrapper of both we use that class.
 */
void DaemonMetricCollector::dump_asok_metric(std::stringstream &ss,
                                             json_object perf_info,
                                             json_value perf_values,
                                             std::string name,
                                             std::string labels) {
  int64_t type = perf_info["type"].as_int64();
  std::string metric_type =
      boost_string_to_std(perf_info["metric_type"].as_string());
  std::string description =
      boost_string_to_std(perf_info["description"].as_string());

  if (type & PERFCOUNTER_LONGRUNAVG) {
    int64_t count = perf_values.as_object()["avgcount"].as_int64();
    add_metric(ss, count, name + "_count", description, metric_type, labels);
    json_value sum_value = perf_values.as_object()["sum"];
    add_double_or_int_metric(ss, sum_value, name + "_sum", description,
                             metric_type, labels);
  } else if (type & PERFCOUNTER_TIME) {
    if (perf_values.is_int64()) {
      double value = perf_values.as_int64() / 1000000000.0f;
      add_metric(ss, value, name, description, metric_type, labels);
    } else if (perf_values.is_double()) {
      double value = perf_values.as_double() / 1000000000.0f;
      add_metric(ss, value, name, description, metric_type, labels);
    }
  } else {
    add_double_or_int_metric(ss, perf_values, name, description, metric_type,
                             labels);
  }
}
void DaemonMetricCollector::update_sockets() {
  // std:: cout << SOCKETDIR << std::endl;
  clients.clear();
  for (const auto &entry :
       std::filesystem::directory_iterator(SOCKETDIR)) {
    if (entry.path().extension() == ".asok") {
      std::string daemon_socket_name = entry.path().filename().string();
      std::string daemon_name =
          daemon_socket_name.substr(0, daemon_socket_name.size() - 5);
      if (clients.find(daemon_name) == clients.end() &&
          !(daemon_name.find("mgr") != std::string::npos) && 
          !(daemon_name.find("client") != std::string::npos)) {
        AdminSocketClient sock(entry.path().string());
        std::cout << entry.path().string() << std::endl;
        clients.insert({daemon_name, std::move(sock)});
      }
    }
  }
}

DaemonMetricCollector &collector_instance() {
  static DaemonMetricCollector instance;
  return instance;
}
