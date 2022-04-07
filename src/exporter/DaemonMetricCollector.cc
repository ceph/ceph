#include "DaemonMetricCollector.h"
#include "common/admin_socket_client.h"
#include "include/common_fwd.h"
#include "common/perf_counters.h"

#include <string>
#include <iostream>
#include <filesystem>
#include <regex>
#include <boost/json/src.hpp>

using json_object = boost::json::object;
using json_value = boost::json::value;
using json_array = boost::json::array;

void DaemonMetricCollector::request_loop(boost::asio::deadline_timer &timer) {
  timer.async_wait([&](const boost::system::error_code& e) {
    std::cerr << e << std::endl;
    update_sockets();
    send_requests();
    timer.expires_from_now(boost::posix_time::seconds(stats_period));
    request_loop(timer);
  });
}

void DaemonMetricCollector::main() {
  stats_period = 5; // TODO: let's do 5 for now and expose this to change in the future
  boost::asio::io_service io;
  boost::asio::deadline_timer timer(io, boost::posix_time::seconds(stats_period));
  request_loop(timer);
  io.run();
}

std::string DaemonMetricCollector::get_metrics() {
  return metrics;
}

template <class T>
void add_metric(std::stringstream &ss, T value, std::string name, std::string description,
                                       std::string mtype, std::string labels) {
  ss << "# HELP " << name << " " << description << "\n";
  ss << "# TYPE " << name << " " << mtype << "\n";
  ss << name << "{" << labels << "} " << value << "\n";
}

void add_double_or_int_metric(std::stringstream &ss, json_value value, std::string name, std::string description,
                                                     std::string mtype, std::string labels) {
  if (value.is_int64()) {
    int64_t v = value.as_int64();
    add_metric(ss, v, name, description, mtype, labels);
  }
  else if (value.is_double()) {
    double v = value.as_double();
    add_metric(ss, v, name, description, mtype, labels);
  }
}

std::string boost_string_to_std(boost::json::string js) {
  std::string res(js.data());
  return res;
}

std::string quote(std::string value) {
	return "\"" + value + "\"";
}

void DaemonMetricCollector::send_requests() {
  std::string result;
  for(auto client : clients) {
    AdminSocketClient &sock_client = client.second;
    std::string daemon_name = client.first;
    std::string request("{\"prefix\":\"perf dump\"}");
    std::string response;
    sock_client.do_request(request, &response);
    if (response.size() > 0) {
      json_object dump = boost::json::parse(response).as_object();
      request = "{\"prefix\":\"perf schema\"}";
      response = "";
      sock_client.do_request(request, &response);
      json_object schema = boost::json::parse(response).as_object();
      for (auto perf : schema) {
        std::string perf_group = perf.key().to_string();
        json_object perf_group_object = perf.value().as_object();
        for (auto perf_counter : perf_group_object) {
          std::string perf_name = perf_counter.key().to_string();
          json_object perf_info = perf_counter.value().as_object();
          if (perf_info["priority"].as_int64() < PerfCountersBuilder::PRIO_USEFUL) {
            continue;
          }
          int64_t type = perf_info["type"].as_int64();
          std::string mtype = boost_string_to_std(perf_info["metric_type"].as_string());
          std::string description = boost_string_to_std(perf_info["description"].as_string());
          std::stringstream ss;
          std::string name = "ceph_" + perf_group + "_" + perf_name;

          std::string labels;
          // Labels
          // FIXME: test this, based on mgr_module perfpath_to_path_labels
          if (daemon_name.substr(0, 4) == "rgw.") {
            labels = std::string("instance_id=") + quote(daemon_name.substr(4, std::string::npos));
          } else {
            labels = "ceph_daemon=" + quote(daemon_name);
            if (daemon_name.substr(0, 11) == "rbd-mirror.") {
              std::regex re("^rbd_mirror_image_([^/]+)/(?:(?:([^/]+)/)?)(.*)\\.(replay(?:_bytes|_latency)?)$");
              std::smatch match;
              if (std::regex_search(daemon_name, match, re) == true) {
                name = "ceph_rbd_mirror_image_" + match.str(4);
                labels += ",pool=" + quote(match.str(1));
                labels += ",namespace=" + quote(match.str(2));
								labels += ",image=" + quote(match.str(3));
              }
            }
          }

          // value and add_metric
          json_value perf_values = dump[perf_group].as_object()[perf_name];
          if (type & PERFCOUNTER_LONGRUNAVG) {
            int64_t count = perf_values.as_object()["avgcount"].as_int64();
            add_metric(ss, count, name + "_count", description, mtype, labels);
            json_value sum_value = perf_values.as_object()["sum"];
            add_double_or_int_metric(ss, sum_value, name + "_sum", description, mtype, labels);
          } else if(type & PERFCOUNTER_TIME) {
            if (perf_values.is_int64()) {
              double value = perf_values.as_int64() / 1000000000.0f;
              add_metric(ss, value, name, description, mtype, labels);
            }
            else if (perf_values.is_double()) {
              double value = perf_values.as_double() / 1000000000.0f;
              add_metric(ss, value, name, description, mtype, labels);
            }
          } else {
            add_double_or_int_metric(ss, perf_values, name, description, mtype, labels);
          }
          result += ss.str();
        }
      }
    }
  }
  metrics = result;
}

void DaemonMetricCollector::update_sockets() {
  std::string path = "/var/run/ceph/";
  for (const auto & entry : std::filesystem::recursive_directory_iterator(path)) {
    if (entry.path().extension() == ".asok") {
      std::string daemon_socket_name = entry.path().filename().string();
      // remove .asok
      std::string daemon_name = daemon_socket_name.substr(0, daemon_socket_name.size() - 5);
      if (clients.find(daemon_name) == clients.end()) {
        AdminSocketClient sock(entry.path().string());
        clients.insert({daemon_name, std::move(sock)});
      }
    }
  }
}

DaemonMetricCollector *_collector_instance = nullptr;

DaemonMetricCollector& collector_instance() {
  if (_collector_instance == nullptr) {
    _collector_instance = new DaemonMetricCollector();
  }
  return *_collector_instance;
}
