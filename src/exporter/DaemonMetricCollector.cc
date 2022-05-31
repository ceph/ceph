#include "DaemonMetricCollector.h"
#include "common/admin_socket_client.h"
#include "common/hostname.h"
#include "common/perf_counters.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/split.h"
#include "include/common_fwd.h"
#include "util.h"

#include <boost/json/src.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <utility>

using json_object = boost::json::object;
using json_value = boost::json::value;
using json_array = boost::json::array;

void DaemonMetricCollector::request_loop(boost::asio::steady_timer &timer) {
  timer.async_wait([&](const boost::system::error_code &e) {
    std::cerr << e << std::endl;
    update_sockets();
    dump_asok_metrics();
    auto stats_period = g_conf().get_val<int64_t>("exporter_stats_period");
    // time to wait before sending requests again
    timer.expires_from_now(std::chrono::seconds(stats_period));
    request_loop(timer);
  });
}

void DaemonMetricCollector::main() {
  boost::asio::io_service io;
  boost::asio::steady_timer timer{io, std::chrono::seconds(0)};
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
  BlockTimer timer(__FILE__, __FUNCTION__);

  std::stringstream ss;
  std::vector<std::pair<std::string, int>> daemon_pids;
  for (auto &[daemon_name, sock_client] : clients) {
    bool ok;
    sock_client.ping(&ok);
    if (!ok) {
      continue;
    }
    std::string perf_dump_response = asok_request(sock_client, "perf dump");
    if (perf_dump_response.size() == 0) {
      continue;
    }
    std::string perf_schema_response = asok_request(sock_client, "perf schema");
    if (perf_schema_response.size() == 0) {
      continue;
    }
    std::string config_show = asok_request(sock_client, "config show");
    json_object pid_file_json = boost::json::parse(config_show).as_object();
    std::string pid_path =
        boost_string_to_std(pid_file_json["pid_file"].as_string());
    std::string pid_str = read_file_to_string(pid_path);
    std::cout << daemon_name << std::endl;
    std::cout << pid_str << std::endl;
    if (!pid_path.size()) {
      continue;
    }
    daemon_pids.push_back({daemon_name, std::stoi(pid_str)});
    json_object dump = boost::json::parse(perf_dump_response).as_object();
    json_object schema = boost::json::parse(perf_schema_response).as_object();
    for (auto &perf : schema) {
      std::string perf_group = perf.key().to_string();
      json_object perf_group_object = perf.value().as_object();
      for (auto &perf_counter : perf_group_object) {
        std::string perf_name = perf_counter.key().to_string();
        json_object perf_info = perf_counter.value().as_object();
        auto prio_limit = g_conf().get_val<int64_t>("exporter_prio_limit");
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
  // get time spent on this function
  timer.stop();
  std::string scrap_desc(
      "Time spent scraping and transforming perfcounters to metrics");
  std::string scrap_labels("host=");
  std::string host = ceph_get_hostname();
  scrap_labels += quote(host);
  add_metric(ss, timer.get_ms(), "ceph_exporter_scrape_time", scrap_desc,
             "gauge", scrap_labels);

  const std::lock_guard<std::mutex> lock(metrics_mutex);
  metrics = ss.str();
  metrics += get_process_metrics(daemon_pids);
}

std::vector<std::string> read_proc_stat_file(std::string path) {
  std::string stat = read_file_to_string(path);
  std::vector<std::string> strings;
  auto parts = ceph::split(stat);
  strings.assign(parts.begin(), parts.end());
  return strings;
}

struct pstat read_pid_stat(int pid) {
  std::string stat_path("/proc/" + std::to_string(pid) + "/stat");
  std::vector<std::string> stats = read_proc_stat_file(stat_path);
  struct pstat stat;
  stat.minflt = std::stoul(stats[9]);
  stat.majflt = std::stoul(stats[11]);
  stat.utime = std::stoul(stats[13]);
  stat.stime = std::stoul(stats[14]);
  stat.num_threads = std::stoul(stats[19]);
  stat.start_time = std::stoul(stats[21]);
  stat.vm_size = std::stoul(stats[22]);
  stat.resident_size = std::stoi(stats[23]);
  return stat;
}

std::string DaemonMetricCollector::get_process_metrics(
    std::vector<std::pair<std::string, int>> daemon_pids) {
  std::string path("/proc");
  std::stringstream ss;
  for (auto &[daemon_name, pid] : daemon_pids) {
    std::vector<std::string> uptimes = read_proc_stat_file("/proc/uptime");
    struct pstat stat = read_pid_stat(pid);
    int clk_tck = sysconf(_SC_CLK_TCK);
    double start_time_seconds = stat.start_time / (double)clk_tck;
    double total_time_seconds = (stat.utime + stat.stime) / (double)clk_tck;
    double uptime = std::stod(uptimes[0]);
    double elapsed_time = uptime - start_time_seconds;
    double usage = total_time_seconds * 100 / elapsed_time;

    std::string labels = "ceph_daemon=";
    labels += quote(daemon_name);
    add_metric(ss, stat.minflt, "ceph_exporter_minflt",
               "Number of minor page faults of daemon", "gauge", labels);
    add_metric(ss, stat.majflt, "ceph_exporter_majflt",
               "Number of major page faults of daemon", "gauge", labels);
    add_metric(ss, stat.num_threads, "ceph_exporter_num_threads",
               "Number of threads used by daemon", "gauge", labels);
    add_metric(ss, usage, "ceph_exporter_cpu_usage", "CPU usage of a daemon",
               "gauge", labels);
    add_metric(ss, stat.vm_size, "ceph_exporter_vm_size", "dec",
               "Virtual memory used in a daemon", labels);
    add_metric(ss, stat.resident_size, "ceph_exporter_resident_size",
               "Resident memory in a daemon", "gauge", labels);
  }
  return ss.str();
}

std::string DaemonMetricCollector::asok_request(AdminSocketClient &asok,
                                                std::string command) {
  std::string request("{\"prefix\": \"" + command + "\"}");
  std::string response;
  std::string err = asok.do_request(request, &response);
  if (err.length() > 0 || response.substr(0, 5) == "ERROR") {
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
  std::string sock_dir = g_conf().get_val<std::string>("exporter_sock_dir");
  clients.clear();
  for (const auto &entry :
       std::filesystem::directory_iterator(sock_dir)) {
    if (entry.path().extension() == ".asok") {
      std::string daemon_socket_name = entry.path().filename().string();
      std::string daemon_name =
          daemon_socket_name.substr(0, daemon_socket_name.size() - 5);
      if (clients.find(daemon_name) == clients.end() &&
          !(daemon_name.find("mgr") != std::string::npos)) {
        AdminSocketClient sock(entry.path().string());
        clients.insert({daemon_name, std::move(sock)});
      }
    }
  }
}

DaemonMetricCollector &collector_instance() {
  static DaemonMetricCollector instance;
  return instance;
}
