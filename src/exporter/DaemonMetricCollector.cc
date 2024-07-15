#include "DaemonMetricCollector.h"

#include <boost/asio/io_context.hpp>
#include <boost/json/src.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <utility>

#include "common/admin_socket_client.h"
#include "common/debug.h"
#include "common/hostname.h"
#include "common/perf_counters.h"
#include "common/split.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/common_fwd.h"
#include "util.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ceph_exporter

using json_object = boost::json::object;
using json_value = boost::json::value;
using json_array = boost::json::array;

void DaemonMetricCollector::request_loop(boost::asio::steady_timer &timer) {
  timer.async_wait([&](const boost::system::error_code &e) {
    std::cerr << e << std::endl;
    update_sockets();

    bool sort_metrics = g_conf().get_val<bool>("exporter_sort_metrics");
    auto prio_limit = g_conf().get_val<int64_t>("exporter_prio_limit");
    std::string dump_response;
    std::string schema_response;
    dump_asok_metrics(sort_metrics, prio_limit, true, dump_response, schema_response, true);
    auto stats_period = g_conf().get_val<int64_t>("exporter_stats_period");
    // time to wait before sending requests again
    timer.expires_from_now(std::chrono::seconds(stats_period));
    request_loop(timer);
  });
}

void DaemonMetricCollector::main() {
  // time to wait before sending requests again

  boost::asio::io_context io;
  boost::asio::steady_timer timer{io, std::chrono::seconds(0)};
  request_loop(timer);
  io.run();
}

std::string DaemonMetricCollector::get_metrics() {
  const std::lock_guard<std::mutex> lock(metrics_mutex);
  return metrics;
}

template <class T>
void add_metric(std::unique_ptr<MetricsBuilder> &builder, T value,
                std::string name, std::string description, std::string mtype,
                labels_t labels) {
  builder->add(std::to_string(value), name, description, mtype, labels);
}

void add_double_or_int_metric(std::unique_ptr<MetricsBuilder> &builder,
                              json_value value, std::string name,
                              std::string description, std::string mtype,
                              labels_t labels) {
  if (value.is_int64()) {
    int64_t v = value.as_int64();
    add_metric(builder, v, name, description, mtype, labels);
  } else if (value.is_double()) {
    double v = value.as_double();
    add_metric(builder, v, name, description, mtype, labels);
  }
}

std::string boost_string_to_std(boost::json::string js) {
  std::string res(js.data());
  return res;
}

std::string quote(std::string value) { return "\"" + value + "\""; }

void DaemonMetricCollector::parse_asok_metrics(
    std::string &counter_dump_response, std::string &counter_schema_response,
    int64_t prio_limit, const std::string &daemon_name) {
  json_object counter_dump =
      boost::json::parse(counter_dump_response).as_object();
  json_object counter_schema =
      boost::json::parse(counter_schema_response).as_object();

  for (auto &perf_group_item : counter_schema) {
    std::string perf_group = {perf_group_item.key().begin(),
                              perf_group_item.key().end()};
    json_array perf_group_schema_array = perf_group_item.value().as_array();
    json_array perf_group_dump_array = counter_dump[perf_group].as_array();
    for (auto schema_itr = perf_group_schema_array.begin(),
              dump_itr = perf_group_dump_array.begin();
         schema_itr != perf_group_schema_array.end() &&
         dump_itr != perf_group_dump_array.end();
         ++schema_itr, ++dump_itr) {
      auto counters = schema_itr->at("counters").as_object();
      auto counters_labels = schema_itr->at("labels").as_object();
      auto counters_values = dump_itr->at("counters").as_object();
      labels_t labels;

      for (auto &label : counters_labels) {
        std::string label_key = {label.key().begin(), label.key().end()};
        labels[label_key] = quote(label.value().as_string().c_str());
      }
      for (auto &counter : counters) {
        json_object counter_group = counter.value().as_object();
        if (counter_group["priority"].as_int64() < prio_limit) {
          continue;
        }
        std::string counter_name_init = {counter.key().begin(),
                                         counter.key().end()};
        std::string counter_name = perf_group + "_" + counter_name_init;
        promethize(counter_name);

        auto extra_labels = get_extra_labels(daemon_name);
        if (extra_labels.empty()) {
          dout(1) << "Unable to parse instance_id from daemon_name: "
                  << daemon_name << dendl;
          continue;
        }
        labels.insert(extra_labels.begin(), extra_labels.end());

        // For now this is only required for rgw multi-site metrics
        auto multisite_labels_and_name = add_fixed_name_metrics(counter_name);
        if (!multisite_labels_and_name.first.empty()) {
          labels.insert(multisite_labels_and_name.first.begin(),
                        multisite_labels_and_name.first.end());
          counter_name = multisite_labels_and_name.second;
        }
        auto perf_values = counters_values.at(counter_name_init);
        dump_asok_metric(counter_group, perf_values, counter_name, labels);
      }
    }
  }
}


void DaemonMetricCollector::dump_asok_metrics(bool sort_metrics, int64_t counter_prio,
                                              bool sockClientsPing, std::string &dump_response,
                                              std::string &schema_response,
                                              bool config_show_response) {
  BlockTimer timer(__FILE__, __FUNCTION__);

  std::vector<std::pair<std::string, int>> daemon_pids;

  int failures = 0;
  if (sort_metrics) {
    builder =
        std::unique_ptr<OrderedMetricsBuilder>(new OrderedMetricsBuilder());
  } else {
    builder =
        std::unique_ptr<UnorderedMetricsBuilder>(new UnorderedMetricsBuilder());
  }
  auto prio_limit = counter_prio;
  for (auto &[daemon_name, sock_client] : clients) {
    if (sockClientsPing) {
      bool ok;
      sock_client.ping(&ok);
      if (!ok) {
        failures++;
        continue;
      } 
    }
    std::string counter_dump_response = dump_response.size() > 0 ? dump_response :
      asok_request(sock_client, "counter dump", daemon_name);
    if (counter_dump_response.size() == 0) {
        failures++;
        continue;
    }
    std::string counter_schema_response = schema_response.size() > 0 ? schema_response :
        asok_request(sock_client, "counter schema", daemon_name);
    if (counter_schema_response.size() == 0) {
      failures++;
      continue;
    }

    try {
      parse_asok_metrics(counter_dump_response, counter_schema_response,
                         prio_limit, daemon_name);

      std::string config_show = !config_show_response ? "" :
        asok_request(sock_client, "config show", daemon_name);
      if (config_show.size() == 0) {
        failures++;
        continue;
      }
      json_object pid_file_json = boost::json::parse(config_show).as_object();
      std::string pid_path =
          boost_string_to_std(pid_file_json["pid_file"].as_string());
      std::string pid_str = read_file_to_string(pid_path);
      if (!pid_path.size()) {
        dout(1) << "pid path is empty; process metrics won't be fetched for: "
                << daemon_name << dendl;
      }
      if (!pid_str.empty()) {
        daemon_pids.push_back({daemon_name, std::stoi(pid_str)});
      }
    } catch (const std::invalid_argument &e) {
      failures++;
      dout(1) << "failed to handle " << daemon_name << ": " << e.what()
              << dendl;
      continue;
    } catch (const std::runtime_error &e) {
      failures++;
      dout(1) << "failed to parse json for " << daemon_name << ": " << e.what()
              << dendl;
      continue;
    }
  }
  dout(10) << "Perf counters retrieved for " << clients.size() - failures << "/"
           << clients.size() << " daemons." << dendl;
  // get time spent on this function
  timer.stop();
  std::string scrap_desc(
      "Time spent scraping and transforming perf counters to metrics");
  labels_t scrap_labels;
  scrap_labels["host"] = quote(ceph_get_hostname());
  scrap_labels["function"] = quote(__FUNCTION__);
  add_metric(builder, timer.get_ms(), "ceph_exporter_scrape_time", scrap_desc,
             "gauge", scrap_labels);

  const std::lock_guard<std::mutex> lock(metrics_mutex);
  // only get metrics if there's pid path for some or all daemons isn't empty
  if (daemon_pids.size() != 0) {
    get_process_metrics(daemon_pids);
  }
  metrics = builder->dump();
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

void DaemonMetricCollector::get_process_metrics(
    std::vector<std::pair<std::string, int>> daemon_pids) {
  std::string path("/proc");
  std::stringstream ss;
  for (auto &[daemon_name, pid] : daemon_pids) {
    std::vector<std::string> uptimes = read_proc_stat_file("/proc/uptime");
    struct pstat stat = read_pid_stat(pid);
    int clk_tck = sysconf(_SC_CLK_TCK);
    double start_time_seconds = stat.start_time / (double)clk_tck;
    double user_time = stat.utime / (double)clk_tck;
    double kernel_time = stat.stime / (double)clk_tck;
    double total_time_seconds = user_time + kernel_time;
    double uptime = std::stod(uptimes[0]);
    double elapsed_time = uptime - start_time_seconds;
    double idle_time = elapsed_time - total_time_seconds;
    double usage = total_time_seconds * 100 / elapsed_time;

    labels_t labels;
    labels["ceph_daemon"] = quote(daemon_name);
    add_metric(builder, stat.minflt, "ceph_exporter_minflt_total",
               "Number of minor page faults of daemon", "counter", labels);
    add_metric(builder, stat.majflt, "ceph_exporter_majflt_total",
               "Number of major page faults of daemon", "counter", labels);
    add_metric(builder, stat.num_threads, "ceph_exporter_num_threads",
               "Number of threads used by daemon", "gauge", labels);
    add_metric(builder, usage, "ceph_exporter_cpu_usage",
               "CPU usage of a daemon", "gauge", labels);

    std::string cpu_time_desc = "Process time in kernel/user/idle mode";
    labels_t cpu_total_labels;
    cpu_total_labels["ceph_daemon"] = quote(daemon_name);
    cpu_total_labels["mode"] = quote("kernel");
    add_metric(builder, kernel_time, "ceph_exporter_cpu_total", cpu_time_desc,
               "counter", cpu_total_labels);
    cpu_total_labels["mode"] = quote("user");
    add_metric(builder, user_time, "ceph_exporter_cpu_total", cpu_time_desc,
               "counter", cpu_total_labels);
    cpu_total_labels["mode"] = quote("idle");
    add_metric(builder, idle_time, "ceph_exporter_cpu_total", cpu_time_desc,
               "counter", cpu_total_labels);
    add_metric(builder, stat.vm_size, "ceph_exporter_vm_size",
               "Virtual memory used in a daemon", "gauge", labels);
    add_metric(builder, stat.resident_size, "ceph_exporter_resident_size",
               "Resident memory in a daemon", "gauge", labels);
  }
}

std::string DaemonMetricCollector::asok_request(AdminSocketClient &asok,
                                                std::string command,
                                                std::string daemon_name) {
  std::string request("{\"prefix\": \"" + command + "\"}");
  std::string response;
  std::string err = asok.do_request(request, &response);
  if (err.length() > 0 || response.substr(0, 5) == "ERROR") {
    dout(1) << "command " << command << "failed for daemon " << daemon_name
            << "with error: " << err << dendl;
    return "";
  }
  return response;
}

labels_t DaemonMetricCollector::get_extra_labels(std::string daemon_name) {
  labels_t labels;
  const std::string ceph_daemon_prefix = "ceph-";
  const std::string ceph_client_prefix = "client.";
  if (daemon_name.rfind(ceph_daemon_prefix, 0) == 0) {
    daemon_name = daemon_name.substr(ceph_daemon_prefix.size());
  }
  if (daemon_name.rfind(ceph_client_prefix, 0) == 0) {
    daemon_name = daemon_name.substr(ceph_client_prefix.size());
  }
  // In vstart cluster socket files for rgw are stored as radosgw.<instance_id>.asok
  if (daemon_name.find("radosgw") != std::string::npos) {
    std::size_t pos = daemon_name.find_last_of('.');
    std::string tmp = daemon_name.substr(pos+1);
    labels["instance_id"] = quote(tmp);
  }
  else if (daemon_name.find("rgw") != std::string::npos) {
    // fetch intance_id for e.g. "hrgsea" from daemon_name=rgw.foo.ceph-node-00.hrgsea.2.94739968030880
    std::vector<std::string> elems;
    std::stringstream ss;
    ss.str(daemon_name);
    std::string item;
    while (std::getline(ss, item, '.')) {
        elems.push_back(item);
    }
    if (elems.size() >= 4) {
      labels["instance_id"] = quote(elems[3]);
    } else {
      return labels_t();
    }
  } else {
    labels.insert({"ceph_daemon", quote(daemon_name)});
  }
  return labels;
}

// Add fixed name metrics from existing ones that have details in their names
// that should be in labels (not in name). For backward compatibility,
// a new fixed name metric is created (instead of replacing)and details are put
// in new labels. Intended for RGW sync perf. counters but extendable as required.
// See: https://tracker.ceph.com/issues/45311
std::pair<labels_t, std::string>
DaemonMetricCollector::add_fixed_name_metrics(std::string metric_name) {
  std::string new_metric_name;
  labels_t labels;
  new_metric_name = metric_name;

  std::regex re("data_sync_from_([^_]*)");
  std::smatch match;
  if (std::regex_search(metric_name, match, re)) {
      new_metric_name = std::regex_replace(metric_name, re, "data_sync_from_zone");
      labels["source_zone"] = quote(match.str(1));
      return {labels, new_metric_name};
  }

  return {};
}

/*
perf_values can be either a int/double or a json_object. Since
   json_value is a wrapper of both we use that class.
 */
void DaemonMetricCollector::dump_asok_metric(json_object perf_info,
                                             json_value perf_values,
                                             std::string name,
                                             labels_t labels) {
  int64_t type = perf_info["type"].as_int64();
  std::string metric_type =
      boost_string_to_std(perf_info["metric_type"].as_string());
  std::string description =
      boost_string_to_std(perf_info["description"].as_string());

  if (type & PERFCOUNTER_LONGRUNAVG) {
    int64_t count = perf_values.as_object()["avgcount"].as_int64();
    add_metric(builder, count, name + "_count", description + " Count", "counter",
               labels);
    json_value sum_value = perf_values.as_object()["sum"];
    add_double_or_int_metric(builder, sum_value, name + "_sum", description + " Total",
                             metric_type, labels);
  } else {
    add_double_or_int_metric(builder, perf_values, name, description,
                             metric_type, labels);
  }
}

void DaemonMetricCollector::update_sockets() {
  std::string sock_dir = g_conf().get_val<std::string>("exporter_sock_dir");
  clients.clear();
  std::filesystem::path sock_path = sock_dir;
  if (!std::filesystem::is_directory(sock_path.parent_path())) {
    dout(1) << "ERROR: No such directory exist" << sock_dir << dendl;
    return;
  }
  for (const auto &entry : std::filesystem::directory_iterator(sock_dir)) {
    if (entry.path().extension() == ".asok") {
      std::string daemon_socket_name = entry.path().filename().string();
      std::string daemon_name =
          daemon_socket_name.substr(0, daemon_socket_name.size() - 5);
      if (clients.find(daemon_name) == clients.end() &&
          !(daemon_name.find("mgr") != std::string::npos) &&
          !(daemon_name.find("ceph-exporter") != std::string::npos)) {
        AdminSocketClient sock(entry.path().string());
        clients.insert({daemon_name, std::move(sock)});
      }
    }
  }
}

void OrderedMetricsBuilder::add(std::string value, std::string name,
                                std::string description, std::string mtype,
                                labels_t labels) {
  if (metrics.find(name) == metrics.end()) {
    Metric metric(name, mtype, description);
    metrics[name] = std::move(metric);
  }
  Metric &metric = metrics[name];
  metric.add(labels, value);
}

std::string OrderedMetricsBuilder::dump() {
  for (auto &[name, metric] : metrics) {
    out += metric.dump() + "\n";
  }
  return out;
}

void UnorderedMetricsBuilder::add(std::string value, std::string name,
                                  std::string description, std::string mtype,
                                  labels_t labels) {
  Metric metric(name, mtype, description);
  metric.add(labels, value);
  out += metric.dump() + "\n\n";
}

std::string UnorderedMetricsBuilder::dump() { return out; }

void Metric::add(labels_t labels, std::string value) {
  metric_entry entry;
  entry.labels = labels;
  entry.value = value;
  entries.push_back(entry);
}

std::string Metric::dump() {
  std::stringstream metric_ss;
  metric_ss << "# HELP " << name << " " << description << "\n";
  metric_ss << "# TYPE " << name << " " << mtype << "\n";
  for (auto &entry : entries) {
    std::stringstream labels_ss;
    size_t i = 0;
    for (auto &[label_name, label_value] : entry.labels) {
      labels_ss << label_name << "=" << label_value;
      if (i < entry.labels.size() - 1) {
        labels_ss << ",";
      }
      i++;
    }
    metric_ss << name << "{" << labels_ss.str() << "} " << entry.value;
    if (&entry != &entries.back()) {
      metric_ss << "\n";
    }
  }
  return metric_ss.str();
}

DaemonMetricCollector &collector_instance() {
  static DaemonMetricCollector instance;
  return instance;
}
