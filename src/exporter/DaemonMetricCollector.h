#pragma once

#include "common/admin_socket_client.h"
#include <map>
#include <string>
#include <vector>

#include <boost/asio/steady_timer.hpp>
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

class MetricsBuilder;
class OrderedMetricsBuilder;
class UnorderedMetricsBuilder;
class Metric;

typedef std::map<std::string, std::string> labels_t;

class DaemonMetricCollector {
public:
  void main();
  std::string get_metrics();
  labels_t get_extra_labels(std::string daemon_name);
  void dump_asok_metrics(bool sort_metrics, int64_t counter_prio,
                         bool sockClientsPing, std::string &dump_response,
                         std::string &schema_response,
                         bool config_show_response);
  std::map<std::string, AdminSocketClient> clients;
  std::string metrics;
  std::pair<labels_t, std::string> add_fixed_name_metrics(std::string metric_name);

private:
  std::mutex metrics_mutex;
  std::unique_ptr<MetricsBuilder> builder;
  void update_sockets();
  void request_loop(boost::asio::steady_timer &timer);

  void dump_asok_metric(boost::json::object perf_info,
                        boost::json::value perf_values, std::string name,
                        labels_t labels);
  void parse_asok_metrics(std::string &counter_dump_response,
                          std::string &counter_schema_response,
                          int64_t prio_limit, const std::string &daemon_name);
  void get_process_metrics(std::vector<std::pair<std::string, int>> daemon_pids);
  std::string asok_request(AdminSocketClient &asok, std::string command, std::string daemon_name);
};

class Metric {
private:
  struct metric_entry {
    labels_t labels;
    std::string value;
  };
  std::string name;
  std::string mtype;
  std::string description;
  std::vector<metric_entry> entries;

public:
  Metric(std::string name, std::string mtype, std::string description)
      : name(name), mtype(mtype), description(description) {}
  Metric(const Metric &) = default;
  Metric() = default;
  void add(labels_t labels, std::string value);
  std::string dump();
};

class MetricsBuilder {
public:
  virtual ~MetricsBuilder() = default;
  virtual std::string dump() = 0;
  virtual void add(std::string value, std::string name, std::string description,
                   std::string mtype, labels_t labels) = 0;

protected:
  std::string out;
};

class OrderedMetricsBuilder : public MetricsBuilder {
private:
  std::map<std::string, Metric> metrics;

public:
  std::string dump();
  void add(std::string value, std::string name, std::string description,
           std::string mtype, labels_t labels);
};

class UnorderedMetricsBuilder : public MetricsBuilder {
public:
  std::string dump();
  void add(std::string value, std::string name, std::string description,
           std::string mtype, labels_t labels);
};

DaemonMetricCollector &collector_instance();
