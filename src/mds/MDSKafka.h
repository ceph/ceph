#pragma once

#include "MDSNotificationInterface.h"
#include "common/ceph_context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>
#include <boost/functional/hash.hpp>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <string>

namespace mds::kafka {

class MDSKafka;

struct connection_t {
  std::string broker;
  bool use_ssl;
  std::string user;
  std::string password;
  std::optional<std::string> ca_location;
  std::optional<std::string> mechanism;
  uint64_t hash_key;
  connection_t() = default;
  connection_t(const std::string &broker, bool use_ssl, const std::string &user,
               const std::string &password,
               const std::optional<std::string> &ca_location,
               const std::optional<std::string> &mechanism);
  connection_t(connection_t &&other);
  void combine_hash();
  std::string to_string() { return broker + ":" + user; }
};

class MDSKafkaTopic : public std::enable_shared_from_this<MDSKafkaTopic> {
public:
  MDSKafkaTopic() = delete;
  int push_unack_event();
  void acknowledge_event(int idx);
  void drop_last_event();
  static std::shared_ptr<MDSKafkaTopic> create(const std::string &topic_name,
                                               CephContext *cct,
                                               connection_t &&connection);

private:
  MDSKafkaTopic(const std::string &topic_name, CephContext *cct,
                connection_t &&connection);
  bool validate(connection_t &&connection);
  std::string topic_name;
  CephContext *const cct;
  std::vector<bool> delivery_ring;
  std::mutex ring_mutex;
  int head, tail, inflight_count;
  static const size_t MAX_INFLIGHT_DEFAULT = 8192;
  std::shared_ptr<MDSKafka> kafka_endpoint;
};

class MDSKafka : public MDSNotificationInterface, RdKafka::DeliveryReportCb {
public:
  MDSKafka() = delete;
  static std::shared_ptr<MDSKafka> create(CephContext *cct,
                                          connection_t &&connection);
  uint64_t publish_internal(std::shared_ptr<bufferlist> &message) override;
  bool need_to_collect_ack() override { return true; }
  uint64_t poll() override;
  void add_topic(const std::string &topic_name,
                 const std::shared_ptr<MDSKafkaTopic> &topic);
  void remove_topic(const std::string &topic_name);

private:
  std::shared_mutex topic_mutex;
  std::unordered_map<std::string, std::weak_ptr<MDSKafkaTopic>> topics;
  std::unique_ptr<RdKafka::Producer> producer;
  CephContext *const cct;
  connection_t connection;
  uint64_t read_timeout;
  MDSKafka(CephContext *cct, connection_t &&connection);
  RdKafka::Conf *create_configuration();
  void dr_cb(RdKafka::Message &message) override;
  static void log_callback(const rd_kafka_t *rk, int level, const char *fac,
                           const char *buf);
  static void poll_err_callback(rd_kafka_t *rk, int err, const char *reason,
                                void *opaque);
};

} // namespace mds::kafka
