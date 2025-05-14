#pragma once

#include "MDSNotificationMessage.h"
#include "MDSRank.h"
#include "common/ceph_context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>
#include <boost/functional/hash.hpp>
#include <librdkafka/rdkafka.h>
#include <string>

class MDSKafka;
class MDSKafkaTopic;

struct MDSKafkaConnection {
  std::string broker;
  bool use_ssl;
  std::string user;
  std::string password;
  std::optional<std::string> ca_location;
  std::optional<std::string> mechanism;
  uint64_t hash_key;
  MDSKafkaConnection() = default;
  MDSKafkaConnection(const std::string &broker, bool use_ssl,
                     const std::string &user, const std::string &password,
                     const std::optional<std::string> &ca_location,
                     const std::optional<std::string> &mechanism);
  void combine_hash();
  bool is_empty() const;
  std::string to_string() const { return broker + ":" + user; }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<MDSKafkaConnection *> &o);
};

WRITE_CLASS_ENCODER(MDSKafkaConnection)

class MDSKafkaManager {
public:
  MDSKafkaManager(MDSRank *mds);
  int init();
  void activate();
  void pause();
  int add_topic(const std::string &topic_name, const std::string &endpoint_name,
                const MDSKafkaConnection &connection, bool write_into_disk);
  int remove_topic(const std::string &topic_name,
                   const std::string &endpoint_name, bool write_into_disk);
  int send(const std::shared_ptr<MDSNotificationMessage> &message);
  CephContext *cct;

private:
  void run();
  uint64_t publish(const std::shared_ptr<MDSNotificationMessage> &message);
  int load_data(std::map<std::string, bufferlist> &mp);
  int add_topic_into_disk(const std::string &topic_name,
                          const std::string &endpoint_name,
                          const MDSKafkaConnection &connection);
  int remove_topic_from_disk(const std::string &topic_name,
                             const std::string &endpoint_name);
  int update_omap(const std::map<std::string, bufferlist> &mp);
  int remove_keys(const std::set<std::string> &st);
  void sync_endpoints();
  static const size_t MAX_CONNECTIONS_DEFAULT = 32;
  static const size_t MAX_QUEUE_DEFAULT = 131072;
  std::shared_mutex endpoint_mutex;
  std::unordered_map<std::string, std::shared_ptr<MDSKafka>>
      candidate_endpoints, effective_endpoints;
  std::mutex queue_mutex;
  std::queue<std::shared_ptr<MDSNotificationMessage>> message_queue;
  std::thread worker;
  std::condition_variable pick_message;
  std::atomic<bool> paused;
  MDSRank *mds;
  std::string object_name;
  std::atomic<uint64_t> endpoints_epoch = 0;
  uint64_t prev_endpoints_epoch = 0;
};

class MDSKafkaTopic {
public:
  MDSKafkaTopic() = delete;
  MDSKafkaTopic(const std::string &topic_name);
  static std::shared_ptr<MDSKafkaTopic>
  create(CephContext *_cct, const std::string &topic_name,
         const std::shared_ptr<MDSKafka> &kafka_endpoint);
  static void kafka_topic_deleter(rd_kafka_topic_t *topic_ptr);
  std::unique_ptr<rd_kafka_topic_t, decltype(&kafka_topic_deleter)>
      kafka_topic_ptr{nullptr, kafka_topic_deleter};
  friend class MDSKafkaManager;
  friend class MDSKafka;

private:
  std::string topic_name;
  static CephContext *cct;
};

class MDSKafka {
public:
  MDSKafka() = delete;
  MDSKafka(const MDSKafkaConnection &connection,
           const std::string &endpoint_name);
  ~MDSKafka() {
    stop_polling_thread();
  }
  static std::shared_ptr<MDSKafka> create(CephContext *_cct,
                                          const MDSKafkaConnection &connection,
                                          const std::string &endpoint_name);
  uint64_t
  publish_internal(const std::shared_ptr<MDSNotificationMessage> &message);
  uint64_t poll(int read_timeout);
  void add_topic(const std::string &topic_name,
                 const std::shared_ptr<MDSKafkaTopic> &topic);
  int remove_topic(const std::string &topic_name, bool &is_empty);
  static void kafka_producer_deleter(rd_kafka_t *producer_ptr);
  friend class MDSKafkaManager;
  friend class MDSKafkaTopic;

private:
  std::unique_ptr<rd_kafka_t, decltype(&kafka_producer_deleter)> producer{
      nullptr, kafka_producer_deleter};
  std::shared_mutex topic_mutex;
  std::unordered_map<std::string, std::shared_ptr<MDSKafkaTopic>> topics;
  static CephContext *cct;
  MDSKafkaConnection connection;
  std::string endpoint_name;
  static void message_callback(rd_kafka_t *rk,
                               const rd_kafka_message_t *rkmessage,
                               void *opaque);
  static void log_callback(const rd_kafka_t *rk, int level, const char *fac,
                           const char *buf);
  static void poll_err_callback(rd_kafka_t *rk, int err, const char *reason,
                                void *opaque);
  int64_t push_unack_event();
  void acknowledge_event(int64_t idx);
  void run_polling();
  void stop_polling_thread();
  void start_polling_thread();
  std::atomic<bool> stopped;
  std::condition_variable pick_delivery_ack;
  std::thread polling_thread;
  std::unordered_set <int64_t> unacked_delivery;
  int64_t delivery_seq = 0;
  std::mutex delivery_mutex;
  static const size_t MAX_INFLIGHT_DEFAULT = 1 << 20;
};