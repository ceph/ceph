
#include "MDSKafka.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/fs_types.h"

#define dout_subsys ceph_subsys_mds

CephContext *MDSKafka::cct = nullptr;
CephContext *MDSKafkaTopic::cct = nullptr;

MDSKafkaConnection::MDSKafkaConnection(
    const std::string &broker, bool use_ssl, const std::string &user,
    const std::string &password, const std::optional<std::string> &ca_location,
    const std::optional<std::string> &mechanism)
    : broker(broker), use_ssl(use_ssl), user(user), password(password),
      ca_location(ca_location), mechanism(mechanism) {
  combine_hash();
}

void MDSKafkaConnection::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(broker, bl);
  encode(use_ssl, bl);
  encode(user, bl);
  encode(password, bl);
  encode(ca_location, bl);
  encode(mechanism, bl);
  ENCODE_FINISH(bl);
}

void MDSKafkaConnection::decode(ceph::buffer::list::const_iterator &iter) {
  DECODE_START(1, iter);
  decode(broker, iter);
  decode(use_ssl, iter);
  decode(user, iter);
  decode(password, iter);
  decode(ca_location, iter);
  decode(mechanism, iter);
  DECODE_FINISH(iter);
}

void MDSKafkaConnection::dump(ceph::Formatter *f) const {
  f->dump_string("broker", broker);
  f->dump_bool("use_ssl", use_ssl);
  f->dump_string("user", user);
  f->dump_string("password", password);
  if (ca_location.has_value()) {
    f->dump_string("ca_location", ca_location.value());
  }
  if (mechanism.has_value()) {
    f->dump_string("mechanism", mechanism.value());
  }
}

void MDSKafkaConnection::generate_test_instances(
    std::list<MDSKafkaConnection *> &o) {
  o.push_back(new MDSKafkaConnection);
}

bool MDSKafkaConnection::is_empty() const {
  return broker.empty() && !use_ssl && user.empty() && password.empty() &&
         !ca_location.has_value() && !mechanism.has_value();
}

MDSKafkaManager::MDSKafkaManager(MDSRank *mds)
    : mds(mds), cct(mds->cct), paused(true), object_name("mds_kafka_topics"),
      endpoints_epoch(0), prev_endpoints_epoch(0) {}

int MDSKafkaManager::load_data(std::map<std::string, bufferlist> &mp) {
  int r = update_omap(std::map<std::string, bufferlist>());
  if (r < 0) {
    return r;
  }
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_get_vals("", "", UINT_MAX, &mp, NULL, NULL);
  mds->objecter->read(object_t(object_name),
                      object_locator_t(mds->get_metadata_pool()), op,
                      CEPH_NOSNAP, NULL, 0, &sync_finisher);
  r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error reading omap values from object '" << object_name
                    << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSKafkaManager::update_omap(const std::map<std::string, bufferlist> &mp) {
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_set(mp);
  mds->objecter->mutate(
      object_t(object_name), object_locator_t(mds->get_metadata_pool()), op,
      SnapContext(), ceph::real_clock::now(), 0, &sync_finisher);
  int r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error updating omap of object '" << object_name
                    << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSKafkaManager::remove_keys(const std::set<std::string> &st) {
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_rm_keys(st);
  mds->objecter->mutate(
      object_t(object_name), object_locator_t(mds->get_metadata_pool()), op,
      SnapContext(), ceph::real_clock::now(), 0, &sync_finisher);
  int r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error removing keys from omap of object '"
                    << object_name << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSKafkaManager::add_topic_into_disk(const std::string &topic_name,
                                         const std::string &endpoint_name,
                                         const MDSKafkaConnection &connection) {
  std::map<std::string, bufferlist> mp;
  std::string key = topic_name + "," + endpoint_name;
  bufferlist bl;
  encode(connection, bl);
  mp[key] = std::move(bl);
  int r = update_omap(mp);
  return r;
}

int MDSKafkaManager::remove_topic_from_disk(const std::string &topic_name,
                                            const std::string &endpoint_name) {
  std::set<std::string> st;
  std::string key = topic_name + "," + endpoint_name;
  st.insert(key);
  int r = remove_keys(st);
  return r;
}

int MDSKafkaManager::init() {
  std::map<std::string, bufferlist> mp;
  int r = load_data(mp);
  if (r < 0) {
    lderr(cct) << "Error occurred while initilizing kafka topics" << dendl;
  }
  for (auto &[key, val] : mp) {
    try {
      MDSKafkaConnection connection;
      auto iter = val.cbegin();
      decode(connection, iter);
      size_t pos = key.find(',');
      std::string topic_name = key.substr(0, pos);
      std::string endpoint_name = key.substr(pos + 1);
      add_topic(topic_name, endpoint_name, connection, false);
      endpoints_epoch++;
    } catch (const ceph::buffer::error &e) {
      ldout(cct, 1) << "Undecodable kafka topic found:" << e.what() << dendl;
    }
  }
  return r;
}

int MDSKafkaManager::remove_topic(const std::string &topic_name,
                                  const std::string &endpoint_name,
                                  bool write_into_disk) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  int r = 0;
  bool is_empty = false;
  auto it = candidate_endpoints.find(endpoint_name);
  if (it == candidate_endpoints.end()) {
    ldout(cct, 1) << "No kafka endpoint exist having name '" << endpoint_name
                  << "'" << dendl;
    r = -CEPHFS_EINVAL;
    goto error_occurred;
  }
  r = it->second->remove_topic(topic_name, is_empty);
  if (r < 0) {
    ldout(cct, 1) << "No kafka topic exist with topic name '" << topic_name
                  << "' with endpoint having endpoint name '" << endpoint_name
                  << "'" << dendl;
    goto error_occurred;
  }
  if (is_empty) {
    candidate_endpoints.erase(it);
    endpoints_epoch++;
  }
  if (write_into_disk) {
    r = remove_topic_from_disk(topic_name, endpoint_name);
    if (r < 0) {
      goto error_occurred;
    }
  }
  ldout(cct, 1) << "Kafka topic named '" << topic_name
                << "' having endpoint name '" << endpoint_name
                << "' is removed successfully" << dendl;
  if (candidate_endpoints.empty()) {
    pause();
  }
  return r;

error_occurred:
  lderr(cct) << "Kafka topic named '" << topic_name
             << "' having endpoint name '" << endpoint_name
             << "' can not be removed, failed with an error:" << cpp_strerror(r)
             << dendl;
  return r;
}

int MDSKafkaManager::add_topic(const std::string &topic_name,
                               const std::string &endpoint_name,
                               const MDSKafkaConnection &connection,
                               bool write_into_disk) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  auto it = candidate_endpoints.find(endpoint_name);
  std::shared_ptr<MDSKafka> kafka;
  std::shared_ptr<MDSKafkaTopic> topic;
  bool created = false;
  int r = 0;
  if (it == candidate_endpoints.end()) {
    if (candidate_endpoints.size() >= MAX_CONNECTIONS_DEFAULT) {
      ldout(cct, 1) << "Kafka connect: max connections exceeded" << dendl;
      r = -CEPHFS_ENOMEM;
      goto error_occurred;
    }
    kafka = MDSKafka::create(cct, connection, endpoint_name);
    if (!kafka) {
      r = -CEPHFS_ECANCELED;
      goto error_occurred;
    }
    created = true;
  } else {
    if (!connection.is_empty() &&
        connection.hash_key != it->second->connection.hash_key) {
      ldout(cct, 1)
          << "Kafka endpoint name already exist with different endpoint "
             "information"
          << dendl;
      r = -CEPHFS_EINVAL;
      goto error_occurred;
    }
    kafka = it->second;
  }
  topic = MDSKafkaTopic::create(cct, topic_name, kafka);
  if (!topic) {
    r = -CEPHFS_ECANCELED;
    goto error_occurred;
  }
  kafka->add_topic(topic_name, topic);
  if (created) {
    candidate_endpoints[endpoint_name] = kafka;
    endpoints_epoch++;
  }
  if (write_into_disk) {
    r = add_topic_into_disk(topic_name, endpoint_name, connection);
    if (r < 0) {
      goto error_occurred;
    }
  }
  ldout(cct, 1) << "Kafka topic named '" << topic_name
                << "' having endpoint name '" << endpoint_name
                << "' is added successfully" << dendl;
  activate();
  return r;

error_occurred:
  lderr(cct) << "Kafka topic named '" << topic_name
             << "' having endpoint name '" << endpoint_name
             << "' can not be added, failed with an error:" << cpp_strerror(r)
             << dendl;
  return r;
}

void MDSKafkaManager::activate() {
  if (!paused) {
    return;
  }
  worker = std::thread(&MDSKafkaManager::run, this);
  paused = false;
  ldout(cct, 1) << "KafkaManager worker thread started" << dendl;
}

void MDSKafkaManager::pause() {
  if (paused) {
    return;
  }
  paused = true;
  pick_message.notify_one();
  if (worker.joinable()) {
    worker.join();
  }
  ldout(cct, 1) << "KafkaManager worker thread paused" << dendl;
}

int MDSKafkaManager::send(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  if (paused) {
    return -CEPHFS_ECANCELED;
  }
  std::unique_lock<std::mutex> lock(queue_mutex);
  if (message_queue.size() >= MAX_QUEUE_DEFAULT) {
    ldout(cct, 5) << "Notification message for kafka with seq_id="
                  << message->seq_id << " is dropped as queue is full" << dendl;
    return -CEPHFS_EBUSY;
  }
  message_queue.push(message);
  lock.unlock();
  pick_message.notify_one();
  return 0;
}

void MDSKafkaManager::sync_endpoints() {
  uint64_t current_epoch = endpoints_epoch.load();
  if (prev_endpoints_epoch != current_epoch) {
    effective_endpoints = candidate_endpoints;
    prev_endpoints_epoch = current_epoch;
  }
}

uint64_t MDSKafkaManager::publish(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  sync_endpoints();
  uint64_t reply_count = 0;
  for (auto &[key, endpoint] : effective_endpoints) {
    reply_count += endpoint->publish_internal(message);
  }
  return reply_count;
}

void MDSKafkaManager::run() {
  while (true) {
    std::unique_lock<std::mutex> queue_lock(queue_mutex);
    pick_message.wait(queue_lock,
                      [this] { return paused || !message_queue.empty(); });
    if (paused) {
      break;
    }
    std::queue<std::shared_ptr<MDSNotificationMessage>> local_message_queue;
    swap(local_message_queue, message_queue);
    ceph_assert(message_queue.empty());
    queue_lock.unlock();
    while (!local_message_queue.empty() && !paused) {
      std::shared_ptr<MDSNotificationMessage> message =
          local_message_queue.front();
      local_message_queue.pop();
      publish(message);
    }
  }
}

void MDSKafkaConnection::combine_hash() {
  hash_key = 0;
  boost::hash_combine(hash_key, broker);
  boost::hash_combine(hash_key, use_ssl);
  boost::hash_combine(hash_key, user);
  boost::hash_combine(hash_key, password);
  if (ca_location.has_value()) {
    boost::hash_combine(hash_key, ca_location.value());
  }
  if (mechanism.has_value()) {
    boost::hash_combine(hash_key, mechanism.value());
  }
}

void MDSKafkaTopic::kafka_topic_deleter(rd_kafka_topic_t *topic_ptr) {
  if (topic_ptr) {
    rd_kafka_topic_destroy(topic_ptr);
  }
}

MDSKafkaTopic::MDSKafkaTopic(const std::string &topic_name)
    : topic_name(topic_name) {}

std::shared_ptr<MDSKafkaTopic>
MDSKafkaTopic::create(CephContext *_cct, const std::string &topic_name,
                      const std::shared_ptr<MDSKafka> &kafka_endpoint) {
  try {
    if (!MDSKafkaTopic::cct && _cct) {
      MDSKafkaTopic::cct = _cct;
    }

    std::shared_ptr<MDSKafkaTopic> topic_ptr =
        std::make_shared<MDSKafkaTopic>(topic_name);
    topic_ptr->kafka_topic_ptr.reset(rd_kafka_topic_new(
        kafka_endpoint->producer.get(), topic_name.c_str(), nullptr));
    if (!topic_ptr->kafka_topic_ptr) {
      return nullptr;
    }
    return topic_ptr;
  } catch (...) {
  }
  return nullptr;
}

int64_t MDSKafka::push_unack_event() {
  std::unique_lock<std::mutex> lock(delivery_mutex);
  if (unacked_delivery.size() >= (int)MAX_INFLIGHT_DEFAULT) {
    return -1;
  }
  unacked_delivery.insert(delivery_seq);
  int64_t idx = delivery_seq++;
  if (unacked_delivery.size() >=
      cct->_conf->mds_kafka_unacked_message_threshold) {
    lock.unlock();
    pick_delivery_ack.notify_one();
  }

  return idx;
}

void MDSKafka::acknowledge_event(int64_t idx) {
  std::unique_lock<std::mutex> lock(delivery_mutex);
  auto it = unacked_delivery.find(idx);
  if (it == unacked_delivery.end()) {
    return;
  }
  unacked_delivery.erase(it);
  // ldout(cct, 0) << ": Message acknowledged=" << idx << dendl;
}

void MDSKafka::kafka_producer_deleter(rd_kafka_t *producer_ptr) {
  if (producer_ptr) {
    rd_kafka_flush(producer_ptr,
                   10 * 1000);      // Wait for max 10 seconds to flush.
    rd_kafka_destroy(producer_ptr); // Destroy producer instance.
  }
}

MDSKafka::MDSKafka(const MDSKafkaConnection &connection,
                   const std::string &endpoint_name)
    : connection(connection), stopped(true), endpoint_name(endpoint_name) {}

void MDSKafka::run_polling() {
  while (true) {
    std::unique_lock<std::mutex> lock(delivery_mutex);
    pick_delivery_ack.wait(
        lock, [this] { return (stopped || !unacked_delivery.empty()); });
    if (stopped) {
      break;
    }
    lock.unlock();
    poll(cct->_conf->mds_kafka_sleep_timeout);
  }
}

std::shared_ptr<MDSKafka> MDSKafka::create(CephContext *_cct,
                                           const MDSKafkaConnection &connection,
                                           const std::string &endpoint_name) {
  try {
    if (!MDSKafka::cct && _cct) {
      MDSKafka::cct = _cct;
    }
    // validation before creating kafka interface
    if (connection.broker.empty()) {
      return nullptr;
    }
    if (connection.user.empty() != connection.password.empty()) {
      return nullptr;
    }
    if (!connection.user.empty() && !connection.use_ssl &&
        !g_conf().get_val<bool>(
            "mds_allow_notification_secrets_in_cleartext")) {
      ldout(cct, 1) << "Kafka connect: user/password are only allowed over "
                       "secure connection"
                    << dendl;
      return nullptr;
    }
    std::shared_ptr<MDSKafka> kafka_ptr =
        std::make_shared<MDSKafka>(connection, endpoint_name);
    char errstr[512] = {0};
    auto kafka_conf_deleter = [](rd_kafka_conf_t *conf) {
      rd_kafka_conf_destroy(conf);
    };
    std::unique_ptr<rd_kafka_conf_t, decltype(kafka_conf_deleter)> conf(
        rd_kafka_conf_new(), kafka_conf_deleter);
    if (!conf) {
      ldout(cct, 1) << "Kafka connect: failed to allocate configuration"
                    << dendl;
      return nullptr;
    }
    constexpr std::uint64_t min_message_timeout = 1;
    const auto message_timeout =
        std::max(min_message_timeout,
                 cct->_conf.get_val<uint64_t>("mds_kafka_message_timeout"));
    if (rd_kafka_conf_set(conf.get(), "message.timeout.ms",
                          std::to_string(message_timeout).c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      goto conf_error;
    }
    if (rd_kafka_conf_set(conf.get(), "bootstrap.servers",
                          connection.broker.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      goto conf_error;
    }

    if (connection.use_ssl) {
      if (!connection.user.empty()) {
        // use SSL+SASL
        if (rd_kafka_conf_set(conf.get(), "security.protocol", "SASL_SSL",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf.get(), "sasl.username",
                              connection.user.c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf.get(), "sasl.password",
                              connection.password.c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20)
            << "Kafka connect: successfully configured SSL+SASL security"
            << dendl;

        if (connection.mechanism) {
          if (rd_kafka_conf_set(conf.get(), "sasl.mechanism",
                                connection.mechanism->c_str(), errstr,
                                sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            goto conf_error;
          }
          ldout(cct, 20)
              << "Kafka connect: successfully configured SASL mechanism"
              << dendl;
        } else {
          if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", "PLAIN", errstr,
                                sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            goto conf_error;
          }
          ldout(cct, 20) << "Kafka connect: using default SASL mechanism"
                         << dendl;
        }
      } else {
        // use only SSL
        if (rd_kafka_conf_set(conf.get(), "security.protocol", "SSL", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: successfully configured SSL security"
                       << dendl;
      }
      if (connection.ca_location) {
        if (rd_kafka_conf_set(conf.get(), "ssl.ca.location",
                              connection.ca_location->c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: successfully configured CA location"
                       << dendl;
      } else {
        ldout(cct, 20) << "Kafka connect: using default CA location" << dendl;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured security"
                     << dendl;
    } else if (!connection.user.empty()) {
      // use SASL+PLAINTEXT
      if (rd_kafka_conf_set(conf.get(), "security.protocol", "SASL_PLAINTEXT",
                            errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
          rd_kafka_conf_set(conf.get(), "sasl.username",
                            connection.user.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK ||
          rd_kafka_conf_set(conf.get(), "sasl.password",
                            connection.password.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured SASL_PLAINTEXT"
                     << dendl;

      if (connection.mechanism) {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism",
                              connection.mechanism->c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20)
            << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", "PLAIN", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: using default SASL mechanism"
                       << dendl;
      }
    }
    rd_kafka_conf_set_dr_msg_cb(conf.get(), message_callback);
    rd_kafka_conf_set_opaque(conf.get(), kafka_ptr.get());
    rd_kafka_conf_set_log_cb(conf.get(), log_callback);
    rd_kafka_conf_set_error_cb(conf.get(), poll_err_callback);
    {
      rd_kafka_t *prod = rd_kafka_new(RD_KAFKA_PRODUCER, conf.release(), errstr,
                                      sizeof(errstr));
      if (!prod) {
        ldout(cct, 1) << "Kafka connect: failed to create producer: " << errstr
                      << dendl;
        return nullptr;
      }
      kafka_ptr->producer.reset(prod);
    }
    ldout(cct, 1) << "Kafka connect: successfully created new producer"
                  << dendl;
    {
      const auto log_level = cct->_conf->subsys.get_log_level(ceph_subsys_mds);
      if (log_level <= 1) {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 3);
      } else if (log_level <= 2) {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 5);
      } else if (log_level <= 10) {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 5);
      } else {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 5);
      }
    }
    return kafka_ptr;

  conf_error:
    ldout(cct, 1) << "Kafka connect: configuration failed: " << errstr << dendl;
    return nullptr;
  } catch (...) {
  }
  return nullptr;
}

void MDSKafka::add_topic(const std::string &topic_name,
                         const std::shared_ptr<MDSKafkaTopic> &topic) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  topics[topic_name] = topic;
  start_polling_thread();
}

void MDSKafka::start_polling_thread() {
  if (!stopped) {
    return;
  }
  stopped = false;
  polling_thread = std::thread(&MDSKafka::run_polling, this);
  ldout(cct, 1) << ": polling thread started for kafka enpoint name="
                << endpoint_name << dendl;
}

void MDSKafka::stop_polling_thread() {
  if (stopped) {
    return;
  }
  stopped = true;
  pick_delivery_ack.notify_one();
  if (polling_thread.joinable()) {
    polling_thread.join();
  }
  ldout(cct, 1) << ": polling thread stopped for kafka enpoint name="
                << endpoint_name << dendl;
}

int MDSKafka::remove_topic(const std::string &topic_name, bool &is_empty) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  auto it = topics.find(topic_name);
  if (it == topics.end()) {
    return -CEPHFS_EINVAL;
  }
  topics.erase(it);
  is_empty = topics.empty();
  if (is_empty) {
    stop_polling_thread();
  }
  return 0;
}

void MDSKafka::log_callback(const rd_kafka_t *rk, int level, const char *fac,
                            const char *buf) {
  if (!cct) {
    return;
  }
  if (level <= 3) {
    ldout(cct, 1) << "RDKAFKA-" << level << "-" << fac << ": "
                  << rd_kafka_name(rk) << ": " << buf << dendl;
  } else if (level <= 5) {
    ldout(cct, 2) << "RDKAFKA-" << level << "-" << fac << ": "
                  << rd_kafka_name(rk) << ": " << buf << dendl;
  } else if (level <= 6) {
    ldout(cct, 10) << "RDKAFKA-" << level << "-" << fac << ": "
                   << rd_kafka_name(rk) << ": " << buf << dendl;
  } else {
    ldout(cct, 20) << "RDKAFKA-" << level << "-" << fac << ": "
                   << rd_kafka_name(rk) << ": " << buf << dendl;
  }
}

void MDSKafka::poll_err_callback(rd_kafka_t *rk, int err, const char *reason,
                                 void *opaque) {
  if (!cct) {
    return;
  }
  ldout(cct, 10) << "Kafka run: poll error(" << err << "): " << reason << dendl;
}

uint64_t MDSKafka::publish_internal(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  uint64_t reply_count = 0;
  std::shared_lock<std::shared_mutex> lock(topic_mutex);
  uint64_t read_timeout =
      cct->_conf->mds_kafka_sleep_timeout;
  for (auto [topic_name, topic_ptr] : topics) {
    int64_t idx = push_unack_event();
    if (idx == -1) {
      ldout(cct, 5) << "Kafka publish (with callback): failed with error: "
                       "callback queue full, trying to poll again"
                    << dendl;
      reply_count += rd_kafka_poll(producer.get(), 3 * read_timeout);
      idx = push_unack_event();
      if (idx == -1) {
        ldout(cct, 5)
            << "Kafka publish (with callback): failed with error: "
               "message dropped, callback queue full event after polling for "
            << 3 * read_timeout << "ms" << dendl;
        continue;
      }
    }
    int64_t *tag = new int64_t(idx);
    // RdKafka::ErrorCode response = producer->produce(
    //     topic_name, RdKafka::Topic::PARTITION_UA,
    //     RdKafka::Producer::RK_MSG_COPY, const_cast<char
    //     *>(message->c_str()), message->length(), nullptr, 0, 0, tag);
    const auto response = rd_kafka_produce(
        topic_ptr->kafka_topic_ptr.get(), RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY, const_cast<char *>(message->message.c_str()),
        message->message.length(), nullptr, 0, tag);
    if (response == -1) {
      const auto err = rd_kafka_last_error();
      ldout(cct, 5) << "Kafka publish: failed to produce for topic: "
                    << topic_name << ". with error: " << rd_kafka_err2str(err)
                    << dendl;

      delete tag;
      acknowledge_event(idx);
      continue;
    }
    reply_count += rd_kafka_poll(producer.get(), 0);
  }
  return reply_count;
}

uint64_t MDSKafka::poll(int read_timeout) {
  return rd_kafka_poll(producer.get(), read_timeout);
}

void MDSKafka::message_callback(rd_kafka_t *rk,
                                const rd_kafka_message_t *rkmessage,
                                void *opaque) {
  const auto result = rkmessage->err;
  const auto kafka_ptr = reinterpret_cast<MDSKafka *>(opaque);
  if (result == 0) {
    ldout(cct, 20) << "Kafka run: ack received with result="
                   << rd_kafka_err2str(result) << dendl;
  } else {
    ldout(cct, 5) << "Kafka run: nack received with result="
                  << rd_kafka_err2str(result)
                  << " for kafka endpoint having endpoint name: "
                  << kafka_ptr->endpoint_name << dendl;
  }
  if (!rkmessage->_private) {
    ldout(cct, 20) << "Kafka run: n/ack received without a callback" << dendl;
    return;
  }
  int64_t *tag = reinterpret_cast<int64_t *>(rkmessage->_private);
  kafka_ptr->acknowledge_event(*tag);
  delete tag;
}