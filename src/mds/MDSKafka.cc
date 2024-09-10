
#include "MDSKafka.h"

#define dout_subsys ceph_subsys_mds

namespace mds::kafka {

connection_t::connection_t(const std::string &broker, bool use_ssl,
                           const std::string &user, const std::string &password,
                           const std::optional<std::string> &ca_location,
                           const std::optional<std::string> &mechanism)
    : broker(broker), use_ssl(use_ssl), user(user), password(password),
      ca_location(ca_location), mechanism(mechanism) {}

connection_t::connection_t(connection_t &&other)
    : broker(std::move(other.broker)), use_ssl(other.use_ssl),
      user(std::move(other.user)), password(std::move(other.password)),
      ca_location(std::move(other.ca_location)),
      mechanism(std::move(other.mechanism)) {}

void connection_t::combine_hash() {
  hash_key = 0;
  boost::hash_combine(hash_key, broker);
  boost::hash_combine(hash_key, use_ssl);
  boost::hash_combine(hash_key, user);
  boost::hash_combine(hash_key, password);
  if (ca_location) {
    boost::hash_combine(hash_key, ca_location.value());
  }
  if (mechanism) {
    boost::hash_combine(hash_key, mechanism.value());
  }
}

MDSKafkaTopic::MDSKafkaTopic(const std::string &topic_name, CephContext *cct,
                             connection_t &&connection)
    : topic_name(topic_name), cct(cct), head(0), tail(0), inflight_count(0) {
  if (!validate(std::move(connection))) {
    throw;
  }
  kafka_endpoint = MDSKafka::create(cct, std::move(connection));
  if (!kafka_endpoint) {
    throw;
  }
  delivery_ring = std::vector<bool>(MAX_INFLIGHT_DEFAULT, false);
}

bool MDSKafkaTopic::validate(connection_t &&connection) {
  if (connection.user.empty() != connection.password.empty()) {
    return false;
  }
  if (!connection.user.empty() && !connection.use_ssl &&
      !g_conf().get_val<bool>("mds_allow_notification_secrets_in_cleartext")) {
    ldout(cct, 1) << "Kafka connect: user/password are only allowed over "
                     "secure connection"
                  << dendl;
    return false;
  }
  if (MDSNotificationManager::is_full()) {
    ldout(cct, 1) << "Kafka connect: max connections exceeded" << dendl;
    return false;
  }
  if (MDSNotificationManager::interface_exist(connection.hash_key)) {
    ldout(cct, 20) << "Kafka connect: connection found: "
                   << connection.to_string() << dendl;
    return false;
  }
  return true;
}

std::shared_ptr<MDSKafkaTopic>
MDSKafkaTopic::create(const std::string &topic_name, CephContext *cct,
                      connection_t &&connection) {
  try {
    std::shared_ptr<MDSKafkaTopic> topic_ptr(
        new MDSKafkaTopic(topic_name, cct, std::move(connection)));
    topic_ptr->kafka_endpoint->add_topic(topic_name, topic_ptr);
    MDSNotificationManager::add_interface(connection.hash_key,
                                          topic_ptr->kafka_endpoint);
    return topic_ptr;
  } catch (...) {
  }
  return nullptr;
}

int MDSKafkaTopic::push_unack_event() {
  std::unique_lock<std::mutex> lock(ring_mutex);
  if (inflight_count >= (int)MAX_INFLIGHT_DEFAULT) {
    return -1;
  }
  lock.unlock();
  delivery_ring[tail] = true;
  int idx = tail;
  tail = (tail + 1) % MAX_INFLIGHT_DEFAULT;
  return idx;
}

void MDSKafkaTopic::acknowledge_event(int idx) {
  if (!(idx >= 0 && idx < MAX_INFLIGHT_DEFAULT)) {
    ldout(cct, 10) << "Kafka run: unsolicited n/ack received with tag=" << idx
                   << dendl;
    return;
  }
  std::unique_lock<std::mutex> lock(ring_mutex);
  delivery_ring[idx] = false;
  while (inflight_count > 0 && !delivery_ring[head]) {
    head = (head + 1) % MAX_INFLIGHT_DEFAULT;
    --inflight_count;
  }
}

MDSKafka::MDSKafka(CephContext *cct, connection_t &&connection)
    : cct(cct), connection(std::move(connection)) {
  std::unique_ptr<RdKafka::Conf> conf(create_configuration());
  if (!conf)
    throw;
  std::string errstr;
  producer = std::unique_ptr<RdKafka::Producer>(
      RdKafka::Producer::create(conf.get(), errstr));
  if (!producer) {
    ldout(cct, 1) << "Kafka connect: failed to create producer: " << errstr
                  << dendl;
    throw;
  }
  ldout(cct, 20) << "Kafka connect: successfully created new producer" << dendl;
  const auto log_level = cct->_conf->subsys.get_log_level(ceph_subsys_mds);
  if (log_level <= 1) {
    rd_kafka_set_log_level(static_cast<rd_kafka_t *>(producer->c_ptr()), 3);
  } else if (log_level <= 2) {
    rd_kafka_set_log_level(static_cast<rd_kafka_t *>(producer->c_ptr()), 5);
  } else if (log_level <= 10) {
    rd_kafka_set_log_level(static_cast<rd_kafka_t *>(producer->c_ptr()), 6);
  } else {
    rd_kafka_set_log_level(static_cast<rd_kafka_t *>(producer->c_ptr()), 7);
  }
  read_timeout = cct->_conf.get_val<uint64_t>("mds_kafka_sleep_timeout");
}

std::shared_ptr<MDSKafka> MDSKafka::create(CephContext *cct,
                                           connection_t &&connection) {
  try {
    return std::shared_ptr<MDSKafka>(new MDSKafka(cct, std::move(connection)));
  } catch (...) {
  }
  return nullptr;
}

RdKafka::Conf *MDSKafka::create_configuration() {
  std::string errstr;
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  rd_kafka_conf_t *c_conf =
      static_cast<rd_kafka_conf_t *>(conf->c_ptr_global());
  if (!conf) {
    ldout(cct, 1) << "Kafka connect: failed to allocate configuration" << dendl;
    return nullptr;
  }
  constexpr std::uint64_t min_message_timeout = 1;
  const auto message_timeout =
      std::max(min_message_timeout,
               cct->_conf.get_val<uint64_t>("mds_kafka_message_timeout"));
  if (conf->set("message.timeout.ms", std::to_string(message_timeout),
                errstr) != RdKafka::Conf::CONF_OK) {
    goto conf_error;
  }
  if (conf->set("bootstrap.servers", connection.broker, errstr) !=
      RdKafka::Conf::CONF_OK) {
    goto conf_error;
  }

  if (connection.use_ssl) {
    if (!connection.user.empty()) {
      // use SSL+SASL
      if (conf->set("security.protocol", "SASL_SSL", errstr) !=
              RdKafka::Conf::CONF_OK ||
          conf->set("sasl.username", connection.user, errstr) !=
              RdKafka::Conf::CONF_OK ||
          conf->set("sasl.password", connection.password, errstr) !=
              RdKafka::Conf::CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20)
          << "Kafka connect: successfully configured SSL+SASL security"
          << dendl;

      if (connection.mechanism) {
        if (conf->set("sasl.mechanism", connection.mechanism.value(), errstr) !=
            RdKafka::Conf::CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20)
            << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (conf->set("sasl.mechanism", "PLAIN", errstr) !=
            RdKafka::Conf::CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: using default SASL mechanism"
                       << dendl;
      }
    } else {
      // use only SSL
      if (conf->set("security.protocol", "SSL", errstr) !=
          RdKafka::Conf::CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured SSL security"
                     << dendl;
    }
    if (connection.ca_location) {
      if (conf->set("ssl.ca.location", connection.ca_location.value(),
                    errstr) != RdKafka::Conf::CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured CA location"
                     << dendl;
    } else {
      ldout(cct, 20) << "Kafka connect: using default CA location" << dendl;
    }
  } else if (!connection.user.empty()) {
    // use SASL+PLAINTEXT
    if (conf->set("security.protocol", "SASL_PLAINTEXT", errstr) !=
            RdKafka::Conf::CONF_OK ||
        conf->set("sasl.username", connection.user, errstr) !=
            RdKafka::Conf::CONF_OK ||
        conf->set("sasl.password", connection.password, errstr) !=
            RdKafka::Conf::CONF_OK) {
      goto conf_error;
    }
    ldout(cct, 20) << "Kafka connect: successfully configured SASL_PLAINTEXT"
                   << dendl;

    if (connection.mechanism) {
      if (conf->set("sasl.mechanism", connection.mechanism.value(), errstr) !=
          RdKafka::Conf::CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured SASL mechanism"
                     << dendl;
    } else {
      if (conf->set("sasl.mechanism", "PLAIN", errstr) !=
          RdKafka::Conf::CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20) << "Kafka connect: using default SASL mechanism" << dendl;
    }
  }
  conf->set("dr_cb", this, errstr);
  rd_kafka_conf_set_opaque(c_conf, this);
  rd_kafka_conf_set_log_cb(c_conf, log_callback);
  rd_kafka_conf_set_error_cb(c_conf, poll_err_callback);
  return conf;

conf_error:
  ldout(cct, 1) << "Kafka connect: configuration failed: " << errstr << dendl;
  delete conf;
  return nullptr;
}

void MDSKafka::add_topic(const std::string &topic_name,
                         const std::shared_ptr<MDSKafkaTopic> &topic) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  topics[topic_name] = topic;
}

void MDSKafka::remove_topic(const std::string &topic_name) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  auto it = topics.find(topic_name);
  if (it == topics.end()) {
    return;
  }
  topics.erase(it);
}

void MDSKafka::log_callback(const rd_kafka_t *rk, int level, const char *fac,
                            const char *buf) {
  const auto kafka_ptr = reinterpret_cast<MDSKafka *>(rd_kafka_opaque(rk));
  if (!kafka_ptr) {
    return;
  }
  if (level <= 3) {
    ldout(kafka_ptr->cct, 1) << "RDKAFKA-" << level << "-" << fac << ": "
                             << rd_kafka_name(rk) << ": " << buf << dendl;
  } else if (level <= 5) {
    ldout(kafka_ptr->cct, 2) << "RDKAFKA-" << level << "-" << fac << ": "
                             << rd_kafka_name(rk) << ": " << buf << dendl;
  } else if (level <= 6) {
    ldout(kafka_ptr->cct, 10) << "RDKAFKA-" << level << "-" << fac << ": "
                              << rd_kafka_name(rk) << ": " << buf << dendl;
  } else {
    ldout(kafka_ptr->cct, 20) << "RDKAFKA-" << level << "-" << fac << ": "
                              << rd_kafka_name(rk) << ": " << buf << dendl;
  }
}

void MDSKafka::poll_err_callback(rd_kafka_t *rk, int err, const char *reason,
                                 void *opaque) {
  const auto kafka_ptr = reinterpret_cast<MDSKafka *>(rd_kafka_opaque(rk));
  if (!kafka_ptr) {
    return;
  }
  ldout(kafka_ptr->cct, 10)
      << "Kafka run: poll error(" << err << "): " << reason << dendl;
}

uint64_t MDSKafka::publish_internal(std::shared_ptr<bufferlist> &message) {
  uint64_t reply_count = 0;
  std::shared_lock<std::shared_mutex> lock(topic_mutex);
  for (auto [topic_name, topic_weak] : topics) {
    std::shared_ptr<MDSKafkaTopic> topic_shared = topic_weak.lock();
    int idx = topic_shared->push_unack_event();
    if (idx == -1) {
      ldout(cct, 1) << "Kafka publish (with callback): failed with error: "
                       "callback queue full, trying to poll again"
                    << dendl;
      reply_count += producer->poll(3 * read_timeout);
      idx = topic_shared->push_unack_event();
      if (idx == -1) {
        ldout(cct, 1)
            << "Kafka publish (with callback): failed with error: "
               "message dropped, callback queue full event after polling for "
            << 3 * read_timeout << "ms" << dendl;
        continue;
      }
    }
    int *tag = new int(idx);
    RdKafka::ErrorCode response = producer->produce(
        topic_name, RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(message->c_str()),
        message->length(), nullptr, 0, 0, tag);
    if (response != RdKafka::ERR_NO_ERROR) {
      ldout(cct, 1) << "Kafka publish: failed to produce for topic: "
                    << topic_name
                    << ". with error: " << RdKafka::err2str(response) << dendl;

      delete tag;
      topic_shared->drop_last_event();
      continue;
    }
    reply_count += producer->poll(0);
  }
  return reply_count;
}

void MDSKafka::dr_cb(RdKafka::Message &message) {
  if (message.err() == RdKafka::ERR_NO_ERROR) {
    ldout(cct, 20) << "Kafka run: ack received with result=" << message.errstr()
                   << dendl;
  } else {
    ldout(cct, 1) << "Kafka run: nack received with result=" << message.errstr()
                  << " for broker: " << connection.broker << dendl;
  }
  if (!message.msg_opaque()) {
    ldout(cct, 20) << "Kafka run: n/ack received without a callback" << dendl;
  }
  int *tag = reinterpret_cast<int *>(message.msg_opaque());
  std::string topic_name = message.topic_name();
  std::shared_lock<std::shared_mutex> lock(topic_mutex);
  if (topics.find(topic_name) == topics.end()) {
    ldout(cct, 20) << "Kafka run: topic=" << topic_name
                   << " is removed before ack" << dendl;
    delete tag;
    return;
  }
  std::shared_ptr<MDSKafkaTopic> topic_ptr = topics[topic_name].lock();
  topic_ptr->acknowledge_event(*tag);
  delete tag;
}

} // namespace mds::kafka