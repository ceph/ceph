#pragma once
#include "messages/MMDSOp.h"

class MNotificationInfoKafkaTopic : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  std::string topic_name;
  std::string endpoint_name;
  std::string broker;
  bool use_ssl;
  std::string user, password;
  std::optional<std::string> ca_location;
  std::optional<std::string> mechanism;
  bool is_remove;

protected:
  MNotificationInfoKafkaTopic()
      : MMDSOp(MSG_MDS_NOTIFICATION_INFO_KAFKA_TOPIC, HEAD_VERSION,
               COMPAT_VERSION) {}
  MNotificationInfoKafkaTopic(const std::string &topic_name,
                              const std::string &endpoint_name, bool is_remove)
      : MMDSOp(MSG_MDS_NOTIFICATION_INFO_KAFKA_TOPIC, HEAD_VERSION,
               COMPAT_VERSION),
        topic_name(topic_name), endpoint_name(endpoint_name),
        is_remove(is_remove) {}
  MNotificationInfoKafkaTopic(const std::string &topic_name,
                              const std::string &endpoint_name,
                              const std::string &broker, bool use_ssl,
                              const std::string &user,
                              const std::string &password,
                              const std::optional<std::string> &ca_location,
                              const std::optional<std::string> &mechanism,
                              bool is_remove)
      : MMDSOp(MSG_MDS_NOTIFICATION_INFO_KAFKA_TOPIC, HEAD_VERSION,
               COMPAT_VERSION),
        topic_name(topic_name), endpoint_name(endpoint_name), broker(broker),
        use_ssl(use_ssl), user(user), password(password),
        ca_location(ca_location), mechanism(mechanism), is_remove(is_remove) {}
  ~MNotificationInfoKafkaTopic() final {}

public:
  std::string_view get_type_name() const override { return "mdskafka_topic"; }

  void print(std::ostream &out) const override { out << "mdskafka_topic"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(topic_name, payload);
    encode(endpoint_name, payload);
    encode(broker, payload);
    encode(use_ssl, payload);
    encode(user, payload);
    encode(password, payload);
    encode(ca_location, payload);
    encode(mechanism, payload);
    encode(is_remove, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(topic_name, p);
    decode(endpoint_name, p);
    decode(broker, p);
    decode(use_ssl, p);
    decode(user, p);
    decode(password, p);
    decode(ca_location, p);
    decode(mechanism, p);
    decode(is_remove, p);
  }

private:
  template <class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args &&...args);
  template <class T, typename... Args>
  friend MURef<T> crimson::make_message(Args &&...args);
};