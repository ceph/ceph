#pragma once
#include "messages/MMDSOp.h"

class MNotificationInfoUDPEndpoint : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  std::string name;
  std::string ip;
  int port;
  bool is_remove;

protected:
  MNotificationInfoUDPEndpoint()
      : MMDSOp(MSG_MDS_NOTIFICATION_INFO_UDP_ENDPOINT, HEAD_VERSION,
               COMPAT_VERSION) {}
  MNotificationInfoUDPEndpoint(const std::string &name, bool is_remove)
      : MMDSOp(MSG_MDS_NOTIFICATION_INFO_UDP_ENDPOINT, HEAD_VERSION,
               COMPAT_VERSION),
        name(name), is_remove(is_remove) {}
  MNotificationInfoUDPEndpoint(const std::string &name, const std::string &ip,
                               int port, bool is_remove)
      : MMDSOp(MSG_MDS_NOTIFICATION_INFO_UDP_ENDPOINT, HEAD_VERSION,
               COMPAT_VERSION),
        name(name), ip(ip), port(port), is_remove(is_remove) {}
  ~MNotificationInfoUDPEndpoint() final {}

public:
  std::string_view get_type_name() const override {
    return "mdsudp_notification_client";
  }

  void print(std::ostream &out) const override {
    out << "mdsudp_notification_client";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(name, payload);
    encode(ip, payload);
    encode(port, payload);
    encode(is_remove, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(name, p);
    decode(ip, p);
    decode(port, p);
    decode(is_remove, p);
  }

private:
  template <class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args &&...args);
  template <class T, typename... Args>
  friend MURef<T> crimson::make_message(Args &&...args);
};