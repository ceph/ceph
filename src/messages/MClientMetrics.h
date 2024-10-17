// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_CLIENT_METRICS_H
#define CEPH_MDS_CLIENT_METRICS_H

#include <vector>

#include "msg/Message.h"
#include "include/cephfs/metrics/Types.h"

class MClientMetrics final : public SafeMessage {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  static constexpr int PRIORITY = CEPH_MSG_PRIO_HIGH-1;

public:
  std::vector<ClientMetricMessage> updates;

protected:
  MClientMetrics() : MClientMetrics(std::vector<ClientMetricMessage>{}) {
    set_priority(PRIORITY);
  }
  MClientMetrics(std::vector<ClientMetricMessage> updates)
    : SafeMessage(CEPH_MSG_CLIENT_METRICS, HEAD_VERSION, COMPAT_VERSION), updates(updates) {
    set_priority(PRIORITY);
  }
  ~MClientMetrics() final {}

public:
  std::string_view get_type_name() const override {
    return "client_metrics";
  }

  void print(std::ostream &out) const override {
    out << "client_metrics ";
    for (auto &i : updates) {
      i.print(&out);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(updates, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto iter = payload.cbegin();
    decode(updates, iter);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif // CEPH_MDS_CLIENT_METRICS_H
