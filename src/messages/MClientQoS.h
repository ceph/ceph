#ifndef CEPH_MCLIENTQOS_H
#define CEPH_MCLIENTQOS_H

#include "msg/Message.h"
#include "mds/mdstypes.h" 

class MClientQoS : public SafeMessage {
public:
  inodeno_t ino;
  dmclock_info_t dmclock_info;

protected:
  MClientQoS() :
    SafeMessage{CEPH_MSG_CLIENT_QOS},
    ino(0)
  {}
  ~MClientQoS() override {}

public:
  std::string_view get_type_name() const override { return "client_qos"; }
  void print(std::ostream& out) const override {
    out << "client_qos(";
    out << " [" << ino << "] ";
    out << dmclock_info << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(dmclock_info, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(dmclock_info, p);
    ceph_assert(p.end());
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
