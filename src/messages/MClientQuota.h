#ifndef CEPH_MCLIENTQUOTA_H
#define CEPH_MCLIENTQUOTA_H

#include "msg/Message.h"
#include "include/cephfs/types.h" // for nest_info_t, quota_info_t

class MClientQuota final : public SafeMessage {
public:
  inodeno_t ino;
  nest_info_t rstat;
  quota_info_t quota;

protected:
  MClientQuota() :
    SafeMessage{CEPH_MSG_CLIENT_QUOTA},
    ino(0)
  {}
  ~MClientQuota() final {}

public:
  std::string_view get_type_name() const override { return "client_quota"; }
  void print(std::ostream& out) const override {
    out << "client_quota(";
    out << " [" << ino << "] ";
    out << rstat << " ";
    out << quota;
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(rstat.rctime, payload);
    encode(rstat.rbytes, payload);
    encode(rstat.rfiles, payload);
    encode(rstat.rsubdirs, payload);
    encode(quota, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(rstat.rctime, p);
    decode(rstat.rbytes, p);
    decode(rstat.rfiles, p);
    decode(rstat.rsubdirs, p);
    decode(quota, p);
    ceph_assert(p.end());
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
