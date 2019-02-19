#ifndef CEPH_MCLIENTWORM_H
#define CEPH_MCLIENTWORM_H

#include "msg/Message.h"

struct MClientWorm : public Message {
public:
  inodeno_t ino;
  worm_info_t worm;
  
protected:
  MClientWorm() :
    Message(CEPH_MSG_CLIENT_WORM)
  {}

  ~MClientWorm() override {}

public:
  std::string_view get_type_name() const override { return "client_worm"; }
  void print(ostream& out) const override {
    out << "client_worm(";    
    out << " [" << ino << "] ";
    out << worm;
    out << ")";
  }

  void encode_payload(uint64_t features) override {  
    using ceph::encode;
    encode(ino, payload);
    encode(worm, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(ino, p);
    decode(worm, p);
    ceph_assert(p.end());
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);

};
#endif
