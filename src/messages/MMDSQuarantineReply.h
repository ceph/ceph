#pragma once
#include "messages/MMDSOp.h"

// only success is reported within timeout
// after timeout, the auth mds will anyway report a failure
class MMDSQuarantineReply final : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  inodeno_t qtine_root_ino; // the inode number of the top level/sub-volume dir
  int	    oserrno;

protected:
  MMDSQuarantineReply() : MMDSOp{MSG_MDS_QUARANTINEDIR_REPLY, HEAD_VERSION, COMPAT_VERSION} {}
  MMDSQuarantineReply(inodeno_t ino, int oserrno) :
    MMDSOp{MSG_MDS_QUARANTINEDIR_REPLY, HEAD_VERSION, COMPAT_VERSION},
    qtine_root_ino(ino), oserrno(oserrno) {}
  ~MMDSQuarantineReply() final {}

public:
  std::string_view get_type_name() const override { return "handle_quarantine_reply"; }
  void print(std::ostream& o) const override {
    o << "handle_quarantine_reply(ino:" << qtine_root_ino << ", oserrno:" << oserrno << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(qtine_root_ino, payload);
    encode(oserrno, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(qtine_root_ino, p);
    decode(oserrno, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};
