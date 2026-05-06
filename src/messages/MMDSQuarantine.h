#ifndef CEPH_MMDSQUARANTINE_H
#define CEPH_MMDSQUARANTINE_H

#include "messages/MMDSOp.h"


class MMDSQuarantine final : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  inodeno_t qtine_root_ino;
  unsigned qtine_op = 0; // QUARANTINE_NONE (NOP)

protected:
  MMDSQuarantine() : MMDSOp{MSG_MDS_QUARANTINEDIR, HEAD_VERSION, COMPAT_VERSION} {}
  MMDSQuarantine(inodeno_t ino, unsigned qtine_op) :
    MMDSOp{MSG_MDS_QUARANTINEDIR, HEAD_VERSION, COMPAT_VERSION},
    qtine_root_ino(ino), qtine_op(qtine_op) {}
  ~MMDSQuarantine() final {}

public:
  std::string_view get_type_name() const override { return "handle_quarantine"; }
  void print(std::ostream& o) const override {
    o << "handle_quarantine(ino:" << qtine_root_ino << ", qtine_op:" << qtine_op << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(qtine_root_ino, payload);
    encode(qtine_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(qtine_root_ino, p);
    decode(qtine_op, p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
