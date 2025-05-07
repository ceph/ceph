// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include <iostream>

namespace rbd {
namespace mirror {
namespace image_map {

const std::string UNMAPPED_INSTANCE_ID("");

namespace {

template <typename E>
class GetTypeVisitor {
public:
  template <typename T>
  inline E operator()(const T&) const {
    return T::TYPE;
  }
};

class EncodeVisitor {
public:
  explicit EncodeVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename T>
  inline void operator()(const T& t) const {
    using ceph::encode;
    encode(static_cast<uint32_t>(T::TYPE), m_bl);
    t.encode(m_bl);
  }
private:
  bufferlist &m_bl;
};

class DecodeVisitor {
public:
  DecodeVisitor(__u8 version, bufferlist::const_iterator &iter)
    : m_version(version), m_iter(iter) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    t.decode(m_version, m_iter);
  }
private:
  __u8 m_version;
  bufferlist::const_iterator &m_iter;
};

class DumpVisitor {
public:
  explicit DumpVisitor(Formatter *formatter, const std::string &key)
    : m_formatter(formatter), m_key(key) {}

  template <typename T>
  inline void operator()(const T& t) const {
    auto type = T::TYPE;
    m_formatter->dump_string(m_key.c_str(), stringify(type));
    t.dump(m_formatter);
  }
private:
  ceph::Formatter *m_formatter;
  std::string m_key;
};

} // anonymous namespace

PolicyMetaType PolicyData::get_policy_meta_type() const {
  return std::visit(GetTypeVisitor<PolicyMetaType>(), policy_meta);
}

void PolicyData::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  std::visit(EncodeVisitor(bl), policy_meta);
  ENCODE_FINISH(bl);
}

void PolicyData::decode(bufferlist::const_iterator& it) {
  DECODE_START(1, it);

  uint32_t policy_meta_type;
  decode(policy_meta_type, it);

  switch (policy_meta_type) {
  case POLICY_META_TYPE_NONE:
    policy_meta = PolicyMetaNone();
    break;
  default:
    policy_meta = PolicyMetaUnknown();
    break;
  }

  std::visit(DecodeVisitor(struct_v, it), policy_meta);
  DECODE_FINISH(it);
}

void PolicyData::dump(Formatter *f) const {
  std::visit(DumpVisitor(f, "policy_meta_type"), policy_meta);
}

void PolicyData::generate_test_instances(std::list<PolicyData *> &o) {
  o.push_back(new PolicyData(PolicyMetaNone()));
}

std::ostream &operator<<(std::ostream &os, const ActionType& action_type) {
  switch (action_type) {
  case ACTION_TYPE_NONE:
    os << "NONE";
    break;
  case ACTION_TYPE_MAP_UPDATE:
    os << "MAP_UPDATE";
    break;
  case ACTION_TYPE_MAP_REMOVE:
    os << "MAP_REMOVE";
    break;
  case ACTION_TYPE_ACQUIRE:
    os << "ACQUIRE";
    break;
  case ACTION_TYPE_RELEASE:
    os << "RELEASE";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(action_type) << ")";
    break;
  }
  return os;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
