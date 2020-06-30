// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "onode.h"
#include "include/encoding.h"

namespace crimson::os::seastore {

size_t Onode::size() const
{
  return ceph::encoded_sizeof(*this);
}

void Onode::encode(void* buffer, size_t len)
{
  struct [[gnu::packed]] encoded_t {
    uint8_t struct_v;
    uint8_t struct_compat;
    uint32_t struct_len;
    uint32_t len;
    char data[];
  };
  auto p = reinterpret_cast<encoded_t*>(buffer);
  assert(std::numeric_limits<uint16_t>::max() >= size());
  assert(len >= size());
  p->struct_v = 1;
  p->struct_compat = 1;
  p->struct_len = sizeof(encoded_t) + payload.size();
  p->len = payload.size();
  std::memcpy(p->data, payload.data(), payload.size());
}

bool operator==(const Onode& lhs, const Onode& rhs)
{
  return lhs.get() == rhs.get();
}

std::ostream& operator<<(std::ostream &out, const Onode &rhs)
{
  return out << rhs.get();
}

}

