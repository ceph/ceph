// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

#include <string_view>
#include "common/Formatter.h"

namespace librbd {
namespace crypto {

void ParentCryptoParams::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  uint32_t key_size = wrapped_key.size();
  encode(key_size, bl);
  bl.append(wrapped_key.data(), key_size);
  encode(block_size, bl);
  encode(data_offset, bl);
  ENCODE_FINISH(bl);
}

void ParentCryptoParams::decode(bufferlist::const_iterator& it) {
  DECODE_START(1, it);
  uint32_t key_size;
  decode(key_size, it);
  wrapped_key.resize(key_size);
  it.copy(key_size, wrapped_key.data());
  decode(block_size, it);
  decode(data_offset, it);
  DECODE_FINISH(it);
}

void ParentCryptoParams::dump(Formatter *f) const {
  f->dump_string("wrapped_key",
                 std::string_view(wrapped_key.data(), wrapped_key.size()));
  f->dump_unsigned("block_size", block_size);
  f->dump_unsigned("data_offset", data_offset);
}

void ParentCryptoParams::generate_test_instances(
        std::list<ParentCryptoParams *> &o) {
  o.push_back(new ParentCryptoParams("mykey", 123, 456));
}

} // namespace crypto
} // namespace librbd
