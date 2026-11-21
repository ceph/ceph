// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <ostream>
#include <vector>

#include "include/buffer.h"
#include "include/encoding.h"

namespace ceph {

/// A set of feature bit numbers without a size limit, for features
/// negotiated beyond the 64 connection feature bits.  It encodes like
/// CephFS's feature_bitset_t: a byte length, then little-endian words.
class feature_set_t {
public:
  using block_type = uint64_t;
  static constexpr unsigned bits_per_block = 64;
  /// decode() ignores longer sets' later words, so a peer cannot make us
  /// allocate
  static constexpr unsigned max_blocks = 64;
  /// so feature numbers must stay below this
  static constexpr unsigned max_bits = max_blocks * bits_per_block;

  feature_set_t() = default;
  feature_set_t(std::initializer_list<unsigned> bits) {
    for (auto b : bits) {
      insert(b);
    }
  }

  bool empty() const {
    return blocks.empty();
  }
  unsigned num_blocks() const {
    return blocks.size();
  }
  bool test(unsigned bit) const {
    unsigned n = bit / bits_per_block;
    return n < blocks.size() &&
      (blocks[n] & (block_type(1) << (bit % bits_per_block)));
  }
  /// bits from max_bits on are ignored: no peer could decode them
  void insert(unsigned bit) {
    if (bit >= max_bits) {
      return;
    }
    unsigned n = bit / bits_per_block;
    if (n >= blocks.size()) {
      blocks.resize(n + 1);
    }
    blocks[n] |= block_type(1) << (bit % bits_per_block);
  }
  void erase(unsigned bit) {
    unsigned n = bit / bits_per_block;
    if (n < blocks.size()) {
      blocks[n] &= ~(block_type(1) << (bit % bits_per_block));
      trim();
    }
  }
  void clear() {
    blocks.clear();
  }

  /// the bits of this set that are not in @p other
  feature_set_t operator-(const feature_set_t& other) const {
    feature_set_t r = *this;
    for (size_t i = 0; i < std::min(r.blocks.size(), other.blocks.size()); ++i) {
      r.blocks[i] &= ~other.blocks[i];
    }
    r.trim();
    return r;
  }
  feature_set_t operator&(const feature_set_t& other) const {
    feature_set_t r;
    r.blocks.resize(std::min(blocks.size(), other.blocks.size()));
    for (size_t i = 0; i < r.blocks.size(); ++i) {
      r.blocks[i] = blocks[i] & other.blocks[i];
    }
    r.trim();
    return r;
  }
  feature_set_t& operator|=(const feature_set_t& other) {
    if (other.blocks.size() > blocks.size()) {
      blocks.resize(other.blocks.size());
    }
    for (size_t i = 0; i < other.blocks.size(); ++i) {
      blocks[i] |= other.blocks[i];
    }
    return *this;
  }
  bool contains(const feature_set_t& other) const {
    return (other - *this).empty();
  }
  bool operator==(const feature_set_t& other) const {
    return blocks == other.blocks;
  }
  bool operator<(const feature_set_t& other) const {
    // blocks are trimmed, so a longer set is a larger number
    if (blocks.size() != other.blocks.size()) {
      return blocks.size() < other.blocks.size();
    }
    return std::lexicographical_compare(blocks.rbegin(), blocks.rend(),
                                        other.blocks.rbegin(),
                                        other.blocks.rend());
  }

  template <typename F>
  void for_each(F&& f) const {
    for (unsigned i = 0; i < blocks.size(); ++i) {
      for (unsigned b = 0; b < bits_per_block; ++b) {
        if (blocks[i] & (block_type(1) << b)) {
          f(i * bits_per_block + b);
        }
      }
    }
  }

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(uint32_t(blocks.size() * sizeof(block_type)), bl);
    for (auto b : blocks) {
      encode(b, bl);
    }
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    uint32_t len;
    decode(len, p);
    if (len % sizeof(block_type)) {
      throw ceph::buffer::malformed_input("bad feature set length");
    }
    uint32_t n = len / sizeof(block_type);
    blocks.resize(std::min(n, max_blocks));
    for (auto& b : blocks) {
      decode(b, p);
    }
    // a later release may know more features than any we could decode
    p += (n - blocks.size()) * sizeof(block_type);
    trim();
  }

  friend std::ostream& operator<<(std::ostream& out, const feature_set_t& s) {
    out << "{";
    bool first = true;
    s.for_each([&](unsigned bit) {
      out << (first ? "" : ",") << bit;
      first = false;
    });
    return out << "}";
  }

private:
  void trim() {
    while (!blocks.empty() && blocks.back() == 0) {
      blocks.pop_back();
    }
  }

  std::vector<block_type> blocks;
};
WRITE_CLASS_ENCODER(feature_set_t)

} // namespace ceph
