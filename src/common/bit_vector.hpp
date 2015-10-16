// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#ifndef BIT_VECTOR_HPP
#define BIT_VECTOR_HPP

#include "common/Formatter.h"
#include "include/assert.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <stdint.h>
#include <cmath>
#include <list>
#include <vector>
#include <boost/static_assert.hpp>

namespace ceph {

template <uint8_t _bit_count>
class BitVector
{
private:
  static const uint8_t BITS_PER_BYTE = 8;
  static const uint32_t ELEMENTS_PER_BLOCK = BITS_PER_BYTE / _bit_count;
  static const uint8_t MASK = static_cast<uint8_t>((1 << _bit_count) - 1);

  // must be power of 2
  BOOST_STATIC_ASSERT((_bit_count != 0) && !(_bit_count & (_bit_count - 1)));
  BOOST_STATIC_ASSERT(_bit_count <= BITS_PER_BYTE);
public:

  class ConstReference {
  public:
    operator uint8_t() const;
  private:
    friend class BitVector;
    const BitVector &m_bit_vector;
    uint64_t m_offset;

    ConstReference(const BitVector &bit_vector, uint64_t offset);
  };

  class Reference {
  public:
    operator uint8_t() const;
    Reference& operator=(uint8_t v);
  private:
    friend class BitVector;
    BitVector &m_bit_vector;
    uint64_t m_offset;

    Reference(BitVector &bit_vector, uint64_t offset);
  };

  static const uint8_t BIT_COUNT = _bit_count;

  BitVector();

  void set_crc_enabled(bool enabled) {
    m_crc_enabled = enabled;
  }
  void clear();

  void resize(uint64_t elements);
  uint64_t size() const;

  const bufferlist& get_data() const;

  Reference operator[](uint64_t offset);
  ConstReference operator[](uint64_t offset) const;

  void encode_header(bufferlist& bl) const;
  void decode_header(bufferlist::iterator& it);
  uint64_t get_header_length() const;

  void encode_data(bufferlist& bl, uint64_t byte_offset,
		   uint64_t byte_length) const;
  void decode_data(bufferlist::iterator& it, uint64_t byte_offset);
  void get_data_extents(uint64_t offset, uint64_t length,
		        uint64_t *byte_offset, uint64_t *byte_length) const;

  void encode_footer(bufferlist& bl) const;
  void decode_footer(bufferlist::iterator& it);
  uint64_t get_footer_offset() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  bool operator==(const BitVector &b) const;

  static void generate_test_instances(std::list<BitVector *> &o);
private:

  bufferlist m_data;
  uint64_t m_size;
  bool m_crc_enabled;

  mutable __u32 m_header_crc;
  mutable std::vector<__u32> m_data_crcs;

  static void compute_index(uint64_t offset, uint64_t *index, uint64_t *shift);

};

template <uint8_t _b>
BitVector<_b>::BitVector() : m_size(0), m_crc_enabled(true)
{
}

template <uint8_t _b>
void BitVector<_b>::clear() {
  m_data.clear();
  m_data_crcs.clear();
  m_size = 0;
  m_header_crc = 0;
}

template <uint8_t _b>
void BitVector<_b>::resize(uint64_t size) {
  uint64_t buffer_size = (size + ELEMENTS_PER_BLOCK - 1) / ELEMENTS_PER_BLOCK;
  if (buffer_size > m_data.length()) {
    m_data.append_zero(buffer_size - m_data.length());
  } else if (buffer_size < m_data.length()) {
    bufferlist bl;
    bl.substr_of(m_data, 0, buffer_size);
    bl.swap(m_data);
  }
  m_size = size;

  uint64_t block_count = (buffer_size + CEPH_PAGE_SIZE - 1) / CEPH_PAGE_SIZE;
  m_data_crcs.resize(block_count);
}

template <uint8_t _b>
uint64_t BitVector<_b>::size() const {
  return m_size;
}

template <uint8_t _b>
const bufferlist& BitVector<_b>::get_data() const {
  return m_data;
}

template <uint8_t _b>
void BitVector<_b>::compute_index(uint64_t offset, uint64_t *index, uint64_t *shift) {
  *index = offset / ELEMENTS_PER_BLOCK;
  *shift = ((ELEMENTS_PER_BLOCK - 1) - (offset % ELEMENTS_PER_BLOCK)) * _b;
}

template <uint8_t _b>
void BitVector<_b>::encode_header(bufferlist& bl) const {
  bufferlist header_bl;
  ENCODE_START(1, 1, header_bl);
  ::encode(m_size, header_bl);
  ENCODE_FINISH(header_bl);
  m_header_crc = header_bl.crc32c(0);

  ::encode(header_bl, bl);
}

template <uint8_t _b>
void BitVector<_b>::decode_header(bufferlist::iterator& it) {
  bufferlist header_bl;
  ::decode(header_bl, it);

  bufferlist::iterator header_it = header_bl.begin();
  uint64_t size;
  DECODE_START(1, header_it);
  ::decode(size, header_it);
  DECODE_FINISH(header_it);

  resize(size);
  m_header_crc = header_bl.crc32c(0);
}

template <uint8_t _b>
uint64_t BitVector<_b>::get_header_length() const {
  // 4 byte bl length, 6 byte encoding header, 8 byte size
  return 18;
}

template <uint8_t _b>
void BitVector<_b>::encode_data(bufferlist& bl, uint64_t byte_offset,
				uint64_t byte_length) const {
  assert(byte_offset % CEPH_PAGE_SIZE == 0);
  assert(byte_offset + byte_length == m_data.length() ||
	 byte_length % CEPH_PAGE_SIZE == 0);

  uint64_t end_offset = byte_offset + byte_length;
  while (byte_offset < end_offset) {
    uint64_t len = MIN(CEPH_PAGE_SIZE, end_offset - byte_offset);

    bufferlist bit;
    bit.substr_of(m_data, byte_offset, len);
    m_data_crcs[byte_offset / CEPH_PAGE_SIZE] = bit.crc32c(0);

    bl.claim_append(bit);
    byte_offset += CEPH_PAGE_SIZE;
  }
}

template <uint8_t _b>
void BitVector<_b>::decode_data(bufferlist::iterator& it, uint64_t byte_offset) {
  assert(byte_offset % CEPH_PAGE_SIZE == 0);
  if (it.end()) {
    return;
  }

  uint64_t end_offset = byte_offset + it.get_remaining();
  if (end_offset > m_data.length()) {
    throw buffer::end_of_buffer();
  }

  bufferlist data;
  if (byte_offset > 0) {
    data.substr_of(m_data, 0, byte_offset);
  }

  while (byte_offset < end_offset) {
    uint64_t len = MIN(CEPH_PAGE_SIZE, end_offset - byte_offset);

    bufferlist bit;
    it.copy(len, bit);
    if (m_crc_enabled &&
	m_data_crcs[byte_offset / CEPH_PAGE_SIZE] != bit.crc32c(0)) {
      throw buffer::malformed_input("invalid data block CRC");
    }
    data.append(bit);
    byte_offset += bit.length();
  }

  if (m_data.length() > end_offset) {
    bufferlist tail;
    tail.substr_of(m_data, end_offset, m_data.length() - end_offset);
    data.append(tail);
  }
  assert(data.length() == m_data.length());
  data.swap(m_data);
}

template <uint8_t _b>
void BitVector<_b>::get_data_extents(uint64_t offset, uint64_t length,
				     uint64_t *byte_offset,
				     uint64_t *byte_length) const {
  // read CEPH_PAGE_SIZE-aligned chunks
  assert(length > 0 && offset + length <= m_size);
  uint64_t shift;
  compute_index(offset, byte_offset, &shift);
  *byte_offset -= (*byte_offset % CEPH_PAGE_SIZE);

  uint64_t end_offset;
  compute_index(offset + length - 1, &end_offset, &shift);
  end_offset += (CEPH_PAGE_SIZE - (end_offset % CEPH_PAGE_SIZE));
  assert(*byte_offset <= end_offset);

  *byte_length = end_offset - *byte_offset;
  if (*byte_offset + *byte_length > m_data.length()) {
    *byte_length = m_data.length() - *byte_offset;
  }
}

template <uint8_t _b>
void BitVector<_b>::encode_footer(bufferlist& bl) const {
  bufferlist footer_bl;
  if (m_crc_enabled) {
    ::encode(m_header_crc, footer_bl);
    ::encode(m_data_crcs, footer_bl);
  }
  ::encode(footer_bl, bl);
}

template <uint8_t _b>
void BitVector<_b>::decode_footer(bufferlist::iterator& it) {
  bufferlist footer_bl;
  ::decode(footer_bl, it);

  m_crc_enabled = (footer_bl.length() > 0);
  if (m_crc_enabled) {
    bufferlist::iterator footer_it = footer_bl.begin();

    __u32 header_crc;
    ::decode(header_crc, footer_it);
    if (m_header_crc != header_crc) {
      throw buffer::malformed_input("incorrect header CRC");
    }

    uint64_t block_count = (m_data.length() + CEPH_PAGE_SIZE - 1) / CEPH_PAGE_SIZE;
    ::decode(m_data_crcs, footer_it);
    if (m_data_crcs.size() != block_count) {
      throw buffer::malformed_input("invalid data block CRCs");
    }
  }
}

template <uint8_t _b>
uint64_t BitVector<_b>::get_footer_offset() const {
  return get_header_length() + m_data.length();
}

template <uint8_t _b>
void BitVector<_b>::encode(bufferlist& bl) const {
  encode_header(bl);
  encode_data(bl, 0, m_data.length());
  encode_footer(bl);
}

template <uint8_t _b>
void BitVector<_b>::decode(bufferlist::iterator& it) {
  decode_header(it);

  bufferlist data_bl;
  if (m_data.length() > 0) {
    it.copy(m_data.length(), data_bl);
  }

  decode_footer(it);

  bufferlist::iterator data_it = data_bl.begin();
  decode_data(data_it, 0);
}

template <uint8_t _b>
void BitVector<_b>::dump(Formatter *f) const {
  f->dump_unsigned("size", m_size);
  f->open_array_section("bit_table");
  for (unsigned i = 0; i < m_data.length(); ++i) {
    f->dump_format("byte", "0x%02hhX", m_data[i]);
  }
  f->close_section();
}

template <uint8_t _b>
bool BitVector<_b>::operator==(const BitVector &b) const {
  return (this->m_size == b.m_size && this->m_data == b.m_data);
}

template <uint8_t _b>
typename BitVector<_b>::Reference BitVector<_b>::operator[](uint64_t offset) {
  return Reference(*this, offset);
}

template <uint8_t _b>
typename BitVector<_b>::ConstReference BitVector<_b>::operator[](uint64_t offset) const {
  return ConstReference(*this, offset);
}

template <uint8_t _b>
BitVector<_b>::ConstReference::ConstReference(const BitVector<_b> &bit_vector,
					      uint64_t offset)
  : m_bit_vector(bit_vector), m_offset(offset)
{
}

template <uint8_t _b>
BitVector<_b>::ConstReference::operator uint8_t() const {
  uint64_t index;
  uint64_t shift;
  this->m_bit_vector.compute_index(this->m_offset, &index, &shift);

  return (this->m_bit_vector.m_data[index] >> shift) & MASK;
}

template <uint8_t _b>
BitVector<_b>::Reference::Reference(BitVector<_b> &bit_vector, uint64_t offset)
  : m_bit_vector(bit_vector), m_offset(offset)
{
}

template <uint8_t _b>
BitVector<_b>::Reference::operator uint8_t() const {
  uint64_t index;
  uint64_t shift;
  this->m_bit_vector.compute_index(this->m_offset, &index, &shift);

  return (this->m_bit_vector.m_data[index] >> shift) & MASK;
}

template <uint8_t _b>
typename BitVector<_b>::Reference& BitVector<_b>::Reference::operator=(uint8_t v) {
  uint64_t index;
  uint64_t shift;
  this->m_bit_vector.compute_index(this->m_offset, &index, &shift);

  uint8_t mask = MASK << shift;
  char packed_value = (this->m_bit_vector.m_data[index] & ~mask) |
		      ((v << shift) & mask);
  this->m_bit_vector.m_data.copy_in(index, 1, &packed_value);
  return *this;
}

template <uint8_t _b>
void BitVector<_b>::generate_test_instances(std::list<BitVector *> &o) {
  o.push_back(new BitVector());

  BitVector *b = new BitVector();
  const uint64_t radix = 1 << b->BIT_COUNT;
  const uint64_t size = 1024;

  b->resize(size);
  for (uint64_t i = 0; i < size; ++i) {
    (*b)[i] = rand() % radix;
  }
  o.push_back(b);
}

}

WRITE_CLASS_ENCODER(ceph::BitVector<2>)

template <uint8_t _b>
inline std::ostream& operator<<(std::ostream& out, const ceph::BitVector<_b> &b)
{
  out << "ceph::BitVector<" << _b << ">(size=" << b.size() << ", data="
      << b.get_data() << ")";
  return out;
}

#endif // BIT_VECTOR_HPP
