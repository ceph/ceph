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
#include "include/ceph_assert.h"
#include "include/encoding.h"
#include <memory>
#include <utility>
#include <vector>

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

  template <typename DataIterator>
  class ReferenceImpl {
  protected:
    DataIterator m_data_iterator;
    uint64_t m_shift;

    ReferenceImpl(const DataIterator& data_iterator, uint64_t shift)
      : m_data_iterator(data_iterator), m_shift(shift) {
    }
    ReferenceImpl(DataIterator&& data_iterator, uint64_t shift)
      : m_data_iterator(std::move(data_iterator)), m_shift(shift) {
    }

  public:
    inline operator uint8_t() const {
      return (*m_data_iterator >> m_shift) & MASK;
    }
  };

public:

  class ConstReference : public ReferenceImpl<bufferlist::const_iterator> {
  private:
    friend class BitVector;

    ConstReference(const bufferlist::const_iterator& data_iterator,
                   uint64_t shift)
      : ReferenceImpl<bufferlist::const_iterator>(data_iterator, shift) {
    }
    ConstReference(bufferlist::const_iterator&& data_iterator, uint64_t shift)
      : ReferenceImpl<bufferlist::const_iterator>(std::move(data_iterator),
                                                  shift) {
    }
  };

  class Reference : public ReferenceImpl<bufferlist::iterator> {
  public:
    Reference& operator=(uint8_t v);

  private:
    friend class BitVector;

    Reference(const bufferlist::iterator& data_iterator, uint64_t shift)
      : ReferenceImpl<bufferlist::iterator>(data_iterator, shift) {
    }
    Reference(bufferlist::iterator&& data_iterator, uint64_t shift)
      : ReferenceImpl<bufferlist::iterator>(std::move(data_iterator), shift) {
    }
  };

public:
  template <typename BitVectorT, typename DataIterator>
  class IteratorImpl {
  private:
    friend class BitVector;

    uint64_t m_offset = 0;
    BitVectorT *m_bit_vector;

    // cached derived values
    uint64_t m_index = 0;
    uint64_t m_shift = 0;
    DataIterator m_data_iterator;

    IteratorImpl(BitVectorT *bit_vector, uint64_t offset)
      : m_bit_vector(bit_vector),
        m_data_iterator(bit_vector->m_data.begin()) {
      *this += offset;
    }

  public:
    inline IteratorImpl& operator++() {
      ++m_offset;

      uint64_t index;
      compute_index(m_offset, &index, &m_shift);

      ceph_assert(index == m_index || index == m_index + 1);
      if (index > m_index) {
        m_index = index;
        ++m_data_iterator;
      }
      return *this;
    }
    inline IteratorImpl& operator+=(uint64_t offset) {
      m_offset += offset;
      compute_index(m_offset, &m_index, &m_shift);
      if (m_offset < m_bit_vector->size()) {
        m_data_iterator.seek(m_index);
      } else {
        m_data_iterator = m_bit_vector->m_data.end();
      }
      return *this;
    }

    inline IteratorImpl operator++(int) {
      IteratorImpl iterator_impl(*this);
      ++iterator_impl;
      return iterator_impl;
    }
    inline IteratorImpl operator+(uint64_t offset) {
      IteratorImpl iterator_impl(*this);
      iterator_impl += offset;
      return iterator_impl;
    }

    inline bool operator==(const IteratorImpl& rhs) const {
      return (m_offset == rhs.m_offset && m_bit_vector == rhs.m_bit_vector);
    }
    inline bool operator!=(const IteratorImpl& rhs) const {
      return (m_offset != rhs.m_offset || m_bit_vector != rhs.m_bit_vector);
    }

    inline ConstReference operator*() const {
      return ConstReference(m_data_iterator, m_shift);
    }
    inline Reference operator*() {
      return Reference(m_data_iterator, m_shift);
    }
  };

  typedef IteratorImpl<const BitVector,
                       bufferlist::const_iterator> ConstIterator;
  typedef IteratorImpl<BitVector, bufferlist::iterator> Iterator;

  static const uint32_t BLOCK_SIZE;
  static const uint8_t BIT_COUNT = _bit_count;

  BitVector();

  inline ConstIterator begin() const {
    return ConstIterator(this, 0);
  }
  inline ConstIterator end() const {
    return ConstIterator(this, m_size);
  }
  inline Iterator begin() {
    return Iterator(this, 0);
  }
  inline Iterator end() {
    return Iterator(this, m_size);
  }

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
  void decode_header(bufferlist::const_iterator& it);
  uint64_t get_header_length() const;

  void encode_data(bufferlist& bl, uint64_t data_byte_offset,
		   uint64_t byte_length) const;
  void decode_data(bufferlist::const_iterator& it, uint64_t data_byte_offset);
  void get_data_extents(uint64_t offset, uint64_t length,
                        uint64_t *data_byte_offset,
                        uint64_t *object_byte_offset,
                        uint64_t *byte_length) const;

  void encode_footer(bufferlist& bl) const;
  void decode_footer(bufferlist::const_iterator& it);
  uint64_t get_footer_offset() const;

  void decode_header_crc(bufferlist::const_iterator& it);
  void get_header_crc_extents(uint64_t *byte_offset,
                              uint64_t *byte_length) const;

  void encode_data_crcs(bufferlist& bl, uint64_t offset,
                        uint64_t length) const;
  void decode_data_crcs(bufferlist::const_iterator& it, uint64_t offset);
  void get_data_crcs_extents(uint64_t offset, uint64_t length,
                             uint64_t *byte_offset,
                             uint64_t *byte_length) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  bool operator==(const BitVector &b) const;

  static void generate_test_instances(std::list<BitVector *> &o);
private:
  struct NoInitAllocator : public std::allocator<__u32> {
    NoInitAllocator(const std::allocator<__u32>& alloc)
      : std::allocator<__u32>(alloc) {
    }

    template <class U, class... Args>
    void construct(U* p, Args&&... args) const {
    }
  };

  bufferlist m_data;
  uint64_t m_size;
  bool m_crc_enabled;

  mutable __u32 m_header_crc;
  mutable std::vector<__u32, NoInitAllocator> m_data_crcs;

  void resize(uint64_t elements, bool zero);

  static void compute_index(uint64_t offset, uint64_t *index, uint64_t *shift);

};

template <uint8_t _b>
const uint32_t BitVector<_b>::BLOCK_SIZE = 4096;

template <uint8_t _b>
BitVector<_b>::BitVector() : m_size(0), m_crc_enabled(true), m_header_crc(0)
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
  resize(size, true);
}

template <uint8_t _b>
void BitVector<_b>::resize(uint64_t size, bool zero) {
  uint64_t buffer_size = (size + ELEMENTS_PER_BLOCK - 1) / ELEMENTS_PER_BLOCK;
  if (buffer_size > m_data.length()) {
    if (zero) {
      m_data.append_zero(buffer_size - m_data.length());
    } else {
      m_data.append(std::move(buffer::ptr(buffer_size - m_data.length())));
    }
  } else if (buffer_size < m_data.length()) {
    bufferlist bl;
    bl.substr_of(m_data, 0, buffer_size);
    bl.swap(m_data);
  }
  m_size = size;

  uint64_t block_count = (buffer_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
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
  encode(m_size, header_bl);
  ENCODE_FINISH(header_bl);
  m_header_crc = header_bl.crc32c(0);

  encode(header_bl, bl);
}

template <uint8_t _b>
void BitVector<_b>::decode_header(bufferlist::const_iterator& it) {
  using ceph::decode;
  bufferlist header_bl;
  decode(header_bl, it);

  auto header_it = header_bl.cbegin();
  uint64_t size;
  DECODE_START(1, header_it);
  decode(size, header_it);
  DECODE_FINISH(header_it);

  resize(size, false);
  m_header_crc = header_bl.crc32c(0);
}

template <uint8_t _b>
uint64_t BitVector<_b>::get_header_length() const {
  // 4 byte bl length, 6 byte encoding header, 8 byte size
  return 18;
}

template <uint8_t _b>
void BitVector<_b>::encode_data(bufferlist& bl, uint64_t data_byte_offset,
				uint64_t byte_length) const {
  ceph_assert(data_byte_offset % BLOCK_SIZE == 0);
  ceph_assert(data_byte_offset + byte_length == m_data.length() ||
              byte_length % BLOCK_SIZE == 0);

  uint64_t end_offset = data_byte_offset + byte_length;
  while (data_byte_offset < end_offset) {
    uint64_t len = std::min<uint64_t>(BLOCK_SIZE,
                                      end_offset - data_byte_offset);

    bufferlist bit;
    bit.substr_of(m_data, data_byte_offset, len);
    m_data_crcs[data_byte_offset / BLOCK_SIZE] = bit.crc32c(0);

    bl.claim_append(bit);
    data_byte_offset += BLOCK_SIZE;
  }
}

template <uint8_t _b>
void BitVector<_b>::decode_data(bufferlist::const_iterator& it,
                                uint64_t data_byte_offset) {
  ceph_assert(data_byte_offset % BLOCK_SIZE == 0);
  if (it.end()) {
    return;
  }

  uint64_t end_offset = data_byte_offset + it.get_remaining();
  if (end_offset > m_data.length()) {
    throw buffer::end_of_buffer();
  }

  bufferlist data;
  if (data_byte_offset > 0) {
    data.substr_of(m_data, 0, data_byte_offset);
  }

  while (data_byte_offset < end_offset) {
    uint64_t len = std::min<uint64_t>(BLOCK_SIZE, end_offset - data_byte_offset);

    bufferptr ptr;
    it.copy_deep(len, ptr);

    bufferlist bit;
    bit.append(ptr);
    if (m_crc_enabled &&
	m_data_crcs[data_byte_offset / BLOCK_SIZE] != bit.crc32c(0)) {
      throw buffer::malformed_input("invalid data block CRC");
    }
    data.append(bit);
    data_byte_offset += bit.length();
  }

  if (m_data.length() > end_offset) {
    bufferlist tail;
    tail.substr_of(m_data, end_offset, m_data.length() - end_offset);
    data.append(tail);
  }
  ceph_assert(data.length() == m_data.length());
  data.swap(m_data);
}

template <uint8_t _b>
void BitVector<_b>::get_data_extents(uint64_t offset, uint64_t length,
                                     uint64_t *data_byte_offset,
                                     uint64_t *object_byte_offset,
                                     uint64_t *byte_length) const {
  // read BLOCK_SIZE-aligned chunks
  ceph_assert(length > 0 && offset + length <= m_size);
  uint64_t shift;
  compute_index(offset, data_byte_offset, &shift);
  *data_byte_offset -= (*data_byte_offset % BLOCK_SIZE);

  uint64_t end_offset;
  compute_index(offset + length - 1, &end_offset, &shift);
  end_offset += (BLOCK_SIZE - (end_offset % BLOCK_SIZE));
  ceph_assert(*data_byte_offset <= end_offset);

  *object_byte_offset = get_header_length() + *data_byte_offset;
  *byte_length = end_offset - *data_byte_offset;
  if (*data_byte_offset + *byte_length > m_data.length()) {
    *byte_length = m_data.length() - *data_byte_offset;
  }
}

template <uint8_t _b>
void BitVector<_b>::encode_footer(bufferlist& bl) const {
  using ceph::encode;
  bufferlist footer_bl;
  if (m_crc_enabled) {
    encode(m_header_crc, footer_bl);

    __u32 size = m_data_crcs.size();
    encode(size, footer_bl);
    encode_data_crcs(footer_bl, 0, m_size);
  }
  encode(footer_bl, bl);
}

template <uint8_t _b>
void BitVector<_b>::decode_footer(bufferlist::const_iterator& it) {
  using ceph::decode;
  bufferlist footer_bl;
  decode(footer_bl, it);

  m_crc_enabled = (footer_bl.length() > 0);
  if (m_crc_enabled) {
    auto footer_it = footer_bl.cbegin();
    decode_header_crc(footer_it);

    __u32 data_src_size;
    decode(data_src_size, footer_it);
    decode_data_crcs(footer_it, 0);

    uint64_t block_count = (m_data.length() + BLOCK_SIZE - 1) / BLOCK_SIZE;
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
void BitVector<_b>::decode_header_crc(bufferlist::const_iterator& it) {
  if (it.get_remaining() > 0) {
    __u32 header_crc;
    ceph::decode(header_crc, it);
    if (m_header_crc != header_crc) {
      throw buffer::malformed_input("incorrect header CRC");
    }
  }
}

template <uint8_t _b>
void BitVector<_b>::get_header_crc_extents(uint64_t *byte_offset,
                                           uint64_t *byte_length) const {
  // footer is prefixed with a bufferlist length
  *byte_offset = get_footer_offset() + sizeof(__u32);
  *byte_length = sizeof(__u32);
}

template <uint8_t _b>
void BitVector<_b>::encode_data_crcs(bufferlist& bl, uint64_t offset,
                                     uint64_t length) const {
  if (length == 0) {
    return;
  }

  uint64_t index;
  uint64_t shift;
  compute_index(offset, &index, &shift);
  uint64_t crc_index = index / BLOCK_SIZE;

  compute_index(offset + length - 1, &index, &shift);
  uint64_t end_crc_index = index / BLOCK_SIZE;
  while (crc_index <= end_crc_index) {
    __u32 crc = m_data_crcs[crc_index++];
    ceph::encode(crc, bl);
  }
}

template <uint8_t _b>
void BitVector<_b>::decode_data_crcs(bufferlist::const_iterator& it,
                                     uint64_t offset) {
  if (it.end()) {
    return;
  }

  uint64_t index;
  uint64_t shift;
  compute_index(offset, &index, &shift);

  uint64_t crc_index = index / BLOCK_SIZE;
  uint64_t remaining = it.get_remaining() / sizeof(__u32);
  while (remaining > 0) {
    __u32 crc;
    ceph::decode(crc, it);
    m_data_crcs[crc_index++] = crc;
    --remaining;
  }
}

template <uint8_t _b>
void BitVector<_b>::get_data_crcs_extents(uint64_t offset, uint64_t length,
                                          uint64_t *byte_offset,
                                          uint64_t *byte_length) const {
  // data CRCs immediately follow the header CRC
  get_header_crc_extents(byte_offset, byte_length);
  *byte_offset += *byte_length;

  // skip past data CRC vector size
  *byte_offset += sizeof(__u32);

  // CRCs are computed over BLOCK_SIZE chunks
  ceph_assert(length > 0 && offset + length <= m_size);
  uint64_t index;
  uint64_t shift;
  compute_index(offset, &index, &shift);
  uint64_t start_byte_offset =
    *byte_offset + ((index / BLOCK_SIZE) * sizeof(__u32));

  compute_index(offset + length, &index, &shift);
  uint64_t end_byte_offset =
    *byte_offset + (((index / BLOCK_SIZE) + 1) * sizeof(__u32));
  ceph_assert(start_byte_offset < end_byte_offset);

  *byte_offset = start_byte_offset;
  *byte_length = end_byte_offset - start_byte_offset;
}

template <uint8_t _b>
void BitVector<_b>::encode(bufferlist& bl) const {
  encode_header(bl);
  encode_data(bl, 0, m_data.length());
  encode_footer(bl);
}

template <uint8_t _b>
void BitVector<_b>::decode(bufferlist::const_iterator& it) {
  decode_header(it);

  bufferlist data_bl;
  if (m_data.length() > 0) {
    it.copy(m_data.length(), data_bl);
  }

  decode_footer(it);

  auto data_it = data_bl.cbegin();
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
  uint64_t index;
  uint64_t shift;
  compute_index(offset, &index, &shift);

  bufferlist::iterator data_iterator(m_data.begin());
  data_iterator.seek(index);
  return Reference(std::move(data_iterator), shift);
}

template <uint8_t _b>
typename BitVector<_b>::ConstReference BitVector<_b>::operator[](uint64_t offset) const {
  uint64_t index;
  uint64_t shift;
  compute_index(offset, &index, &shift);

  bufferlist::const_iterator data_iterator(m_data.begin());
  data_iterator.seek(index);
  return ConstReference(std::move(data_iterator), shift);
}

template <uint8_t _b>
typename BitVector<_b>::Reference& BitVector<_b>::Reference::operator=(uint8_t v) {
  uint8_t mask = MASK << this->m_shift;
  char packed_value = (*this->m_data_iterator & ~mask) |
                      ((v << this->m_shift) & mask);
  bufferlist::iterator it(this->m_data_iterator);
  it.copy_in(1, &packed_value, true);
  return *this;
}

template <uint8_t _b>
void BitVector<_b>::generate_test_instances(std::list<BitVector *> &o) {
  o.push_back(new BitVector());

  BitVector *b = new BitVector();
  const uint64_t radix = 1 << b->BIT_COUNT;
  const uint64_t size = 1024;

  b->resize(size, false);
  for (uint64_t i = 0; i < size; ++i) {
    (*b)[i] = rand() % radix;
  }
  o.push_back(b);
}


WRITE_CLASS_ENCODER(ceph::BitVector<2>)

template <uint8_t _b>
inline std::ostream& operator<<(std::ostream& out, const ceph::BitVector<_b> &b)
{
  out << "ceph::BitVector<" << _b << ">(size=" << b.size() << ", data="
      << b.get_data() << ")";
  return out;
}
}

#endif // BIT_VECTOR_HPP
