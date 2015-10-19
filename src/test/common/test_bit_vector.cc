// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <gtest/gtest.h>
#include <cmath>
#include "common/bit_vector.hpp"
#include <boost/assign/list_of.hpp>

using namespace ceph;

template <uint8_t _bit_count>
class TestParams {
public:
  static const uint8_t BIT_COUNT = _bit_count;
};

template <typename T>
class BitVectorTest : public ::testing::Test {
public:
  typedef BitVector<T::BIT_COUNT> bit_vector_t;
};

typedef ::testing::Types<TestParams<2> > BitVectorTypes;
TYPED_TEST_CASE(BitVectorTest, BitVectorTypes);

TYPED_TEST(BitVectorTest, resize) {
  typename TestFixture::bit_vector_t bit_vector;

  size_t size = 2357;

  double elements_per_byte = 8 / bit_vector.BIT_COUNT;

  bit_vector.resize(size);
  ASSERT_EQ(bit_vector.size(), size);
  ASSERT_EQ(bit_vector.get_data().length(), static_cast<uint64_t>(std::ceil(
    size / elements_per_byte)));
}

TYPED_TEST(BitVectorTest, clear) {
  typename TestFixture::bit_vector_t bit_vector;

  bit_vector.resize(123);
  bit_vector.clear();
  ASSERT_EQ(0ull, bit_vector.size());
  ASSERT_EQ(0ull, bit_vector.get_data().length());
}

TYPED_TEST(BitVectorTest, bit_order) {
  typename TestFixture::bit_vector_t bit_vector;
  bit_vector.resize(1);

  uint8_t value = 1;
  bit_vector[0] = value;

  value <<= (8 - bit_vector.BIT_COUNT);
  ASSERT_EQ(value, bit_vector.get_data()[0]);
}

TYPED_TEST(BitVectorTest, get_set) {
  typename TestFixture::bit_vector_t bit_vector;
  std::vector<uint64_t> ref;

  uint64_t radix = 1 << bit_vector.BIT_COUNT;

  size_t size = 1024;
  bit_vector.resize(size);
  ref.resize(size);
  for (size_t i = 0; i < size; ++i) {
    uint64_t v = rand() % radix;
    ref[i] = v;
    bit_vector[i] = v;
  }

  const typename TestFixture::bit_vector_t &const_bit_vector(bit_vector);
  for (size_t i = 0; i < size; ++i) {
    ASSERT_EQ(ref[i], bit_vector[i]);
    ASSERT_EQ(ref[i], const_bit_vector[i]);
  }
}

TYPED_TEST(BitVectorTest, get_buffer_extents) {
  typename TestFixture::bit_vector_t bit_vector;

  uint64_t element_count = 2 * CEPH_PAGE_SIZE + 51;
  uint64_t elements_per_byte = 8 / bit_vector.BIT_COUNT;
  bit_vector.resize(element_count * elements_per_byte);

  uint64_t offset = (CEPH_PAGE_SIZE + 11) * elements_per_byte;
  uint64_t length = (CEPH_PAGE_SIZE + 31) * elements_per_byte;
  uint64_t byte_offset;
  uint64_t byte_length;
  bit_vector.get_data_extents(offset, length, &byte_offset, &byte_length);
  ASSERT_EQ(CEPH_PAGE_SIZE, byte_offset);
  ASSERT_EQ(CEPH_PAGE_SIZE + (element_count % CEPH_PAGE_SIZE), byte_length);

  bit_vector.get_data_extents(1, 1, &byte_offset, &byte_length);
  ASSERT_EQ(0U, byte_offset);
  ASSERT_EQ(CEPH_PAGE_SIZE, byte_length);
}

TYPED_TEST(BitVectorTest, get_header_length) {
  typename TestFixture::bit_vector_t bit_vector;

  bufferlist bl;
  bit_vector.encode_header(bl);
  ASSERT_EQ(bl.length(), bit_vector.get_header_length());
}

TYPED_TEST(BitVectorTest, get_footer_offset) {
  typename TestFixture::bit_vector_t bit_vector;

  bit_vector.resize(5111);

  uint64_t byte_offset;
  uint64_t byte_length;
  bit_vector.get_data_extents(0, bit_vector.size(), &byte_offset, &byte_length);

  ASSERT_EQ(bit_vector.get_header_length() + byte_length,
	    bit_vector.get_footer_offset());
}

TYPED_TEST(BitVectorTest, partial_decode_encode) {
  typename TestFixture::bit_vector_t bit_vector;

  uint64_t elements_per_byte = 8 / bit_vector.BIT_COUNT;
  bit_vector.resize(9161 * elements_per_byte);
  for (uint64_t i = 0; i < bit_vector.size(); ++i) {
    bit_vector[i] = i % 4;
  }

  bufferlist bl;
  ::encode(bit_vector, bl);
  bit_vector.clear();

  bufferlist header_bl;
  header_bl.substr_of(bl, 0, bit_vector.get_header_length());
  bufferlist::iterator header_it = header_bl.begin();
  bit_vector.decode_header(header_it);

  bufferlist footer_bl;
  footer_bl.substr_of(bl, bit_vector.get_footer_offset(),
		      bl.length() - bit_vector.get_footer_offset());
  bufferlist::iterator footer_it = footer_bl.begin();
  bit_vector.decode_footer(footer_it);

  typedef std::pair<uint64_t, uint64_t> Extent;
  typedef std::list<Extent> Extents;

  Extents extents = boost::assign::list_of(
    std::make_pair(0, 1))(
    std::make_pair((CEPH_PAGE_SIZE * elements_per_byte) - 2, 4))(
    std::make_pair((CEPH_PAGE_SIZE * elements_per_byte) + 2, 2))(
    std::make_pair((2 * CEPH_PAGE_SIZE * elements_per_byte) - 2, 4))(
    std::make_pair((2 * CEPH_PAGE_SIZE * elements_per_byte) + 2, 2))(
    std::make_pair(2, 2 * CEPH_PAGE_SIZE));
  for (Extents::iterator it = extents.begin(); it != extents.end(); ++it) {
    uint64_t element_offset = it->first;
    uint64_t element_length = it->second;
    uint64_t byte_offset;
    uint64_t byte_length;
    bit_vector.get_data_extents(element_offset, element_length, &byte_offset,
                                &byte_length);

    bufferlist data_bl;
    data_bl.substr_of(bl, bit_vector.get_header_length() + byte_offset,
		      byte_length);
    bufferlist::iterator data_it = data_bl.begin();
    bit_vector.decode_data(data_it, byte_offset);

    data_bl.clear();
    bit_vector.encode_data(data_bl, byte_offset, byte_length);

    footer_bl.clear();
    bit_vector.encode_footer(footer_bl);

    bufferlist updated_bl;
    updated_bl.substr_of(bl, 0, bit_vector.get_header_length() + byte_offset);
    updated_bl.append(data_bl);

    if (byte_offset + byte_length < bit_vector.get_footer_offset()) {
      uint64_t tail_data_offset = bit_vector.get_header_length() + byte_offset +
                                  byte_length;
      data_bl.substr_of(bl, tail_data_offset,
		        bit_vector.get_footer_offset() - tail_data_offset);
      updated_bl.append(data_bl);
    }

    updated_bl.append(footer_bl);
    ASSERT_EQ(bl, updated_bl);

    bufferlist::iterator updated_it = updated_bl.begin();
    ::decode(bit_vector, updated_it);
  }
}

TYPED_TEST(BitVectorTest, header_crc) {
  typename TestFixture::bit_vector_t bit_vector;

  bufferlist header;
  bit_vector.encode_header(header);

  bufferlist footer;
  bit_vector.encode_footer(footer);

  bufferlist::iterator it = footer.begin();
  bit_vector.decode_footer(it);

  bit_vector.resize(1);
  bit_vector.encode_header(header);

  it = footer.begin();
  ASSERT_THROW(bit_vector.decode_footer(it), buffer::malformed_input);
}

TYPED_TEST(BitVectorTest, data_crc) {
  typename TestFixture::bit_vector_t bit_vector1;
  typename TestFixture::bit_vector_t bit_vector2;

  uint64_t elements_per_byte = 8 / bit_vector1.BIT_COUNT;
  bit_vector1.resize((CEPH_PAGE_SIZE + 1) * elements_per_byte);
  bit_vector2.resize((CEPH_PAGE_SIZE + 1) * elements_per_byte);

  uint64_t byte_offset;
  uint64_t byte_length;
  bit_vector1.get_data_extents(0, bit_vector1.size(), &byte_offset,
			       &byte_length);

  bufferlist data;
  bit_vector1.encode_data(data, byte_offset, byte_length);

  bufferlist::iterator data_it = data.begin();
  bit_vector1.decode_data(data_it, byte_offset); 

  bit_vector2[bit_vector2.size() - 1] = 1;

  bufferlist dummy_data;
  bit_vector2.encode_data(dummy_data, byte_offset, byte_length);

  data_it = data.begin();
  ASSERT_THROW(bit_vector2.decode_data(data_it, byte_offset),
	       buffer::malformed_input);
}
