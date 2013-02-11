// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <tr1/memory>
#include <limits.h>

#include "include/buffer.h"
#include "include/encoding.h"
#include "common/environment.h"

#include "gtest/gtest.h"
#include "stdlib.h"

#define MAX_TEST 1000000

TEST(BufferList, constructors) {
  bool ceph_buffer_track = get_env_bool("CEPH_BUFFER_TRACK");
  unsigned len = 17;
  //
  // buffer::create
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    bufferptr ptr(buffer::create(len));
    EXPECT_EQ(len, ptr.length());
    if (ceph_buffer_track)
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
  }
  //
  // buffer::claim_char
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    char* str = new char[len];
    ::memset(str, 'X', len);
    bufferptr ptr(buffer::claim_char(len, str));
    if (ceph_buffer_track)
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    bufferptr clone = ptr.clone();
    EXPECT_EQ(0, ::memcmp(clone.c_str(), ptr.c_str(), len));
  }
  //
  // buffer::create_static
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    char* str = new char[len];
    bufferptr ptr(buffer::create_static(len, str));
    if (ceph_buffer_track)
      EXPECT_EQ(0, buffer::get_total_alloc());
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    delete [] str;
  }
  //
  // buffer::create_malloc
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    bufferptr ptr(buffer::create_malloc(len));
    if (ceph_buffer_track)
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
    EXPECT_EQ(len, ptr.length());
    EXPECT_THROW(buffer::create_malloc((unsigned)ULLONG_MAX), buffer::bad_alloc);
  }
  //
  // buffer::claim_malloc
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    char* str = (char*)malloc(len);
    ::memset(str, 'X', len);
    bufferptr ptr(buffer::claim_malloc(len, str));
    if (ceph_buffer_track)
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    bufferptr clone = ptr.clone();
    EXPECT_EQ(0, ::memcmp(clone.c_str(), ptr.c_str(), len));
  }
  //
  // buffer::copy
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    const std::string expected(len, 'X');
    bufferptr ptr(buffer::copy(expected.c_str(), expected.size()));
    if (ceph_buffer_track)
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
    EXPECT_NE(expected.c_str(), ptr.c_str());
    EXPECT_EQ(0, ::memcmp(expected.c_str(), ptr.c_str(), len));
  }
  //
  // buffer::create_page_aligned
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    bufferptr ptr(buffer::create_page_aligned(len));
    ::memset(ptr.c_str(), 'X', len);
    if (ceph_buffer_track)
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
    EXPECT_THROW(buffer::create_page_aligned((unsigned)ULLONG_MAX), buffer::bad_alloc);
#ifndef DARWIN
    ASSERT_TRUE(ptr.is_page_aligned());
#endif // DARWIN 
    bufferptr clone = ptr.clone();
    EXPECT_EQ(0, ::memcmp(clone.c_str(), ptr.c_str(), len));
  }
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
}

TEST(BufferList, ptr_constructors) {
  unsigned len = 17;
  //
  // ptr::ptr()
  //
  {
    buffer::ptr ptr;
    EXPECT_FALSE(ptr.have_raw());
    EXPECT_EQ((unsigned)0, ptr.offset());
    EXPECT_EQ((unsigned)0, ptr.length());
  }
  //
  // ptr::ptr(raw *r)
  //
  {
    bufferptr ptr(buffer::create(len));
    EXPECT_TRUE(ptr.have_raw());
    EXPECT_EQ((unsigned)0, ptr.offset());
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(ptr.raw_length(), ptr.length());
    EXPECT_EQ(1, ptr.raw_nref());
  }
  //
  // ptr::ptr(unsigned l)
  //
  {
    bufferptr ptr(len);
    EXPECT_TRUE(ptr.have_raw());
    EXPECT_EQ((unsigned)0, ptr.offset());
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(1, ptr.raw_nref());
  }
  //
  // ptr(const char *d, unsigned l)
  //
  {
    const std::string str(len, 'X');
    bufferptr ptr(str.c_str(), len);
    EXPECT_TRUE(ptr.have_raw());
    EXPECT_EQ((unsigned)0, ptr.offset());
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(1, ptr.raw_nref());
    EXPECT_EQ(0, ::memcmp(str.c_str(), ptr.c_str(), len));
  }
  //
  // ptr(const ptr& p)
  //
  {
    const std::string str(len, 'X');
    bufferptr original(str.c_str(), len);
    bufferptr ptr(original);
    EXPECT_TRUE(ptr.have_raw());
    EXPECT_EQ(original.get_raw(), ptr.get_raw());
    EXPECT_EQ(2, ptr.raw_nref());
    EXPECT_EQ(0, ::memcmp(original.c_str(), ptr.c_str(), len));
  }
  //
  // ptr(const ptr& p, unsigned o, unsigned l)
  //
  {
    const std::string str(len, 'X');
    bufferptr original(str.c_str(), len);
    bufferptr ptr(original, 0, 0);
    EXPECT_TRUE(ptr.have_raw());
    EXPECT_EQ(original.get_raw(), ptr.get_raw());
    EXPECT_EQ(2, ptr.raw_nref());
    EXPECT_EQ(0, ::memcmp(original.c_str(), ptr.c_str(), len));
    EXPECT_THROW(bufferptr(original, 0, original.length() + 1), FailedAssertion);
    EXPECT_THROW(bufferptr(bufferptr(), 0, 0), FailedAssertion);
  }
}

TEST(BufferList, clone) {
  unsigned len = 17;
  bufferptr ptr(len);
  ::memset(ptr.c_str(), 'X', len);
  bufferptr clone = ptr.clone();
  EXPECT_EQ(0, ::memcmp(clone.c_str(), ptr.c_str(), len));
}

TEST(BufferList, swap) {
  unsigned len = 17;

  bufferptr ptr1(len);
  ::memset(ptr1.c_str(), 'X', len);
  unsigned ptr1_offset = 4;
  ptr1.set_offset(ptr1_offset);
  unsigned ptr1_length = 3;
  ptr1.set_length(ptr1_length);

  bufferptr ptr2(len);
  ::memset(ptr2.c_str(), 'Y', len);
  unsigned ptr2_offset = 5;
  ptr2.set_offset(ptr2_offset);
  unsigned ptr2_length = 7;
  ptr2.set_length(ptr2_length);

  ptr1.swap(ptr2);

  EXPECT_EQ(ptr2_length, ptr1.length());
  EXPECT_EQ(ptr2_offset, ptr1.offset());
  EXPECT_EQ('Y', ptr1[0]);

  EXPECT_EQ(ptr1_length, ptr2.length());
  EXPECT_EQ(ptr1_offset, ptr2.offset());
  EXPECT_EQ('X', ptr2[0]);
}

TEST(BufferList, release) {
  unsigned len = 17;

  bufferptr ptr1(len);
  {
    bufferptr ptr2(ptr1);
    EXPECT_EQ(2, ptr1.raw_nref());
  }
  EXPECT_EQ(1, ptr1.raw_nref());
}

TEST(BufferList, have_raw) {
  {
    bufferptr ptr;
    EXPECT_FALSE(ptr.have_raw());
  }
  {
    bufferptr ptr(1);
    EXPECT_TRUE(ptr.have_raw());
  }
}

TEST(BufferList, at_buffer_head) {
  bufferptr ptr(2);
  EXPECT_TRUE(ptr.at_buffer_head());
  ptr.set_offset(1);
  EXPECT_FALSE(ptr.at_buffer_head());
}

TEST(BufferList, at_buffer_tail) {
  bufferptr ptr(2);
  EXPECT_TRUE(ptr.at_buffer_tail());
  ptr.set_length(1);
  EXPECT_FALSE(ptr.at_buffer_tail());
}

TEST(BufferList, is_n_page_sized) {
  {
    bufferptr ptr(CEPH_PAGE_SIZE);
    EXPECT_TRUE(ptr.is_n_page_sized());
  }
  {
    bufferptr ptr(1);
    EXPECT_FALSE(ptr.is_n_page_sized());
  }
}

TEST(BufferList, accessors) {
  unsigned len = 17;
  bufferptr ptr(len);
  ptr.c_str()[0] = 'X';
  ptr[1] = 'Y';
  const bufferptr const_ptr(ptr);

  EXPECT_NE((void*)NULL, (void*)ptr.get_raw());
  EXPECT_EQ('X', ptr.c_str()[0]);
  {
    bufferptr ptr;
    EXPECT_THROW(ptr.c_str(), FailedAssertion);
    EXPECT_THROW(ptr[0], FailedAssertion);
  }
  EXPECT_EQ('X', const_ptr.c_str()[0]);
  {
    const bufferptr const_ptr;
    EXPECT_THROW(const_ptr.c_str(), FailedAssertion);
    EXPECT_THROW(const_ptr[0], FailedAssertion);
  }
  EXPECT_EQ(len, const_ptr.length());
  EXPECT_EQ((unsigned)0, const_ptr.offset());
  EXPECT_EQ((unsigned)0, const_ptr.start());
  EXPECT_EQ(len, const_ptr.end());
  EXPECT_EQ(len, const_ptr.end());
  {
    bufferptr ptr(len);
    unsigned unused = 1;
    ptr.set_length(ptr.length() - unused);
    EXPECT_EQ(unused, ptr.unused_tail_length());
  }
  {
    bufferptr ptr;
    EXPECT_EQ((unsigned)0, ptr.unused_tail_length());
  }
  EXPECT_THROW(ptr[len], FailedAssertion);
  EXPECT_THROW(const_ptr[len], FailedAssertion);
  {
    const bufferptr const_ptr;
    EXPECT_THROW(const_ptr.raw_c_str(), FailedAssertion);
    EXPECT_THROW(const_ptr.raw_length(), FailedAssertion);
    EXPECT_THROW(const_ptr.raw_nref(), FailedAssertion);
  }
  EXPECT_NE((const char *)NULL, const_ptr.raw_c_str());
  EXPECT_EQ(len, const_ptr.raw_length());
  EXPECT_EQ(2, const_ptr.raw_nref());
  {
    bufferptr ptr(len);
    unsigned wasted = 1;
    ptr.set_length(ptr.length() - wasted * 2);
    ptr.set_offset(wasted);
    EXPECT_EQ(wasted * 2, ptr.wasted());
  }
}

TEST(BufferList, is_zero) {
  char str[2] = { '\0', 'X' };
  {
    const bufferptr ptr(buffer::create_static(2, str));
    ASSERT_FALSE(ptr.is_zero());
  }
  {
    const bufferptr ptr(buffer::create_static(1, str));
    ASSERT_TRUE(ptr.is_zero());
  }
}

TEST(BufferList, copy_out) {
  {
    const bufferptr ptr;
    EXPECT_THROW(ptr.copy_out((unsigned)0, (unsigned)0, NULL), FailedAssertion);
  }
  {
    char in[] = "ABC";
    const bufferptr ptr(buffer::create_static(strlen(in), in));
    EXPECT_THROW(ptr.copy_out((unsigned)0, strlen(in) + 1, NULL), buffer::end_of_buffer);
    EXPECT_THROW(ptr.copy_out(strlen(in) + 1, (unsigned)0, NULL), buffer::end_of_buffer);
    char out[2] = { 'X', '\0' };
    ptr.copy_out((unsigned)1, (unsigned)1, out);
    ASSERT_EQ('X', out[0]);
  }
}

TEST(BufferList, copy_in) {
  {
    bufferptr ptr;
    EXPECT_THROW(ptr.copy_in((unsigned)0, (unsigned)0, NULL), FailedAssertion);
  }
  {
    char in[] = "ABCD";
    bufferptr ptr(2);
    EXPECT_THROW(ptr.copy_in((unsigned)0, strlen(in) + 1, NULL), FailedAssertion);
    EXPECT_THROW(ptr.copy_in(strlen(in) + 1, (unsigned)0, NULL), FailedAssertion);
    ptr.copy_in((unsigned)0, (unsigned)2, in);
    ASSERT_EQ(in[0], ptr[0]);
    ASSERT_EQ(in[1], ptr[1]);
  }
}

TEST(BufferList, append) {
  {
    bufferptr ptr;
    EXPECT_THROW(ptr.append('A'), FailedAssertion);
    EXPECT_THROW(ptr.append("B", (unsigned)1), FailedAssertion);
  }
  {
    bufferptr ptr(2);
    EXPECT_THROW(ptr.append('A'), FailedAssertion);
    EXPECT_THROW(ptr.append("B", (unsigned)1), FailedAssertion);
    ptr.set_length(0);
    ptr.append('A');
    ASSERT_EQ((unsigned)1, ptr.length());
    ASSERT_EQ('A', ptr[0]);
    ptr.append("B", (unsigned)1);
    ASSERT_EQ((unsigned)2, ptr.length());
    ASSERT_EQ('B', ptr[1]);
  }
}

TEST(BufferList, zero) {
  char str[] = "XXXX";
  bufferptr ptr(buffer::create_static(strlen(str), str));
  ASSERT_THROW(ptr.zero(ptr.length() + 1, 0), FailedAssertion);
  ptr.zero(1, 1);
  ASSERT_EQ('X', ptr[0]);
  ASSERT_EQ('\0', ptr[1]);
  ASSERT_EQ('X', ptr[2]);
  ptr.zero();
  ASSERT_EQ('\0', ptr[0]);
}

TEST(BufferList, EmptyAppend) {
  bufferlist bl;
  bufferptr ptr;
  bl.push_back(ptr);
  ASSERT_EQ(bl.begin().end(), 1);
}

TEST(BufferList, TestPtrAppend) {
  bufferlist bl;
  char correct[MAX_TEST];
  int curpos = 0;
  int length = random() % 5 > 0 ? random() % 1000 : 0;
  while (curpos + length < MAX_TEST) {
    if (!length) {
      bufferptr ptr;
      bl.push_back(ptr);
    } else {
      char *current = correct + curpos;
      for (int i = 0; i < length; ++i) {
        char next = random() % 255;
        correct[curpos++] = next;
      }
      bufferptr ptr(current, length);
      bl.append(ptr);
    }
    length = random() % 5 > 0 ? random() % 1000 : 0;
  }
  ASSERT_EQ(memcmp(bl.c_str(), correct, curpos), 0);
}

TEST(BufferList, ptr_assignment) {
  unsigned len = 17;
  //
  // override a bufferptr set with the same raw
  //
  {
    bufferptr original(len);
    bufferptr same_raw(original.get_raw());
    unsigned offset = 5;
    unsigned length = len - offset;
    original.set_offset(offset);
    original.set_length(length);
    same_raw = original;
    ASSERT_EQ(2, original.raw_nref());
    ASSERT_EQ(same_raw.get_raw(), original.get_raw());
    ASSERT_EQ(same_raw.offset(), original.offset());
    ASSERT_EQ(same_raw.length(), original.length());
  }

  //
  // self assignment is a noop
  //
  {
    bufferptr original(len);
    original = original;
    ASSERT_EQ(1, original.raw_nref());
    ASSERT_EQ((unsigned)0, original.offset());
    ASSERT_EQ(len, original.length());
  }
  
  //
  // a copy points to the same raw
  //
  {
    bufferptr original(len);
    unsigned offset = 5;
    unsigned length = len - offset;
    original.set_offset(offset);
    original.set_length(length);
    bufferptr ptr;
    ptr = original;
    ASSERT_EQ(2, original.raw_nref());
    ASSERT_EQ(ptr.get_raw(), original.get_raw());
    ASSERT_EQ(original.offset(), ptr.offset());
    ASSERT_EQ(original.length(), ptr.length());
  }
}

TEST(BufferList, TestDirectAppend) {
  bufferlist bl;
  char correct[MAX_TEST];
  int curpos = 0;
  int length = random() % 5 > 0 ? random() % 1000 : 0;
  while (curpos + length < MAX_TEST) {
    char *current = correct + curpos;
    for (int i = 0; i < length; ++i) {
      char next = random() % 255;
      correct[curpos++] = next;
    }
    bl.append(current, length);
    length = random() % 5 > 0 ? random() % 1000 : 0;
  }
  ASSERT_EQ(memcmp(bl.c_str(), correct, curpos), 0);
}

TEST(BufferList, TestCopyAll) {
  const static size_t BIG_SZ = 10737414;
  std::tr1::shared_ptr <unsigned char> big(
      (unsigned char*)malloc(BIG_SZ), free);
  unsigned char c = 0;
  for (size_t i = 0; i < BIG_SZ; ++i) {
    big.get()[i] = c++;
  }
  bufferlist bl;
  bl.append((const char*)big.get(), BIG_SZ);
  bufferlist::iterator i = bl.begin();
  bufferlist bl2;
  i.copy_all(bl2);
  ASSERT_EQ(bl2.length(), BIG_SZ);
  unsigned char big2[BIG_SZ];
  bl2.copy(0, BIG_SZ, (char*)big2);
  ASSERT_EQ(memcmp(big.get(), big2, BIG_SZ), 0);
}

// Local Variables:
// compile-command: "cd .. ; make unittest_bufferlist ; ulimit -s unlimited ; CEPH_BUFFER_TRACK=true valgrind --max-stackframe=20000000 --tool=memcheck ./unittest_bufferlist # --gtest_filter=BufferList.constructors"
// End:
