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

#include "include/memory.h"
#include <limits.h>
#include <errno.h>
#include <sys/uio.h>

#include "include/buffer.h"
#include "include/utime.h"
#include "include/encoding.h"
#include "common/environment.h"
#include "common/Clock.h"
#include "common/safe_io.h"

#include "gtest/gtest.h"
#include "stdlib.h"
#include "fcntl.h"
#include "sys/stat.h"

#define MAX_TEST 1000000
#define FILENAME "bufferlist"

static char cmd[128];

TEST(Buffer, constructors) {
  bool ceph_buffer_track = get_env_bool("CEPH_BUFFER_TRACK");
  unsigned len = 17;
  uint64_t history_alloc_bytes = 0;
  uint64_t history_alloc_num = 0;
  //
  // buffer::create
  //
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    bufferptr ptr(buffer::create(len));
    history_alloc_bytes += len;
    history_alloc_num++;
    EXPECT_EQ(len, ptr.length());
    if (ceph_buffer_track) {
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
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
    if (ceph_buffer_track) {
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    bufferptr clone = ptr.clone();
    history_alloc_bytes += len;
    history_alloc_num++;
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
    if (ceph_buffer_track) {
      EXPECT_EQ(0, buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
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
    history_alloc_bytes += len;
    history_alloc_num++;
    if (ceph_buffer_track) {
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
    EXPECT_EQ(len, ptr.length());
    // this doesn't throw on my x86_64 wheezy box --sage
    //EXPECT_THROW(buffer::create_malloc((unsigned)ULLONG_MAX), buffer::bad_alloc);
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
    if (ceph_buffer_track) {
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    bufferptr clone = ptr.clone();
    history_alloc_bytes += len;
    history_alloc_num++;
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
    history_alloc_bytes += len;
    history_alloc_num++;
    if (ceph_buffer_track) {
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
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
    history_alloc_bytes += len;
    history_alloc_num++;
    ::memset(ptr.c_str(), 'X', len);
    if (ceph_buffer_track) {
      EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
    // doesn't throw on my x86_64 wheezy box --sage
    //EXPECT_THROW(buffer::create_page_aligned((unsigned)ULLONG_MAX), buffer::bad_alloc);
#ifndef DARWIN
    ASSERT_TRUE(ptr.is_page_aligned());
#endif // DARWIN 
    bufferptr clone = ptr.clone();
    history_alloc_bytes += len;
    history_alloc_num++;
    EXPECT_EQ(0, ::memcmp(clone.c_str(), ptr.c_str(), len));
    if (ceph_buffer_track) {
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
  }
#ifdef CEPH_HAVE_SPLICE
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
  {
    // no fd
    EXPECT_THROW(buffer::create_zero_copy(len, -1, NULL), buffer::error_code);
    history_alloc_bytes += len;
    history_alloc_num++;

    unsigned zc_len = 4;
    ::unlink(FILENAME);
    snprintf(cmd, sizeof(cmd), "echo ABC > %s", FILENAME);
    EXPECT_EQ(0, ::system(cmd));
    int fd = ::open(FILENAME, O_RDONLY);
    bufferptr ptr(buffer::create_zero_copy(zc_len, fd, NULL));
    history_alloc_bytes += zc_len;
    history_alloc_num++;
    EXPECT_EQ(zc_len, ptr.length());
    if (ceph_buffer_track) {
      EXPECT_EQ(zc_len, (unsigned)buffer::get_total_alloc());
      EXPECT_EQ(history_alloc_bytes, buffer::get_history_alloc_bytes());
      EXPECT_EQ(history_alloc_num, buffer::get_history_alloc_num());
    }
    ::close(fd);
    ::unlink(FILENAME);
  }
#endif
  if (ceph_buffer_track)
    EXPECT_EQ(0, buffer::get_total_alloc());
}

TEST(BufferRaw, ostream) {
  bufferptr ptr(1);
  std::ostringstream stream;
  stream << *ptr.get_raw();
  EXPECT_GT(stream.str().size(), stream.str().find("buffer::raw("));
  EXPECT_GT(stream.str().size(), stream.str().find("len 1 nref 1)"));
}

#ifdef CEPH_HAVE_SPLICE
class TestRawPipe : public ::testing::Test {
protected:
  virtual void SetUp() {
    len = 4;
    ::unlink(FILENAME);
    snprintf(cmd, sizeof(cmd), "echo ABC > %s", FILENAME);
    EXPECT_EQ(0, ::system(cmd));
    fd = ::open(FILENAME, O_RDONLY);
    assert(fd >= 0);
  }
  virtual void TearDown() {
    ::close(fd);
    ::unlink(FILENAME);
  }
  int fd;
  unsigned len;
};

TEST_F(TestRawPipe, create_zero_copy) {
  bufferptr ptr(buffer::create_zero_copy(len, fd, NULL));
  EXPECT_EQ(len, ptr.length());
  if (get_env_bool("CEPH_BUFFER_TRACK"))
    EXPECT_EQ(len, (unsigned)buffer::get_total_alloc());
}

TEST_F(TestRawPipe, c_str_no_fd) {
  EXPECT_THROW(bufferptr ptr(buffer::create_zero_copy(len, -1, NULL)),
	       buffer::error_code);
}

TEST_F(TestRawPipe, c_str_basic) {
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len, fd, NULL));
  EXPECT_EQ(0, memcmp(ptr.c_str(), "ABC\n", len));
  EXPECT_EQ(len, ptr.length());
}

TEST_F(TestRawPipe, c_str_twice) {
  // make sure we're creating a copy of the data and not consuming it
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len, fd, NULL));
  EXPECT_EQ(len, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "ABC\n", len));
  EXPECT_EQ(0, memcmp(ptr.c_str(), "ABC\n", len));
}

TEST_F(TestRawPipe, c_str_basic_offset) {
  loff_t offset = len - 1;
  ::lseek(fd, offset, SEEK_SET);
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len - offset, fd, NULL));
  EXPECT_EQ(len - offset, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "\n", len - offset));
}

TEST_F(TestRawPipe, c_str_dest_short) {
  ::lseek(fd, 1, SEEK_SET);
  bufferptr ptr = bufferptr(buffer::create_zero_copy(2, fd, NULL));
  EXPECT_EQ(2u, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "BC", 2));
}

TEST_F(TestRawPipe, c_str_source_short) {
  ::lseek(fd, 1, SEEK_SET);
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len, fd, NULL));
  EXPECT_EQ(len - 1, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "BC\n", len - 1));
}

TEST_F(TestRawPipe, c_str_explicit_zero_offset) {
  int64_t offset = 0;
  ::lseek(fd, 1, SEEK_SET);
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len, fd, &offset));
  EXPECT_EQ(len, offset);
  EXPECT_EQ(len, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "ABC\n", len));
}

TEST_F(TestRawPipe, c_str_explicit_positive_offset) {
  int64_t offset = 1;
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len - offset, fd,
						     &offset));
  EXPECT_EQ(len, offset);
  EXPECT_EQ(len - 1, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "BC\n", len - 1));
}

TEST_F(TestRawPipe, c_str_explicit_positive_empty_result) {
  int64_t offset = len;
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len - offset, fd,
						     &offset));
  EXPECT_EQ(len, offset);
  EXPECT_EQ(0u, ptr.length());
}

TEST_F(TestRawPipe, c_str_source_short_explicit_offset) {
  int64_t offset = 1;
  bufferptr ptr = bufferptr(buffer::create_zero_copy(len, fd, &offset));
  EXPECT_EQ(len, offset);
  EXPECT_EQ(len - 1, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "BC\n", len - 1));
}

TEST_F(TestRawPipe, c_str_dest_short_explicit_offset) {
  int64_t offset = 1;
  bufferptr ptr = bufferptr(buffer::create_zero_copy(2, fd, &offset));
  EXPECT_EQ(3, offset);
  EXPECT_EQ(2u, ptr.length());
  EXPECT_EQ(0, memcmp(ptr.c_str(), "BC", 2));
}

TEST_F(TestRawPipe, buffer_list_read_fd_zero_copy) {
  bufferlist bl;
  EXPECT_EQ(-EBADF, bl.read_fd_zero_copy(-1, len));
  bl = bufferlist();
  EXPECT_EQ(0, bl.read_fd_zero_copy(fd, len));
  EXPECT_EQ(len, bl.length());
  EXPECT_EQ(0u, bl.buffers().front().unused_tail_length());
  EXPECT_EQ(1u, bl.buffers().size());
  EXPECT_EQ(len, bl.buffers().front().raw_length());
  EXPECT_EQ(0, memcmp(bl.c_str(), "ABC\n", len));
  EXPECT_TRUE(bl.can_zero_copy());
}

TEST_F(TestRawPipe, buffer_list_write_fd_zero_copy) {
  ::unlink(FILENAME);
  bufferlist bl;
  EXPECT_EQ(0, bl.read_fd_zero_copy(fd, len));
  EXPECT_TRUE(bl.can_zero_copy());
  int out_fd = ::open(FILENAME, O_RDWR|O_CREAT|O_TRUNC, 0600);
  EXPECT_EQ(0, bl.write_fd_zero_copy(out_fd));
  struct stat st;
  memset(&st, 0, sizeof(st));
  EXPECT_EQ(0, ::stat(FILENAME, &st));
  EXPECT_EQ(len, st.st_size);
  char buf[len + 1];
  EXPECT_EQ((int)len, safe_read(out_fd, buf, len + 1));
  EXPECT_EQ(0, memcmp(buf, "ABC\n", len));
  ::close(out_fd);
  ::unlink(FILENAME);
}
#endif // CEPH_HAVE_SPLICE

//                                     
// +-----------+                +-----+
// |           |                |     |
// |  offset   +----------------+     |
// |           |                |     |
// |  length   +----            |     |
// |           |    \-------    |     |
// +-----------+            \---+     |
// |   ptr     |                +-----+
// +-----------+                | raw |
//                              +-----+
//
TEST(BufferPtr, constructors) {
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
    EXPECT_DEATH(bufferptr(original, 0, original.length() + 1), "");
    EXPECT_DEATH(bufferptr(bufferptr(), 0, 0), "");
  }
}

TEST(BufferPtr, assignment) {
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

TEST(BufferPtr, clone) {
  unsigned len = 17;
  bufferptr ptr(len);
  ::memset(ptr.c_str(), 'X', len);
  bufferptr clone = ptr.clone();
  EXPECT_EQ(0, ::memcmp(clone.c_str(), ptr.c_str(), len));
}

TEST(BufferPtr, swap) {
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

TEST(BufferPtr, release) {
  unsigned len = 17;

  bufferptr ptr1(len);
  {
    bufferptr ptr2(ptr1);
    EXPECT_EQ(2, ptr1.raw_nref());
  }
  EXPECT_EQ(1, ptr1.raw_nref());
}

TEST(BufferPtr, have_raw) {
  {
    bufferptr ptr;
    EXPECT_FALSE(ptr.have_raw());
  }
  {
    bufferptr ptr(1);
    EXPECT_TRUE(ptr.have_raw());
  }
}

TEST(BufferPtr, at_buffer_head) {
  bufferptr ptr(2);
  EXPECT_TRUE(ptr.at_buffer_head());
  ptr.set_offset(1);
  EXPECT_FALSE(ptr.at_buffer_head());
}

TEST(BufferPtr, at_buffer_tail) {
  bufferptr ptr(2);
  EXPECT_TRUE(ptr.at_buffer_tail());
  ptr.set_length(1);
  EXPECT_FALSE(ptr.at_buffer_tail());
}

TEST(BufferPtr, is_n_page_sized) {
  {
    bufferptr ptr(CEPH_PAGE_SIZE);
    EXPECT_TRUE(ptr.is_n_page_sized());
  }
  {
    bufferptr ptr(1);
    EXPECT_FALSE(ptr.is_n_page_sized());
  }
}

TEST(BufferPtr, accessors) {
  unsigned len = 17;
  bufferptr ptr(len);
  ptr.c_str()[0] = 'X';
  ptr[1] = 'Y';
  const bufferptr const_ptr(ptr);

  EXPECT_NE((void*)NULL, (void*)ptr.get_raw());
  EXPECT_EQ('X', ptr.c_str()[0]);
  {
    bufferptr ptr;
    EXPECT_DEATH(ptr.c_str(), "");
    EXPECT_DEATH(ptr[0], "");
  }
  EXPECT_EQ('X', const_ptr.c_str()[0]);
  {
    const bufferptr const_ptr;
    EXPECT_DEATH(const_ptr.c_str(), "");
    EXPECT_DEATH(const_ptr[0], "");
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
  EXPECT_DEATH(ptr[len], "");
  EXPECT_DEATH(const_ptr[len], "");
  {
    const bufferptr const_ptr;
    EXPECT_DEATH(const_ptr.raw_c_str(), "");
    EXPECT_DEATH(const_ptr.raw_length(), "");
    EXPECT_DEATH(const_ptr.raw_nref(), "");
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

TEST(BufferPtr, cmp) {
  bufferptr empty;
  bufferptr a("A", 1);
  bufferptr ab("AB", 2);
  bufferptr af("AF", 2);
  bufferptr acc("ACC", 3);
  EXPECT_GE(-1, empty.cmp(a));
  EXPECT_LE(1, a.cmp(empty));
  EXPECT_GE(-1, a.cmp(ab));
  EXPECT_LE(1, ab.cmp(a));
  EXPECT_EQ(0, ab.cmp(ab));
  EXPECT_GE(-1, ab.cmp(af));
  EXPECT_LE(1, af.cmp(ab));
  EXPECT_GE(-1, acc.cmp(af));
  EXPECT_LE(1, af.cmp(acc));
}

TEST(BufferPtr, is_zero) {
  char str[2] = { '\0', 'X' };
  {
    const bufferptr ptr(buffer::create_static(2, str));
    EXPECT_FALSE(ptr.is_zero());
  }
  {
    const bufferptr ptr(buffer::create_static(1, str));
    EXPECT_TRUE(ptr.is_zero());
  }
}

TEST(BufferPtr, copy_out) {
  {
    const bufferptr ptr;
    EXPECT_DEATH(ptr.copy_out((unsigned)0, (unsigned)0, NULL), "");
  }
  {
    char in[] = "ABC";
    const bufferptr ptr(buffer::create_static(strlen(in), in));
    EXPECT_THROW(ptr.copy_out((unsigned)0, strlen(in) + 1, NULL), buffer::end_of_buffer);
    EXPECT_THROW(ptr.copy_out(strlen(in) + 1, (unsigned)0, NULL), buffer::end_of_buffer);
    char out[1] = { 'X' };
    ptr.copy_out((unsigned)1, (unsigned)1, out);
    EXPECT_EQ('B', out[0]);
  }
}

TEST(BufferPtr, copy_out_bench) {
  for (int s=1; s<=8; s*=2) {
    utime_t start = ceph_clock_now(NULL);
    int buflen = 1048576;
    int count = 1000;
    uint64_t v;
    for (int i=0; i<count; ++i) {
      bufferptr bp(buflen);
      for (int64_t j=0; j<buflen; j += s) {
	bp.copy_out(j, s, (char *)&v);
      }
    }
    utime_t end = ceph_clock_now(NULL);
    cout << count << " fills of buffer len " << buflen
	 << " with " << s << " byte copy_in in "
	 << (end - start) << std::endl;
  }
}

TEST(BufferPtr, copy_in) {
  {
    bufferptr ptr;
    EXPECT_DEATH(ptr.copy_in((unsigned)0, (unsigned)0, NULL), "");
  }
  {
    char in[] = "ABCD";
    bufferptr ptr(2);
    EXPECT_DEATH(ptr.copy_in((unsigned)0, strlen(in) + 1, NULL), "");
    EXPECT_DEATH(ptr.copy_in(strlen(in) + 1, (unsigned)0, NULL), "");
    ptr.copy_in((unsigned)0, (unsigned)2, in);
    EXPECT_EQ(in[0], ptr[0]);
    EXPECT_EQ(in[1], ptr[1]);
  }
}

TEST(BufferPtr, copy_in_bench) {
  for (int s=1; s<=8; s*=2) {
    utime_t start = ceph_clock_now(NULL);
    int buflen = 1048576;
    int count = 1000;
    for (int i=0; i<count; ++i) {
      bufferptr bp(buflen);
      for (int64_t j=0; j<buflen; j += s) {
	bp.copy_in(j, s, (char *)&j, false);
      }
    }
    utime_t end = ceph_clock_now(NULL);
    cout << count << " fills of buffer len " << buflen
	 << " with " << s << " byte copy_in in "
	 << (end - start) << std::endl;
  }
}

TEST(BufferPtr, append) {
  {
    bufferptr ptr;
    EXPECT_DEATH(ptr.append('A'), "");
    EXPECT_DEATH(ptr.append("B", (unsigned)1), "");
  }
  {
    bufferptr ptr(2);
    EXPECT_DEATH(ptr.append('A'), "");
    EXPECT_DEATH(ptr.append("B", (unsigned)1), "");
    ptr.set_length(0);
    ptr.append('A');
    EXPECT_EQ((unsigned)1, ptr.length());
    EXPECT_EQ('A', ptr[0]);
    ptr.append("B", (unsigned)1);
    EXPECT_EQ((unsigned)2, ptr.length());
    EXPECT_EQ('B', ptr[1]);
  }
}

TEST(BufferPtr, append_bench) {
  for (int s=1; s<=8; s*=2) {
    utime_t start = ceph_clock_now(NULL);
    int buflen = 1048576;
    int count = 1000;
    for (int i=0; i<count; ++i) {
      bufferptr bp(buflen);
      bp.set_length(0);
      for (int64_t j=0; j<buflen; j += s) {
	bp.append((char *)&j, s);
      }
    }
    utime_t end = ceph_clock_now(NULL);
    cout << count << " fills of buffer len " << buflen
	 << " with " << s << " byte appends in "
	 << (end - start) << std::endl;
  }
}

TEST(BufferPtr, zero) {
  char str[] = "XXXX";
  bufferptr ptr(buffer::create_static(strlen(str), str));
  EXPECT_DEATH(ptr.zero(ptr.length() + 1, 0), "");
  ptr.zero(1, 1);
  EXPECT_EQ('X', ptr[0]);
  EXPECT_EQ('\0', ptr[1]);
  EXPECT_EQ('X', ptr[2]);
  ptr.zero();
  EXPECT_EQ('\0', ptr[0]);
}

TEST(BufferPtr, ostream) {
  {
    bufferptr ptr;
    std::ostringstream stream;
    stream << ptr;
    EXPECT_GT(stream.str().size(), stream.str().find("buffer:ptr(0~0 no raw"));
  }
  {
    char str[] = "XXXX";
    bufferptr ptr(buffer::create_static(strlen(str), str));
    std::ostringstream stream;
    stream << ptr;
    EXPECT_GT(stream.str().size(), stream.str().find("len 4 nref 1)"));
  }  
}

//
//                                             +---------+
//                                             | +-----+ |
//    list              ptr                    | |     | |
// +----------+       +-----+                  | |     | |
// | append_  >------->     >-------------------->     | |
// |  buffer  |       +-----+                  | |     | |
// +----------+                        ptr     | |     | |
// |   _len   |      list            +-----+   | |     | |
// +----------+    +------+     ,--->+     >----->     | |
// | _buffers >---->      >-----     +-----+   | +-----+ |
// +----------+    +----^-+     \      ptr     |   raw   |
// |  last_p  |        /         `-->+-----+   | +-----+ |
// +--------+-+       /              +     >----->     | |
//          |       ,-          ,--->+-----+   | |     | |
//          |      /        ,---               | |     | |
//          |     /     ,---                   | |     | |
//        +-v--+-^--+--^+-------+              | |     | |
//        | bl | ls | p | p_off >--------------->|     | |
//        +----+----+-----+-----+              | +-----+ |
//        |               | off >------------->|   raw   |
//        +---------------+-----+              |         |
//              iterator                       +---------+
//
TEST(BufferListIterator, constructors) {
  //
  // iterator()
  //
  {
    buffer::list::iterator i;
    EXPECT_EQ((unsigned)0, i.get_off());
  }

  //
  // iterator(list *l, unsigned o=0)
  //
  {
    bufferlist bl;
    bl.append("ABC", 3);

    {
      bufferlist::iterator i(&bl);
      EXPECT_EQ((unsigned)0, i.get_off());
      EXPECT_EQ('A', *i);
    }
    {
      bufferlist::iterator i(&bl, 1);
      EXPECT_EQ('B', *i);
      EXPECT_EQ((unsigned)2, i.get_remaining());
    }
  }

  //
  // iterator(list *l, unsigned o, std::list<ptr>::iterator ip, unsigned po)
  // not tested because of http://tracker.ceph.com/issues/4101

  //
  // iterator(const iterator& other)
  //
  {
    bufferlist bl;
    bl.append("ABC", 3);
    bufferlist::iterator i(&bl, 1);
    bufferlist::iterator j(i);
    EXPECT_EQ(*i, *j);
    ++j;
    EXPECT_NE(*i, *j);
    EXPECT_EQ('B', *i);
    EXPECT_EQ('C', *j);
    bl.c_str()[1] = 'X';
    j.advance(-1);
    EXPECT_EQ('X', *j);
  }
}

TEST(BufferListIterator, operator_equal) {
  bufferlist bl;
  bl.append("ABC", 3);
  bufferlist::iterator i(&bl, 1);

  i = i;
  EXPECT_EQ('B', *i);
  bufferlist::iterator j;
  j = i;
  EXPECT_EQ('B', *j);
}

TEST(BufferListIterator, get_off) {
  bufferlist bl;
  bl.append("ABC", 3);
  bufferlist::iterator i(&bl, 1);
  EXPECT_EQ((unsigned)1, i.get_off());
}

TEST(BufferListIterator, get_remaining) {
  bufferlist bl;
  bl.append("ABC", 3);
  bufferlist::iterator i(&bl, 1);
  EXPECT_EQ((unsigned)2, i.get_remaining());
}

TEST(BufferListIterator, end) {
  bufferlist bl;
  {
    bufferlist::iterator i(&bl);
    EXPECT_TRUE(i.end());
  }
  bl.append("ABC", 3);
  {
    bufferlist::iterator i(&bl);
    EXPECT_FALSE(i.end());
  }
}

TEST(BufferListIterator, advance) {
  bufferlist bl;
  const std::string one("ABC");
  bl.append(bufferptr(one.c_str(), one.size()));
  const std::string two("DEF");
  bl.append(bufferptr(two.c_str(), two.size()));

  {
    bufferlist::iterator i(&bl);
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer);
  }
  {
    bufferlist::iterator i(&bl);
    EXPECT_THROW(i.advance(-1), buffer::end_of_buffer);
  }
  {
    bufferlist::iterator i(&bl);
    EXPECT_EQ('A', *i);
    i.advance(1);
    EXPECT_EQ('B', *i);
    i.advance(3);
    EXPECT_EQ('E', *i);
    i.advance(-3);
    EXPECT_EQ('B', *i);
    i.advance(-1);
    EXPECT_EQ('A', *i);
  }
}

TEST(BufferListIterator, seek) {
  bufferlist bl;
  bl.append("ABC", 3);
  bufferlist::iterator i(&bl, 1);
  EXPECT_EQ('B', *i);
  i.seek(2);
  EXPECT_EQ('C', *i);
}

TEST(BufferListIterator, operator_star) {
  bufferlist bl;
  {
    bufferlist::iterator i(&bl);
    EXPECT_THROW(*i, buffer::end_of_buffer);
  }
  bl.append("ABC", 3);
  {
    bufferlist::iterator i(&bl);
    EXPECT_EQ('A', *i);
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer);
    EXPECT_THROW(*i, buffer::end_of_buffer);
  }
}

TEST(BufferListIterator, operator_plus_plus) {
  bufferlist bl;
  {
    bufferlist::iterator i(&bl);
    EXPECT_THROW(++i, buffer::end_of_buffer);
  }
  bl.append("ABC", 3);
  {
    bufferlist::iterator i(&bl);
    ++i;
    EXPECT_EQ('B', *i);
  }  
}

TEST(BufferListIterator, get_current_ptr) {
  bufferlist bl;
  {
    bufferlist::iterator i(&bl);
    EXPECT_THROW(++i, buffer::end_of_buffer);
  }
  bl.append("ABC", 3);
  {
    bufferlist::iterator i(&bl, 1);
    const buffer::ptr ptr = i.get_current_ptr();
    EXPECT_EQ('B', ptr[0]);
    EXPECT_EQ((unsigned)1, ptr.offset());
    EXPECT_EQ((unsigned)2, ptr.length());
  }  
}

TEST(BufferListIterator, copy) {
  bufferlist bl;
  const char *expected = "ABC";
  bl.append(expected, 3);
  //
  // void copy(unsigned len, char *dest);
  //
  {
    char* copy = (char*)malloc(3);
    ::memset(copy, 'X', 3);
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer); 
    i.copy(2, copy);
    EXPECT_EQ(0, ::memcmp(copy, expected, 2));
    EXPECT_EQ('X', copy[2]);
    i.seek(0);
    i.copy(3, copy);
    EXPECT_EQ(0, ::memcmp(copy, expected, 3));
  }
  //
  // void buffer::list::iterator::copy(unsigned len, ptr &dest)
  //
  {
    bufferptr ptr;
    bufferlist::iterator i(&bl);
    i.copy(2, ptr);
    EXPECT_EQ((unsigned)2, ptr.length());
    EXPECT_EQ('A', ptr[0]);
    EXPECT_EQ('B', ptr[1]);
  }
  //
  // void buffer::list::iterator::copy(unsigned len, list &dest)
  //
  {
    bufferlist copy;
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer); 
    i.copy(2, copy);
    EXPECT_EQ(0, ::memcmp(copy.c_str(), expected, 2));
    i.seek(0);
    i.copy(3, copy);
    EXPECT_EQ('A', copy[0]);
    EXPECT_EQ('B', copy[1]);
    EXPECT_EQ('A', copy[2]);
    EXPECT_EQ('B', copy[3]);
    EXPECT_EQ('C', copy[4]);
    EXPECT_EQ((unsigned)(2 + 3), copy.length());
  }
  //
  // void buffer::list::iterator::copy_all(list &dest)
  //
  {
    bufferlist copy;
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer); 
    i.copy_all(copy);
    EXPECT_EQ('A', copy[0]);
    EXPECT_EQ('B', copy[1]);
    EXPECT_EQ('C', copy[2]);
    EXPECT_EQ((unsigned)3, copy.length());
  }
  //
  // void copy(unsigned len, std::string &dest)
  //
  {
    std::string copy;
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer); 
    i.copy(2, copy);
    EXPECT_EQ(0, ::memcmp(copy.c_str(), expected, 2));
    i.seek(0);
    i.copy(3, copy);
    EXPECT_EQ('A', copy[0]);
    EXPECT_EQ('B', copy[1]);
    EXPECT_EQ('A', copy[2]);
    EXPECT_EQ('B', copy[3]);
    EXPECT_EQ('C', copy[4]);
    EXPECT_EQ((unsigned)(2 + 3), copy.length());
  }
}

TEST(BufferListIterator, copy_in) {
  bufferlist bl;
  const char *existing = "XXX";
  bl.append(existing, 3);
  //
  // void buffer::list::iterator::copy_in(unsigned len, const char *src)
  //
  {
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer); 
    const char *expected = "ABC";
    i.copy_in(3, expected);
    EXPECT_EQ(0, ::memcmp(bl.c_str(), expected, 3));
    EXPECT_EQ('A', bl[0]);
    EXPECT_EQ('B', bl[1]);
    EXPECT_EQ('C', bl[2]);
    EXPECT_EQ((unsigned)3, bl.length());
  }
  //
  // void buffer::list::iterator::copy_in(unsigned len, const list& otherl)
  //
  {
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i.advance(200), buffer::end_of_buffer); 
    bufferlist expected;
    expected.append("ABC", 3);
    i.copy_in(3, expected);
    EXPECT_EQ(0, ::memcmp(bl.c_str(), expected.c_str(), 3));
    EXPECT_EQ('A', bl[0]);
    EXPECT_EQ('B', bl[1]);
    EXPECT_EQ('C', bl[2]);
    EXPECT_EQ((unsigned)3, bl.length());
  }
}

// iterator& buffer::list::const_iterator::operator++()
TEST(BufferListConstIterator, operator_plus_plus) {
  bufferlist bl;
  {
    bufferlist::const_iterator i(&bl);
    EXPECT_THROW(++i, buffer::end_of_buffer);
  }
  bl.append("ABC", 3);
  {
    const bufferlist const_bl(bl);
    bufferlist::const_iterator i(const_bl.begin());
    ++i;
    EXPECT_EQ('B', *i);
  }

}

TEST(BufferList, constructors) {
  //
  // list()
  //
  {
    bufferlist bl;
    ASSERT_EQ((unsigned)0, bl.length());
  }
  //
  // list(unsigned prealloc)
  //
  {
    bufferlist bl(1);
    ASSERT_EQ((unsigned)0, bl.length());
    bl.append('A');
    ASSERT_EQ('A', bl[0]);
  }
  //
  // list(const list& other)
  //
  {
    bufferlist bl(1);
    bl.append('A');
    ASSERT_EQ('A', bl[0]);
    bufferlist copy(bl);
    ASSERT_EQ('A', copy[0]);
  }
  //
  // list(list&& other)
  //
  {
    bufferlist bl(1);
    bl.append('A');
    bufferlist copy = std::move(bl);
    ASSERT_EQ(0U, bl.length());
    ASSERT_EQ(1U, copy.length());
    ASSERT_EQ('A', copy[0]);
  }
}

TEST(BufferList, operator_equal) {
  bufferlist bl;
  bl.append("ABC", 3);
  {
    std::string dest;
    bl.copy(1, 1, dest);
    ASSERT_EQ('B', dest[0]);
  }
  bufferlist copy;
  copy = bl;
  {
    std::string dest;
    copy.copy(1, 1, dest);
    ASSERT_EQ('B', dest[0]);
  }
}

TEST(BufferList, buffers) {
  bufferlist bl;
  ASSERT_EQ((unsigned)0, bl.buffers().size());
  bl.append('A');
  ASSERT_EQ((unsigned)1, bl.buffers().size());
}

TEST(BufferList, get_contiguous) {
  {
    bufferptr a("foobarbaz", 9);
    bufferptr b("123456789", 9);
    bufferptr c("ABCDEFGHI", 9);
    bufferlist bl;
    ASSERT_EQ(0, bl.get_contiguous(0, 0));

    bl.append(a);
    bl.append(b);
    bl.append(c);
    ASSERT_EQ(3u, bl.buffers().size());
    ASSERT_EQ(0, memcmp("bar", bl.get_contiguous(3, 3), 3));
    ASSERT_EQ(0, memcmp("456", bl.get_contiguous(12, 3), 3));
    ASSERT_EQ(0, memcmp("ABC", bl.get_contiguous(18, 3), 3));
    ASSERT_EQ(3u, bl.buffers().size());
    ASSERT_EQ(0, memcmp("789ABC", bl.get_contiguous(15, 6), 6));
    ASSERT_EQ(2u, bl.buffers().size());
  }

  {
    bufferptr a("foobarbaz", 9);
    bufferptr b("123456789", 9);
    bufferptr c("ABCDEFGHI", 9);
    bufferlist bl;

    bl.append(a);
    bl.append(b);
    bl.append(c);

    ASSERT_EQ(0, memcmp("789ABCDEFGHI", bl.get_contiguous(15, 12), 12));
    ASSERT_EQ(2u, bl.buffers().size());
  }

  {
    bufferptr a("foobarbaz", 9);
    bufferptr b("123456789", 9);
    bufferptr c("ABCDEFGHI", 9);
    bufferlist bl;

    bl.append(a);
    bl.append(b);
    bl.append(c);

    ASSERT_EQ(0, memcmp("z123456789AB", bl.get_contiguous(8, 12), 12));
    ASSERT_EQ(1u, bl.buffers().size());
  }
}

TEST(BufferList, swap) {
  bufferlist b1;
  b1.append('A');

  bufferlist b2;
  b2.append('B');

  b1.swap(b2);

  std::string s1;
  b1.copy(0, 1, s1);
  ASSERT_EQ('B', s1[0]);

  std::string s2;
  b2.copy(0, 1, s2);
  ASSERT_EQ('A', s2[0]);
}

TEST(BufferList, length) {
  bufferlist bl;
  ASSERT_EQ((unsigned)0, bl.length());
  bl.append('A');
  ASSERT_EQ((unsigned)1, bl.length());
}

TEST(BufferList, contents_equal) {
  //
  // A BB
  // AB B
  //
  bufferlist bl1;
  bl1.append("A");
  bl1.append("BB");
  bufferlist bl2;
  ASSERT_FALSE(bl1.contents_equal(bl2)); // different length
  bl2.append("AB");
  bl2.append("B");
  ASSERT_TRUE(bl1.contents_equal(bl2)); // same length same content
  //
  // ABC
  //
  bufferlist bl3;
  bl3.append("ABC");
  ASSERT_FALSE(bl1.contents_equal(bl3)); // same length different content
}

TEST(BufferList, is_aligned) {
  const int SIMD_ALIGN = 32;
  {
    bufferlist bl;
    EXPECT_TRUE(bl.is_aligned(SIMD_ALIGN));
  }
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_aligned(2, SIMD_ALIGN));
    ptr.set_offset(1);
    ptr.set_length(1);
    bl.append(ptr);
    EXPECT_FALSE(bl.is_aligned(SIMD_ALIGN));
    bl.rebuild_aligned(SIMD_ALIGN);
    EXPECT_TRUE(bl.is_aligned(SIMD_ALIGN));
  }
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_aligned(SIMD_ALIGN + 1, SIMD_ALIGN));
    ptr.set_offset(1);
    ptr.set_length(SIMD_ALIGN);
    bl.append(ptr);
    EXPECT_FALSE(bl.is_aligned(SIMD_ALIGN));
    bl.rebuild_aligned(SIMD_ALIGN);
    EXPECT_TRUE(bl.is_aligned(SIMD_ALIGN));
  }
}

TEST(BufferList, is_n_align_sized) {
  const int SIMD_ALIGN = 32;
  {
    bufferlist bl;
    EXPECT_TRUE(bl.is_n_align_sized(SIMD_ALIGN));
  }
  {
    bufferlist bl;
    bl.append_zero(1);
    EXPECT_FALSE(bl.is_n_align_sized(SIMD_ALIGN));
  }
  {
    bufferlist bl;
    bl.append_zero(SIMD_ALIGN);
    EXPECT_TRUE(bl.is_n_align_sized(SIMD_ALIGN));
  }
}

TEST(BufferList, is_page_aligned) {
  {
    bufferlist bl;
    EXPECT_TRUE(bl.is_page_aligned());
  }
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_page_aligned(2));
    ptr.set_offset(1);
    ptr.set_length(1);
    bl.append(ptr);
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild_page_aligned();
    EXPECT_TRUE(bl.is_page_aligned());
  }
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE + 1));
    ptr.set_offset(1);
    ptr.set_length(CEPH_PAGE_SIZE);
    bl.append(ptr);
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild_page_aligned();
    EXPECT_TRUE(bl.is_page_aligned());
  }
}

TEST(BufferList, is_n_page_sized) {
  {
    bufferlist bl;
    EXPECT_TRUE(bl.is_n_page_sized());
  }
  {
    bufferlist bl;
    bl.append_zero(1);
    EXPECT_FALSE(bl.is_n_page_sized());
  }
  {
    bufferlist bl;
    bl.append_zero(CEPH_PAGE_SIZE);
    EXPECT_TRUE(bl.is_n_page_sized());
  }
}

TEST(BufferList, rebuild_aligned_size_and_memory) {
  const unsigned SIMD_ALIGN = 32;
  const unsigned BUFFER_SIZE = 67;

  bufferlist bl;
  // These two must be concatenated into one memory + size aligned
  // bufferptr
  {
    bufferptr ptr(buffer::create_aligned(2, SIMD_ALIGN));
    ptr.set_offset(1);
    ptr.set_length(1);
    bl.append(ptr);
  }
  {
    bufferptr ptr(buffer::create_aligned(BUFFER_SIZE - 1, SIMD_ALIGN));
    bl.append(ptr);
  }
  // This one must be left alone
  {
    bufferptr ptr(buffer::create_aligned(BUFFER_SIZE, SIMD_ALIGN));
    bl.append(ptr);
  }
  // These two must be concatenated into one memory + size aligned
  // bufferptr
  {
    bufferptr ptr(buffer::create_aligned(2, SIMD_ALIGN));
    ptr.set_offset(1);
    ptr.set_length(1);
    bl.append(ptr);
  }
  {
    bufferptr ptr(buffer::create_aligned(BUFFER_SIZE - 1, SIMD_ALIGN));
    bl.append(ptr);
  }
  EXPECT_FALSE(bl.is_aligned(SIMD_ALIGN));
  EXPECT_FALSE(bl.is_n_align_sized(BUFFER_SIZE));
  EXPECT_EQ(BUFFER_SIZE * 3, bl.length());
  EXPECT_FALSE(bl.buffers().front().is_aligned(SIMD_ALIGN));
  EXPECT_FALSE(bl.buffers().front().is_n_align_sized(BUFFER_SIZE));
  EXPECT_EQ(5U, bl.buffers().size());
  bl.rebuild_aligned_size_and_memory(BUFFER_SIZE, SIMD_ALIGN);
  EXPECT_TRUE(bl.is_aligned(SIMD_ALIGN));
  EXPECT_TRUE(bl.is_n_align_sized(BUFFER_SIZE));
  EXPECT_EQ(3U, bl.buffers().size());
}

TEST(BufferList, is_zero) {
  {
    bufferlist bl;
    EXPECT_TRUE(bl.is_zero());
  }
  {
    bufferlist bl;
    bl.append('A');
    EXPECT_FALSE(bl.is_zero());
  }
  {
    bufferlist bl;
    bl.append_zero(1);
    EXPECT_TRUE(bl.is_zero());
  }
}

TEST(BufferList, clear) {
  bufferlist bl;
  unsigned len = 17;
  bl.append_zero(len);
  bl.clear();
  EXPECT_EQ((unsigned)0, bl.length());
  EXPECT_EQ((unsigned)0, bl.buffers().size());
}

TEST(BufferList, push_front) {
  //
  // void push_front(ptr& bp)
  //
  {
    bufferlist bl;
    bufferptr ptr;
    bl.push_front(ptr);
    EXPECT_EQ((unsigned)0, bl.length());
    EXPECT_EQ((unsigned)0, bl.buffers().size());
  }
  unsigned len = 17;
  {
    bufferlist bl;
    bl.append('A');
    bufferptr ptr(len);
    ptr.c_str()[0] = 'B';
    bl.push_front(ptr);
    EXPECT_EQ((unsigned)(1 + len), bl.length());
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ('B', bl.buffers().front()[0]);
    EXPECT_EQ(ptr.get_raw(), bl.buffers().front().get_raw());
  }
  //
  // void push_front(raw *r)
  //
  {
    bufferlist bl;
    bl.append('A');
    bufferptr ptr(len);
    ptr.c_str()[0] = 'B';
    bl.push_front(ptr.get_raw());
    EXPECT_EQ((unsigned)(1 + len), bl.length());
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ('B', bl.buffers().front()[0]);
    EXPECT_EQ(ptr.get_raw(), bl.buffers().front().get_raw());
  }
}

TEST(BufferList, push_back) {
  //
  // void push_back(ptr& bp)
  //
  {
    bufferlist bl;
    bufferptr ptr;
    bl.push_back(ptr);
    EXPECT_EQ((unsigned)0, bl.length());
    EXPECT_EQ((unsigned)0, bl.buffers().size());
  }
  unsigned len = 17;
  {
    bufferlist bl;
    bl.append('A');
    bufferptr ptr(len);
    ptr.c_str()[0] = 'B';
    bl.push_back(ptr);
    EXPECT_EQ((unsigned)(1 + len), bl.length());
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ('B', bl.buffers().back()[0]);
    EXPECT_EQ(ptr.get_raw(), bl.buffers().back().get_raw());
  }
  //
  // void push_back(raw *r)
  //
  {
    bufferlist bl;
    bl.append('A');
    bufferptr ptr(len);
    ptr.c_str()[0] = 'B';
    bl.push_back(ptr.get_raw());
    EXPECT_EQ((unsigned)(1 + len), bl.length());
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ('B', bl.buffers().back()[0]);
    EXPECT_EQ(ptr.get_raw(), bl.buffers().back().get_raw());
  }
}

TEST(BufferList, is_contiguous) {
  bufferlist bl;
  EXPECT_TRUE(bl.is_contiguous());
  EXPECT_EQ((unsigned)0, bl.buffers().size());
  bl.append('A');  
  EXPECT_TRUE(bl.is_contiguous());
  EXPECT_EQ((unsigned)1, bl.buffers().size());
  bufferptr ptr(1);
  bl.push_back(ptr);
  EXPECT_FALSE(bl.is_contiguous());
  EXPECT_EQ((unsigned)2, bl.buffers().size());
}

TEST(BufferList, rebuild) {
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_page_aligned(2));
    ptr[0] = 'X';
    ptr[1] = 'Y';
    ptr.set_offset(1);
    ptr.set_length(1);
    bl.append(ptr);
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild();
    EXPECT_EQ(1U, bl.length());
    EXPECT_EQ('Y', *bl.begin());
  }
  {
    bufferlist bl;
    const std::string str(CEPH_PAGE_SIZE, 'X');
    bl.append(str.c_str(), str.size());
    bl.append(str.c_str(), str.size());
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_TRUE(bl.is_aligned(CEPH_BUFFER_APPEND_SIZE));
    bl.rebuild();
    EXPECT_TRUE(bl.is_page_aligned());
    EXPECT_EQ((unsigned)1, bl.buffers().size());
  }
  {
    bufferlist bl;
    char t1[] = "X";
    bufferlist a2;
    a2.append(t1, 1);
    bl.rebuild();
    bl.append(a2);
    EXPECT_EQ((unsigned)1, bl.length());
    bufferlist::iterator p = bl.begin();
    char dst[1];
    p.copy(1, dst);
    EXPECT_EQ(0, memcmp(dst, "X", 1));
  }
}

TEST(BufferList, rebuild_page_aligned) {
  {
    bufferlist bl;
    {
      bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE + 1));
      ptr.set_offset(1);
      ptr.set_length(CEPH_PAGE_SIZE);
      bl.append(ptr);
    }
    EXPECT_EQ((unsigned)1, bl.buffers().size());
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild_page_aligned();
    EXPECT_TRUE(bl.is_page_aligned());
    EXPECT_EQ((unsigned)1, bl.buffers().size());
  }
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_page_aligned(1));
    char *p = ptr.c_str();
    bl.append(ptr);
    bl.rebuild_page_aligned();
    EXPECT_EQ(p, bl.buffers().front().c_str());
  }
  {
    bufferlist bl;
    {
      bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE));
      EXPECT_TRUE(ptr.is_page_aligned());
      EXPECT_TRUE(ptr.is_n_page_sized());
      bl.append(ptr);
    }
    {
      bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE + 1));
      EXPECT_TRUE(ptr.is_page_aligned());
      EXPECT_FALSE(ptr.is_n_page_sized());
      bl.append(ptr);
    }
    {
      bufferptr ptr(buffer::create_page_aligned(2));
      ptr.set_offset(1);
      ptr.set_length(1);
      EXPECT_FALSE(ptr.is_page_aligned());
      EXPECT_FALSE(ptr.is_n_page_sized());
      bl.append(ptr);
    }
    {
      bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE - 2));
      EXPECT_TRUE(ptr.is_page_aligned());
      EXPECT_FALSE(ptr.is_n_page_sized());
      bl.append(ptr);
    }
    {
      bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE));
      EXPECT_TRUE(ptr.is_page_aligned());
      EXPECT_TRUE(ptr.is_n_page_sized());
      bl.append(ptr);
    }
    {
      bufferptr ptr(buffer::create_page_aligned(CEPH_PAGE_SIZE + 1));
      ptr.set_offset(1);
      ptr.set_length(CEPH_PAGE_SIZE);
      EXPECT_FALSE(ptr.is_page_aligned());
      EXPECT_TRUE(ptr.is_n_page_sized());
      bl.append(ptr);
    }
    EXPECT_EQ((unsigned)6, bl.buffers().size());
    EXPECT_TRUE((bl.length() & ~CEPH_PAGE_MASK) == 0);
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild_page_aligned();
    EXPECT_TRUE(bl.is_page_aligned());
    EXPECT_EQ((unsigned)4, bl.buffers().size());
  }
}

TEST(BufferList, claim) {
  bufferlist from;
  {
    bufferptr ptr(2);
    from.append(ptr);
  }
  bufferlist to;
  {
    bufferptr ptr(4);
    to.append(ptr);
  }
  EXPECT_EQ((unsigned)4, to.length());
  EXPECT_EQ((unsigned)1, to.buffers().size());
  to.claim(from);
  EXPECT_EQ((unsigned)2, to.length());
  EXPECT_EQ((unsigned)1, to.buffers().size());
  EXPECT_EQ((unsigned)0, from.buffers().size());
  EXPECT_EQ((unsigned)0, from.length());
}

TEST(BufferList, claim_append) {
  bufferlist from;
  {
    bufferptr ptr(2);
    from.append(ptr);
  }
  bufferlist to;
  {
    bufferptr ptr(4);
    to.append(ptr);
  }
  EXPECT_EQ((unsigned)4, to.length());
  EXPECT_EQ((unsigned)1, to.buffers().size());
  to.claim_append(from);
  EXPECT_EQ((unsigned)(4 + 2), to.length());
  EXPECT_EQ((unsigned)4, to.buffers().front().length());
  EXPECT_EQ((unsigned)2, to.buffers().back().length());
  EXPECT_EQ((unsigned)2, to.buffers().size());
  EXPECT_EQ((unsigned)0, from.buffers().size());
  EXPECT_EQ((unsigned)0, from.length());
}

TEST(BufferList, claim_prepend) {
  bufferlist from;
  {
    bufferptr ptr(2);
    from.append(ptr);
  }
  bufferlist to;
  {
    bufferptr ptr(4);
    to.append(ptr);
  }
  EXPECT_EQ((unsigned)4, to.length());
  EXPECT_EQ((unsigned)1, to.buffers().size());
  to.claim_prepend(from);
  EXPECT_EQ((unsigned)(2 + 4), to.length());
  EXPECT_EQ((unsigned)2, to.buffers().front().length());
  EXPECT_EQ((unsigned)4, to.buffers().back().length());
  EXPECT_EQ((unsigned)2, to.buffers().size());
  EXPECT_EQ((unsigned)0, from.buffers().size());
  EXPECT_EQ((unsigned)0, from.length());
}

TEST(BufferList, begin) {
  bufferlist bl;
  bl.append("ABC");
  bufferlist::iterator i = bl.begin();
  EXPECT_EQ('A', *i);
}

TEST(BufferList, end) {
  bufferlist bl;
  bl.append("ABC");
  bufferlist::iterator i = bl.end();
  i.advance(-1);
  EXPECT_EQ('C', *i);
}

TEST(BufferList, copy) {
  //
  // void copy(unsigned off, unsigned len, char *dest) const;
  //
  {
    bufferlist bl;
    EXPECT_THROW(bl.copy((unsigned)100, (unsigned)100, (char*)0), buffer::end_of_buffer);
    const char *expected = "ABC";
    bl.append(expected);
    char *dest = new char[2];
    bl.copy(1, 2, dest);
    EXPECT_EQ(0, ::memcmp(expected + 1, dest, 2));
    delete [] dest;
  }
  //
  // void copy(unsigned off, unsigned len, list &dest) const;
  //
  {
    bufferlist bl;
    bufferlist dest;
    EXPECT_THROW(bl.copy((unsigned)100, (unsigned)100, dest), buffer::end_of_buffer);
    const char *expected = "ABC";
    bl.append(expected);
    bl.copy(1, 2, dest);
    EXPECT_EQ(0, ::memcmp(expected + 1, dest.c_str(), 2));
  }
  //
  // void copy(unsigned off, unsigned len, std::string &dest) const;
  //
  {
    bufferlist bl;
    std::string dest;
    EXPECT_THROW(bl.copy((unsigned)100, (unsigned)100, dest), buffer::end_of_buffer);
    const char *expected = "ABC";
    bl.append(expected);
    bl.copy(1, 2, dest);
    EXPECT_EQ(0, ::memcmp(expected + 1, dest.c_str(), 2));
  }
}

TEST(BufferList, copy_in) {
  //
  // void copy_in(unsigned off, unsigned len, const char *src);
  //
  {
    bufferlist bl;
    bl.append("XXX");
    EXPECT_THROW(bl.copy_in((unsigned)100, (unsigned)100, (char*)0), buffer::end_of_buffer);
    bl.copy_in(1, 2, "AB");
    EXPECT_EQ(0, ::memcmp("XAB", bl.c_str(), 3));
  }
  //
  // void copy_in(unsigned off, unsigned len, const list& src);
  //
  {
    bufferlist bl;
    bl.append("XXX");
    bufferlist src;
    src.append("ABC");
    EXPECT_THROW(bl.copy_in((unsigned)100, (unsigned)100, src), buffer::end_of_buffer);
    bl.copy_in(1, 2, src);
    EXPECT_EQ(0, ::memcmp("XAB", bl.c_str(), 3));    
  }
}

TEST(BufferList, append) {
  //
  // void append(char c);
  //
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.buffers().size());
    bl.append('A');
    EXPECT_EQ((unsigned)1, bl.buffers().size());
    EXPECT_TRUE(bl.is_aligned(CEPH_BUFFER_APPEND_SIZE));
  }
  //
  // void append(const char *data, unsigned len);
  //
  {
    bufferlist bl(CEPH_PAGE_SIZE);
    std::string str(CEPH_PAGE_SIZE * 2, 'X');
    bl.append(str.c_str(), str.size());
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.buffers().front().length());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.buffers().back().length());
  }
  //
  // void append(const std::string& s);
  //
  {
    bufferlist bl(CEPH_PAGE_SIZE);
    std::string str(CEPH_PAGE_SIZE * 2, 'X');
    bl.append(str);
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.buffers().front().length());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.buffers().back().length());
  }
  //
  // void append(const ptr& bp);
  //
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.buffers().size());
    EXPECT_EQ((unsigned)0, bl.length());
    {
      bufferptr ptr;
      bl.append(ptr);
      EXPECT_EQ((unsigned)0, bl.buffers().size());
      EXPECT_EQ((unsigned)0, bl.length());
    }
    {
      bufferptr ptr(3);
      bl.append(ptr);
      EXPECT_EQ((unsigned)1, bl.buffers().size());
      EXPECT_EQ((unsigned)3, bl.length());
    }
  }
  //
  // void append(const ptr& bp, unsigned off, unsigned len);
  //
  {
    bufferlist bl;
    bl.append('A');
    bufferptr back(bl.buffers().back());
    bufferptr in(back);
    EXPECT_EQ((unsigned)1, bl.buffers().size());
    EXPECT_EQ((unsigned)1, bl.length());
    EXPECT_DEATH(bl.append(in, (unsigned)100, (unsigned)100), "");
    EXPECT_LT((unsigned)0, in.unused_tail_length());
    in.append('B');
    bl.append(in, back.end(), 1);
    EXPECT_EQ((unsigned)1, bl.buffers().size());
    EXPECT_EQ((unsigned)2, bl.length());
    EXPECT_EQ('B', bl[1]);
  }
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.buffers().size());
    EXPECT_EQ((unsigned)0, bl.length());
    bufferptr ptr(2);
    ptr.set_length(0);
    ptr.append("AB", 2);
    bl.append(ptr, 1, 1);
    EXPECT_EQ((unsigned)1, bl.buffers().size());
    EXPECT_EQ((unsigned)1, bl.length());
  }
  //
  // void append(const list& bl);
  //
  {
    bufferlist bl;
    bl.append('A');
    bufferlist other;
    other.append('B');
    bl.append(other);
    EXPECT_EQ((unsigned)2, bl.buffers().size());
    EXPECT_EQ('B', bl[1]);
  }
  //
  // void append(std::istream& in);
  //
  {
    bufferlist bl;
    std::string expected("ABC\nDEF\n");
    std::istringstream is("ABC\n\nDEF");
    bl.append(is);
    EXPECT_EQ(0, ::memcmp(expected.c_str(), bl.c_str(), expected.size()));
    EXPECT_EQ(expected.size(), bl.length());
  }
}

TEST(BufferList, append_zero) {
  bufferlist bl;
  bl.append('A');
  EXPECT_EQ((unsigned)1, bl.buffers().size());
  EXPECT_EQ((unsigned)1, bl.length());
  bl.append_zero(1);
  EXPECT_EQ((unsigned)2, bl.buffers().size());
  EXPECT_EQ((unsigned)2, bl.length());
  EXPECT_EQ('\0', bl[1]);
}

TEST(BufferList, operator_brackets) {
  bufferlist bl;
  EXPECT_THROW(bl[1], buffer::end_of_buffer);
  bl.append('A');
  bufferlist other;
  other.append('B');
  bl.append(other);
  EXPECT_EQ((unsigned)2, bl.buffers().size());
  EXPECT_EQ('B', bl[1]);
}

TEST(BufferList, c_str) {
  bufferlist bl;
  EXPECT_EQ((const char*)NULL, bl.c_str());
  bl.append('A');
  bufferlist other;
  other.append('B');
  bl.append(other);
  EXPECT_EQ((unsigned)2, bl.buffers().size());
  EXPECT_EQ(0, ::memcmp("AB", bl.c_str(), 2));
}

TEST(BufferList, substr_of) {
  bufferlist bl;
  EXPECT_THROW(bl.substr_of(bl, 1, 1), buffer::end_of_buffer);
  const char *s[] = {
    "ABC",
    "DEF",
    "GHI",
    "JKL"
  };
  for (unsigned i = 0; i < 4; i++) {
    bufferptr ptr(s[i], strlen(s[i]));
    bl.push_back(ptr);
  }
  EXPECT_EQ((unsigned)4, bl.buffers().size());

  bufferlist other;
  other.append("TO BE CLEARED");
  other.substr_of(bl, 4, 4);
  EXPECT_EQ((unsigned)2, other.buffers().size());
  EXPECT_EQ((unsigned)4, other.length());
  EXPECT_EQ(0, ::memcmp("EFGH", other.c_str(), 4));
}

TEST(BufferList, splice) {
  bufferlist bl;
  EXPECT_THROW(bl.splice(1, 1), buffer::end_of_buffer);
  const char *s[] = {
    "ABC",
    "DEF",
    "GHI",
    "JKL"
  };
  for (unsigned i = 0; i < 4; i++) {
    bufferptr ptr(s[i], strlen(s[i]));
    bl.push_back(ptr);
  }
  EXPECT_EQ((unsigned)4, bl.buffers().size());
  bl.splice(0, 0);

  bufferlist other;
  other.append('X');
  bl.splice(4, 4, &other);
  EXPECT_EQ((unsigned)3, other.buffers().size());
  EXPECT_EQ((unsigned)5, other.length());
  EXPECT_EQ(0, ::memcmp("XEFGH", other.c_str(), other.length()));
  EXPECT_EQ((unsigned)8, bl.length());
  {
    bufferlist tmp(bl);
    EXPECT_EQ(0, ::memcmp("ABCDIJKL", tmp.c_str(), tmp.length()));
  }

  bl.splice(4, 4);
  EXPECT_EQ((unsigned)4, bl.length());
  EXPECT_EQ(0, ::memcmp("ABCD", bl.c_str(), bl.length()));

  {
    bl.clear();
    bufferptr ptr1("0123456789", 10);
    bl.push_back(ptr1);
    bufferptr ptr2("abcdefghij", 10);
    bl.append(ptr2, 5, 5);
    other.clear();
    bl.splice(10, 4, &other);
    EXPECT_EQ((unsigned)11, bl.length());
    EXPECT_EQ(0, ::memcmp("fghi", other.c_str(), other.length()));
  }
}

TEST(BufferList, write) {
  std::ostringstream stream;
  bufferlist bl;
  bl.append("ABC");
  bl.write(1, 2, stream);
  EXPECT_EQ("BC", stream.str());
}

TEST(BufferList, encode_base64) {
  bufferlist bl;
  bl.append("ABCD");
  bufferlist other;
  bl.encode_base64(other);
  const char *expected = "QUJDRA==";
  EXPECT_EQ(0, ::memcmp(expected, other.c_str(), strlen(expected)));
}

TEST(BufferList, decode_base64) {
  bufferlist bl;
  bl.append("QUJDRA==");
  bufferlist other;
  other.decode_base64(bl);
  const char *expected = "ABCD";
  EXPECT_EQ(0, ::memcmp(expected, other.c_str(), strlen(expected)));
  bufferlist malformed;
  malformed.append("QUJDRA");
  EXPECT_THROW(other.decode_base64(malformed), buffer::malformed_input);
}

TEST(BufferList, hexdump) {
  bufferlist bl;
  std::ostringstream stream;
  bl.append("013245678901234\0006789012345678901234", 32);
  bl.hexdump(stream);
  EXPECT_EQ("0000 : 30 31 33 32 34 35 36 37 38 39 30 31 32 33 34 00 : 013245678901234.\n"
	    "0010 : 36 37 38 39 30 31 32 33 34 35 36 37 38 39 30 31 : 6789012345678901\n",
	    stream.str());
}

TEST(BufferList, read_file) {
  std::string error;
  bufferlist bl;
  ::unlink(FILENAME);
  EXPECT_EQ(-ENOENT, bl.read_file("UNLIKELY", &error));
  snprintf(cmd, sizeof(cmd), "echo ABC > %s ; chmod 0 %s", FILENAME, FILENAME);
  EXPECT_EQ(0, ::system(cmd));
  if (getuid() != 0)
    EXPECT_EQ(-EACCES, bl.read_file(FILENAME, &error));
  snprintf(cmd, sizeof(cmd), "chmod +r %s", FILENAME);
  EXPECT_EQ(0, ::system(cmd));
  EXPECT_EQ(0, bl.read_file(FILENAME, &error));
  ::unlink(FILENAME);
  EXPECT_EQ((unsigned)4, bl.length());
  std::string actual(bl.c_str(), bl.length());
  EXPECT_EQ("ABC\n", actual);
}

TEST(BufferList, read_fd) {
  unsigned len = 4;
  ::unlink(FILENAME);
  snprintf(cmd, sizeof(cmd), "echo ABC > %s", FILENAME);
  EXPECT_EQ(0, ::system(cmd));
  int fd = -1;
  bufferlist bl;
  EXPECT_EQ(-EBADF, bl.read_fd(fd, len));
  fd = ::open(FILENAME, O_RDONLY);
  EXPECT_EQ(len, (unsigned)bl.read_fd(fd, len));
  EXPECT_EQ(CEPH_BUFFER_APPEND_SIZE - len, bl.buffers().front().unused_tail_length());
  EXPECT_EQ(len, bl.length());
  ::close(fd);
  ::unlink(FILENAME);
}

TEST(BufferList, write_file) {
  ::unlink(FILENAME);
  int mode = 0600;
  bufferlist bl;
  EXPECT_EQ(-ENOENT, bl.write_file("un/like/ly", mode));
  bl.append("ABC");
  EXPECT_EQ(0, bl.write_file(FILENAME, mode));
  struct stat st;
  memset(&st, 0, sizeof(st));
  ::stat(FILENAME, &st);
  EXPECT_EQ((unsigned)(mode | S_IFREG), st.st_mode);
  ::unlink(FILENAME);
}

TEST(BufferList, write_fd) {
  ::unlink(FILENAME);
  int fd = ::open(FILENAME, O_WRONLY|O_CREAT|O_TRUNC, 0600);
  bufferlist bl;
  for (unsigned i = 0; i < IOV_MAX * 2; i++) {
    bufferptr ptr("A", 1);
    bl.push_back(ptr);
  }
  EXPECT_EQ(0, bl.write_fd(fd));
  ::close(fd);
  struct stat st;
  memset(&st, 0, sizeof(st));
  ::stat(FILENAME, &st);
  EXPECT_EQ(IOV_MAX * 2, st.st_size);
  ::unlink(FILENAME);
}

TEST(BufferList, crc32c) {
  bufferlist bl;
  __u32 crc = 0;
  bl.append("A");
  crc = bl.crc32c(crc);
  EXPECT_EQ((unsigned)0xB3109EBF, crc);
  crc = bl.crc32c(crc);
  EXPECT_EQ((unsigned)0x5FA5C0CC, crc);
}

TEST(BufferList, crc32c_append) {
  bufferlist bl1;
  bufferlist bl2;

  for (int j = 0; j < 200; ++j) {
    bufferlist bl;
    for (int i = 0; i < 200; ++i) {
      char x = rand();
      bl.append(x);
      bl1.append(x);
    }
    bl.crc32c(rand()); // mess with the cached bufferptr crc values
    bl2.append(bl);
  }
  ASSERT_EQ(bl1.crc32c(0), bl2.crc32c(0));
}

TEST(BufferList, crc32c_append_perf) {
  int len = 256 * 1024 * 1024;
  bufferptr a(len);
  bufferptr b(len);
  bufferptr c(len);
  bufferptr d(len);
  std::cout << "populating large buffers (a, b=c=d)" << std::endl;
  char *pa = a.c_str();
  char *pb = b.c_str();
  char *pc = c.c_str();
  char *pd = c.c_str();
  for (int i=0; i<len; i++) {
    pa[i] = (i & 0xff) ^ 73;
    pb[i] = (i & 0xff) ^ 123;
    pc[i] = (i & 0xff) ^ 123;
    pd[i] = (i & 0xff) ^ 123;
  }

  // track usage of cached crcs
  buffer::track_cached_crc(true);

  int base_cached = buffer::get_cached_crc();
  int base_cached_adjusted = buffer::get_cached_crc_adjusted();

  bufferlist bla;
  bla.push_back(a);
  bufferlist blb;
  blb.push_back(b);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = bla.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 1138817026u);
  }
  assert(buffer::get_cached_crc() == 0 + base_cached);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = bla.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(0) (again) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 1138817026u);
  }
  assert(buffer::get_cached_crc() == 1 + base_cached);

  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = bla.crc32c(5);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(5) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 3239494520u);
  }
  assert(buffer::get_cached_crc() == 1 + base_cached);
  assert(buffer::get_cached_crc_adjusted() == 1 + base_cached_adjusted);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = bla.crc32c(5);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(5) (again) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 3239494520u);
  }
  assert(buffer::get_cached_crc() == 1 + base_cached);
  assert(buffer::get_cached_crc_adjusted() == 2 + base_cached_adjusted);

  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = blb.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "b.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2481791210u);
  }
  assert(buffer::get_cached_crc() == 1 + base_cached);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = blb.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "b.crc32c(0) (again)= " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2481791210u);
  }
  assert(buffer::get_cached_crc() == 2 + base_cached);

  bufferlist ab;
  ab.push_back(a);
  ab.push_back(b);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = ab.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)ab.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ab.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2988268779u);
  }
  assert(buffer::get_cached_crc() == 3 + base_cached);
  assert(buffer::get_cached_crc_adjusted() == 3 + base_cached_adjusted);
  bufferlist ac;
  ac.push_back(a);
  ac.push_back(c);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = ac.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)ac.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ac.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2988268779u);
  }
  assert(buffer::get_cached_crc() == 4 + base_cached);
  assert(buffer::get_cached_crc_adjusted() == 3 + base_cached_adjusted);

  bufferlist ba;
  ba.push_back(b);
  ba.push_back(a);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = ba.crc32c(0);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)ba.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ba.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 169240695u);
  }
  assert(buffer::get_cached_crc() == 5 + base_cached);
  assert(buffer::get_cached_crc_adjusted() == 4 + base_cached_adjusted);
  {
    utime_t start = ceph_clock_now(NULL);
    uint32_t r = ba.crc32c(5);
    utime_t end = ceph_clock_now(NULL);
    float rate = (float)ba.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ba.crc32c(5) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 1265464778u);
  }
  assert(buffer::get_cached_crc() == 5 + base_cached);
  assert(buffer::get_cached_crc_adjusted() == 6 + base_cached_adjusted);

  cout << "crc cache hits (same start) = " << buffer::get_cached_crc() << std::endl;
  cout << "crc cache hits (adjusted) = " << buffer::get_cached_crc_adjusted() << std::endl;
}

TEST(BufferList, compare) {
  bufferlist a;
  a.append("A");
  bufferlist ab;
  ab.append("AB");
  bufferlist ac;
  ac.append("AC");
  //
  // bool operator>(bufferlist& l, bufferlist& r)
  //
  ASSERT_FALSE(a > ab);
  ASSERT_TRUE(ab > a);
  ASSERT_TRUE(ac > ab);
  ASSERT_FALSE(ab > ac);
  ASSERT_FALSE(ab > ab);
  //
  // bool operator>=(bufferlist& l, bufferlist& r)
  //
  ASSERT_FALSE(a >= ab);
  ASSERT_TRUE(ab >= a);
  ASSERT_TRUE(ac >= ab);
  ASSERT_FALSE(ab >= ac);
  ASSERT_TRUE(ab >= ab);
  //
  // bool operator<(bufferlist& l, bufferlist& r)
  //
  ASSERT_TRUE(a < ab);
  ASSERT_FALSE(ab < a);
  ASSERT_FALSE(ac < ab);
  ASSERT_TRUE(ab < ac);
  ASSERT_FALSE(ab < ab);
  //
  // bool operator<=(bufferlist& l, bufferlist& r)
  //
  ASSERT_TRUE(a <= ab);
  ASSERT_FALSE(ab <= a);
  ASSERT_FALSE(ac <= ab);
  ASSERT_TRUE(ab <= ac);
  ASSERT_TRUE(ab <= ab);
  //
  // bool operator==(bufferlist &l, bufferlist &r)
  //
  ASSERT_FALSE(a == ab);
  ASSERT_FALSE(ac == ab);
  ASSERT_TRUE(ab == ab);
}

TEST(BufferList, ostream) {
  std::ostringstream stream;
  bufferlist bl;
  const char *s[] = {
    "ABC",
    "DEF"
  };
  for (unsigned i = 0; i < 2; i++) {
    bufferptr ptr(s[i], strlen(s[i]));
    bl.push_back(ptr);
  }
  stream << bl;
  std::cerr << stream.str() << std::endl;
  EXPECT_GT(stream.str().size(), stream.str().find("list(len=6,"));
  EXPECT_GT(stream.str().size(), stream.str().find("len 3 nref 1),\n"));
  EXPECT_GT(stream.str().size(), stream.str().find("len 3 nref 1)\n"));
}

TEST(BufferList, zero) {
  //
  // void zero()
  //
  {
    bufferlist bl;
    bl.append('A');
    EXPECT_EQ('A', bl[0]);
    bl.zero();
    EXPECT_EQ('\0', bl[0]);
  }
  //
  // void zero(unsigned o, unsigned l)
  //
  const char *s[] = {
    "ABC",
    "DEF",
    "GHI",
    "KLM"
  };
  {
    bufferlist bl;
    bufferptr ptr(s[0], strlen(s[0]));
    bl.push_back(ptr);
    bl.zero((unsigned)0, (unsigned)1);
    EXPECT_EQ(0, ::memcmp("\0BC", bl.c_str(), 3));
  }
  {
    bufferlist bl;
    for (unsigned i = 0; i < 4; i++) {
      bufferptr ptr(s[i], strlen(s[i]));
      bl.push_back(ptr);
    }
    EXPECT_DEATH(bl.zero((unsigned)0, (unsigned)2000), "");
    bl.zero((unsigned)2, (unsigned)5);
    EXPECT_EQ(0, ::memcmp("AB\0\0\0\0\0HIKLM", bl.c_str(), 9));
  }
  {
    bufferlist bl;
    for (unsigned i = 0; i < 4; i++) {
      bufferptr ptr(s[i], strlen(s[i]));
      bl.push_back(ptr);
    }
    bl.zero((unsigned)3, (unsigned)3);
    EXPECT_EQ(0, ::memcmp("ABC\0\0\0GHIKLM", bl.c_str(), 9));
  }
  {
    bufferlist bl;
    bufferptr ptr1(4);
    bufferptr ptr2(4);
    memset(ptr1.c_str(), 'a', 4);
    memset(ptr2.c_str(), 'b', 4);
    bl.append(ptr1);
    bl.append(ptr2);
    bl.zero((unsigned)2, (unsigned)4);
    EXPECT_EQ(0, ::memcmp("aa\0\0\0\0bb", bl.c_str(), 8));
  }
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

TEST(BufferList, TestCloneNonShareable) {
  bufferlist bl;
  std::string str = "sharetest";
  bl.append(str.c_str(), 9);
  bufferlist bl_share;
  bl_share.share(bl);
  bufferlist bl_noshare;
  buffer::ptr unraw = buffer::create_unshareable(10);
  unraw.copy_in(0, 9, str.c_str());
  bl_noshare.append(unraw);
  bufferlist bl_copied_share = bl_share;
  bufferlist bl_copied_noshare = bl_noshare;

  // assert shared bufferlist has same buffers
  bufferlist::iterator iter_bl = bl.begin();
  bufferlist::iterator iter_bl_share = bl_share.begin();
  // ok, this considers ptr::off, but it's still a true assertion (and below)
  ASSERT_TRUE(iter_bl.get_current_ptr().c_str() ==
	      iter_bl_share.get_current_ptr().c_str());

  // assert copy of shareable bufferlist has same buffers
  iter_bl = bl.begin();
  bufferlist::iterator iter_bl_copied_share = bl_copied_share.begin();
  ASSERT_TRUE(iter_bl.get_current_ptr().c_str() ==
	      iter_bl_copied_share.get_current_ptr().c_str());

  // assert copy of non-shareable bufferlist has different buffers
  bufferlist::iterator iter_bl_copied_noshare = bl_copied_noshare.begin();
  ASSERT_FALSE(iter_bl.get_current_ptr().c_str() ==
	       iter_bl_copied_noshare.get_current_ptr().c_str());

  // assert that claim with CLAIM_ALLOW_NONSHAREABLE overrides safe-sharing
  bufferlist bl_claim_noshare_override;
  void* addr = bl_noshare.begin().get_current_ptr().c_str();
  bl_claim_noshare_override.claim(bl_noshare,
				  buffer::list::CLAIM_ALLOW_NONSHAREABLE);
  bufferlist::iterator iter_bl_noshare_override =
    bl_claim_noshare_override.begin();
  ASSERT_TRUE(addr /* the original first segment of bl_noshare() */ ==
	      iter_bl_noshare_override.get_current_ptr().c_str());
}

TEST(BufferList, TestCopyAll) {
  const static size_t BIG_SZ = 10737414;
  ceph::shared_ptr <unsigned char> big(
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
  ceph::shared_ptr <unsigned char> big2(
      (unsigned char*)malloc(BIG_SZ), free);
  bl2.copy(0, BIG_SZ, (char*)big2.get());
  ASSERT_EQ(memcmp(big.get(), big2.get(), BIG_SZ), 0);
}

TEST(BufferList, InvalidateCrc) {
  const static size_t buffer_size = 262144;
  ceph::shared_ptr <unsigned char> big(
      (unsigned char*)malloc(buffer_size), free);
  unsigned char c = 0;
  char* ptr = (char*) big.get();
  char* inptr;
  for (size_t i = 0; i < buffer_size; ++i) {
    ptr[i] = c++;
  }
  bufferlist bl;
  
  // test for crashes (shouldn't crash)
  bl.invalidate_crc();
  
  // put data into bufferlist
  bl.append((const char*)big.get(), buffer_size);
  
  // get its crc
  __u32 crc = bl.crc32c(0);
  
  // modify data in bl without its knowledge
  inptr = (char*) bl.c_str();
  c = 0;
  for (size_t i = 0; i < buffer_size; ++i) {
    inptr[i] = c--;
  }
  
  // make sure data in bl are now different than in big
  EXPECT_NE(memcmp((void*) ptr, (void*) inptr, buffer_size), 0);
  
  // crc should remain the same
  __u32 new_crc = bl.crc32c(0);
  EXPECT_EQ(crc, new_crc);
  
  // force crc invalidate, check if it is updated
  bl.invalidate_crc();
  EXPECT_NE(crc, bl.crc32c(0));
}

TEST(BufferHash, all) {
  {
    bufferlist bl;
    bl.append("A");
    bufferhash hash;
    EXPECT_EQ((unsigned)0, hash.digest());
    hash.update(bl);
    EXPECT_EQ((unsigned)0xB3109EBF, hash.digest());
    hash.update(bl);
    EXPECT_EQ((unsigned)0x5FA5C0CC, hash.digest());
  }
  {
    bufferlist bl;
    bl.append("A");
    bufferhash hash;
    EXPECT_EQ((unsigned)0, hash.digest());
    bufferhash& returned_hash =  hash << bl;
    EXPECT_EQ(&returned_hash, &hash);
    EXPECT_EQ((unsigned)0xB3109EBF, hash.digest());
  }
}

/*
 * Local Variables:
 * compile-command: "cd .. ; make unittest_bufferlist && 
 *    ulimit -s unlimited ; CEPH_BUFFER_TRACK=true valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *    ./unittest_bufferlist # --gtest_filter=BufferList.constructors"
 * End:
 */

