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

#include <limits.h>
#include <errno.h>
#include <sys/uio.h>

#include <iostream> // for std::cout

#include "include/buffer.h"
#include "include/buffer_raw.h"
#include "include/compat.h"
#include "include/utime.h"
#include "include/coredumpctl.h"
#include "include/encoding.h"
#include "common/buffer_instrumentation.h"
#include "common/environment.h"
#include "common/Clock.h"
#include "common/safe_io.h"

#include "gtest/gtest.h"
#include "stdlib.h"
#include "fcntl.h"
#include "sys/stat.h"
#include "include/crc32c.h"
#include "common/sctp_crc32.h"

#define MAX_TEST 1000000
#define FILENAME "bufferlist"

using namespace std;

static char cmd[128];

using ceph::buffer_instrumentation::instrumented_bptr;

TEST(Buffer, constructors) {
  unsigned len = 17;
  //
  // buffer::create
  //
  {
    bufferptr ptr(buffer::create(len));
    EXPECT_EQ(len, ptr.length());
  }
  //
  // buffer::claim_char
  //
  {
    char* str = new char[len];
    ::memset(str, 'X', len);
    bufferptr ptr(buffer::claim_char(len, str));
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    EXPECT_EQ(0, ::memcmp(str, ptr.c_str(), len));
    delete [] str;
  }
  //
  // buffer::create_static
  //
  {
    char* str = new char[len];
    bufferptr ptr(buffer::create_static(len, str));
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    delete [] str;
  }
  //
  // buffer::create_malloc
  //
  {
    bufferptr ptr(buffer::create_malloc(len));
    EXPECT_EQ(len, ptr.length());
    // this doesn't throw on my x86_64 wheezy box --sage
    //EXPECT_THROW(buffer::create_malloc((unsigned)ULLONG_MAX), buffer::bad_alloc);
  }
  //
  // buffer::claim_malloc
  //
  {
    char* str = (char*)malloc(len);
    ::memset(str, 'X', len);
    bufferptr ptr(buffer::claim_malloc(len, str));
    EXPECT_EQ(len, ptr.length());
    EXPECT_EQ(str, ptr.c_str());
    EXPECT_EQ(0, ::memcmp(str, ptr.c_str(), len));
  }
  //
  // buffer::copy
  //
  {
    const std::string expected(len, 'X');
    bufferptr ptr(buffer::copy(expected.c_str(), expected.size()));
    EXPECT_NE(expected.c_str(), ptr.c_str());
    EXPECT_EQ(0, ::memcmp(expected.c_str(), ptr.c_str(), len));
  }
  //
  // buffer::create_page_aligned
  //
  {
    bufferptr ptr(buffer::create_page_aligned(len));
    ::memset(ptr.c_str(), 'X', len);
    // doesn't throw on my x86_64 wheezy box --sage
    //EXPECT_THROW(buffer::create_page_aligned((unsigned)ULLONG_MAX), buffer::bad_alloc);
#ifndef DARWIN
    ASSERT_TRUE(ptr.is_page_aligned());
#endif // DARWIN 
  }
}

void bench_buffer_alloc(int size, int num)
{
  utime_t start = ceph_clock_now();
  for (int i=0; i<num; ++i) {
    bufferptr p = buffer::create(size);
    p.zero();
  }
  utime_t end = ceph_clock_now();
  cout << num << " alloc of size " << size
       << " in " << (end - start) << std::endl;
}

TEST(Buffer, BenchAlloc) {
  bench_buffer_alloc(16384, 1000000);
  bench_buffer_alloc(4096, 1000000);
  bench_buffer_alloc(1024, 1000000);
  bench_buffer_alloc(256, 1000000);
  bench_buffer_alloc(32, 1000000);
  bench_buffer_alloc(4, 1000000);
}

TEST(BufferRaw, ostream) {
  bufferptr ptr(1);
  std::ostringstream stream;
  stream << *static_cast<instrumented_bptr&>(ptr).get_raw();
  EXPECT_GT(stream.str().size(), stream.str().find("buffer::raw("));
  EXPECT_GT(stream.str().size(), stream.str().find("len 1 nref 1)"));
}

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
    EXPECT_EQ(static_cast<instrumented_bptr&>(original).get_raw(),
              static_cast<instrumented_bptr&>(ptr).get_raw());
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
    EXPECT_EQ(static_cast<instrumented_bptr&>(original).get_raw(),
              static_cast<instrumented_bptr&>(ptr).get_raw());
    EXPECT_EQ(2, ptr.raw_nref());
    EXPECT_EQ(0, ::memcmp(original.c_str(), ptr.c_str(), len));
    PrCtl unset_dumpable;
    EXPECT_DEATH(bufferptr(original, 0, original.length() + 1), "");
    EXPECT_DEATH(bufferptr(bufferptr(), 0, 0), "");
  }
  //
  // ptr(ptr&& p)
  //
  {
    const std::string str(len, 'X');
    bufferptr original(str.c_str(), len);
    bufferptr ptr(std::move(original));
    EXPECT_TRUE(ptr.have_raw());
    EXPECT_FALSE(original.have_raw());
    EXPECT_EQ(0, ::memcmp(str.c_str(), ptr.c_str(), len));
    EXPECT_EQ(1, ptr.raw_nref());
  }
}

TEST(BufferPtr, operator_assign) {
  //
  // ptr& operator= (const ptr& p)
  //
  bufferptr ptr(10);
  ptr.copy_in(0, 3, "ABC");
  char dest[1];
  {
    bufferptr copy = ptr;
    copy.copy_out(1, 1, dest);
    ASSERT_EQ('B', dest[0]);
  }

  //
  // ptr& operator= (ptr&& p)
  //
  bufferptr move = std::move(ptr);
  {
    move.copy_out(1, 1, dest);
    ASSERT_EQ('B', dest[0]);
  }
  EXPECT_FALSE(ptr.have_raw());
}

TEST(BufferPtr, assignment) {
  unsigned len = 17;
  //
  // override a bufferptr set with the same raw
  //
  {
    bufferptr original(len);
    bufferptr same_raw(original);
    unsigned offset = 5;
    unsigned length = len - offset;
    original.set_offset(offset);
    original.set_length(length);
    same_raw = original;
    ASSERT_EQ(2, original.raw_nref());
    ASSERT_EQ(static_cast<instrumented_bptr&>(same_raw).get_raw(),
              static_cast<instrumented_bptr&>(original).get_raw());
    ASSERT_EQ(same_raw.offset(), original.offset());
    ASSERT_EQ(same_raw.length(), original.length());
  }

  //
  // self assignment is a noop
  //
  {
    bufferptr original(len);
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign-overloaded"
    original = original;
#pragma clang diagnostic pop
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
    ASSERT_EQ(static_cast<instrumented_bptr&>(ptr).get_raw(),
              static_cast<instrumented_bptr&>(original).get_raw());
    ASSERT_EQ(original.offset(), ptr.offset());
    ASSERT_EQ(original.length(), ptr.length());
  }
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

TEST(BufferPtr, is_partial) {
  bufferptr a;
  EXPECT_FALSE(a.is_partial());
  bufferptr b(10);
  EXPECT_FALSE(b.is_partial());
  bufferptr c(b, 1, 9);
  EXPECT_TRUE(c.is_partial());
  bufferptr d(b, 0, 9);
  EXPECT_TRUE(d.is_partial());
}

TEST(BufferPtr, accessors) {
  unsigned len = 17;
  bufferptr ptr(len);
  ptr.c_str()[0] = 'X';
  ptr[1] = 'Y';
  const bufferptr const_ptr(ptr);

  EXPECT_NE((void*)nullptr, (void*)static_cast<instrumented_bptr&>(ptr).get_raw());
  EXPECT_EQ('X', ptr.c_str()[0]);
  {
    bufferptr ptr;
    PrCtl unset_dumpable;
    EXPECT_DEATH(ptr.c_str(), "");
    EXPECT_DEATH(ptr[0], "");
  }
  EXPECT_EQ('X', const_ptr.c_str()[0]);
  {
    const bufferptr const_ptr;
    PrCtl unset_dumpable;
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
  {
    PrCtl unset_dumpable;
    EXPECT_DEATH(ptr[len], "");
    EXPECT_DEATH(const_ptr[len], "");
  }
  {
    const bufferptr const_ptr;
    PrCtl unset_dumpable;
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
    PrCtl unset_dumpable;
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
    utime_t start = ceph_clock_now();
    int buflen = 1048576;
    int count = 1000;
    uint64_t v;
    for (int i=0; i<count; ++i) {
      bufferptr bp(buflen);
      for (int64_t j=0; j<buflen; j += s) {
	bp.copy_out(j, s, (char *)&v);
      }
    }
    utime_t end = ceph_clock_now();
    cout << count << " fills of buffer len " << buflen
	 << " with " << s << " byte copy_out in "
	 << (end - start) << std::endl;
  }
}

TEST(BufferPtr, copy_in) {
  {
    bufferptr ptr;
    PrCtl unset_dumpable;
    EXPECT_DEATH(ptr.copy_in((unsigned)0, (unsigned)0, NULL), "");
  }
  {
    char in[] = "ABCD";
    bufferptr ptr(2);
    {
      PrCtl unset_dumpable;
      EXPECT_DEATH(ptr.copy_in((unsigned)0, strlen(in) + 1, NULL), "");
      EXPECT_DEATH(ptr.copy_in(strlen(in) + 1, (unsigned)0, NULL), "");
    }
    ptr.copy_in((unsigned)0, (unsigned)2, in);
    EXPECT_EQ(in[0], ptr[0]);
    EXPECT_EQ(in[1], ptr[1]);
  }
}

TEST(BufferPtr, copy_in_bench) {
  for (int s=1; s<=8; s*=2) {
    utime_t start = ceph_clock_now();
    int buflen = 1048576;
    int count = 1000;
    for (int i=0; i<count; ++i) {
      bufferptr bp(buflen);
      for (int64_t j=0; j<buflen; j += s) {
	bp.copy_in(j, s, (char *)&j, false);
      }
    }
    utime_t end = ceph_clock_now();
    cout << count << " fills of buffer len " << buflen
	 << " with " << s << " byte copy_in in "
	 << (end - start) << std::endl;
  }
}

TEST(BufferPtr, append) {
  {
    bufferptr ptr;
    PrCtl unset_dumpable;
    EXPECT_DEATH(ptr.append('A'), "");
    EXPECT_DEATH(ptr.append("B", (unsigned)1), "");
  }
  {
    bufferptr ptr(2);
    {
      PrCtl unset_dumpable;
      EXPECT_DEATH(ptr.append('A'), "");
      EXPECT_DEATH(ptr.append("B", (unsigned)1), "");
    }
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
  char src[1048576];
  memset(src, 0, sizeof(src));
  for (int s=4; s<=16384; s*=4) {
    utime_t start = ceph_clock_now();
    int buflen = 1048576;
    int count = 4000;
    for (int i=0; i<count; ++i) {
      bufferptr bp(buflen);
      bp.set_length(0);
      for (int64_t j=0; j<buflen; j += s) {
	bp.append(src + j, s);
      }
    }
    utime_t end = ceph_clock_now();
    cout << count << " fills of buffer len " << buflen
	 << " with " << s << " byte appends in "
	 << (end - start) << std::endl;
  }
}

TEST(BufferPtr, zero) {
  char str[] = "XXXX";
  bufferptr ptr(buffer::create_static(strlen(str), str));
  {
    PrCtl unset_dumpable;
    EXPECT_DEATH(ptr.zero(ptr.length() + 1, 0), "");
  }
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
  }

  //
  // const_iterator(const iterator& other)
  //
  {
    bufferlist bl;
    bl.append("ABC", 3);
    bufferlist::iterator i(&bl);
    bufferlist::const_iterator ci(i);
    EXPECT_EQ(0u, ci.get_off());
    EXPECT_EQ('A', *ci);
  }
}

TEST(BufferListIterator, empty_create_append_copy) {
  bufferlist bl, bl2, bl3, out;
  bl2.append("bar");
  bl.swap(bl2);
  bl2.append("xxx");
  bl.append(bl2);
  bl.rebuild();
  bl.begin().copy(6, out);
  ASSERT_TRUE(out.contents_equal(bl));
}

TEST(BufferListIterator, operator_assign) {
  bufferlist bl;
  bl.append("ABC", 3);
  bufferlist::iterator i(&bl, 1);

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign-overloaded"
  i = i;
#pragma clang diagnostic pop
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

static void bench_bufferlistiter_deref(const size_t step,
				       const size_t bufsize,
				       const size_t bufnum) {
  const std::string buf(bufsize, 'a');
  ceph::bufferlist bl;

  for (size_t i = 0; i < bufnum; i++) {
    bl.append(ceph::bufferptr(buf.c_str(), buf.size()));
  }

  utime_t start = ceph_clock_now();
  bufferlist::iterator iter = bl.begin();
  while (iter != bl.end()) {
    iter += step;
  }
  utime_t end = ceph_clock_now();
  cout << bufsize * bufnum << " derefs over bl with " << bufnum
       << " buffers, each " << bufsize << " bytes long"
       << " in " << (end - start) << std::endl;
}

TEST(BufferListIterator, BenchDeref) {
  bench_bufferlistiter_deref(1, 1, 4096000);
  bench_bufferlistiter_deref(1, 10, 409600);
  bench_bufferlistiter_deref(1, 100, 40960);
  bench_bufferlistiter_deref(1, 1000, 4096);

  bench_bufferlistiter_deref(4, 1, 1024000);
  bench_bufferlistiter_deref(4, 10, 102400);
  bench_bufferlistiter_deref(4, 100, 10240);
  bench_bufferlistiter_deref(4, 1000, 1024);
}

TEST(BufferListIterator, advance) {
  bufferlist bl;
  const std::string one("ABC");
  bl.append(bufferptr(one.c_str(), one.size()));
  const std::string two("DEF");
  bl.append(bufferptr(two.c_str(), two.size()));

  {
    bufferlist::iterator i(&bl);
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
  }
  {
    bufferlist::iterator i(&bl);
    EXPECT_EQ('A', *i);
    i += 1u;
    EXPECT_EQ('B', *i);
    i += 3u;
    EXPECT_EQ('E', *i);
  }
}

TEST(BufferListIterator, iterate_with_empties) {
  ceph::bufferlist bl;
  EXPECT_EQ(bl.get_num_buffers(), 0u);

  bl.push_back(ceph::buffer::create(0));
  EXPECT_EQ(bl.length(), 0u);
  EXPECT_EQ(bl.get_num_buffers(), 1u);

  encode(int64_t(42), bl);
  EXPECT_EQ(bl.get_num_buffers(), 2u);

  bl.push_back(ceph::buffer::create(0));
  EXPECT_EQ(bl.get_num_buffers(), 3u);

  // append bufferlist with single, 0-sized ptr inside
  {
    ceph::bufferlist bl_with_empty_ptr;
    bl_with_empty_ptr.push_back(ceph::buffer::create(0));
    EXPECT_EQ(bl_with_empty_ptr.length(), 0u);
    EXPECT_EQ(bl_with_empty_ptr.get_num_buffers(), 1u);

    bl.append(bl_with_empty_ptr);
  }

  encode(int64_t(24), bl);
  EXPECT_EQ(bl.get_num_buffers(), 5u);

  auto i = bl.cbegin();
  int64_t val;
  decode(val, i);
  EXPECT_EQ(val, 42l);

  decode(val, i);
  EXPECT_EQ(val, 24l);

  val = 0;
  i.seek(sizeof(val));
  decode(val, i);
  EXPECT_EQ(val, 24l);
  EXPECT_TRUE(i == bl.end());

  i.seek(0);
  decode(val, i);
  EXPECT_EQ(val, 42);
  EXPECT_FALSE(i == bl.end());
}

TEST(BufferListIterator, get_ptr_and_advance)
{
  bufferptr a("one", 3);
  bufferptr b("two", 3);
  bufferptr c("three", 5);
  bufferlist bl;
  bl.append(a);
  bl.append(b);
  bl.append(c);
  const char *ptr;
  bufferlist::iterator p = bl.begin();
  ASSERT_EQ(3u, p.get_ptr_and_advance(11u, &ptr));
  ASSERT_EQ(bl.length() - 3u, p.get_remaining());
  ASSERT_EQ(0, memcmp(ptr, "one", 3));
  ASSERT_EQ(2u, p.get_ptr_and_advance(2u, &ptr));
  ASSERT_EQ(0, memcmp(ptr, "tw", 2));
  ASSERT_EQ(1u, p.get_ptr_and_advance(4u, &ptr));
  ASSERT_EQ(0, memcmp(ptr, "o", 1));
  ASSERT_EQ(5u, p.get_ptr_and_advance(5u, &ptr));
  ASSERT_EQ(0, memcmp(ptr, "three", 5));
  ASSERT_EQ(0u, p.get_remaining());
}

TEST(BufferListIterator, iterator_crc32c) {
  bufferlist bl1;
  bufferlist bl2;
  bufferlist bl3;

  string s1(100, 'a');
  string s2(50, 'b');
  string s3(7, 'c');
  string s;
  bl1.append(s1);
  bl1.append(s2);
  bl1.append(s3);
  s = s1 + s2 + s3;
  bl2.append(s);

  bufferlist::iterator it = bl2.begin();
  ASSERT_EQ(bl1.crc32c(0), it.crc32c(it.get_remaining(), 0));
  ASSERT_EQ(0u, it.get_remaining());

  it = bl1.begin();
  ASSERT_EQ(bl2.crc32c(0), it.crc32c(it.get_remaining(), 0));

  bl3.append(s.substr(98, 55));
  it = bl1.begin();
  it += 98u;
  ASSERT_EQ(bl3.crc32c(0), it.crc32c(55, 0));
  ASSERT_EQ(4u, it.get_remaining());

  bl3.clear();
  bl3.append(s.substr(98 + 55));
  it = bl1.begin();
  it += 98u + 55u;
  ASSERT_EQ(bl3.crc32c(0), it.crc32c(10, 0));
  ASSERT_EQ(0u, it.get_remaining());
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
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
    EXPECT_THROW(*i, buffer::end_of_buffer);
  }
}

TEST(BufferListIterator, operator_equal) {
  bufferlist bl;
  bl.append("ABC", 3);
  {
    bufferlist::iterator i(&bl);
    bufferlist::iterator j(&bl);
    EXPECT_EQ(i, j);
  }
  {
    bufferlist::const_iterator ci = bl.begin();
    bufferlist::iterator i = bl.begin();
    EXPECT_EQ(i, ci);
    EXPECT_EQ(ci, i);
  }
}

TEST(BufferListIterator, operator_nequal) {
  bufferlist bl;
  bl.append("ABC", 3);
  {
    bufferlist::iterator i(&bl);
    bufferlist::iterator j(&bl);
    EXPECT_NE(++i, j);
  }
  {
    bufferlist::const_iterator ci = bl.begin();
    bufferlist::const_iterator cj = bl.begin();
    ++ci;
    EXPECT_NE(ci, cj);
    bufferlist::iterator i = bl.begin();
    EXPECT_NE(i, ci);
    EXPECT_NE(ci, i);
  }
  {
    // tests begin(), end(), operator++() also
    string s("ABC");
    int i = 0;
    for (auto c : bl) {
      EXPECT_EQ(s[i++], c);
    }
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
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
    i.copy(2, copy);
    EXPECT_EQ(0, ::memcmp(copy, expected, 2));
    EXPECT_EQ('X', copy[2]);
    i.seek(0);
    i.copy(3, copy);
    EXPECT_EQ(0, ::memcmp(copy, expected, 3));
    free(copy);
  }
  //
  // void copy(unsigned len, char *dest) via begin(size_t offset)
  //
  {
    bufferlist bl;
    EXPECT_THROW(bl.begin((unsigned)100).copy((unsigned)100, (char*)0), buffer::end_of_buffer);
    const char *expected = "ABC";
    bl.append(expected);
    char *dest = new char[2];
    bl.begin(1).copy(2, dest);
    EXPECT_EQ(0, ::memcmp(expected + 1, dest, 2));
    delete [] dest;
  }
  //
  // void buffer::list::iterator::copy_deep(unsigned len, ptr &dest)
  //
  {
    bufferptr ptr;
    bufferlist::iterator i(&bl);
    i.copy_deep(2, ptr);
    EXPECT_EQ((unsigned)2, ptr.length());
    EXPECT_EQ('A', ptr[0]);
    EXPECT_EQ('B', ptr[1]);
  }
  //
  // void buffer::list::iterator::copy_shallow(unsigned len, ptr &dest)
  //
  {
    bufferptr ptr;
    bufferlist::iterator i(&bl);
    i.copy_shallow(2, ptr);
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
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
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
  // void buffer::list::iterator::copy(unsigned len, list &dest) via begin(size_t offset)
  //
  {
    bufferlist bl;
    bufferlist dest;
    EXPECT_THROW(bl.begin((unsigned)100).copy((unsigned)100, dest), buffer::end_of_buffer);
    const char *expected = "ABC";
    bl.append(expected);
    bl.begin(1).copy(2, dest);
    EXPECT_EQ(0, ::memcmp(expected + 1, dest.c_str(), 2));
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
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
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
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
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
  // void copy(unsigned len, std::string &dest) via begin(size_t offset)
  //
  {
    bufferlist bl;
    std::string dest;
    EXPECT_THROW(bl.begin((unsigned)100).copy((unsigned)100, dest), buffer::end_of_buffer);
    const char *expected = "ABC";
    bl.append(expected);
    bl.begin(1).copy(2, dest);
    EXPECT_EQ(0, ::memcmp(expected + 1, dest.c_str(), 2));
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
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
    const char *expected = "ABC";
    i.copy_in(3, expected);
    EXPECT_EQ(0, ::memcmp(bl.c_str(), expected, 3));
    EXPECT_EQ('A', bl[0]);
    EXPECT_EQ('B', bl[1]);
    EXPECT_EQ('C', bl[2]);
    EXPECT_EQ((unsigned)3, bl.length());
  }
  //
  // void copy_in(unsigned len, const char *src) via begin(size_t offset)
  //
  {
    bufferlist bl;
    bl.append("XXX");
    EXPECT_THROW(bl.begin((unsigned)100).copy_in((unsigned)100, (char*)0), buffer::end_of_buffer);
    bl.begin(1).copy_in(2, "AB");
    EXPECT_EQ(0, ::memcmp("XAB", bl.c_str(), 3));
  }
  //
  // void buffer::list::iterator::copy_in(unsigned len, const list& otherl)
  //
  {
    bufferlist::iterator i(&bl);
    //
    // demonstrates that it seeks back to offset if p == ls->end()
    //
    EXPECT_THROW(i += 200u, buffer::end_of_buffer);
    bufferlist expected;
    expected.append("ABC", 3);
    i.copy_in(3, expected);
    EXPECT_EQ(0, ::memcmp(bl.c_str(), expected.c_str(), 3));
    EXPECT_EQ('A', bl[0]);
    EXPECT_EQ('B', bl[1]);
    EXPECT_EQ('C', bl[2]);
    EXPECT_EQ((unsigned)3, bl.length());
  }
  //
  // void copy_in(unsigned len, const list& src) via begin(size_t offset)
  //
  {
    bufferlist bl;
    bl.append("XXX");
    bufferlist src;
    src.append("ABC");
    EXPECT_THROW(bl.begin((unsigned)100).copy_in((unsigned)100, src), buffer::end_of_buffer);
    bl.begin(1).copy_in(2, src);
    EXPECT_EQ(0, ::memcmp("XAB", bl.c_str(), 3));
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

TEST(BufferList, append_after_move) {
  bufferlist bl(6);
  bl.append("ABC", 3);
  EXPECT_EQ(1, bl.get_num_buffers());

  bufferlist moved_to_bl(std::move(bl));
  moved_to_bl.append("123", 3);
  // it's expected that the list(list&&) ctor will preserve the _carriage
  EXPECT_EQ(1, moved_to_bl.get_num_buffers());
  EXPECT_EQ(0, ::memcmp("ABC123", moved_to_bl.c_str(), 6));
}

void bench_bufferlist_alloc(int size, int num, int per)
{
  utime_t start = ceph_clock_now();
  for (int i=0; i<num; ++i) {
    bufferlist bl;
    for (int j=0; j<per; ++j)
      bl.push_back(buffer::ptr_node::create(buffer::create(size)));
  }
  utime_t end = ceph_clock_now();
  cout << num << " alloc of size " << size
       << " in " << (end - start) << std::endl;
}

TEST(BufferList, BenchAlloc) {
  bench_bufferlist_alloc(32768, 100000, 16);
  bench_bufferlist_alloc(25000, 100000, 16);
  bench_bufferlist_alloc(16384, 100000, 16);
  bench_bufferlist_alloc(10000, 100000, 16);
  bench_bufferlist_alloc(8192, 100000, 16);
  bench_bufferlist_alloc(6000, 100000, 16);
  bench_bufferlist_alloc(4096, 100000, 16);
  bench_bufferlist_alloc(1024, 100000, 16);
  bench_bufferlist_alloc(256, 100000, 16);
  bench_bufferlist_alloc(32, 100000, 16);
  bench_bufferlist_alloc(4, 100000, 16);
}

/*
 * append_bench tests now have multiple variants:
 *
 * Version 1 tests allocate a single bufferlist during loop iteration.
 * Ultimately very little memory is utilized since the bufferlist immediately
 * drops out of scope. This was the original variant of these tests but showed
 * unexpected performance characteristics that appears to be tied to tcmalloc
 * and/or kernel behavior depending on the bufferlist size and step size.
 *
 * Version 2 tests allocate a configurable number of bufferlists that are
 * replaced round-robin during loop iteration.  Version 2 tests are designed
 * to better mimic performance when multiple bufferlists are in memory at the
 * same time.  During testing this showed more consistent and seemingly
 * accurate behavior across bufferlist and step sizes.
 */

TEST(BufferList, append_bench_with_size_hint) {
  std::array<char, 1048576> src = { 0, };

  for (size_t step = 4; step <= 16384; step *= 4) {
    const utime_t start = ceph_clock_now();

    constexpr size_t rounds = 4000;
    for (size_t r = 0; r < rounds; ++r) {
      ceph::bufferlist bl(std::size(src));
      for (auto iter = std::begin(src);
	   iter != std::end(src);
	   iter = std::next(iter, step)) {
	bl.append(&*iter, step);
      }
    }
    cout << rounds << " fills of buffer len " << src.size()
	 << " with " << step << " byte appends in "
	 << (ceph_clock_now() - start) << std::endl;
  }
}

TEST(BufferList, append_bench_with_size_hint2) {
  std::array<char, 1048576> src = { 0, };
  constexpr size_t rounds = 4000;
  constexpr int conc_bl = 400;
  std::vector<ceph::bufferlist*> bls(conc_bl);

  for (int i = 0; i < conc_bl; i++) {
    bls[i] = new ceph::bufferlist;
  }
  for (size_t step = 4; step <= 16384; step *= 4) {
    const utime_t start = ceph_clock_now();
    for (size_t r = 0; r < rounds; ++r) {
      delete bls[r % conc_bl];
      bls[r % conc_bl] = new ceph::bufferlist(std::size(src));
      for (auto iter = std::begin(src);
           iter != std::end(src);
           iter = std::next(iter, step)) {
        bls[r % conc_bl]->append(&*iter, step);
      }
    }
    cout << rounds << " fills of buffer len " << src.size()
         << " with " << step << " byte appends in "
         << (ceph_clock_now() - start) << std::endl;
  }
  for (int i = 0; i < conc_bl; i++) {
    delete bls[i];
  }
}

TEST(BufferList, append_bench) {
  std::array<char, 1048576> src = { 0, };
  for (size_t step = 4; step <= 16384; step *= 4) {
    const utime_t start = ceph_clock_now();
    constexpr size_t rounds = 4000;
    for (size_t r = 0; r < rounds; ++r) {
      ceph::bufferlist bl;
      for (auto iter = std::begin(src);
	   iter != std::end(src);
	   iter = std::next(iter, step)) {
	bl.append(&*iter, step);
      }
    }
    cout << rounds << " fills of buffer len " << src.size()
	 << " with " << step << " byte appends in "
	 << (ceph_clock_now() - start) << std::endl;
  }
}

TEST(BufferList, append_bench2) {
  std::array<char, 1048576> src = { 0, };
  constexpr size_t rounds = 4000;
  constexpr int conc_bl = 400;
  std::vector<ceph::bufferlist*> bls(conc_bl);

  for (int i = 0; i < conc_bl; i++) {
    bls[i] = new ceph::bufferlist;
  }
  for (size_t step = 4; step <= 16384; step *= 4) {
    const utime_t start = ceph_clock_now();
    for (size_t r = 0; r < rounds; ++r) {
      delete bls[r % conc_bl];
      bls[r % conc_bl] = new ceph::bufferlist;
      for (auto iter = std::begin(src);
	   iter != std::end(src);
	   iter = std::next(iter, step)) {
	bls[r % conc_bl]->append(&*iter, step);
      }
    }
    cout << rounds << " fills of buffer len " << src.size()
	 << " with " << step << " byte appends in "
	 << (ceph_clock_now() - start) << std::endl;
  }
  for (int i = 0; i < conc_bl; i++) {
    delete bls[i];
  }
}

TEST(BufferList, append_hole_bench) {
  constexpr size_t targeted_bl_size = 1048576;

  for (size_t step = 512; step <= 65536; step *= 2) {
    const utime_t start = ceph_clock_now();
    constexpr size_t rounds = 80000;
    for (size_t r = 0; r < rounds; ++r) {
      ceph::bufferlist bl;
      while (bl.length() < targeted_bl_size) {
	bl.append_hole(step);
      }
    }
    cout << rounds << " fills of buffer len " << targeted_bl_size
	 << " with " << step << " byte long append_hole in "
	 << (ceph_clock_now() - start) << std::endl;
  }
}

TEST(BufferList, append_hole_bench2) {
  constexpr size_t targeted_bl_size = 1048576;
  constexpr size_t rounds = 80000;
  constexpr int conc_bl = 400;
  std::vector<ceph::bufferlist*> bls(conc_bl);

  for (int i = 0; i < conc_bl; i++) {
    bls[i] = new ceph::bufferlist;
  }
  for (size_t step = 512; step <= 65536; step *= 2) {
    const utime_t start = ceph_clock_now();
    for (size_t r = 0; r < rounds; ++r) {
      delete bls[r % conc_bl];
      bls[r % conc_bl] = new ceph::bufferlist;
      while (bls[r % conc_bl]->length() < targeted_bl_size) {
	bls[r % conc_bl]->append_hole(step);
      }
    }
    cout << rounds << " fills of buffer len " << targeted_bl_size
	 << " with " << step << " byte long append_hole in "
	 << (ceph_clock_now() - start) << std::endl;
  }
  for (int i = 0; i < conc_bl; i++) {
    delete bls[i];
  }
}

TEST(BufferList, operator_assign_rvalue) {
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
  EXPECT_EQ((unsigned)1, to.get_num_buffers());
  to = std::move(from);
  EXPECT_EQ((unsigned)2, to.length());
  EXPECT_EQ((unsigned)1, to.get_num_buffers());
  EXPECT_EQ((unsigned)0, from.get_num_buffers());
  EXPECT_EQ((unsigned)0, from.length());
}

TEST(BufferList, operator_equal) {
  //
  // list& operator= (const list& other)
  //
  bufferlist bl;
  bl.append("ABC", 3);
  {
    std::string dest;
    bl.begin(1).copy(1, dest);
    ASSERT_EQ('B', dest[0]);
  }
  {
    bufferlist copy = bl;
    std::string dest;
    copy.begin(1).copy(1, dest);
    ASSERT_EQ('B', dest[0]);
  }

  //
  // list& operator= (list&& other)
  //
  bufferlist move;
  move = std::move(bl);
  {
    std::string dest;
    move.begin(1).copy(1, dest);
    ASSERT_EQ('B', dest[0]);
  }
  EXPECT_TRUE(move.length());
  EXPECT_TRUE(!bl.length());
}

TEST(BufferList, buffers) {
  bufferlist bl;
  ASSERT_EQ((unsigned)0, bl.get_num_buffers());
  bl.append('A');
  ASSERT_EQ((unsigned)1, bl.get_num_buffers());
}

TEST(BufferList, to_str) {
  {
    bufferlist bl;
    bl.append("foo");
    ASSERT_EQ(bl.to_str(), string("foo"));
  }
  {
    bufferptr a("foobarbaz", 9);
    bufferptr b("123456789", 9);
    bufferptr c("ABCDEFGHI", 9);
    bufferlist bl;
    bl.append(a);
    bl.append(b);
    bl.append(c);
    ASSERT_EQ(bl.to_str(), string("foobarbaz123456789ABCDEFGHI"));
  }
}

TEST(BufferList, swap) {
  bufferlist b1;
  b1.append('A');

  bufferlist b2;
  b2.append('B');

  b1.swap(b2);

  std::string s1;
  b1.begin().copy(1, s1);
  ASSERT_EQ('B', s1[0]);

  std::string s2;
  b2.begin().copy(1, s2);
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

TEST(BufferList, page_aligned_appender) {
  bufferlist bl;
  {
    auto a = bl.get_page_aligned_appender(5);
    a.append("asdf", 4);
    cout << bl << std::endl;
    ASSERT_EQ(1u, bl.get_num_buffers());
    ASSERT_TRUE(bl.contents_equal("asdf", 4));
    a.append("asdf", 4);
    for (unsigned n = 0; n < 3 * CEPH_PAGE_SIZE; ++n) {
      a.append("x", 1);
    }
    cout << bl << std::endl;
    ASSERT_EQ(1u, bl.get_num_buffers());
    // verify the beginning
    {
      bufferlist t;
      t.substr_of(bl, 0, 10);
      ASSERT_TRUE(t.contents_equal("asdfasdfxx", 10));
    }
    for (unsigned n = 0; n < 3 * CEPH_PAGE_SIZE; ++n) {
      a.append("y", 1);
    }
    cout << bl << std::endl;
    ASSERT_EQ(2u, bl.get_num_buffers());

    a.append_zero(42);
    // ensure append_zero didn't introduce a fragmentation
    ASSERT_EQ(2u, bl.get_num_buffers());
    // verify the end is actually zeroed
    {
      bufferlist t;
      t.substr_of(bl, bl.length() - 42, 42);
      ASSERT_TRUE(t.is_zero());
    }

    // let's check whether appending a bufferlist directly to `bl`
    // doesn't fragment further C string appends via appender.
    {
      const auto& initial_back = bl.back();
      {
        bufferlist src;
        src.append("abc", 3);
        bl.claim_append(src);
        // surely the extra `ptr_node` taken from `src` must get
        // reflected in the `bl` instance
        ASSERT_EQ(3u, bl.get_num_buffers());
      }

      // moreover, the next C string-taking `append()` had to
      // create anoter `ptr_node` instance but...
      a.append("xyz", 3);
      ASSERT_EQ(4u, bl.get_num_buffers());

      // ... it should point to the same `buffer::raw` instance
      // (to the same same block of memory).
      ASSERT_EQ(bl.back().raw_c_str(), initial_back.raw_c_str());
    }

    // check whether it'll take the first byte only and whether
    // the auto-flushing works.
    for (unsigned n = 0; n < 10 * CEPH_PAGE_SIZE - 3; ++n) {
      a.append("zasdf", 1);
    }
  }

  {
    cout << bl << std::endl;
    ASSERT_EQ(6u, bl.get_num_buffers());
  }

  // Verify that `page_aligned_appender` does respect the carrying
  // `_carriage` over multiple allocations. Although `append_zero()`
  // is used here, this affects other members of the append family.
  // This part would be crucial for e.g. `encode()`.
  {
    bl.append_zero(42);
    cout << bl << std::endl;
    ASSERT_EQ(6u, bl.get_num_buffers());
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
  EXPECT_FALSE(bl.front().is_aligned(SIMD_ALIGN));
  EXPECT_FALSE(bl.front().is_n_align_sized(BUFFER_SIZE));
  EXPECT_EQ(5U, bl.get_num_buffers());
  bl.rebuild_aligned_size_and_memory(BUFFER_SIZE, SIMD_ALIGN);
  EXPECT_TRUE(bl.is_aligned(SIMD_ALIGN));
  EXPECT_TRUE(bl.is_n_align_sized(BUFFER_SIZE));
  EXPECT_EQ(3U, bl.get_num_buffers());

  {
    /* bug replicator, to test rebuild_aligned_size_and_memory() in the
     * scenario where the first bptr is both size and memory aligned and
     * the second is 0-length */
    bl.clear();
    bl.append(bufferptr{buffer::create_aligned(4096, 4096)});
    bufferptr ptr(buffer::create_aligned(42, 4096));
    /* bl.back().length() must be 0. offset set to 42 guarantees
     * the entire list is unaligned. */
    bl.append(ptr, 42, 0);
    EXPECT_EQ(bl.get_num_buffers(), 2);
    EXPECT_EQ(bl.back().length(), 0);
    EXPECT_FALSE(bl.is_aligned(4096));
    /* rebuild_aligned() calls rebuild_aligned_size_and_memory().
     * we assume the rebuild always happens. */
    EXPECT_TRUE(bl.rebuild_aligned(4096));
    EXPECT_EQ(bl.get_num_buffers(), 1);
  }
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

  for (size_t i = 1; i <= 256; ++i) {
    bufferlist bl;
    bl.append_zero(i);
    EXPECT_TRUE(bl.is_zero());
    bl.append('A');
    // ensure buffer is a single, contiguous before testing
    bl.rebuild();
    EXPECT_FALSE(bl.is_zero());
  }

}

TEST(BufferList, clear) {
  bufferlist bl;
  unsigned len = 17;
  bl.append_zero(len);
  bl.clear();
  EXPECT_EQ((unsigned)0, bl.length());
  EXPECT_EQ((unsigned)0, bl.get_num_buffers());
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
    EXPECT_EQ((unsigned)0, bl.get_num_buffers());
  }
  unsigned len = 17;
  {
    bufferlist bl;
    bl.append('A');
    bufferptr ptr(len);
    ptr.c_str()[0] = 'B';
    bl.push_back(ptr);
    EXPECT_EQ((unsigned)(1 + len), bl.length());
    EXPECT_EQ((unsigned)2, bl.get_num_buffers());
    EXPECT_EQ('B', bl.back()[0]);
    const bufferptr& back_bp = bl.back();
    EXPECT_EQ(static_cast<instrumented_bptr&>(ptr).get_raw(),
              static_cast<const instrumented_bptr&>(back_bp).get_raw());
  }
  //
  // void push_back(ptr&& bp)
  //
  {
    bufferlist bl;
    bufferptr ptr;
    bl.push_back(std::move(ptr));
    EXPECT_EQ((unsigned)0, bl.length());
    EXPECT_EQ((unsigned)0, bl.get_num_buffers());
  }
  {
    bufferlist bl;
    bl.append('A');
    bufferptr ptr(len);
    ptr.c_str()[0] = 'B';
    bl.push_back(std::move(ptr));
    EXPECT_EQ((unsigned)(1 + len), bl.length());
    EXPECT_EQ((unsigned)2, bl.get_num_buffers());
    EXPECT_EQ('B', bl.buffers().back()[0]);
    EXPECT_FALSE(static_cast<instrumented_bptr&>(ptr).get_raw());
  }
}

TEST(BufferList, is_contiguous) {
  bufferlist bl;
  EXPECT_TRUE(bl.is_contiguous());
  EXPECT_EQ((unsigned)0, bl.get_num_buffers());
  bl.append('A');  
  EXPECT_TRUE(bl.is_contiguous());
  EXPECT_EQ((unsigned)1, bl.get_num_buffers());
  bufferptr ptr(1);
  bl.push_back(ptr);
  EXPECT_FALSE(bl.is_contiguous());
  EXPECT_EQ((unsigned)2, bl.get_num_buffers());
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
    EXPECT_EQ((unsigned)2, bl.get_num_buffers());
    //EXPECT_TRUE(bl.is_aligned(CEPH_BUFFER_APPEND_SIZE));
    bl.rebuild();
    EXPECT_TRUE(bl.is_page_aligned());
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
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
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild_page_aligned();
    EXPECT_TRUE(bl.is_page_aligned());
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
  }
  {
    bufferlist bl;
    bufferptr ptr(buffer::create_page_aligned(1));
    char *p = ptr.c_str();
    bl.append(ptr);
    bl.rebuild_page_aligned();
    EXPECT_EQ(p, bl.front().c_str());
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
    EXPECT_EQ((unsigned)6, bl.get_num_buffers());
    EXPECT_TRUE((bl.length() & ~CEPH_PAGE_MASK) == 0);
    EXPECT_FALSE(bl.is_page_aligned());
    bl.rebuild_page_aligned();
    EXPECT_TRUE(bl.is_page_aligned());
    EXPECT_EQ((unsigned)4, bl.get_num_buffers());
  }
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
  EXPECT_EQ((unsigned)1, to.get_num_buffers());
  to.claim_append(from);
  EXPECT_EQ((unsigned)(4 + 2), to.length());
  EXPECT_EQ((unsigned)4, to.front().length());
  EXPECT_EQ((unsigned)2, to.back().length());
  EXPECT_EQ((unsigned)2, to.get_num_buffers());
  EXPECT_EQ((unsigned)0, from.get_num_buffers());
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
  bl.append("AB");
  bufferlist::iterator i = bl.end();
  bl.append("C");
  EXPECT_EQ('C', bl[i.get_off()]);
}

TEST(BufferList, append) {
  //
  // void append(char c);
  //
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.get_num_buffers());
    bl.append('A');
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    //EXPECT_TRUE(bl.is_aligned(CEPH_BUFFER_APPEND_SIZE));
  }
  //
  // void append(const char *data, unsigned len);
  //
  {
    bufferlist bl(CEPH_PAGE_SIZE);
    std::string str(CEPH_PAGE_SIZE * 2, 'X');
    bl.append(str.c_str(), str.size());
    EXPECT_EQ((unsigned)2, bl.get_num_buffers());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.front().length());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.back().length());
  }
  //
  // void append(const std::string& s);
  //
  {
    bufferlist bl(CEPH_PAGE_SIZE);
    std::string str(CEPH_PAGE_SIZE * 2, 'X');
    bl.append(str);
    EXPECT_EQ((unsigned)2, bl.get_num_buffers());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.front().length());
    EXPECT_EQ(CEPH_PAGE_SIZE, bl.back().length());
  }
  //
  // void append(const ptr& bp);
  //
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.get_num_buffers());
    EXPECT_EQ((unsigned)0, bl.length());
    {
      bufferptr ptr;
      bl.append(ptr);
      EXPECT_EQ((unsigned)0, bl.get_num_buffers());
      EXPECT_EQ((unsigned)0, bl.length());
    }
    {
      bufferptr ptr(3);
      bl.append(ptr);
      EXPECT_EQ((unsigned)1, bl.get_num_buffers());
      EXPECT_EQ((unsigned)3, bl.length());
    }
  }
  //
  // void append(const ptr& bp, unsigned off, unsigned len);
  //
  {
    bufferlist bl;
    bl.append('A');
    bufferptr back(bl.back());
    bufferptr in(back);
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)1, bl.length());
    {
      PrCtl unset_dumpable;
      EXPECT_DEATH(bl.append(in, (unsigned)100, (unsigned)100), "");
    }
    EXPECT_LT((unsigned)0, in.unused_tail_length());
    in.append('B');
    bl.append(in, back.end(), 1);
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)2, bl.length());
    EXPECT_EQ('B', bl[1]);
  }
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.get_num_buffers());
    EXPECT_EQ((unsigned)0, bl.length());
    bufferptr ptr(2);
    ptr.set_length(0);
    ptr.append("AB", 2);
    bl.append(ptr, 1, 1);
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
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
    EXPECT_EQ((unsigned)2, bl.get_num_buffers());
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
  //
  // void append(ptr&& bp);
  //
  {
    bufferlist bl;
    EXPECT_EQ((unsigned)0, bl.get_num_buffers());
    EXPECT_EQ((unsigned)0, bl.length());
    {
      bufferptr ptr;
      bl.append(std::move(ptr));
      EXPECT_EQ((unsigned)0, bl.get_num_buffers());
      EXPECT_EQ((unsigned)0, bl.length());
    }
    {
      bufferptr ptr(3);
      bl.append(std::move(ptr));
      EXPECT_EQ((unsigned)1, bl.get_num_buffers());
      EXPECT_EQ((unsigned)3, bl.length());
      EXPECT_FALSE(static_cast<instrumented_bptr&>(ptr).get_raw());
    }
  }
}

TEST(BufferList, append_hole) {
  {
    bufferlist bl;
    auto filler = bl.append_hole(1);
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)1, bl.length());

    bl.append("BC", 2);
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)3, bl.length());

    const char a = 'A';
    filler.copy_in((unsigned)1, &a);
    EXPECT_EQ((unsigned)3, bl.length());

    EXPECT_EQ(0, ::memcmp("ABC", bl.c_str(), 3));
  }

  {
    bufferlist bl;
    bl.append('A');
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)1, bl.length());

    auto filler = bl.append_hole(1);
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)2, bl.length());

    bl.append('C');
    EXPECT_EQ((unsigned)1, bl.get_num_buffers());
    EXPECT_EQ((unsigned)3, bl.length());

    const char b = 'B';
    filler.copy_in((unsigned)1, &b);
    EXPECT_EQ((unsigned)3, bl.length());

    EXPECT_EQ(0, ::memcmp("ABC", bl.c_str(), 3));
  }
}

TEST(BufferList, append_zero) {
  bufferlist bl;
  bl.append('A');
  EXPECT_EQ((unsigned)1, bl.get_num_buffers());
  EXPECT_EQ((unsigned)1, bl.length());
  bl.append_zero(1);
  EXPECT_EQ((unsigned)1, bl.get_num_buffers());
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
  EXPECT_EQ((unsigned)2, bl.get_num_buffers());
  EXPECT_EQ('B', bl[1]);
}

TEST(BufferList, c_str) {
  bufferlist bl;
  EXPECT_EQ((const char*)NULL, bl.c_str());
  bl.append('A');
  bufferlist other;
  other.append('B');
  bl.append(other);
  EXPECT_EQ((unsigned)2, bl.get_num_buffers());
  EXPECT_EQ(0, ::memcmp("AB", bl.c_str(), 2));
}

TEST(BufferList, c_str_carriage) {
  // verify the c_str() optimization for carriage handling
  buffer::ptr bp("A", 1);
  bufferlist bl;
  bl.append(bp);
  bl.append('B');
  EXPECT_EQ(2U, bl.get_num_buffers());
  EXPECT_EQ(2U, bl.length());

  // this should leave an empty bptr for carriage at the end of the bl
  bl.splice(1, 1);
  EXPECT_EQ(2U, bl.get_num_buffers());
  EXPECT_EQ(1U, bl.length());

  std::ignore = bl.c_str();
  // if we have an empty bptr at the end, we don't need to rebuild
  EXPECT_EQ(2U, bl.get_num_buffers());
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
  EXPECT_EQ((unsigned)4, bl.get_num_buffers());

  bufferlist other;
  other.append("TO BE CLEARED");
  other.substr_of(bl, 4, 4);
  EXPECT_EQ((unsigned)2, other.get_num_buffers());
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
  EXPECT_EQ((unsigned)4, bl.get_num_buffers());
  bl.splice(0, 0);

  bufferlist other;
  other.append('X');
  bl.splice(4, 4, &other);
  EXPECT_EQ((unsigned)3, other.get_num_buffers());
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
  EXPECT_EQ("00000000  30 31 33 32 34 35 36 37  38 39 30 31 32 33 34 00  |013245678901234.|\n"
	    "00000010  36 37 38 39 30 31 32 33  34 35 36 37 38 39 30 31  |6789012345678901|\n"
	    "00000020\n",
	    stream.str());
}

TEST(BufferList, read_file) {
  std::string error;
  bufferlist bl;
  ::unlink(FILENAME);
  EXPECT_EQ(-ENOENT, bl.read_file("UNLIKELY", &error));
  snprintf(cmd, sizeof(cmd), "echo ABC> %s", FILENAME);
  EXPECT_EQ(0, ::system(cmd));
  #ifndef _WIN32
  snprintf(cmd, sizeof(cmd), "chmod 0 %s", FILENAME);
  EXPECT_EQ(0, ::system(cmd));
  if (getuid() != 0) {
    EXPECT_EQ(-EACCES, bl.read_file(FILENAME, &error));
  }
  snprintf(cmd, sizeof(cmd), "chmod +r %s", FILENAME);
  EXPECT_EQ(0, ::system(cmd));
  #endif /* _WIN32 */
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
  ASSERT_NE(-1, fd);
  EXPECT_EQ(len, (unsigned)bl.read_fd(fd, len));
  //EXPECT_EQ(CEPH_BUFFER_APPEND_SIZE - len, bl.front().unused_tail_length());
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
  ASSERT_EQ(0, ::stat(FILENAME, &st));
  #ifndef _WIN32
  EXPECT_EQ((unsigned)(mode | S_IFREG), st.st_mode);
  #endif
  ::unlink(FILENAME);
}

TEST(BufferList, write_fd) {
  ::unlink(FILENAME);
  int fd = ::open(FILENAME, O_WRONLY|O_CREAT|O_TRUNC, 0600);
  ASSERT_NE(-1, fd);
  bufferlist bl;
  for (unsigned i = 0; i < IOV_MAX * 2; i++) {
    bufferptr ptr("A", 1);
    bl.push_back(ptr);
  }
  EXPECT_EQ(0, bl.write_fd(fd));
  ::close(fd);
  struct stat st;
  memset(&st, 0, sizeof(st));
  ASSERT_EQ(0, ::stat(FILENAME, &st));
  EXPECT_EQ(IOV_MAX * 2, st.st_size);
  ::unlink(FILENAME);
}

TEST(BufferList, write_fd_offset) {
  ::unlink(FILENAME);
  int fd = ::open(FILENAME, O_WRONLY|O_CREAT|O_TRUNC, 0600);
  ASSERT_NE(-1, fd);
  bufferlist bl;
  for (unsigned i = 0; i < IOV_MAX * 2; i++) {
    bufferptr ptr("A", 1);
    bl.push_back(ptr);
  }
  uint64_t offset = 200;
  EXPECT_EQ(0, bl.write_fd(fd, offset));
  ::close(fd);
  struct stat st;
  memset(&st, 0, sizeof(st));
  ASSERT_EQ(0, ::stat(FILENAME, &st));
  EXPECT_EQ(IOV_MAX * 2 + offset, (unsigned)st.st_size);
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

TEST(BufferList, crc32c_zeros) {
  char buffer[4*1024];
  for (size_t i=0; i < sizeof(buffer); i++)
  {
    buffer[i] = i;
  }

  bufferlist bla;
  bufferlist blb;

  for (size_t j=0; j < 1000; j++)
  {
    bufferptr a(buffer, sizeof(buffer));

    bla.push_back(a);
    uint32_t crca = bla.crc32c(111);

    blb.push_back(a);
    uint32_t crcb = ceph_crc32c(111, (unsigned char*)blb.c_str(), blb.length());

    EXPECT_EQ(crca, crcb);
  }
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

  [[maybe_unused]] int base_cached = buffer::get_cached_crc();
  [[maybe_unused]] int base_cached_adjusted = buffer::get_cached_crc_adjusted();

  bufferlist bla;
  bla.push_back(a);
  bufferlist blb;
  blb.push_back(b);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = bla.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 1138817026u);
  }
  ceph_assert(buffer::get_cached_crc() == 0 + base_cached);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = bla.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(0) (again) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 1138817026u);
  }
  ceph_assert(buffer::get_cached_crc() == 1 + base_cached);

  {
    utime_t start = ceph_clock_now();
    uint32_t r = bla.crc32c(5);
    utime_t end = ceph_clock_now();
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(5) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 3239494520u);
  }
  ceph_assert(buffer::get_cached_crc() == 1 + base_cached);
  ceph_assert(buffer::get_cached_crc_adjusted() == 1 + base_cached_adjusted);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = bla.crc32c(5);
    utime_t end = ceph_clock_now();
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "a.crc32c(5) (again) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 3239494520u);
  }
  ceph_assert(buffer::get_cached_crc() == 1 + base_cached);
  ceph_assert(buffer::get_cached_crc_adjusted() == 2 + base_cached_adjusted);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = blb.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "b.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2481791210u);
  }
  ceph_assert(buffer::get_cached_crc() == 1 + base_cached);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = blb.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)len / (float)(1024*1024) / (float)(end - start);
    std::cout << "b.crc32c(0) (again)= " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2481791210u);
  }
  ceph_assert(buffer::get_cached_crc() == 2 + base_cached);

  bufferlist ab;
  ab.push_back(a);
  ab.push_back(b);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = ab.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)ab.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ab.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2988268779u);
  }
  ceph_assert(buffer::get_cached_crc() == 3 + base_cached);
  ceph_assert(buffer::get_cached_crc_adjusted() == 3 + base_cached_adjusted);
  bufferlist ac;
  ac.push_back(a);
  ac.push_back(c);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = ac.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)ac.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ac.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 2988268779u);
  }
  ceph_assert(buffer::get_cached_crc() == 4 + base_cached);
  ceph_assert(buffer::get_cached_crc_adjusted() == 3 + base_cached_adjusted);

  bufferlist ba;
  ba.push_back(b);
  ba.push_back(a);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = ba.crc32c(0);
    utime_t end = ceph_clock_now();
    float rate = (float)ba.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ba.crc32c(0) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 169240695u);
  }
  ceph_assert(buffer::get_cached_crc() == 5 + base_cached);
  ceph_assert(buffer::get_cached_crc_adjusted() == 4 + base_cached_adjusted);
  {
    utime_t start = ceph_clock_now();
    uint32_t r = ba.crc32c(5);
    utime_t end = ceph_clock_now();
    float rate = (float)ba.length() / (float)(1024*1024) / (float)(end - start);
    std::cout << "ba.crc32c(5) = " << r << " at " << rate << " MB/sec" << std::endl;
    ASSERT_EQ(r, 1265464778u);
  }
  ceph_assert(buffer::get_cached_crc() == 5 + base_cached);
  ceph_assert(buffer::get_cached_crc_adjusted() == 6 + base_cached_adjusted);

  cout << "crc cache hits (same start) = " << buffer::get_cached_crc() << std::endl;
  cout << "crc cache hits (adjusted) = " << buffer::get_cached_crc_adjusted() << std::endl;
}

TEST(BufferList, compare) {
  bufferlist a;
  a.append("A");
  bufferlist ab; // AB in segments
  ab.append(bufferptr("A", 1));
  ab.append(bufferptr("B", 1));
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
    {
      PrCtl unset_dumpable;
      EXPECT_DEATH(bl.zero((unsigned)0, (unsigned)2000), "");
    }
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

TEST(BufferList, InternalCarriage) {
  ceph::bufferlist bl;
  EXPECT_EQ(bl.get_num_buffers(), 0u);

  encode(int64_t(42), bl);
  EXPECT_EQ(bl.get_num_buffers(), 1u);

  {
    ceph::bufferlist bl_with_foo;
    bl_with_foo.append("foo", 3);
    EXPECT_EQ(bl_with_foo.length(), 3u);
    EXPECT_EQ(bl_with_foo.get_num_buffers(), 1u);

    bl.append(bl_with_foo);
    EXPECT_EQ(bl.get_num_buffers(), 2u);
  }

  encode(int64_t(24), bl);
  EXPECT_EQ(bl.get_num_buffers(), 3u);
}

TEST(BufferList, ContiguousAppender) {
  ceph::bufferlist bl;
  EXPECT_EQ(bl.get_num_buffers(), 0u);

  // we expect a flush in ~contiguous_appender
  {
    auto ap = bl.get_contiguous_appender(100);

    denc(int64_t(42), ap);
    EXPECT_EQ(bl.get_num_buffers(), 1u);

    // append bufferlist with single ptr inside. This should
    // commit changes to bl::_len and the underlying bp::len.
    {
      ceph::bufferlist bl_with_foo;
      bl_with_foo.append("foo", 3);
      EXPECT_EQ(bl_with_foo.length(), 3u);
      EXPECT_EQ(bl_with_foo.get_num_buffers(), 1u);

      ap.append(bl_with_foo);
      // 3 as the ap::append(const bl&) splits the bp with free
      // space.
      EXPECT_EQ(bl.get_num_buffers(), 3u);
    }

    denc(int64_t(24), ap);
    EXPECT_EQ(bl.get_num_buffers(), 3u);
    EXPECT_EQ(bl.length(), sizeof(int64_t) + 3u);
  }
  EXPECT_EQ(bl.length(), 2u * sizeof(int64_t) + 3u);
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

TEST(BufferList, TestCopyAll) {
  const static size_t BIG_SZ = 10737414;
  std::shared_ptr <unsigned char> big(
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
  std::shared_ptr <unsigned char> big2(
      (unsigned char*)malloc(BIG_SZ), free);
  bl2.begin().copy(BIG_SZ, (char*)big2.get());
  ASSERT_EQ(memcmp(big.get(), big2.get(), BIG_SZ), 0);
}

TEST(BufferList, InvalidateCrc) {
  const static size_t buffer_size = 262144;
  std::shared_ptr <unsigned char> big(
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

TEST(BufferList, TestIsProvidedBuffer) {
  char buff[100];
  bufferlist bl;
  bl.push_back(buffer::create_static(100, buff));
  ASSERT_TRUE(bl.is_provided_buffer(buff));
  bl.append_zero(100);
  ASSERT_FALSE(bl.is_provided_buffer(buff));
}

TEST(BufferList, DISABLED_DanglingLastP) {
  bufferlist bl;
  {
    // previously we're using the unsharable buffer type to distinguish
    // the last_p-specific problem from the generic crosstalk issues we
    // had since the very beginning:
    // https://gist.github.com/rzarzynski/aed18372e88aed392101adac3bd87bbc
    // this is no longer possible as `buffer::create_unsharable()` has
    // been dropped.
    bufferptr bp(buffer::create(10));
    bp.copy_in(0, 3, "XXX");
    bl.push_back(std::move(bp));
    EXPECT_EQ(0, ::memcmp("XXX", bl.c_str(), 3));

    // let `copy_in` to set `last_p` member of bufferlist
    bl.begin().copy_in(2, "AB");
    EXPECT_EQ(0, ::memcmp("ABX", bl.c_str(), 3));
  }

  bufferlist empty;
  // before the fix this would have left `last_p` unchanged leading to
  // the dangerous dangling state – keep in mind that the initial,
  // unsharable bptr will be freed.
  bl = const_cast<const bufferlist&>(empty);
  bl.append("123");

  // we must continue from where the previous copy_in had finished.
  // Otherwise `bl::copy_in` will call `seek()` and refresh `last_p`.
  bl.begin(2).copy_in(1, "C");
  EXPECT_EQ(0, ::memcmp("12C", bl.c_str(), 3));
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
 *    ulimit -s unlimited ; valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *    ./unittest_bufferlist # --gtest_filter=BufferList.constructors"
 * End:
 */

