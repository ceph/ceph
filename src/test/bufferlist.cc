#include <tr1/memory>

#include "include/buffer.h"
#include "include/encoding.h"

#include "gtest/gtest.h"
#include "stdlib.h"


#define MAX_TEST 1000000

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
    EXPECT_THROW(bl.zero((unsigned)0, (unsigned)2000), FailedAssertion);
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
