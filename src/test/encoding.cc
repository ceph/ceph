#include "config.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/common_init.h"

#include "gtest/gtest.h"

template < typename T >
static void test_encode_and_decode(const T& src)
{
  bufferlist bl(1000000);
  encode(src, bl);
  T dst;
  bufferlist::iterator i(bl.begin());
  decode(dst, i);
  ASSERT_EQ(src, dst) << "Encoding roundtrip changed the string: orig=" << src << ", but new=" << dst;
}

TEST(Encoding, RoundTripSimple) {
  string my_str("I am the very model of a modern major general");
  test_encode_and_decode < std::string >(my_str);
}
