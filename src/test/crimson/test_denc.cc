#include <string>
#include <seastar/core/temporary_buffer.hh>
#include <gtest/gtest.h>
#include "include/denc.h"
#include "crimson/common/buffer_seastar.h"

using temporary_buffer = seastar::temporary_buffer<char>;
using buffer_iterator = seastar_buffer_iterator;
using const_buffer_iterator = const_seastar_buffer_iterator;

template<typename T>
void test_denc(T v) {
  // estimate
  size_t s = 0;
  denc(v, s);
  ASSERT_NE(s, 0u);

  // encode
  temporary_buffer buf{s};
  buffer_iterator enc{buf};
  denc(v, enc);
  size_t len = enc.get() - buf.begin();
  ASSERT_LE(len, s);

  // decode
  T out;
  temporary_buffer encoded = buf.share();
  encoded.trim(len);
  const_buffer_iterator dec{encoded};
  denc(out, dec);
  ASSERT_EQ(v, out);
  ASSERT_EQ(dec.get(), enc.get());
}

TEST(denc, simple)
{
  test_denc((uint8_t)4);
  test_denc((int8_t)-5);
  test_denc((uint16_t)6);
  test_denc((int16_t)-7);
  test_denc((uint32_t)8);
  test_denc((int32_t)-9);
  test_denc((uint64_t)10);
  test_denc((int64_t)-11);
}

TEST(denc, string)
{
  std::string a, b("hi"), c("multi\nline\n");
  test_denc(a);
  test_denc(b);
  test_denc(c);
}
