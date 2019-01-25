#include "test_shared.h"

#include <cstring>
#include "gtest/gtest.h"
#include "include/buffer.h"

using namespace ceph;

std::string get_temp_pool_name(const std::string &prefix)
{
  char hostname[80];
  char out[160];
  memset(hostname, 0, sizeof(hostname));
  memset(out, 0, sizeof(out));
  gethostname(hostname, sizeof(hostname)-1);
  static int num = 1;
  snprintf(out, sizeof(out), "%s-%d-%d", hostname, getpid(), num);
  num++;
  return prefix + out;
}

void assert_eq_sparse(bufferlist& expected,
                      const std::map<uint64_t, uint64_t>& extents,
                      bufferlist& actual) {
  auto i = expected.begin();
  auto p = actual.begin();
  uint64_t pos = 0;
  for (auto extent : extents) {
    const uint64_t start = extent.first;
    const uint64_t end = start + extent.second;
    for (; pos < end; ++i, ++pos) {
      ASSERT_FALSE(i.end());
      if (pos < start) {
        // check the hole
        ASSERT_EQ('\0', *i);
      } else {
        // then the extent
        ASSERT_EQ(*i, *p);
        ++p;
      }
    }
  }
  ASSERT_EQ(expected.length(), pos);
}
