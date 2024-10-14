#include "include/utime.h"

#include <iostream> // for std::cout

#include "gtest/gtest.h"
#include "include/stringify.h"
#include "common/ceph_context.h"

using namespace std;

TEST(utime_t, localtime)
{
  utime_t t(1556122013, 839991182);
  string s = stringify(t);
  cout << s << std::endl;
  // time zone may vary where unit test is run, so be cirsumspect...
  ASSERT_EQ(s.size(), strlen("2019-04-24T11:06:53.839991-0500"));
  ASSERT_TRUE(s[26] == '-' || s[26] == '+');
  ASSERT_EQ(s.substr(0, 9), "2019-04-2");
}

TEST(utime_t, gmtime)
{
  utime_t t(1556122013, 39991182);
  {
    ostringstream ss;
    t.gmtime(ss);
    ASSERT_EQ(ss.str(), "2019-04-24T16:06:53.039991Z");
  }
  {
    ostringstream ss;
    t.gmtime_nsec(ss);
    ASSERT_EQ(ss.str(), "2019-04-24T16:06:53.039991182Z");
  }
}

TEST(utime_t, asctime)
{
  utime_t t(1556122013, 839991182);
  ostringstream ss;
  t.asctime(ss);
  string s = ss.str();
  ASSERT_EQ(s, "Wed Apr 24 16:06:53 2019");
}

const char *v[][2] = {
  { "2019-04-24T16:06:53.039991Z", "2019-04-24T16:06:53.039991Z" },
  { "2019-04-24 16:06:53.039991Z", "2019-04-24T16:06:53.039991Z" },
  { "2019-04-24 16:06:53.039991+0000", "2019-04-24T16:06:53.039991Z" },
  { "2019-04-24 16:06:53.039991-0100", "2019-04-24T17:06:53.039991Z" },
  { "2019-04-24 16:06:53.039991+0430", "2019-04-24T11:36:53.039991Z" },
  { "2019-04-24 16:06:53+0000", "2019-04-24T16:06:53.000000Z" },
  { "2019-04-24T16:06:53-0100", "2019-04-24T17:06:53.000000Z" },
  { "2019-04-24 16:06:53+0430", "2019-04-24T11:36:53.000000Z" },
  { "2019-04-24", "2019-04-24T00:00:00.000000Z" },
  { 0, 0 },
};

TEST(utime_t, parse_date)
{
  for (unsigned i = 0; v[i][0]; ++i) {
    cout << v[i][0] << " -> " << v[i][1] << std::endl;
    utime_t t;
    bool r = t.parse(string(v[i][0]));
    ASSERT_TRUE(r);
    ostringstream ss;
    t.gmtime(ss);
    ASSERT_EQ(ss.str(), v[i][1]);
  }
}
