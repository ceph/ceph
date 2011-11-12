
#include "include/types.h"
#include "include/str_list.h"

#include <list>
#include <string>

#include "gtest/gtest.h"


const char *tests[][10] = {
  { "foo,bar", "foo", "bar", 0 },
  { "foo", "foo", 0 },
  { "foo;bar", "foo", "bar", 0 },
  { "foo bar", "foo", "bar", 0 },
  { " foo bar", "foo", "bar", 0 },
  { " foo bar ", "foo", "bar", 0 },
  { "a,b,c", "a", "b", "c", 0 },
  { " a\tb\tc\t", "a", "b", "c", 0 },
  { "a, b, c", "a", "b", "c", 0 },
  { "a b c", "a", "b", "c", 0 },
  { 0 },
};

TEST(StrList, All)
{
  for (unsigned i=0; tests[i][0]; ++i) {
    std::string src = tests[i][0];
    std::list<std::string> expected;
    for (unsigned j=1; tests[i][j]; ++j)
      expected.push_back(tests[i][j]);
    std::list<std::string> actual;
    get_str_list(src, actual);
    std::cout << "'" << src << "' -> " << actual << std::endl;
    ASSERT_EQ(actual, expected);
  }
}
