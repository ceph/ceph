#include <string>
#include <sstream>
#include <utility>

#include <gtest/gtest.h>

#include "rgw_common.h"
#include "rgw_formats.h"

namespace {

std::string flush(RGWFormatter_Plain& formatter)
{
  std::ostringstream output;
  formatter.flush(output);

  return std::move(output).str();
}

TEST(RGWFormatterPlain, PreservesNestedPlainSelection)
{
  RGWFormatter_Plain formatter;
  formatter.open_array_section("root");
  formatter.dump_string("first", "one");
  formatter.dump_string("second", "two");
  formatter.open_object_section("nested");
  formatter.dump_string("third", "three");
  formatter.close_section();
  formatter.close_section();

  EXPECT_EQ("one", flush(formatter));
}

TEST(RGWFormatterPlain, PreservesNestedKeyValueLayout)
{
  RGWFormatter_Plain formatter {true};
  formatter.open_object_section("root");
  formatter.dump_string("name", "alpha");
  formatter.open_array_section("values");
  formatter.dump_int("", 1);
  formatter.dump_int("", 2);
  formatter.close_section();
  formatter.close_section();

  EXPECT_EQ("name: alpha\nvalues: \n1\n2", flush(formatter));
}

TEST(RGWFormatterPlain, PreservesDeepNesting)
{
  RGWFormatter_Plain formatter;

  for (int depth = 0; depth != 6; ++depth) {
    formatter.open_object_section("level");
  }

  formatter.dump_string("leaf", "value");

  for (int depth = 0; depth != 6; ++depth) {
    formatter.close_section();
  }

  EXPECT_EQ("value", flush(formatter));
}

} // namespace
