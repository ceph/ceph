#include "gtest/gtest.h"

#include "common/Formatter.h"
#include <iostream>
#include <sstream>
#include <string>

using namespace ceph;

TEST(tableformatter, singleline)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n";
  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, longfloat)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_float("float", 1.0 / 7);
  formatter.flush(sout);

  std::string cmp = ""
    "+----------------------+\n"
    "| float                |\n"
    "+----------------------+\n"
    "| 0.14285714285714285  |\n"
    "+----------------------+\n";
  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, multiline)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "string");

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "| 20       | 20     | string  |\n"
    "+----------+--------+---------+\n";

  formatter.flush(sout);
  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, multiflush)
{
  std::stringstream sout1;
  std::stringstream sout2;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout1);

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n";

  EXPECT_EQ(cmp, sout1.str());

  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout2);

  cmp = ""
    "| 20       | 20     | string  |\n"
    "+----------+--------+---------+\n";

  EXPECT_EQ(cmp, sout2.str());

}

TEST(tableformatter, multireset)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);
  formatter.reset();
  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n"
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 20       | 20     | string  |\n"
    "+----------+--------+---------+\n";

  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, changingheaderlength)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);
  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "stringstring");
  formatter.flush(sout);

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n"
    "+----------+--------+---------------+\n"
    "| integer  | float  | string        |\n"
    "+----------+--------+---------------+\n"
    "| 20       | 20     | stringstring  |\n"
    "+----------+--------+---------------+\n";

  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, changingheader)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);
  formatter.dump_int("longinteger", 20);
  formatter.dump_float("double", 20.0);
  formatter.dump_string("char*", "stringstring");
  formatter.flush(sout);

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n"
    "+--------------+---------+---------------+\n"
    "| longinteger  | double  | char*         |\n"
    "+--------------+---------+---------------+\n"
    "| 20           | 20      | stringstring  |\n"
    "+--------------+---------+---------------+\n";

  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, extendingheader)
{
  std::stringstream sout;
  TableFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);
  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "string");
  formatter.dump_string("char*", "abcde");
  formatter.flush(sout);

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n"
    "+----------+--------+---------+--------+\n"
    "| integer  | float  | string  | char*  |\n"
    "+----------+--------+---------+--------+\n"
    "| 20       | 20     | string  | abcde  |\n"
    "+----------+--------+---------+--------+\n";

  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, stream)
{
  std::stringstream sout;
  TableFormatter* formatter = (TableFormatter*) Formatter::create("table");
  formatter->dump_stream("integer") << 10;
  formatter->dump_stream("float") << 10.0;
  formatter->dump_stream("string") << "string";
  formatter->flush(sout);
  delete formatter;

  std::string cmp = ""
    "+----------+--------+---------+\n"
    "| integer  | float  | string  |\n"
    "+----------+--------+---------+\n"
    "| 10       | 10     | string  |\n"
    "+----------+--------+---------+\n";

  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, multiline_keyval)
{
  std::stringstream sout;
  TableFormatter* formatter = (TableFormatter*) Formatter::create("table-kv");
  formatter->dump_int("integer", 10);
  formatter->dump_float("float", 10.0);
  formatter->dump_string("string", "string");
  formatter->dump_int("integer", 20);
  formatter->dump_float("float", 20.0);
  formatter->dump_string("string", "string");
  formatter->flush(sout);
  delete formatter;

  std::string cmp = ""
    "key::integer=\"10\" key::float=\"10\" key::string=\"string\" \n"
    "key::integer=\"20\" key::float=\"20\" key::string=\"string\" \n";

  EXPECT_EQ(cmp, sout.str());
}

TEST(tableformatter, dump_large_item) {
  std::stringstream sout;
  TableFormatter* formatter = (TableFormatter*) Formatter::create("table-kv");

  std::string base_url("http://example.com");
  std::string bucket_name("bucket");
  std::string object_key(1024, 'a');

  std::string full_url = base_url + "/" + bucket_name + "/" + object_key;
  formatter->dump_format("Location", "%s/%s/%s", base_url.c_str(), bucket_name.c_str(), object_key.c_str());
  formatter->flush(sout);
  delete formatter;

  std::string cmp = "key::Location=\"" + full_url + "\" \n";
  EXPECT_EQ(cmp, sout.str());
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_tableformatter &&
 *      ./unittest_tableformatter
 *   '
 * End:
 */



