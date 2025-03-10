#include "gtest/gtest.h"

#include "common/Formatter.h"
#include <sstream>
#include <string>

using namespace ceph;


TEST(xmlformatter, oneline)
{

  std::stringstream sout;
  XMLFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout);
  std::string cmp = "<integer>10</integer><float>10</float><string>string</string>";
  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, multiline)
{
  std::stringstream sout;
  XMLFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "string");

  std::string cmp = ""
    "<integer>10</integer><float>10</float><string>string</string>"
    "<integer>20</integer><float>20</float><string>string</string>";

  formatter.flush(sout);
  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, multiflush)
{
  std::stringstream sout1;
  std::stringstream sout2;
  XMLFormatter formatter;
  formatter.dump_int("integer", 10);
  formatter.dump_float("float", 10.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout1);

  std::string cmp = ""
    "<integer>10</integer>"
    "<float>10</float>"
    "<string>string</string>";

  EXPECT_EQ(cmp, sout1.str());

  formatter.dump_int("integer", 20);
  formatter.dump_float("float", 20.0);
  formatter.dump_string("string", "string");
  formatter.flush(sout2);

  cmp = ""
    "<integer>20</integer>"
    "<float>20</float>"
    "<string>string</string>";

  EXPECT_EQ(cmp, sout2.str());
}

TEST(xmlformatter, pretty)
{
  std::stringstream sout;
  XMLFormatter formatter(
      true,   // pretty
      false,   // lowercased
      false); // underscored
  formatter.open_object_section("xml");
  formatter.dump_int("Integer", 10);
  formatter.dump_float("Float", 10.0);
  formatter.dump_string("String", "String");
  formatter.close_section();
  formatter.flush(sout);
  std::string cmp = ""
    "<xml>\n"
    " <Integer>10</Integer>\n"
    " <Float>10</Float>\n"
    " <String>String</String>\n"
    "</xml>\n\n";
  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, lowercased)
{
  std::stringstream sout;
  XMLFormatter formatter(
      false,  // pretty
      true,   // lowercased
      false); // underscored
  formatter.dump_int("Integer", 10);
  formatter.dump_float("Float", 10.0);
  formatter.dump_string("String", "String");
  formatter.flush(sout);
  std::string cmp = ""
    "<integer>10</integer>"
    "<float>10</float>"
    "<string>String</string>";
  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, underscored)
{
  std::stringstream sout;
  XMLFormatter formatter(
      false,  // pretty
      false,   // lowercased
      true); // underscored
  formatter.dump_int("Integer Item", 10);
  formatter.dump_float("Float Item", 10.0);
  formatter.dump_string("String Item", "String");
  formatter.flush(sout);
  std::string cmp = ""
    "<Integer_Item>10</Integer_Item>"
    "<Float_Item>10</Float_Item>"
    "<String_Item>String</String_Item>";

  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, lowercased_underscored)
{
  std::stringstream sout;
  XMLFormatter formatter(
      false,  // pretty
      true,   // lowercased
      true); // underscored
  formatter.dump_int("Integer Item", 10);
  formatter.dump_float("Float Item", 10.0);
  formatter.dump_string("String Item", "String");
  formatter.flush(sout);
  std::string cmp = ""
    "<integer_item>10</integer_item>"
    "<float_item>10</float_item>"
    "<string_item>String</string_item>";
  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, pretty_lowercased_underscored)
{
  std::stringstream sout;
  XMLFormatter formatter(
      true,  // pretty
      true,   // lowercased
      true); // underscored
  formatter.dump_int("Integer Item", 10);
  formatter.dump_float("Float Item", 10.0);
  formatter.dump_string("String Item", "String");
  formatter.flush(sout);
  std::string cmp = ""
    "<integer_item>10</integer_item>\n"
    "<float_item>10</float_item>\n"
    "<string_item>String</string_item>\n\n";
  EXPECT_EQ(cmp, sout.str());
}

TEST(xmlformatter, dump_format_large_item)
{
  std::stringstream sout;
  XMLFormatter formatter(
      true,  // pretty
      false, // lowercased
      false); // underscored

  std::string base_url("http://example.com");
  std::string bucket_name("bucket");
  std::string object_key(1024, 'a');

  formatter.dump_format("Location", "%s/%s/%s", base_url.c_str(), bucket_name.c_str(), object_key.c_str());

  formatter.flush(sout);

  std::string uri = base_url + "/" + bucket_name + "/" + object_key;
  std::string expected_output = "<Location>" + uri + "</Location>\n\n";

  EXPECT_EQ(expected_output, sout.str());
}