#include "gtest/gtest.h"

#include "common/HTMLFormatter.h"
#include <sstream>
#include <string>

using namespace ceph;

TEST(htmlformatter, dump_format_large_item)
{
  std::stringstream sout;
  HTMLFormatter formatter(false);

  std::string base_url("http://example.com");
  std::string bucket_name("bucket");
  std::string object_key(1024, 'a');

  formatter.dump_format("Location", "%s/%s/%s", base_url.c_str(), bucket_name.c_str(), object_key.c_str());

  formatter.flush(sout);

  std::string uri = base_url + "/" + bucket_name + "/" + object_key;
  std::string expected_output = "<li>Location: " + uri + "</li>";

  EXPECT_EQ(expected_output, sout.str());
}