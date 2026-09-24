// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/cufile_config.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "json_spirit/json_spirit.h"

using ceph::rdma::cufile_json_with_addrs;

namespace {

json_spirit::mObject parse(const std::string& s)
{
  json_spirit::mValue v;
  EXPECT_TRUE(json_spirit::read(s, v));
  return v.get_obj();
}

std::vector<std::string> addr_list(const json_spirit::mObject& o)
{
  std::vector<std::string> out;
  for (const auto& a : o.at("properties").get_obj()
         .at("rdma_dev_addr_list").get_array()) {
    out.push_back(a.get_str());
  }
  return out;
}

} // anonymous namespace

// shaped like the cufile.json NVIDIA ships: // comments throughout, a
// commented-out example, and strings that contain comment markers
static const char* stock = R"({
    // NOTE : Application can override custom configuration via export CUFILE_ENV_PATH_JSON=<filepath>
    "logging": {
        //"dir": "/home/<xxxx>",
        "level": "ERROR" /* one of ERROR, WARN, INFO */
    },
    "properties": {
        "max_direct_io_size_kb" : 16384,
        "use_poll_mode" : false,
        //"rdma_dev_addr_list": [ "192.168.0.12", "192.168.1.12" ],
        "rdma_dev_addr_list": [ ],
        "rdma_dc_key": "0xffeeddcc",
        "note": "a // inside a string, and a \" before /* this */"
    }
})";

TEST(CufileConfig, ReplacesAddrListKeepsTheRest)
{
  auto out = cufile_json_with_addrs(stock, {"10.64.2.7"});
  ASSERT_FALSE(out.empty());
  auto o = parse(out);
  EXPECT_EQ(std::vector<std::string>{"10.64.2.7"}, addr_list(o));
  const auto& props = o.at("properties").get_obj();
  EXPECT_EQ(16384, props.at("max_direct_io_size_kb").get_int());
  EXPECT_FALSE(props.at("use_poll_mode").get_bool());
  EXPECT_EQ("0xffeeddcc", props.at("rdma_dc_key").get_str());
  EXPECT_EQ("a // inside a string, and a \" before /* this */",
            props.at("note").get_str());
  EXPECT_EQ("ERROR", o.at("logging").get_obj().at("level").get_str());
}

TEST(CufileConfig, OneAddrPerRail)
{
  auto out = cufile_json_with_addrs(stock, {"10.64.2.7", "10.64.3.7"});
  ASSERT_FALSE(out.empty());
  EXPECT_EQ((std::vector<std::string>{"10.64.2.7", "10.64.3.7"}),
            addr_list(parse(out)));
}

TEST(CufileConfig, NoHostFile)
{
  // what setup_cufile_json starts from when /etc/cufile.json is absent
  auto out = cufile_json_with_addrs("{}", {"10.64.2.7"});
  ASSERT_FALSE(out.empty());
  EXPECT_EQ(std::vector<std::string>{"10.64.2.7"}, addr_list(parse(out)));
}

TEST(CufileConfig, NotAnObject)
{
  EXPECT_TRUE(cufile_json_with_addrs("", {"10.64.2.7"}).empty());
  EXPECT_TRUE(cufile_json_with_addrs("[1, 2]", {"10.64.2.7"}).empty());
  EXPECT_TRUE(cufile_json_with_addrs("{\"properties\": ",
                                     {"10.64.2.7"}).empty());
  EXPECT_TRUE(cufile_json_with_addrs("{ /* unterminated",
                                     {"10.64.2.7"}).empty());
}
