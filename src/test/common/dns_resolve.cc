// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <arpa/nameser_compat.h>

#include "common/dns_resolve.h"
#include "test/common/dns_messages.h"

#include "common/debug.h"
#include "gmock/gmock.h"

#include <sstream>
#include <set>

#ifndef T_AAAA
#define T_AAAA ns_t_aaaa
#endif

#define TEST_DEBUG 20

#define dout_subsys ceph_subsys_

using namespace std;

using ::testing::Return;
using ::testing::_;
using ::testing::SetArrayArgument;
using ::testing::DoAll;
using ::testing::StrEq;

class DNSResolverTest : public ::testing::Test {
  protected:
    void SetUp() override {
      g_ceph_context->_conf->subsys.set_log_level(dout_subsys, TEST_DEBUG);
    }

    void TearDown() override  {
      DNSResolver::get_instance(nullptr);
    }
};

TEST_F(DNSResolverTest, resolve_ip_addr) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

  int lena = sizeof(ns_query_msg_mon_a_payload);
#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_,StrEq("mon.a.ceph.com"), C_IN, T_A,_,_))
       .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_payload,
            ns_query_msg_mon_a_payload+lena), Return(lena)));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_A,_,_))
       .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_payload,
            ns_query_msg_mon_a_payload+lena), Return(lena)));
#endif

  entity_addr_t addr;
  DNSResolver::get_instance(resolvH)->resolve_ip_addr(g_ceph_context,
      "mon.a.ceph.com", &addr);

  std::ostringstream os;
  os << addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.11:0/0");
}

TEST_F(DNSResolverTest, resolve_ip_addr_fail) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_,StrEq("not_exists.com"), C_IN, T_A,_,_))
       .WillOnce(Return(0));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("not_exists.com"), C_IN, T_A,_,_))
       .WillOnce(Return(0));
#endif

  entity_addr_t addr;
  int ret = DNSResolver::get_instance(resolvH)->resolve_ip_addr(g_ceph_context, 
      "not_exists.com", &addr);

  ASSERT_EQ(ret, -1);
  std::ostringstream os;
  os << addr;
  ASSERT_EQ(os.str(), "-");
}


TEST_F(DNSResolverTest, resolve_srv_hosts_empty_domain) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();


  int len = sizeof(ns_search_msg_ok_payload);
  int lena = sizeof(ns_query_msg_mon_a_payload);
  int lenb = sizeof(ns_query_msg_mon_b_payload);
  int lenc = sizeof(ns_query_msg_mon_c_payload);

  using ::testing::InSequence;
  {
    InSequence s;

#ifdef HAVE_RES_NQUERY
    EXPECT_CALL(*resolvH, res_nsearch(_, StrEq("_cephmon._tcp"), C_IN, T_SRV, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_search_msg_ok_payload,
            ns_search_msg_ok_payload+len), Return(len)));

    EXPECT_CALL(*resolvH, res_nquery(_,StrEq("mon.a.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_payload,
            ns_query_msg_mon_a_payload+lena), Return(lena)));

    EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.c.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_c_payload,
            ns_query_msg_mon_c_payload+lenc), Return(lenc)));

    EXPECT_CALL(*resolvH, res_nquery(_,StrEq("mon.b.ceph.com"), C_IN, T_A, _,_))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_b_payload,
            ns_query_msg_mon_b_payload+lenb), Return(lenb)));
#else
    EXPECT_CALL(*resolvH, res_search(StrEq("_cephmon._tcp"), C_IN, T_SRV, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_search_msg_ok_payload,
            ns_search_msg_ok_payload+len), Return(len)));

    EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_payload,
            ns_query_msg_mon_a_payload+lena), Return(lena)));

    EXPECT_CALL(*resolvH, res_query(StrEq("mon.c.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_c_payload,
            ns_query_msg_mon_c_payload+lenc), Return(lenc)));

    EXPECT_CALL(*resolvH, res_query(StrEq("mon.b.ceph.com"), C_IN, T_A, _,_))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_b_payload,
            ns_query_msg_mon_b_payload+lenb), Return(lenb)));
#endif
  }

  map<string, DNSResolver::Record> records;
  DNSResolver::get_instance(resolvH)->resolve_srv_hosts(g_ceph_context, "cephmon", 
      DNSResolver::SRV_Protocol::TCP, &records);

  ASSERT_EQ(records.size(), (unsigned int)3);
  auto it = records.find("mon.a");
  ASSERT_NE(it, records.end());
  std::ostringstream os;
  os << it->second.addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.11:6789/0");
  os.str("");
  ASSERT_EQ(it->second.priority, 10);
  ASSERT_EQ(it->second.weight, 40);
  it = records.find("mon.b");
  ASSERT_NE(it, records.end());
  os << it->second.addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.12:6789/0");
  os.str("");
  ASSERT_EQ(it->second.priority, 10);
  ASSERT_EQ(it->second.weight, 35);
  it = records.find("mon.c");
  ASSERT_NE(it, records.end());
  os << it->second.addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.13:6789/0");
  ASSERT_EQ(it->second.priority, 10);
  ASSERT_EQ(it->second.weight, 25);
}

TEST_F(DNSResolverTest, resolve_srv_hosts_full_domain) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();


  int len = sizeof(ns_search_msg_ok_payload);
  int lena = sizeof(ns_query_msg_mon_a_payload);
  int lenb = sizeof(ns_query_msg_mon_b_payload);
  int lenc = sizeof(ns_query_msg_mon_c_payload);

  using ::testing::InSequence;
  {
    InSequence s;

#ifdef HAVE_RES_NQUERY
    EXPECT_CALL(*resolvH, res_nsearch(_, StrEq("_cephmon._tcp.ceph.com"), C_IN, T_SRV, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_search_msg_ok_payload,
            ns_search_msg_ok_payload+len), Return(len)));

    EXPECT_CALL(*resolvH, res_nquery(_,StrEq("mon.a.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_payload,
            ns_query_msg_mon_a_payload+lena), Return(lena)));

    EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.c.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_c_payload,
            ns_query_msg_mon_c_payload+lenc), Return(lenc)));

    EXPECT_CALL(*resolvH, res_nquery(_,StrEq("mon.b.ceph.com"), C_IN, T_A, _,_))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_b_payload,
            ns_query_msg_mon_b_payload+lenb), Return(lenb)));
#else
    EXPECT_CALL(*resolvH, res_search(StrEq("_cephmon._tcp.ceph.com"), C_IN, T_SRV, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_search_msg_ok_payload,
            ns_search_msg_ok_payload+len), Return(len)));

    EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_payload,
            ns_query_msg_mon_a_payload+lena), Return(lena)));

    EXPECT_CALL(*resolvH, res_query(StrEq("mon.c.ceph.com"), C_IN, T_A,_,_))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_c_payload,
            ns_query_msg_mon_c_payload+lenc), Return(lenc)));

    EXPECT_CALL(*resolvH, res_query(StrEq("mon.b.ceph.com"), C_IN, T_A, _,_))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_b_payload,
            ns_query_msg_mon_b_payload+lenb), Return(lenb)));
#endif
  }

  map<string, DNSResolver::Record> records;
  DNSResolver::get_instance(resolvH)->resolve_srv_hosts(g_ceph_context, "cephmon", 
      DNSResolver::SRV_Protocol::TCP, "ceph.com", &records);

  ASSERT_EQ(records.size(), (unsigned int)3);
  auto it = records.find("mon.a");
  ASSERT_NE(it, records.end());
  std::ostringstream os;
  os << it->second.addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.11:6789/0");
  os.str("");
  it = records.find("mon.b");
  ASSERT_NE(it, records.end());
  os << it->second.addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.12:6789/0");
  os.str("");
  it = records.find("mon.c");
  ASSERT_NE(it, records.end());
  os << it->second.addr;
  ASSERT_EQ(os.str(), "v2:192.168.1.13:6789/0");
}

TEST_F(DNSResolverTest, resolve_srv_hosts_fail) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();


  using ::testing::InSequence;
  {
    InSequence s;

#ifdef HAVE_RES_NQUERY
    EXPECT_CALL(*resolvH, res_nsearch(_, StrEq("_noservice._tcp"), C_IN, T_SRV, _, _))
      .WillOnce(Return(0));
#else
    EXPECT_CALL(*resolvH, res_search(StrEq("_noservice._tcp"), C_IN, T_SRV, _, _))
      .WillOnce(Return(0));
#endif
  }

  map<string, DNSResolver::Record> records;
  int ret = DNSResolver::get_instance(resolvH)->resolve_srv_hosts(
      g_ceph_context, "noservice", DNSResolver::SRV_Protocol::TCP, "", &records);

  ASSERT_EQ(0, ret);
  ASSERT_TRUE(records.empty());
}

TEST_F(DNSResolverTest, resolve_all_addrs_ipv4_only) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

  int lena = sizeof(ns_query_msg_mon_a_payload);

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.a.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_payload,
          ns_query_msg_mon_a_payload + lena), Return(lena)));

  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.a.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(Return(-1));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_payload,
          ns_query_msg_mon_a_payload + lena), Return(lena)));

  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(Return(-1));
#endif

  std::vector<entity_addr_t> addrs;
  int ret = DNSResolver::get_instance(resolvH)->resolve_all_addrs(
      g_ceph_context, "mon.a.ceph.com", &addrs);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(addrs.size(), 1u);
  std::ostringstream os;
  os << addrs[0];
  ASSERT_EQ(os.str(), "v2:192.168.1.11:0/0");
}

TEST_F(DNSResolverTest, resolve_all_addrs_ipv6_only) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

  int len_aaaa = sizeof(ns_query_msg_mon_a_aaaa_payload);

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.a.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(Return(-1));

  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.a.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_aaaa_payload,
          ns_query_msg_mon_a_aaaa_payload + len_aaaa), Return(len_aaaa)));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(Return(-1));

  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_aaaa_payload,
          ns_query_msg_mon_a_aaaa_payload + len_aaaa), Return(len_aaaa)));
#endif

  std::vector<entity_addr_t> addrs;
  int ret = DNSResolver::get_instance(resolvH)->resolve_all_addrs(
      g_ceph_context, "mon.a.ceph.com", &addrs);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(addrs.size(), 1u);
  std::ostringstream os;
  os << addrs[0];
  ASSERT_EQ(os.str(), "v2:[2001:db8::1]:0/0");
}

TEST_F(DNSResolverTest, resolve_all_addrs_no_results) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("nonexistent.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(Return(-1));

  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("nonexistent.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(Return(-1));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("nonexistent.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(Return(-1));

  EXPECT_CALL(*resolvH, res_query(StrEq("nonexistent.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(Return(-1));
#endif

  std::vector<entity_addr_t> addrs;
  int ret = DNSResolver::get_instance(resolvH)->resolve_all_addrs(
      g_ceph_context, "nonexistent.ceph.com", &addrs);

  ASSERT_LT(ret, 0);
  ASSERT_TRUE(addrs.empty());
}

TEST_F(DNSResolverTest, resolve_all_addrs_multiple_ipv4) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

  int len_multi_a = sizeof(ns_query_msg_multi_a_payload);

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("multi.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_multi_a_payload,
          ns_query_msg_multi_a_payload + len_multi_a), Return(len_multi_a)));

  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("multi.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(Return(-1));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("multi.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_multi_a_payload,
          ns_query_msg_multi_a_payload + len_multi_a), Return(len_multi_a)));

  EXPECT_CALL(*resolvH, res_query(StrEq("multi.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(Return(-1));
#endif

  std::vector<entity_addr_t> addrs;
  int ret = DNSResolver::get_instance(resolvH)->resolve_all_addrs(
      g_ceph_context, "multi.ceph.com", &addrs);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(addrs.size(), 3u);

  std::set<std::string> expected = {
    "v2:192.168.1.100:0/0",
    "v2:192.168.1.101:0/0",
    "v2:192.168.1.102:0/0"
  };
  std::set<std::string> actual;
  for (const auto& addr : addrs) {
    std::ostringstream os;
    os << addr;
    actual.insert(os.str());
  }
  ASSERT_EQ(actual, expected);
}

TEST_F(DNSResolverTest, resolve_all_addrs_multiple_ipv6) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

  int len_multi_aaaa = sizeof(ns_query_msg_multi_aaaa_payload);

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("multi.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(Return(-1));

  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("multi.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_multi_aaaa_payload,
          ns_query_msg_multi_aaaa_payload + len_multi_aaaa), Return(len_multi_aaaa)));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("multi.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(Return(-1));

  EXPECT_CALL(*resolvH, res_query(StrEq("multi.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_multi_aaaa_payload,
          ns_query_msg_multi_aaaa_payload + len_multi_aaaa), Return(len_multi_aaaa)));
#endif

  std::vector<entity_addr_t> addrs;
  int ret = DNSResolver::get_instance(resolvH)->resolve_all_addrs(
      g_ceph_context, "multi.ceph.com", &addrs);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(addrs.size(), 2u);

  std::set<std::string> expected = {
    "v2:[2001:db8::1]:0/0",
    "v2:[2001:db8::2]:0/0"
  };
  std::set<std::string> actual;
  for (const auto& addr : addrs) {
    std::ostringstream os;
    os << addr;
    actual.insert(os.str());
  }
  ASSERT_EQ(actual, expected);
}

TEST_F(DNSResolverTest, resolve_all_addrs_both_families) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();

  int lena = sizeof(ns_query_msg_mon_a_payload);
  int len_aaaa = sizeof(ns_query_msg_mon_a_aaaa_payload);

#ifdef HAVE_RES_NQUERY
  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.a.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_payload,
          ns_query_msg_mon_a_payload + lena), Return(lena)));

  EXPECT_CALL(*resolvH, res_nquery(_, StrEq("mon.a.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(DoAll(SetArrayArgument<4>(ns_query_msg_mon_a_aaaa_payload,
          ns_query_msg_mon_a_aaaa_payload + len_aaaa), Return(len_aaaa)));
#else
  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_A, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_payload,
          ns_query_msg_mon_a_payload + lena), Return(lena)));

  EXPECT_CALL(*resolvH, res_query(StrEq("mon.a.ceph.com"), C_IN, T_AAAA, _, _))
      .WillOnce(DoAll(SetArrayArgument<3>(ns_query_msg_mon_a_aaaa_payload,
          ns_query_msg_mon_a_aaaa_payload + len_aaaa), Return(len_aaaa)));
#endif

  std::vector<entity_addr_t> addrs;
  int ret = DNSResolver::get_instance(resolvH)->resolve_all_addrs(
      g_ceph_context, "mon.a.ceph.com", &addrs);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(addrs.size(), 2u);

  // Check we got both IPv4 and IPv6
  bool found_ipv4 = false;
  bool found_ipv6 = false;
  for (const auto& addr : addrs) {
    std::ostringstream os;
    os << addr;
    if (os.str().find('[') != std::string::npos) {
      found_ipv6 = true;
      ASSERT_EQ(os.str(), "v2:[2001:db8::1]:0/0");
    } else {
      found_ipv4 = true;
      ASSERT_EQ(os.str(), "v2:192.168.1.11:0/0");
    }
  }
  ASSERT_TRUE(found_ipv4);
  ASSERT_TRUE(found_ipv6);
}
