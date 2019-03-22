// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#define TEST_DEBUG 20

#define dout_subsys ceph_subsys_


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

