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
#include "mon/MonMap.h"
#include "common/ceph_context.h"
#include "common/dns_resolve.h"
#include "test/common/dns_messages.h"

#include "common/debug.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <sstream>

#define TEST_DEBUG 20

#define dout_subsys ceph_subsys_mon


using ::testing::Return;
using ::testing::_;
using ::testing::SetArrayArgument;
using ::testing::DoAll;
using ::testing::StrEq;


class MonMapTest : public ::testing::Test {
  protected:
    virtual void SetUp() {
      g_ceph_context->_conf->subsys.set_log_level(dout_subsys, TEST_DEBUG);
    }

    virtual void TearDown()  {
      DNSResolver::get_instance(nullptr);
    }
};

TEST_F(MonMapTest, DISABLED_build_initial_config_from_dns) {

  MockResolvHWrapper *resolvH = new MockResolvHWrapper();
  DNSResolver::get_instance(resolvH);

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



  boost::intrusive_ptr<CephContext> cct(new CephContext(CEPH_ENTITY_TYPE_MON), false);
  cct->_conf.set_val("mon_dns_srv_name", "cephmon");
  MonMap monmap;
  int r = monmap.build_initial(cct.get(), false, std::cerr);

  ASSERT_EQ(r, 0);
  ASSERT_EQ(monmap.mon_info.size(), (unsigned int)3);
  auto it = monmap.mon_info.find("mon.a");
  ASSERT_NE(it, monmap.mon_info.end());
  std::ostringstream os;
  os << it->second.public_addrs;
  ASSERT_EQ(os.str(), "192.168.1.11:6789/0");
  os.str("");
  it = monmap.mon_info.find("mon.b");
  ASSERT_NE(it, monmap.mon_info.end());
  os << it->second.public_addrs;
  ASSERT_EQ(os.str(), "192.168.1.12:6789/0");
  os.str("");
  it = monmap.mon_info.find("mon.c");
  ASSERT_NE(it, monmap.mon_info.end());
  os << it->second.public_addrs;
  ASSERT_EQ(os.str(), "192.168.1.13:6789/0");
}

TEST_F(MonMapTest, DISABLED_build_initial_config_from_dns_fail) {
  MockResolvHWrapper *resolvH = new MockResolvHWrapper();
  DNSResolver::get_instance(resolvH);


#ifdef HAVE_RES_NQUERY
    EXPECT_CALL(*resolvH, res_nsearch(_, StrEq("_ceph-mon._tcp"), C_IN, T_SRV, _, _))
      .WillOnce(Return(0));
#else
    EXPECT_CALL(*resolvH, res_search(StrEq("_ceph-mon._tcp"), C_IN, T_SRV, _, _))
      .WillOnce(Return(0));
#endif

  boost::intrusive_ptr<CephContext> cct(new CephContext(CEPH_ENTITY_TYPE_MON), false);
  // using default value of mon_dns_srv_name option
  MonMap monmap;
  int r = monmap.build_initial(cct.get(), false, std::cerr);

  ASSERT_EQ(r, -ENOENT);
  ASSERT_EQ(monmap.mon_info.size(), (unsigned int)0);

}

TEST_F(MonMapTest, DISABLED_build_initial_config_from_dns_with_domain) {

  MockResolvHWrapper *resolvH = new MockResolvHWrapper();
  DNSResolver::get_instance(resolvH);

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



  boost::intrusive_ptr<CephContext> cct(new CephContext(CEPH_ENTITY_TYPE_MON), false);
  cct->_conf.set_val("mon_dns_srv_name", "cephmon_ceph.com");
  MonMap monmap;
  int r = monmap.build_initial(cct.get(), false, std::cerr);

  ASSERT_EQ(r, 0);
  ASSERT_EQ(monmap.mon_info.size(), (unsigned int)3);
  auto it = monmap.mon_info.find("mon.a");
  ASSERT_NE(it, monmap.mon_info.end());
  std::ostringstream os;
  os << it->second.public_addrs;
  ASSERT_EQ(os.str(), "192.168.1.11:6789/0");
  os.str("");
  it = monmap.mon_info.find("mon.b");
  ASSERT_NE(it, monmap.mon_info.end());
  os << it->second.public_addrs;
  ASSERT_EQ(os.str(), "192.168.1.12:6789/0");
  os.str("");
  it = monmap.mon_info.find("mon.c");
  ASSERT_NE(it, monmap.mon_info.end());
  os << it->second.public_addrs;
  ASSERT_EQ(os.str(), "192.168.1.13:6789/0");
}

TEST(MonMapBuildInitial, build_initial_mon_host_from_dns) {
  boost::intrusive_ptr<CephContext> cct(new CephContext(CEPH_ENTITY_TYPE_MON), false);
  cct->_conf.set_val("mon_host", "ceph.io");
  MonMap monmap;
  int r = monmap.build_initial(cct.get(), false, std::cerr);
  ASSERT_EQ(r, 0);
  ASSERT_GE(monmap.mon_info.size(), 1u);
  for (const auto& [name, info] : monmap.mon_info) {
    std::cerr << info << std::endl;
  }
}

TEST(MonMapBuildInitial, build_initial_mon_host_from_dns_fail) {
  boost::intrusive_ptr<CephContext> cct(new CephContext(CEPH_ENTITY_TYPE_MON), false);
  cct->_conf.set_val("mon_host", "ceph.noname");
  MonMap monmap;
  int r = monmap.build_initial(cct.get(), false, std::cerr);
  ASSERT_EQ(r, -EINVAL);
}
