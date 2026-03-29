// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_kafka.h"
#include "common/ceph_context.h"
#include <gtest/gtest.h>

using namespace rgw;

class CctCleaner {
  CephContext* cct;
public:
  explicit CctCleaner(CephContext* _cct) : cct(_cct) {}
  ~CctCleaner() {
#ifdef WITH_CRIMSON
    delete cct;
#else
    cct->put();
#endif
  }
};

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

CctCleaner cleaner(cct);

class TestKafka : public ::testing::Test {
protected:
  void SetUp() override {
    ASSERT_TRUE(kafka::init(cct));
  }

  void TearDown() override {
    kafka::shutdown();
  }
};

TEST_F(TestKafka, ConnectionReuseWithSameVerifySSL)
{
  const auto connection_number = kafka::get_connection_count();

  kafka::connection_id_t conn_id1;
  auto rc = kafka::connect(conn_id1,
                           "kafka://localhost:9093",
                           true,
                           true,
                           boost::none,
                           boost::none,
                           boost::none,
                           boost::none,
                           boost::none);
  ASSERT_TRUE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number + 1);

  kafka::connection_id_t conn_id2;
  rc = kafka::connect(conn_id2,
                      "kafka://localhost:9093",
                      true,
                      true,
                      boost::none,
                      boost::none,
                      boost::none,
                      boost::none,
                      boost::none);
  ASSERT_TRUE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number + 1);
}

TEST_F(TestKafka, ConnectionReuseDistinguishesVerifySSL)
{
  const auto connection_number = kafka::get_connection_count();

  kafka::connection_id_t conn_id1;
  auto rc = kafka::connect(conn_id1,
                           "kafka://localhost:9093",
                           true,
                           true,
                           boost::none,
                           boost::none,
                           boost::none,
                           boost::none,
                           boost::none);
  ASSERT_TRUE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number + 1);

  kafka::connection_id_t conn_id2;
  rc = kafka::connect(conn_id2,
                      "kafka://localhost:9093",
                      true,
                      false,
                      boost::none,
                      boost::none,
                      boost::none,
                      boost::none,
                      boost::none);
  ASSERT_TRUE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number + 2);
}
