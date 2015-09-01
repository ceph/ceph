// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Journaler.h"
#include "include/stringify.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "test/journal/RadosTestFixture.h"
#include "include/stringify.h"

class TestJournaler : public RadosTestFixture {
public:

  static const std::string CLIENT_ID;

  static std::string get_temp_journal_id() {
    return stringify(++_journal_id);
  }

  virtual void SetUp() {
    RadosTestFixture::SetUp();
    m_journal_id = get_temp_journal_id();
    m_journaler = new journal::Journaler(m_ioctx, m_ioctx, m_journal_id,
                                         CLIENT_ID);
  }

  virtual void TearDown() {
    delete m_journaler;
    RadosTestFixture::TearDown();
  }

  int create_journal(uint8_t order, uint8_t splay_width) {
    return m_journaler->create(order, splay_width);
  }

  int init_journaler() {
    C_SaferCond cond;
    m_journaler->init(&cond);
    return cond.wait();
  }

  int register_client(const std::string &client_id, const std::string &desc) {
    journal::Journaler journaler(m_ioctx, m_ioctx, m_journal_id, client_id);
    return journaler.register_client(desc);
  }

  static uint64_t _journal_id;

  std::string m_journal_id;
  journal::Journaler *m_journaler;
};

const std::string TestJournaler::CLIENT_ID = "client1";
uint64_t TestJournaler::_journal_id = 0;

TEST_F(TestJournaler, Create) {
  ASSERT_EQ(0, create_journal(12, 8));
}

TEST_F(TestJournaler, CreateDuplicate) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(-EEXIST, create_journal(12, 8));
}

TEST_F(TestJournaler, CreateInvalidParams) {
  ASSERT_EQ(-EDOM, create_journal(1, 8));
  ASSERT_EQ(-EDOM, create_journal(123, 8));
  ASSERT_EQ(-EINVAL, create_journal(12, 0));
}

TEST_F(TestJournaler, Init) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));
  ASSERT_EQ(0, init_journaler());
}

TEST_F(TestJournaler, InitDNE) {
  ASSERT_EQ(-ENOENT, init_journaler());
}

TEST_F(TestJournaler, RegisterClientDuplicate) {
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));
  ASSERT_EQ(-EEXIST, register_client(CLIENT_ID, "foo2"));
}

