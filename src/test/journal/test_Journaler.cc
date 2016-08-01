// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Journaler.h"
#include "journal/Settings.h"
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
    m_journaler = new journal::Journaler(m_work_queue, m_timer, &m_timer_lock,
                                         m_ioctx, m_journal_id, CLIENT_ID, {});
  }

  virtual void TearDown() {
    delete m_journaler;
    RadosTestFixture::TearDown();
  }

  int create_journal(uint8_t order, uint8_t splay_width) {
    return m_journaler->create(order, splay_width, -1);
  }

  int init_journaler() {
    C_SaferCond cond;
    m_journaler->init(&cond);
    return cond.wait();
  }

  int shut_down_journaler() {
    C_SaferCond ctx;
    m_journaler->shut_down(&ctx);
    return ctx.wait();
  }

  int register_client(const std::string &client_id, const std::string &desc) {
    journal::Journaler journaler(m_work_queue, m_timer, &m_timer_lock,
                                 m_ioctx, m_journal_id, client_id, {});
    bufferlist data;
    data.append(desc);
    C_SaferCond cond;
    journaler.register_client(data, &cond);
    return cond.wait();
  }

  int update_client(const std::string &client_id, const std::string &desc) {
    journal::Journaler journaler(m_work_queue, m_timer, &m_timer_lock,
                                 m_ioctx, m_journal_id, client_id, {});
    bufferlist data;
    data.append(desc);
    C_SaferCond cond;
    journaler.update_client(data, &cond);
    return cond.wait();
  }

  int unregister_client(const std::string &client_id) {
    journal::Journaler journaler(m_work_queue, m_timer, &m_timer_lock,
                                 m_ioctx, m_journal_id, client_id, {});
    C_SaferCond cond;
    journaler.unregister_client(&cond);
    return cond.wait();
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
  ASSERT_EQ(0, shut_down_journaler());
}

TEST_F(TestJournaler, InitDNE) {
  ASSERT_EQ(-ENOENT, init_journaler());
  ASSERT_EQ(0, shut_down_journaler());
}

TEST_F(TestJournaler, RegisterClientDuplicate) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));
  ASSERT_EQ(-EEXIST, register_client(CLIENT_ID, "foo2"));
}

TEST_F(TestJournaler, UpdateClient) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));
  ASSERT_EQ(0, update_client(CLIENT_ID, "foo2"));
}

TEST_F(TestJournaler, UpdateClientDNE) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(-ENOENT, update_client(CLIENT_ID, "foo"));
}

TEST_F(TestJournaler, UnregisterClient) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));
  ASSERT_EQ(0, unregister_client(CLIENT_ID));
  // Test it does not exist and can be registered again
  ASSERT_EQ(-ENOENT, update_client(CLIENT_ID, "foo"));
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));
}

TEST_F(TestJournaler, UnregisterClientDNE) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(-ENOENT, unregister_client(CLIENT_ID));
}

TEST_F(TestJournaler, AllocateTag) {
  ASSERT_EQ(0, create_journal(12, 8));

  cls::journal::Tag tag;

  bufferlist data;
  data.append(std::string(128, '1'));

  // allocate a new tag class
  C_SaferCond ctx1;
  m_journaler->allocate_tag(data, &tag, &ctx1);
  ASSERT_EQ(0, ctx1.wait());
  ASSERT_EQ(cls::journal::Tag(0, 0, data), tag);

  // re-use an existing tag class
  C_SaferCond ctx2;
  m_journaler->allocate_tag(tag.tag_class, bufferlist(), &tag, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(cls::journal::Tag(1, 0, bufferlist()), tag);
}

TEST_F(TestJournaler, GetTags) {
  ASSERT_EQ(0, create_journal(12, 8));
  ASSERT_EQ(0, register_client(CLIENT_ID, "foo"));

  std::list<cls::journal::Tag> expected_tags;
  for (size_t i = 0; i < 256; ++i) {
    C_SaferCond ctx;
    cls::journal::Tag tag;
    if (i < 2) {
      m_journaler->allocate_tag(bufferlist(), &tag, &ctx);
    } else {
      m_journaler->allocate_tag(i % 2, bufferlist(), &tag, &ctx);
    }
    ASSERT_EQ(0, ctx.wait());

    if (i % 2 == 0) {
      expected_tags.push_back(tag);
    }
  }

  std::list<cls::journal::Tag> tags;
  C_SaferCond ctx;
  m_journaler->get_tags(0, &tags, &ctx);
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(expected_tags, tags);
}
