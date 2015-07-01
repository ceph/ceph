// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalRecorder.h"
#include "journal/JournalMetadata.h"
#include "test/journal/RadosTestFixture.h"
#include <limits>
#include <list>

class TestJournalRecorder : public RadosTestFixture {
public:

  virtual void TearDown() {
    for (std::list<journal::JournalRecorder*>::iterator it = m_recorders.begin();
         it != m_recorders.end(); ++it) {
      delete *it;
    }
    RadosTestFixture::TearDown();
  }

  int client_register(const std::string &oid) {
    return RadosTestFixture::client_register(oid, "client", "");
  }

  journal::JournalMetadataPtr create_metadata(const std::string &oid) {
    journal::JournalMetadataPtr metadata(new journal::JournalMetadata(
      m_ioctx, oid, "client", 0.1));
    return metadata;
  }

  journal::JournalRecorder *create_recorder(const std::string &oid,
                                            const journal::JournalMetadataPtr &metadata) {
    journal::JournalRecorder *recorder(new journal::JournalRecorder(
      m_ioctx, oid + ".", metadata, 0, std::numeric_limits<uint32_t>::max(), 0));
    m_recorders.push_back(recorder);
    return recorder;
  }

  std::list<journal::JournalRecorder*> m_recorders;

};

TEST_F(TestJournalRecorder, Append) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, metadata->init());

  journal::JournalRecorder *recorder = create_recorder(oid, metadata);

  journal::Future future1 = recorder->append("tag1", create_payload("payload"));

  C_SaferCond cond;
  future1.flush(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestJournalRecorder, AppendKnownOverflow) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, metadata->init());
  ASSERT_EQ(0U, metadata->get_active_set());

  journal::JournalRecorder *recorder = create_recorder(oid, metadata);

  recorder->append("tag1", create_payload(std::string(1 << 12, '1')));
  journal::Future future2 = recorder->append("tag1", create_payload(std::string(1, '2')));

  C_SaferCond cond;
  future2.flush(&cond);
  ASSERT_EQ(0, cond.wait());

  ASSERT_EQ(1U, metadata->get_active_set());
}

TEST_F(TestJournalRecorder, AppendDelayedOverflow) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, metadata->init());
  ASSERT_EQ(0U, metadata->get_active_set());

  journal::JournalRecorder *recorder1 = create_recorder(oid, metadata);
  journal::JournalRecorder *recorder2 = create_recorder(oid, metadata);

  recorder1->append("tag1", create_payload(std::string(1, '1')));
  recorder2->append("tag2", create_payload(std::string(1 << 12, '2')));

  journal::Future future = recorder2->append("tag1", create_payload(std::string(1, '3')));

  C_SaferCond cond;
  future.flush(&cond);
  ASSERT_EQ(0, cond.wait());

  ASSERT_EQ(1U, metadata->get_active_set());
}

TEST_F(TestJournalRecorder, FutureFlush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, metadata->init());

  journal::JournalRecorder *recorder = create_recorder(oid, metadata);

  journal::Future future1 = recorder->append("tag1", create_payload("payload1"));
  journal::Future future2 = recorder->append("tag1", create_payload("payload2"));

  C_SaferCond cond;
  future2.flush(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_TRUE(future1.is_complete());
  ASSERT_TRUE(future2.is_complete());
}

TEST_F(TestJournalRecorder, Flush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, metadata->init());

  journal::JournalRecorder *recorder = create_recorder(oid, metadata);

  journal::Future future1 = recorder->append("tag1", create_payload("payload1"));
  journal::Future future2 = recorder->append("tag1", create_payload("payload2"));

  recorder->flush();

  C_SaferCond cond;
  future2.wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_TRUE(future1.is_complete());
  ASSERT_TRUE(future2.is_complete());
}

