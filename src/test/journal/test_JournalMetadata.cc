// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalMetadata.h"
#include "test/journal/RadosTestFixture.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include <boost/assign/list_of.hpp>
#include <map>

class TestJournalMetadata : public RadosTestFixture {
public:
  TestJournalMetadata() : m_listener(this) {}

  struct Listener : public journal::JournalMetadata::Listener {
    TestJournalMetadata *test_fixture;
    Mutex mutex;
    Cond cond;
    std::map<journal::JournalMetadata*, uint32_t> updates;

    Listener(TestJournalMetadata *_test_fixture)
      : test_fixture(_test_fixture), mutex("mutex") {}

    virtual void handle_update(journal::JournalMetadata *metadata) {
      Mutex::Locker locker(mutex);
      ++updates[metadata];
      cond.Signal();
    }
  };

  journal::JournalMetadataPtr create_metadata(const std::string &oid,
                                              const std::string &client_id) {
    journal::JournalMetadataPtr metadata(new journal::JournalMetadata(
      m_ioctx, oid, client_id));
    metadata->add_listener(&m_listener);
    return metadata;
  }

  bool wait_for_update(journal::JournalMetadataPtr metadata) {
    Mutex::Locker locker(m_listener.mutex);
    while (m_listener.updates[metadata.get()] == 0) {
      if (m_listener.cond.WaitInterval(
            reinterpret_cast<CephContext*>(m_ioctx.cct()),
            m_listener.mutex, utime_t(10, 0)) != 0) {
        return false;
      }
    }
    --m_listener.updates[metadata.get()];
    return true;
  }

  Listener m_listener;
};

TEST_F(TestJournalMetadata, JournalDNE) {
  std::string oid = get_temp_oid();

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(-ENOENT, metadata1->init());
}

TEST_F(TestJournalMetadata, ClientDNE) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, metadata1->init());

  journal::JournalMetadataPtr metadata2 = create_metadata(oid, "client2");
  ASSERT_EQ(-ENOENT, metadata2->init());
}

TEST_F(TestJournalMetadata, SetCommitPositions) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, metadata1->init());

  journal::JournalMetadataPtr metadata2 = create_metadata(oid, "client1");
  ASSERT_EQ(0, metadata2->init());
  ASSERT_TRUE(wait_for_update(metadata2));

  journal::JournalMetadata::ObjectSetPosition commit_position;
  journal::JournalMetadata::ObjectSetPosition read_commit_position;
  metadata1->get_commit_position(&read_commit_position);
  ASSERT_EQ(commit_position, read_commit_position);

  journal::JournalMetadata::EntryPositions entry_positions;
  entry_positions = boost::assign::list_of(
    cls::journal::EntryPosition("tag1", 122));
  commit_position = journal::JournalMetadata::ObjectSetPosition(1, entry_positions);

  metadata1->set_commit_position(commit_position);
  ASSERT_TRUE(wait_for_update(metadata2));

  metadata2->get_commit_position(&read_commit_position);
  ASSERT_EQ(commit_position, read_commit_position);
}

TEST_F(TestJournalMetadata, UpdateActiveObject) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, metadata1->init());
  ASSERT_TRUE(wait_for_update(metadata1));

  ASSERT_EQ(0U, metadata1->get_active_set());

  metadata1->set_active_set(123);
  ASSERT_TRUE(wait_for_update(metadata1));

  ASSERT_EQ(123U, metadata1->get_active_set());
}
