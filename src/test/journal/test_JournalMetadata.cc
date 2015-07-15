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
  journal::JournalMetadataPtr create_metadata(const std::string &oid,
                                              const std::string &client_id) {
    journal::JournalMetadataPtr metadata(new journal::JournalMetadata(
      m_ioctx, oid, client_id, 0.1));
    metadata->add_listener(&m_listener);
    return metadata;
  }
};

TEST_F(TestJournalMetadata, JournalDNE) {
  std::string oid = get_temp_oid();

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(-ENOENT, init_metadata(metadata1));
}

TEST_F(TestJournalMetadata, ClientDNE) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata1));

  journal::JournalMetadataPtr metadata2 = create_metadata(oid, "client2");
  ASSERT_EQ(-ENOENT, init_metadata(metadata2));
}

TEST_F(TestJournalMetadata, SetCommitPositions) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata1));

  journal::JournalMetadataPtr metadata2 = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata2));
  ASSERT_TRUE(wait_for_update(metadata2));

  journal::JournalMetadata::ObjectSetPosition commit_position;
  journal::JournalMetadata::ObjectSetPosition read_commit_position;
  metadata1->get_commit_position(&read_commit_position);
  ASSERT_EQ(commit_position, read_commit_position);

  journal::JournalMetadata::EntryPositions entry_positions;
  entry_positions = boost::assign::list_of(
    cls::journal::EntryPosition("tag1", 122));
  commit_position = journal::JournalMetadata::ObjectSetPosition(1, entry_positions);

  C_SaferCond cond;
  metadata1->set_commit_position(commit_position, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_TRUE(wait_for_update(metadata2));

  metadata2->get_commit_position(&read_commit_position);
  ASSERT_EQ(commit_position, read_commit_position);
}

TEST_F(TestJournalMetadata, UpdateActiveObject) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata1));
  ASSERT_TRUE(wait_for_update(metadata1));

  ASSERT_EQ(0U, metadata1->get_active_set());

  metadata1->set_active_set(123);
  ASSERT_TRUE(wait_for_update(metadata1));

  ASSERT_EQ(123U, metadata1->get_active_set());
}
