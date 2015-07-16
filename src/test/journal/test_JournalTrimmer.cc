// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalTrimmer.h"
#include "journal/JournalMetadata.h"
#include "test/journal/RadosTestFixture.h"
#include <limits>
#include <list>

class TestJournalTrimmer : public RadosTestFixture {
public:

  virtual void TearDown() {
    for (MetadataList::iterator it = m_metadata_list.begin();
         it != m_metadata_list.end(); ++it) {
      (*it)->remove_listener(&m_listener);
    }
    for (std::list<journal::JournalTrimmer*>::iterator it = m_trimmers.begin();
         it != m_trimmers.end(); ++it) {
      delete *it;
    }
    RadosTestFixture::TearDown();
  }

  using RadosTestFixture::client_register;
  int client_register(const std::string &oid) {
    return RadosTestFixture::client_register(oid, "client", "");
  }

  journal::JournalMetadataPtr create_metadata(const std::string &oid) {
    journal::JournalMetadataPtr metadata(new journal::JournalMetadata(
      m_ioctx, oid, "client", 0.1));
    m_metadata_list.push_back(metadata);
    metadata->add_listener(&m_listener);
    return metadata;
  }

  journal::JournalTrimmer *create_trimmer(const std::string &oid,
                                            const journal::JournalMetadataPtr &metadata) {
    journal::JournalTrimmer *trimmer(new journal::JournalTrimmer(
      m_ioctx, oid + ".", metadata));
    m_trimmers.push_back(trimmer);
    return trimmer;
  }

  int assert_exists(const std::string &oid) {
    librados::ObjectWriteOperation op;
    op.assert_exists();
    return m_ioctx.operate(oid, &op);
  }

  typedef std::list<journal::JournalMetadataPtr> MetadataList;
  MetadataList m_metadata_list;
  std::list<journal::JournalTrimmer*> m_trimmers;
};

TEST_F(TestJournalTrimmer, UpdateCommitPosition) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  metadata->set_active_set(10);
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, append(oid + ".0", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".2", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".3", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".5", create_payload("payload")));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);

  cls::journal::EntryPositions entry_positions;
  cls::journal::ObjectSetPosition object_set_position(5, entry_positions);

  trimmer->update_commit_position(object_set_position);

  while (metadata->get_minimum_set() != 2U) {
    ASSERT_TRUE(wait_for_update(metadata));
  }

  ASSERT_EQ(-ENOENT, assert_exists(oid + ".0"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".2"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".3"));
  ASSERT_EQ(0, assert_exists(oid + ".5"));
}

TEST_F(TestJournalTrimmer, ConcurrentUpdateCommitPosition) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata1));
  ASSERT_TRUE(wait_for_update(metadata1));

  metadata1->set_active_set(10);
  ASSERT_TRUE(wait_for_update(metadata1));

  journal::JournalMetadataPtr metadata2 = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata2));

  ASSERT_EQ(0, append(oid + ".0", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".2", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".3", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".5", create_payload("payload")));

  journal::JournalTrimmer *trimmer1 = create_trimmer(oid, metadata1);
  journal::JournalTrimmer *trimmer2 = create_trimmer(oid, metadata2);

  cls::journal::EntryPositions entry_positions;
  cls::journal::ObjectSetPosition object_set_position1(2, entry_positions);
  trimmer1->update_commit_position(object_set_position1);

  cls::journal::ObjectSetPosition object_set_position2(5, entry_positions);
  trimmer2->update_commit_position(object_set_position2);

  while (metadata1->get_minimum_set() != 2U) {
    ASSERT_TRUE(wait_for_update(metadata1));
  }
  while (metadata2->get_minimum_set() != 2U) {
    ASSERT_TRUE(wait_for_update(metadata2));
  }

  ASSERT_EQ(-ENOENT, assert_exists(oid + ".0"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".2"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".3"));
  ASSERT_EQ(0, assert_exists(oid + ".5"));
}

TEST_F(TestJournalTrimmer, UpdateCommitPositionWithOtherClient) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_register(oid, "client2", "slow client"));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  metadata->set_active_set(10);
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, append(oid + ".0", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".2", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".3", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".5", create_payload("payload")));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);

  cls::journal::EntryPositions entry_positions;
  cls::journal::ObjectSetPosition object_set_position(5, entry_positions);

  trimmer->update_commit_position(object_set_position);
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, assert_exists(oid + ".0"));
  ASSERT_EQ(0, assert_exists(oid + ".2"));
  ASSERT_EQ(0, assert_exists(oid + ".3"));
  ASSERT_EQ(0, assert_exists(oid + ".5"));
}

TEST_F(TestJournalTrimmer, RemoveObjects) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  metadata->set_active_set(10);
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, append(oid + ".0", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".2", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".3", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".5", create_payload("payload")));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);

  ASSERT_EQ(0, trimmer->remove_objects());
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(-ENOENT, assert_exists(oid + ".0"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".2"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".3"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".5"));
}

TEST_F(TestJournalTrimmer, RemoveObjectsWithOtherClient) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_register(oid, "client2", "other client"));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);
  ASSERT_EQ(-EBUSY, trimmer->remove_objects());
}

