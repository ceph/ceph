// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalTrimmer.h"
#include "journal/JournalMetadata.h"
#include "include/stringify.h"
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
    m_metadata_list.clear();

    for (std::list<journal::JournalTrimmer*>::iterator it = m_trimmers.begin();
         it != m_trimmers.end(); ++it) {
      delete *it;
    }
    RadosTestFixture::TearDown();
  }

  int append_payload(journal::JournalMetadataPtr metadata,
                     const std::string &oid, uint64_t object_num,
                     const std::string &payload, uint64_t *commit_tid) {
    int r = append(oid + "." + stringify(object_num), create_payload(payload));
    uint64_t tid = metadata->allocate_commit_tid(object_num, 234, 123);
    if (commit_tid != NULL) {
      *commit_tid = tid;
    }
    return r;
  }

  journal::JournalMetadataPtr create_metadata(const std::string &oid) {
    journal::JournalMetadataPtr metadata = RadosTestFixture::create_metadata(
      oid);
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

TEST_F(TestJournalTrimmer, Committed) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, metadata->set_active_set(10));
  ASSERT_TRUE(wait_for_update(metadata));

  uint64_t commit_tid1;
  uint64_t commit_tid2;
  uint64_t commit_tid3;
  uint64_t commit_tid4;
  uint64_t commit_tid5;
  uint64_t commit_tid6;
  ASSERT_EQ(0, append_payload(metadata, oid, 0, "payload", &commit_tid1));
  ASSERT_EQ(0, append_payload(metadata, oid, 4, "payload", &commit_tid2));
  ASSERT_EQ(0, append_payload(metadata, oid, 5, "payload", &commit_tid3));
  ASSERT_EQ(0, append_payload(metadata, oid, 0, "payload", &commit_tid4));
  ASSERT_EQ(0, append_payload(metadata, oid, 4, "payload", &commit_tid5));
  ASSERT_EQ(0, append_payload(metadata, oid, 5, "payload", &commit_tid6));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);

  trimmer->committed(commit_tid4);
  trimmer->committed(commit_tid6);
  trimmer->committed(commit_tid2);
  trimmer->committed(commit_tid5);
  trimmer->committed(commit_tid3);
  trimmer->committed(commit_tid1);
  while (metadata->get_minimum_set() != 2U) {
    ASSERT_TRUE(wait_for_update(metadata));
  }

  ASSERT_EQ(-ENOENT, assert_exists(oid + ".0"));
  ASSERT_EQ(-ENOENT, assert_exists(oid + ".2"));
  ASSERT_EQ(0, assert_exists(oid + ".5"));
}

TEST_F(TestJournalTrimmer, CommittedWithOtherClient) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_register(oid, "client2", "slow client"));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, metadata->set_active_set(10));
  ASSERT_TRUE(wait_for_update(metadata));

  uint64_t commit_tid1;
  uint64_t commit_tid2;
  uint64_t commit_tid3;
  uint64_t commit_tid4;
  ASSERT_EQ(0, append_payload(metadata, oid, 0, "payload", &commit_tid1));
  ASSERT_EQ(0, append_payload(metadata, oid, 2, "payload", &commit_tid2));
  ASSERT_EQ(0, append_payload(metadata, oid, 3, "payload", &commit_tid3));
  ASSERT_EQ(0, append_payload(metadata, oid, 5, "payload", &commit_tid4));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);

  trimmer->committed(commit_tid1);
  trimmer->committed(commit_tid2);
  trimmer->committed(commit_tid3);
  trimmer->committed(commit_tid4);
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

  ASSERT_EQ(0, metadata->set_active_set(10));
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0, append(oid + ".0", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".2", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".3", create_payload("payload")));
  ASSERT_EQ(0, append(oid + ".5", create_payload("payload")));

  journal::JournalTrimmer *trimmer = create_trimmer(oid, metadata);

  ASSERT_EQ(0, trimmer->remove_objects(false));
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
  ASSERT_EQ(-EBUSY, trimmer->remove_objects(false));
  ASSERT_EQ(0, trimmer->remove_objects(true));
}

