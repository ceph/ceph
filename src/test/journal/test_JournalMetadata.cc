// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalMetadata.h"
#include "test/journal/RadosTestFixture.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include <map>

class TestJournalMetadata : public RadosTestFixture {
public:
  virtual void TearDown() {
    for (MetadataList::iterator it = m_metadata_list.begin();
         it != m_metadata_list.end(); ++it) {
      (*it)->remove_listener(&m_listener);
    }
    m_metadata_list.clear();

    RadosTestFixture::TearDown();
  }

  journal::JournalMetadataPtr create_metadata(const std::string &oid,
                                              const std::string &client_id,
                                              double commit_interval = 0.1,
					      uint64_t max_fetch_bytes = 0,
                                              int max_concurrent_object_sets = 0) {
    journal::JournalMetadataPtr metadata = RadosTestFixture::create_metadata(
      oid, client_id, commit_interval, max_fetch_bytes,
      max_concurrent_object_sets);
    m_metadata_list.push_back(metadata);
    metadata->add_listener(&m_listener);
    return metadata;
  }

  typedef std::list<journal::JournalMetadataPtr> MetadataList;
  MetadataList m_metadata_list;
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

TEST_F(TestJournalMetadata, Committed) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1", 600);
  ASSERT_EQ(0, init_metadata(metadata1));

  journal::JournalMetadataPtr metadata2 = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata2));
  ASSERT_TRUE(wait_for_update(metadata2));

  journal::JournalMetadata::ObjectSetPosition expect_commit_position;
  journal::JournalMetadata::ObjectSetPosition read_commit_position;
  metadata1->get_commit_position(&read_commit_position);
  ASSERT_EQ(expect_commit_position, read_commit_position);

  uint64_t commit_tid1 = metadata1->allocate_commit_tid(0, 0, 0);
  uint64_t commit_tid2 = metadata1->allocate_commit_tid(0, 1, 0);
  uint64_t commit_tid3 = metadata1->allocate_commit_tid(1, 0, 1);
  uint64_t commit_tid4 = metadata1->allocate_commit_tid(0, 0, 2);

  // cannot commit until tid1 + 2 committed
  metadata1->committed(commit_tid2, []() { return nullptr; });
  metadata1->committed(commit_tid3, []() { return nullptr; });

  C_SaferCond cond1;
  metadata1->committed(commit_tid1, [&cond1]() { return &cond1; });

  // given our 10 minute commit internal, this should override the
  // in-flight commit
  C_SaferCond cond2;
  metadata1->committed(commit_tid4, [&cond2]() { return &cond2; });

  ASSERT_EQ(-ESTALE, cond1.wait());
  metadata1->flush_commit_position();
  ASSERT_EQ(0, cond2.wait());

  ASSERT_TRUE(wait_for_update(metadata2));
  metadata2->get_commit_position(&read_commit_position);
  expect_commit_position = {{{0, 0, 2}, {1, 0, 1}}};
  ASSERT_EQ(expect_commit_position, read_commit_position);
}

TEST_F(TestJournalMetadata, UpdateActiveObject) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid, 14, 2));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata1 = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata1));
  ASSERT_TRUE(wait_for_update(metadata1));

  ASSERT_EQ(0U, metadata1->get_active_set());

  ASSERT_EQ(0, metadata1->set_active_set(123));
  ASSERT_TRUE(wait_for_update(metadata1));

  ASSERT_EQ(123U, metadata1->get_active_set());
}

TEST_F(TestJournalMetadata, DisconnectLaggyClient) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid, "client1", ""));
  ASSERT_EQ(0, client_register(oid, "client2", "laggy"));

  int max_concurrent_object_sets = 100;
  journal::JournalMetadataPtr metadata =
    create_metadata(oid, "client1", 0.1, 0, max_concurrent_object_sets);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  ASSERT_EQ(0U, metadata->get_active_set());

  journal::JournalMetadata::RegisteredClients clients;

#define ASSERT_CLIENT_STATES(s1, s2)	\
  ASSERT_EQ(2U, clients.size());	\
  for (auto &c : clients) {		\
    if (c.id == "client1") {		\
      ASSERT_EQ(c.state, s1);		\
    } else if (c.id == "client2") {	\
      ASSERT_EQ(c.state, s2);		\
    } else {				\
      ASSERT_TRUE(false);		\
    }					\
  }

  metadata->get_registered_clients(&clients);
  ASSERT_CLIENT_STATES(cls::journal::CLIENT_STATE_CONNECTED,
		       cls::journal::CLIENT_STATE_CONNECTED);

  // client2 is connected when active set <= max_concurrent_object_sets
  ASSERT_EQ(0, metadata->set_active_set(max_concurrent_object_sets));
  ASSERT_TRUE(wait_for_update(metadata));
  uint64_t commit_tid = metadata->allocate_commit_tid(0, 0, 0);
  C_SaferCond cond1;
  metadata->committed(commit_tid, [&cond1]() { return &cond1; });
  ASSERT_EQ(0, cond1.wait());
  metadata->flush_commit_position();
  ASSERT_TRUE(wait_for_update(metadata));
  ASSERT_EQ(100U, metadata->get_active_set());
  clients.clear();
  metadata->get_registered_clients(&clients);
  ASSERT_CLIENT_STATES(cls::journal::CLIENT_STATE_CONNECTED,
		       cls::journal::CLIENT_STATE_CONNECTED);

  // client2 is disconnected when active set > max_concurrent_object_sets
  ASSERT_EQ(0, metadata->set_active_set(max_concurrent_object_sets + 1));
  ASSERT_TRUE(wait_for_update(metadata));
  commit_tid = metadata->allocate_commit_tid(0, 0, 1);
  C_SaferCond cond2;
  metadata->committed(commit_tid, [&cond2]() { return &cond2; });
  ASSERT_EQ(0, cond2.wait());
  metadata->flush_commit_position();
  ASSERT_TRUE(wait_for_update(metadata));
  ASSERT_EQ(101U, metadata->get_active_set());
  clients.clear();
  metadata->get_registered_clients(&clients);
  ASSERT_CLIENT_STATES(cls::journal::CLIENT_STATE_CONNECTED,
		       cls::journal::CLIENT_STATE_DISCONNECTED);
}

TEST_F(TestJournalMetadata, AssertActiveTag) {
  std::string oid = get_temp_oid();

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid, "client1", ""));

  journal::JournalMetadataPtr metadata = create_metadata(oid, "client1");
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_TRUE(wait_for_update(metadata));

  C_SaferCond ctx1;
  cls::journal::Tag tag1;
  metadata->allocate_tag(cls::journal::Tag::TAG_CLASS_NEW, {}, &tag1, &ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  metadata->assert_active_tag(tag1.tid, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  C_SaferCond ctx3;
  cls::journal::Tag tag2;
  metadata->allocate_tag(tag1.tag_class, {}, &tag2, &ctx3);
  ASSERT_EQ(0, ctx3.wait());

  C_SaferCond ctx4;
  metadata->assert_active_tag(tag1.tid, &ctx4);
  ASSERT_EQ(-ESTALE, ctx4.wait());
}
