// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalRecorder.h"
#include "journal/Entry.h"
#include "journal/JournalMetadata.h"
#include "test/journal/RadosTestFixture.h"
#include <limits>
#include <list>
#include <memory>

class TestJournalRecorder : public RadosTestFixture {
public:
  using JournalRecorderPtr = std::unique_ptr<journal::JournalRecorder,
					     std::function<void(journal::JournalRecorder*)>>;
  JournalRecorderPtr create_recorder(
      const std::string &oid, const ceph::ref_t<journal::JournalMetadata>& metadata) {
    JournalRecorderPtr recorder{
      new journal::JournalRecorder(m_ioctx, oid + ".", metadata, 0),
      [](journal::JournalRecorder* recorder) {
	C_SaferCond cond;
	recorder->shut_down(&cond);
	cond.wait();
	delete recorder;
      }
    };
    recorder->set_append_batch_options(0, std::numeric_limits<uint32_t>::max(), 0);
    return recorder;
  }
};

TEST_F(TestJournalRecorder, Append) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  JournalRecorderPtr recorder = create_recorder(oid, metadata);

  journal::Future future1 = recorder->append(123, create_payload("payload"));

  C_SaferCond cond;
  future1.flush(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestJournalRecorder, AppendKnownOverflow) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_EQ(0U, metadata->get_active_set());

  JournalRecorderPtr recorder = create_recorder(oid, metadata);

  recorder->append(123, create_payload(std::string(metadata->get_object_size() -
                                                   journal::Entry::get_fixed_size(), '1')));
  journal::Future future2 = recorder->append(123, create_payload(std::string(1, '2')));

  C_SaferCond cond;
  future2.flush(&cond);
  ASSERT_EQ(0, cond.wait());

  ASSERT_EQ(1U, metadata->get_active_set());
}

TEST_F(TestJournalRecorder, AppendDelayedOverflow) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_EQ(0U, metadata->get_active_set());

  JournalRecorderPtr recorder1 = create_recorder(oid, metadata);
  JournalRecorderPtr recorder2 = create_recorder(oid, metadata);

  recorder1->append(234, create_payload(std::string(1, '1')));
  recorder2->append(123, create_payload(std::string(metadata->get_object_size() -
                                                    journal::Entry::get_fixed_size(), '2')));

  journal::Future future = recorder2->append(123, create_payload(std::string(1, '3')));

  C_SaferCond cond;
  future.flush(&cond);
  ASSERT_EQ(0, cond.wait());

  ASSERT_EQ(1U, metadata->get_active_set());
}

TEST_F(TestJournalRecorder, FutureFlush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  JournalRecorderPtr recorder = create_recorder(oid, metadata);

  journal::Future future1 = recorder->append(123, create_payload("payload1"));
  journal::Future future2 = recorder->append(123, create_payload("payload2"));

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

  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  JournalRecorderPtr recorder = create_recorder(oid, metadata);

  journal::Future future1 = recorder->append(123, create_payload("payload1"));
  journal::Future future2 = recorder->append(123, create_payload("payload2"));

  C_SaferCond cond1;
  recorder->flush(&cond1);
  ASSERT_EQ(0, cond1.wait());

  C_SaferCond cond2;
  future2.wait(&cond2);
  ASSERT_EQ(0, cond2.wait());
  ASSERT_TRUE(future1.is_complete());
  ASSERT_TRUE(future2.is_complete());
}

TEST_F(TestJournalRecorder, OverflowCommitObjectNumber) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid, 12, 2));
  ASSERT_EQ(0, client_register(oid));

  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  ASSERT_EQ(0U, metadata->get_active_set());

  JournalRecorderPtr recorder = create_recorder(oid, metadata);

  recorder->append(123, create_payload(std::string(metadata->get_object_size() -
                                                   journal::Entry::get_fixed_size(), '1')));
  journal::Future future2 = recorder->append(124, create_payload(std::string(1, '2')));

  C_SaferCond cond;
  future2.flush(&cond);
  ASSERT_EQ(0, cond.wait());

  ASSERT_EQ(1U, metadata->get_active_set());

  uint64_t object_num;
  uint64_t tag_tid;
  uint64_t entry_tid;
  metadata->get_commit_entry(1, &object_num, &tag_tid, &entry_tid);
  ASSERT_EQ(0U, object_num);
  ASSERT_EQ(123U, tag_tid);
  ASSERT_EQ(0U, entry_tid);

  metadata->get_commit_entry(2, &object_num, &tag_tid, &entry_tid);
  ASSERT_EQ(2U, object_num);
  ASSERT_EQ(124U, tag_tid);
  ASSERT_EQ(0U, entry_tid);
}

