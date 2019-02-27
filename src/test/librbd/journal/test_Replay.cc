// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"
#include "journal/Journaler.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ReadResult.h"
#include "librbd/journal/Types.h"

void register_test_journal_replay() {
}

class TestJournalReplay : public TestFixture {
public:

  int when_acquired_lock(librbd::ImageCtx *ictx) {
    C_SaferCond lock_ctx;
    {
      RWLock::WLocker owner_locker(ictx->owner_lock);
      ictx->exclusive_lock->acquire_lock(&lock_ctx);
    }
    int r = lock_ctx.wait();
    if (r < 0) {
      return r;
    }

    C_SaferCond refresh_ctx;
    ictx->state->refresh(&refresh_ctx);
    return refresh_ctx.wait();
  }

  template<typename T>
  void inject_into_journal(librbd::ImageCtx *ictx, T event) {
    C_SaferCond ctx;
    librbd::journal::EventEntry event_entry(event);
    {
      RWLock::RLocker owner_locker(ictx->owner_lock);
      uint64_t tid = ictx->journal->append_io_event(std::move(event_entry),0, 0,
                                                    true, 0);
      ictx->journal->wait_event(tid, &ctx);
    }
    ASSERT_EQ(0, ctx.wait());
  }

  void get_journal_commit_position(librbd::ImageCtx *ictx, int64_t *tag,
                                   int64_t *entry)
  {
    const std::string client_id = "";
    std::string journal_id = ictx->id;

    C_SaferCond close_cond;
    ictx->journal->close(&close_cond);
    ASSERT_EQ(0, close_cond.wait());
    delete ictx->journal;
    ictx->journal = nullptr;

    C_SaferCond cond;
    uint64_t minimum_set;
    uint64_t active_set;
    std::set<cls::journal::Client> registered_clients;
    std::string oid = ::journal::Journaler::header_oid(journal_id);
    cls::journal::client::get_mutable_metadata(ictx->md_ctx, oid, &minimum_set,
	&active_set, &registered_clients, &cond);
    ASSERT_EQ(0, cond.wait());
    std::set<cls::journal::Client>::const_iterator c;
    for (c = registered_clients.begin(); c != registered_clients.end(); ++c) {
      if (c->id == client_id) {
	break;
      }
    }
    if (c == registered_clients.end() ||
        c->commit_position.object_positions.empty()) {
      *tag = 0;
      *entry = -1;
    } else {
      const cls::journal::ObjectPosition &object_position =
        *c->commit_position.object_positions.begin();
      *tag = object_position.tag_tid;
      *entry = object_position.entry_tid;
    }

    C_SaferCond open_cond;
    ictx->journal = new librbd::Journal<>(*ictx);
    ictx->journal->open(&open_cond);
    ASSERT_EQ(0, open_cond.wait());
  }
};

TEST_F(TestJournalReplay, AioDiscardEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  // write to the image w/o using the journal
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->features &= ~RBD_FEATURE_JOURNALING;

  std::string payload(4096, '1');
  bufferlist payload_bl;
  payload_bl.append(payload);
  auto aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_write(aio_comp, 0, payload.size(),
                                 std::move(payload_bl), 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  std::string read_payload(4096, '\0');
  librbd::io::ReadResult read_result{&read_payload[0], read_payload.size()};
  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                librbd::io::ReadResult{read_result}, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(payload, read_payload);
  close_image(ictx);

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject a discard operation into the journal
  inject_into_journal(ictx,
                      librbd::journal::AioDiscardEvent(
                        0, payload.size(), ictx->discard_granularity_bytes));
  close_image(ictx);

  // re-open the journal so that it replays the new entry
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                librbd::io::ReadResult{read_result}, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  if (ictx->discard_granularity_bytes > 0) {
    ASSERT_EQ(payload, read_payload);
  } else {
    ASSERT_EQ(std::string(read_payload.size(), '\0'), read_payload);
  }

  // check the commit position is properly updated
  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(0, current_entry);

  // replay several envents and check the commit position
  inject_into_journal(ictx,
                      librbd::journal::AioDiscardEvent(
                        0, payload.size(), ictx->discard_granularity_bytes));
  inject_into_journal(ictx,
                      librbd::journal::AioDiscardEvent(
                        0, payload.size(), ictx->discard_granularity_bytes));
  close_image(ictx);

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 2, current_tag);
  ASSERT_EQ(1, current_entry);

  // verify lock ordering constraints
  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_discard(aio_comp, 0, read_payload.size(),
                                   ictx->discard_granularity_bytes);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
}

TEST_F(TestJournalReplay, AioWriteEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject a write operation into the journal
  std::string payload(4096, '1');
  bufferlist payload_bl;
  payload_bl.append(payload);
  inject_into_journal(ictx,
      librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));
  close_image(ictx);

  // re-open the journal so that it replays the new entry
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  std::string read_payload(4096, '\0');
  librbd::io::ReadResult read_result{&read_payload[0], read_payload.size()};
  auto aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                std::move(read_result), 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(payload, read_payload);

  // check the commit position is properly updated
  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(0, current_entry);

  // replay several events and check the commit position
  inject_into_journal(ictx,
      librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));
  inject_into_journal(ictx,
      librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));
  close_image(ictx);

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 2, current_tag);
  ASSERT_EQ(1, current_entry);

  // verify lock ordering constraints
  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_write(aio_comp, 0, payload.size(),
                                 bufferlist{payload_bl}, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
}

TEST_F(TestJournalReplay, AioFlushEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject a flush operation into the journal
  inject_into_journal(ictx, librbd::journal::AioFlushEvent());
  close_image(ictx);

  // re-open the journal so that it replays the new entry
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // check the commit position is properly updated
  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(0, current_entry);

  // replay several events and check the commit position
  inject_into_journal(ictx, librbd::journal::AioFlushEvent());
  inject_into_journal(ictx, librbd::journal::AioFlushEvent());
  close_image(ictx);

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 2, current_tag);
  ASSERT_EQ(1, current_entry);

  // verify lock ordering constraints
  auto aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
}

TEST_F(TestJournalReplay, SnapCreate) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx, librbd::journal::SnapCreateEvent(1, cls::rbd::UserSnapshotNamespace(),
							       "snap"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(1, current_entry);

  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    ASSERT_NE(CEPH_NOSNAP, ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(),
					     "snap"));
  }

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap2"));
}

TEST_F(TestJournalReplay, SnapProtect) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx,
		      librbd::journal::SnapProtectEvent(1,
							cls::rbd::UserSnapshotNamespace(),
							"snap"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag, current_tag);
  ASSERT_EQ(initial_entry + 2, current_entry);

  bool is_protected;
  ASSERT_EQ(0, librbd::snap_is_protected(ictx, "snap", &is_protected));
  ASSERT_TRUE(is_protected);

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap2"));
  ASSERT_EQ(0, ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					      "snap2"));
}

TEST_F(TestJournalReplay, SnapUnprotect) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));
  uint64_t snap_id;
  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), "snap");
    ASSERT_NE(CEPH_NOSNAP, snap_id);
  }
  ASSERT_EQ(0, ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					      "snap"));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx,
	librbd::journal::SnapUnprotectEvent(1,
					    cls::rbd::UserSnapshotNamespace(),
					    "snap"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag, current_tag);
  ASSERT_EQ(initial_entry + 2, current_entry);

  bool is_protected;
  ASSERT_EQ(0, librbd::snap_is_protected(ictx, "snap", &is_protected));
  ASSERT_FALSE(is_protected);

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap2"));
  ASSERT_EQ(0, ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					      "snap2"));
  ASSERT_EQ(0, ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(),
						"snap2"));
}

TEST_F(TestJournalReplay, SnapRename) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));
  uint64_t snap_id;
  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), "snap");
    ASSERT_NE(CEPH_NOSNAP, snap_id);
  }

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx, librbd::journal::SnapRenameEvent(1, snap_id, "snap",
                                                             "snap2"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag, current_tag);
  ASSERT_EQ(initial_entry + 2, current_entry);
  ASSERT_EQ(0, ictx->state->refresh());

  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), "snap2");
    ASSERT_NE(CEPH_NOSNAP, snap_id);
  }

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->snap_rename("snap2", "snap3"));
}

TEST_F(TestJournalReplay, SnapRollback) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx,
	  librbd::journal::SnapRollbackEvent(1,
					     cls::rbd::UserSnapshotNamespace(),
					     "snap"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag, current_tag);
  ASSERT_EQ(initial_entry + 2, current_entry);

  // verify lock ordering constraints
  librbd::NoOpProgressContext no_op_progress;
  ASSERT_EQ(0, ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(),
					       "snap",
					       no_op_progress));
}

TEST_F(TestJournalReplay, SnapRemove) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx,
	  librbd::journal::SnapRemoveEvent(1,
					   cls::rbd::UserSnapshotNamespace(),
					   "snap"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag, current_tag);
  ASSERT_EQ(initial_entry + 2, current_entry);

  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    uint64_t snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(),
					 "snap");
    ASSERT_EQ(CEPH_NOSNAP, snap_id);
  }

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));
  ASSERT_EQ(0, ictx->operations->snap_remove(cls::rbd::UserSnapshotNamespace(),
					     "snap"));
}

TEST_F(TestJournalReplay, Rename) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  std::string new_image_name(get_temp_image_name());
  inject_into_journal(ictx, librbd::journal::RenameEvent(1, new_image_name));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(1, current_entry);

  // verify lock ordering constraints
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.rename(m_ioctx, new_image_name.c_str(), m_image_name.c_str()));
}

TEST_F(TestJournalReplay, Resize) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx, librbd::journal::ResizeEvent(1, 16));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(1, current_entry);

  // verify lock ordering constraints
  librbd::NoOpProgressContext no_op_progress;
  ASSERT_EQ(0, ictx->operations->resize(0, true, no_op_progress));
}

TEST_F(TestJournalReplay, Flatten) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					     "snap"));
  ASSERT_EQ(0, ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					      "snap"));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap", m_ioctx,
			     clone_name.c_str(), ictx->features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, when_acquired_lock(ictx2));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx2, &initial_tag, &initial_entry);

  // inject snapshot ops into journal
  inject_into_journal(ictx2, librbd::journal::FlattenEvent(1));
  inject_into_journal(ictx2, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx2);

  // replay journal
  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, when_acquired_lock(ictx2));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx2, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(1, current_entry);
  ASSERT_EQ(0, ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(),
						"snap"));

  // verify lock ordering constraints
  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EINVAL, ictx2->operations->flatten(no_op));
}

TEST_F(TestJournalReplay, UpdateFeatures) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  uint64_t features = RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF;
  bool enabled = !ictx->test_features(features);

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject update_features op into journal
  inject_into_journal(ictx, librbd::journal::UpdateFeaturesEvent(1, features,
                                                                 enabled));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(0, current_entry);

  ASSERT_EQ(enabled, ictx->test_features(features));

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->update_features(features, !enabled));
}

TEST_F(TestJournalReplay, MetadataSet) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject metadata_set op into journal
  inject_into_journal(ictx, librbd::journal::MetadataSetEvent(
    1, "conf_rbd_mirroring_replay_delay", "9876"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(1, current_entry);

  ASSERT_EQ(9876U, ictx->mirroring_replay_delay);

  std::string value;
  ASSERT_EQ(0, librbd::metadata_get(ictx, "conf_rbd_mirroring_replay_delay",
                                    &value));
  ASSERT_EQ("9876", value);

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->metadata_set("key2", "value"));
}

TEST_F(TestJournalReplay, MetadataRemove) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  ASSERT_EQ(0, ictx->operations->metadata_set(
    "conf_rbd_mirroring_replay_delay", "9876"));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  // inject metadata_remove op into journal
  inject_into_journal(ictx, librbd::journal::MetadataRemoveEvent(
    1, "conf_rbd_mirroring_replay_delay"));
  inject_into_journal(ictx, librbd::journal::OpFinishEvent(1, 0));
  close_image(ictx);

  // replay journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag, current_tag);
  ASSERT_EQ(initial_entry + 2, current_entry);
  ASSERT_EQ(0U, ictx->mirroring_replay_delay);

  std::string value;
  ASSERT_EQ(-ENOENT,
            librbd::metadata_get(ictx, "conf_rbd_mirroring_replay_delay",
                                 &value));

  // verify lock ordering constraints
  ASSERT_EQ(0, ictx->operations->metadata_set("key", "value"));
  ASSERT_EQ(0, ictx->operations->metadata_remove("key"));
}

TEST_F(TestJournalReplay, ObjectPosition) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, when_acquired_lock(ictx));

  // get current commit position
  int64_t initial_tag;
  int64_t initial_entry;
  get_journal_commit_position(ictx, &initial_tag, &initial_entry);

  std::string payload(4096, '1');
  bufferlist payload_bl;
  payload_bl.append(payload);
  auto aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_write(aio_comp, 0, payload.size(),
                                 bufferlist{payload_bl}, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  // check the commit position updated
  int64_t current_tag;
  int64_t current_entry;
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(1, current_entry);

  // write again

  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_write(aio_comp, 0, payload.size(),
                                 bufferlist{payload_bl}, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  aio_comp = new librbd::io::AioCompletion();
  ictx->io_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  // user flush requests are ignored when journaling + cache are enabled
  C_SaferCond flush_ctx;
  aio_comp = librbd::io::AioCompletion::create(
    &flush_ctx, ictx, librbd::io::AIO_TYPE_FLUSH);
  auto req = librbd::io::ImageDispatchSpec<>::create_flush_request(
    *ictx, aio_comp, librbd::io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
  delete req;
  ASSERT_EQ(0, flush_ctx.wait());

  // check the commit position updated
  get_journal_commit_position(ictx, &current_tag, &current_entry);
  ASSERT_EQ(initial_tag + 1, current_tag);
  ASSERT_EQ(3, current_entry);
}
