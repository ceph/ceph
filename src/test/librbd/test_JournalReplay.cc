// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"
#include "journal/Journaler.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/JournalTypes.h"

void register_test_journal_replay() {
}

class TestJournalReplay : public TestFixture {
public:

  struct Listener : public librbd::ImageWatcher::Listener{
    Mutex lock;
    Cond cond;

    Listener() : lock("TestJournalReplay::Listener::lock") {
    }
    virtual bool handle_requested_lock() {
      return true;
    }
    virtual void handle_releasing_lock() {
    }
    virtual void handle_lock_updated(
        librbd::ImageWatcher::LockUpdateState state) {
      Mutex::Locker locker(lock);
      cond.Signal();
    }
  };

  void wait_for_lock_owner(librbd::ImageCtx *ictx) {
    Listener listener;
    ictx->image_watcher->register_listener(&listener);
    {
      Mutex::Locker listener_locker(listener.lock);
      RWLock::RLocker owner_locker(ictx->owner_lock);
      while (!ictx->image_watcher->is_lock_owner()) {
        ictx->owner_lock.put_read();
        listener.cond.Wait(listener.lock);
        ictx->owner_lock.get_read();
      }
    }
    ictx->image_watcher->unregister_listener(&listener);
  }

  template<typename T>
  void inject_into_journal(librbd::ImageCtx *ictx, T event) {
    ictx->journal->open();
    ictx->journal->wait_for_journal_ready();

    librbd::journal::EventEntry event_entry(event);
    librbd::Journal::AioObjectRequests requests;
    {
      RWLock::RLocker owner_locker(ictx->owner_lock);
      ictx->journal->append_io_event(NULL, event_entry, requests, 0, 0, true);
    }
    ASSERT_EQ(0, ictx->journal->close());
  }

  void get_journal_commit_position(librbd::ImageCtx *ictx, int64_t *tid)
  {
    const std::string client_id = "";
    const std::string tag = "";
    std::string journal_id = ictx->id;

    C_SaferCond cond;
    uint64_t minimum_set;
    uint64_t active_set;
    std::set<cls::journal::Client> registered_clients;
    std::string oid = ::journal::Journaler::header_oid(journal_id);
    cls::journal::client::get_mutable_metadata(ictx->md_ctx, oid, &minimum_set,
	&active_set, &registered_clients, &cond);
    ASSERT_EQ(0, cond.wait());
    std::set<cls::journal::Client>::const_iterator c;
    for (c = registered_clients.begin(); c != registered_clients.end(); c++) {
      if (c->id == client_id) {
	break;
      }
    }
    if (c == registered_clients.end()) {
      *tid = -1;
      return;
    }
    cls::journal::EntryPositions entry_positions =
	c->commit_position.entry_positions;
    cls::journal::EntryPositions::const_iterator p;
    for (p = entry_positions.begin(); p != entry_positions.end(); p++) {
      if (p->tag == tag) {
	break;
      }
    }
    if (p == entry_positions.end()) {
      *tid = -1;
      return;
    }
    *tid = p->tid;
  }
};

TEST_F(TestJournalReplay, AioDiscardEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  // write to the image w/o using the journal
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->features &= ~RBD_FEATURE_JOURNALING;
  ASSERT_EQ(0, ictx->close_journal(true));

  std::string payload(4096, '1');
  librbd::AioCompletion *aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_write(aio_comp, 0, payload.size(), payload.c_str(),
                                  0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  std::string read_payload(4096, '\0');
  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                 &read_payload[0], NULL, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(payload, read_payload);
  close_image(ictx);

  // inject a discard operation into the journal
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->image_watcher->request_lock();
  }
  wait_for_lock_owner(ictx);
  inject_into_journal(ictx,
                      librbd::journal::AioDiscardEvent(0, payload.size()));

  // get current commit position
  int64_t initial;
  get_journal_commit_position(ictx, &initial);

  // re-open the journal so that it replays the new entry
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                 &read_payload[0], NULL, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(0, ictx->journal->close());
  ASSERT_EQ(std::string(read_payload.size(), '\0'), read_payload);

  // check the commit position is properly updated
  int64_t current;
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 1);

  // replay several envents and check the commit position
  inject_into_journal(ictx,
                      librbd::journal::AioDiscardEvent(0, payload.size()));
  inject_into_journal(ictx,
                      librbd::journal::AioDiscardEvent(0, payload.size()));
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();
  ASSERT_EQ(0, ictx->journal->close());
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 3);
}

TEST_F(TestJournalReplay, AioWriteEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  // inject a write operation into the journal
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->image_watcher->request_lock();
  }
  wait_for_lock_owner(ictx);

  std::string payload(4096, '1');
  bufferlist payload_bl;
  payload_bl.append(payload);
  inject_into_journal(ictx,
      librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));

  // get current commit position
  int64_t initial;
  get_journal_commit_position(ictx, &initial);

  // re-open the journal so that it replays the new entry
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  std::string read_payload(4096, '\0');
  librbd::AioCompletion *aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                 &read_payload[0], NULL, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(0, ictx->journal->close());
  ASSERT_EQ(payload, read_payload);

  // check the commit position is properly updated
  int64_t current;
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 1);

  // replay several events and check the commit position
  inject_into_journal(ictx,
      librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));
  inject_into_journal(ictx,
      librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();
  ASSERT_EQ(0, ictx->journal->close());
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 3);
}

TEST_F(TestJournalReplay, AioFlushEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  // inject a flush operation into the journal
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->image_watcher->request_lock();
  }
  wait_for_lock_owner(ictx);

  inject_into_journal(ictx, librbd::journal::AioFlushEvent());

  // start an AIO write op
  librbd::Journal *journal = ictx->journal;
  ictx->journal = NULL;

  std::string payload(m_image_size, '1');
  librbd::AioCompletion *aio_comp = new librbd::AioCompletion();
  {
    RWLock::RLocker owner_lock(ictx->owner_lock);
    librbd::AioImageRequest::aio_write(ictx, aio_comp, 0, payload.size(),
                                       payload.c_str(), 0);
  }
  ictx->journal = journal;

  // get current commit position
  int64_t initial;
  get_journal_commit_position(ictx, &initial);

  // re-open the journal so that it replays the new entry
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  ASSERT_TRUE(aio_comp->is_complete());
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  std::string read_payload(m_image_size, '\0');
  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                 &read_payload[0], NULL, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(0, ictx->journal->close());
  ASSERT_EQ(payload, read_payload);

  // check the commit position is properly updated
  int64_t current;
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 1);

  // replay several events and check the commit position
  inject_into_journal(ictx, librbd::journal::AioFlushEvent());
  inject_into_journal(ictx, librbd::journal::AioFlushEvent());
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();
  ASSERT_EQ(0, ictx->journal->close());
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 3);
}


TEST_F(TestJournalReplay, EntryPosition) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ictx->image_watcher->request_lock();
  }
  wait_for_lock_owner(ictx);

  // get current commit position
  int64_t initial;
  get_journal_commit_position(ictx, &initial);

  // write with journal
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  std::string payload(4096, '1');
  librbd::AioCompletion *aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_write(aio_comp, 0, payload.size(), payload.c_str(),
                                  0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  // Close the journal, check the commit position updated
  ASSERT_EQ(0, ictx->journal->close());
  int64_t current;
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 2);

  // Reopen the journal and write again
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_write(aio_comp, 0, payload.size(), payload.c_str(),
                                  0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_flush(aio_comp);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();

  // Close the journal, check the commit position updated
  ASSERT_EQ(0, ictx->journal->close());
  get_journal_commit_position(ictx, &current);
  ASSERT_EQ(current, initial + 4);
}
