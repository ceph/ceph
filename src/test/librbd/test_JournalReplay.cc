// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
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
      RWLock::RLocker owner_locker(ictx->owner_lock);
      while (!ictx->image_watcher->is_lock_owner()) {
        ictx->owner_lock.put_read();
        listener.lock.Lock();
        listener.cond.Wait(listener.lock);
        listener.lock.Unlock();
        ictx->owner_lock.get_read();
      }
    }
    ictx->image_watcher->unregister_listener(&listener);
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

  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  librbd::journal::EventEntry event_entry(
    librbd::journal::AioDiscardEvent(0, payload.size()));
  librbd::Journal::AioObjectRequests requests;
  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    ictx->journal->append_io_event(NULL, event_entry, requests, 0, 0, true);
  }
  ASSERT_EQ(0, ictx->journal->close());

  // re-open the journal so that it replays the new entry
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                 &read_payload[0], NULL, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(std::string(read_payload.size(), '\0'), read_payload);
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

  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  std::string payload(4096, '1');
  bufferlist payload_bl;
  payload_bl.append(payload);
  librbd::journal::EventEntry event_entry(
    librbd::journal::AioWriteEvent(0, payload.size(), payload_bl));
  librbd::Journal::AioObjectRequests requests;
  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    ictx->journal->append_io_event(NULL, event_entry, requests, 0, 0, true);
  }
  ASSERT_EQ(0, ictx->journal->close());

  // re-open the journal so that it replays the new entry
  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  std::string read_payload(4096, '\0');
  librbd::AioCompletion *aio_comp = new librbd::AioCompletion();
  ictx->aio_work_queue->aio_read(aio_comp, 0, read_payload.size(),
                                 &read_payload[0], NULL, 0);
  ASSERT_EQ(0, aio_comp->wait_for_complete());
  aio_comp->release();
  ASSERT_EQ(payload, read_payload);
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

  ictx->journal->open();
  ictx->journal->wait_for_journal_ready();

  librbd::journal::AioFlushEvent aio_flush_event;
  librbd::journal::EventEntry event_entry(aio_flush_event);
  librbd::Journal::AioObjectRequests requests;
  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    ictx->journal->append_io_event(NULL, event_entry, requests, 0, 0, true);
  }
  ASSERT_EQ(0, ictx->journal->close());

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
  ASSERT_EQ(payload, read_payload);
}

