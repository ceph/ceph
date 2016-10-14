// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectRecorder.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "test/journal/RadosTestFixture.h"
#include <limits>

using std::shared_ptr;

class TestObjectRecorder : public RadosTestFixture {
public:
  TestObjectRecorder()
    : m_flush_interval(std::numeric_limits<uint32_t>::max()),
      m_flush_bytes(std::numeric_limits<uint64_t>::max()),
      m_flush_age(600)
  {
  }

  struct Handler : public journal::ObjectRecorder::Handler {
    Mutex lock;
    shared_ptr<Mutex> object_lock;
    Cond cond;
    bool is_closed = false;
    uint32_t overflows = 0;

    Handler() : lock("lock") {
    }

    virtual void closed(journal::ObjectRecorder *object_recorder) {
      Mutex::Locker locker(lock);
      is_closed = true;
      cond.Signal();
    }
    virtual void overflow(journal::ObjectRecorder *object_recorder) {
      Mutex::Locker locker(lock);
      journal::AppendBuffers append_buffers;
      object_lock->Lock();
      object_recorder->claim_append_buffers(&append_buffers);
      object_lock->Unlock();

      ++overflows;
      cond.Signal();
    }
  };

  typedef std::list<journal::ObjectRecorderPtr> ObjectRecorders;
  typedef std::map<std::string, shared_ptr<Mutex>> ObjectRecorderLocksMap;

  ObjectRecorders m_object_recorders;
  ObjectRecorderLocksMap m_object_recorder_locks;

  uint32_t m_flush_interval;
  uint64_t m_flush_bytes;
  double m_flush_age;
  Handler m_handler;

  void TearDown() {
    for (ObjectRecorders::iterator it = m_object_recorders.begin();
         it != m_object_recorders.end(); ++it) {
      C_SaferCond cond;
      (*it)->flush(&cond);
      cond.wait();
    }
    m_object_recorders.clear();

    RadosTestFixture::TearDown();
  }

  inline void set_flush_interval(uint32_t i) {
    m_flush_interval = i;
  }
  inline void set_flush_bytes(uint64_t i) {
    m_flush_bytes = i;
  }
  inline void set_flush_age(double i) {
    m_flush_age = i;
  }

  journal::AppendBuffer create_append_buffer(uint64_t tag_tid, uint64_t entry_tid,
                                             const std::string &payload) {
    journal::FutureImplPtr future(new journal::FutureImpl(tag_tid, entry_tid,
                                                          456));
    future->init(journal::FutureImplPtr());

    bufferlist bl;
    bl.append(payload);
    return std::make_pair(future, bl);
  }

  journal::ObjectRecorderPtr create_object(const std::string &oid,
                                           uint8_t order, shared_ptr<Mutex> lock) {
    journal::ObjectRecorderPtr object(new journal::ObjectRecorder(
      m_ioctx, oid, 0, lock, m_work_queue, *m_timer, m_timer_lock, &m_handler,
      order, m_flush_interval, m_flush_bytes, m_flush_age));
    m_object_recorders.push_back(object);
    m_object_recorder_locks.insert(std::make_pair(oid, lock));
    m_handler.object_lock = lock;
    return object;
  }
};

TEST_F(TestObjectRecorder, Append) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(1U, object->get_pending_appends());

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(2U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer2.first->flush(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, AppendFlushByCount) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  set_flush_interval(2);
  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(1U, object->get_pending_appends());

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(0U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, AppendFlushByBytes) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  set_flush_bytes(10);
  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(1U, object->get_pending_appends());

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(0U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, AppendFlushByAge) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  set_flush_age(0.1);
  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, AppendFilledObject) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 12, lock);

  std::string payload(2048, '1');
  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              payload);
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              payload);
  append_buffers = {append_buffer2};
  lock->Lock();
  ASSERT_TRUE(object->append_unlock(std::move(append_buffers)));

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, Flush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(1U, object->get_pending_appends());

  C_SaferCond cond1;
  object->flush(&cond1);
  ASSERT_EQ(0, cond1.wait());

  C_SaferCond cond2;
  append_buffer1.first->wait(&cond2);
  ASSERT_EQ(0, cond2.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, FlushFuture) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer = create_append_buffer(234, 123,
                                                             "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(1U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer.first->wait(&cond);
  lock->Lock();
  object->flush(append_buffer.first);
  ASSERT_TRUE(lock->is_locked());
  lock->Unlock();
  ASSERT_TRUE(append_buffer.first->is_flush_in_progress() ||
              append_buffer.first->is_complete());
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, FlushDetachedFuture) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer = create_append_buffer(234, 123,
                                                             "payload");

  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer};

  lock->Lock();
  object->flush(append_buffer.first);
  ASSERT_TRUE(lock->is_locked());
  lock->Unlock();
  ASSERT_FALSE(append_buffer.first->is_flush_in_progress());
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));

  // should automatically flush once its attached to the object
  C_SaferCond cond;
  append_buffer.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, Close) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  set_flush_interval(2);
  shared_ptr<Mutex> lock(new Mutex("object_recorder_lock"));
  journal::ObjectRecorderPtr object = create_object(oid, 24, lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock->Lock();
  ASSERT_FALSE(object->append_unlock(std::move(append_buffers)));
  ASSERT_EQ(1U, object->get_pending_appends());

  lock->Lock();
  ASSERT_FALSE(object->close());
  ASSERT_TRUE(lock->is_locked());
  lock->Unlock();

  {
    Mutex::Locker locker(m_handler.lock);
    while (!m_handler.is_closed) {
      if (m_handler.cond.WaitInterval(
            reinterpret_cast<CephContext*>(m_ioctx.cct()),
            m_handler.lock, utime_t(10, 0)) != 0) {
        break;
      }
    }
  }

  ASSERT_TRUE(m_handler.is_closed);
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, Overflow) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  shared_ptr<Mutex> lock1(new Mutex("object_recorder_lock_1"));
  journal::ObjectRecorderPtr object1 = create_object(oid, 12, lock1);
  shared_ptr<Mutex> lock2(new Mutex("object_recorder_lock_2"));
  journal::ObjectRecorderPtr object2 = create_object(oid, 12, lock2);

  std::string payload(2048, '1');
  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              payload);
  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              payload);
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1, append_buffer2};
  lock1->Lock();
  ASSERT_TRUE(object1->append_unlock(std::move(append_buffers)));

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object1->get_pending_appends());

  journal::AppendBuffer append_buffer3 = create_append_buffer(456, 123,
                                                              payload);
  append_buffers = {append_buffer3};

  lock2->Lock();
  ASSERT_FALSE(object2->append_unlock(std::move(append_buffers)));
  append_buffer3.first->flush(NULL);

  bool overflowed = false;
  {
    Mutex::Locker locker(m_handler.lock);
    while (m_handler.overflows == 0) {
      if (m_handler.cond.WaitInterval(
            reinterpret_cast<CephContext*>(m_ioctx.cct()),
            m_handler.lock, utime_t(10, 0)) != 0) {
        break;
      }
    }
    if (m_handler.overflows != 0) {
      overflowed = true;
    }
  }

  ASSERT_TRUE(overflowed);
}
