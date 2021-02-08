// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectRecorder.h"
#include "common/Cond.h"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "test/journal/RadosTestFixture.h"
#include <limits>

using std::shared_ptr;

class TestObjectRecorder : public RadosTestFixture {
public:
  TestObjectRecorder() = default;

  struct Handler : public journal::ObjectRecorder::Handler {
    ceph::mutex lock = ceph::make_mutex("lock");
    ceph::mutex* object_lock = nullptr;
    ceph::condition_variable cond;
    bool is_closed = false;
    uint32_t overflows = 0;

    Handler() = default;

    void closed(journal::ObjectRecorder *object_recorder) override {
      std::lock_guard locker{lock};
      is_closed = true;
      cond.notify_all();
    }
    void overflow(journal::ObjectRecorder *object_recorder) override {
      std::lock_guard locker{lock};
      journal::AppendBuffers append_buffers;
      object_lock->lock();
      object_recorder->claim_append_buffers(&append_buffers);
      object_lock->unlock();

      ++overflows;
      cond.notify_all();
    }
  };

  // flush the pending buffers in dtor
  class ObjectRecorderFlusher {
  public:
    ObjectRecorderFlusher(librados::IoCtx& ioctx,
			  ContextWQ* work_queue)
      : m_ioctx{ioctx},
	m_work_queue{work_queue}
    {}
    ObjectRecorderFlusher(librados::IoCtx& ioctx,
			  ContextWQ* work_queue,
			  uint32_t flush_interval,
			  uint16_t flush_bytes,
			  double flush_age,
			  int max_in_flight)
      : m_ioctx{ioctx},
	m_work_queue{work_queue},
	m_flush_interval{flush_interval},
	m_flush_bytes{flush_bytes},
	m_flush_age{flush_age},
	m_max_in_flight_appends{max_in_flight < 0 ?
				std::numeric_limits<uint64_t>::max() :
				static_cast<uint64_t>(max_in_flight)}
    {}
    ~ObjectRecorderFlusher() {
      for (auto& [object_recorder, m] : m_object_recorders) {
	C_SaferCond cond;
	object_recorder->flush(&cond);
	cond.wait();
	std::scoped_lock l{*m};
	if (!object_recorder->is_closed()) {
	  object_recorder->close();
	}
      }
    }
    auto create_object(std::string_view oid, uint8_t order, ceph::mutex* lock) {
      auto object = ceph::make_ref<journal::ObjectRecorder>(
        m_ioctx, oid, 0, lock, m_work_queue, &m_handler,
	order, m_max_in_flight_appends);
      {
	std::lock_guard locker{*lock};
	object->set_append_batch_options(m_flush_interval,
					 m_flush_bytes,
					 m_flush_age);
      }
      m_object_recorders.emplace_back(object, lock);
      m_handler.object_lock = lock;
      return object;
    }
    bool wait_for_closed() {
      std::unique_lock locker{m_handler.lock};
      return m_handler.cond.wait_for(locker, 10s,
				     [this] { return m_handler.is_closed; });
    }
    bool wait_for_overflow() {
      std::unique_lock locker{m_handler.lock};
      if (m_handler.cond.wait_for(locker, 10s,
				  [this] { return m_handler.overflows > 0; })) {
	m_handler.overflows = 0;
	return true;
      } else {
	return false;
      }
    }
  private:
    librados::IoCtx& m_ioctx;
    ContextWQ *m_work_queue;
    uint32_t m_flush_interval = std::numeric_limits<uint32_t>::max();
    uint64_t m_flush_bytes = std::numeric_limits<uint64_t>::max();
    double m_flush_age = 600;
    uint64_t m_max_in_flight_appends = 0;
    using ObjectRecorders =
      std::list<std::pair<ceph::ref_t<journal::ObjectRecorder>, ceph::mutex*>>;
    ObjectRecorders m_object_recorders;
    Handler m_handler;
  };

  journal::AppendBuffer create_append_buffer(uint64_t tag_tid,
                                             uint64_t entry_tid,
                                             const std::string &payload) {
    auto future = ceph::make_ref<journal::FutureImpl>(tag_tid, entry_tid, 456);
    future->init(ceph::ref_t<journal::FutureImpl>());

    bufferlist bl;
    bl.append(payload);
    return std::make_pair(future, bl);
  }
};

TEST_F(TestObjectRecorder, Append) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 0, 0, 0, 0);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(0U, object->get_pending_appends());

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(0U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer2.first->flush(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, AppendFlushByCount) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 2, 0, 0, -1);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(1U, object->get_pending_appends());

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(0U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, AppendFlushByBytes) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 0, 10, 0, -1);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(1U, object->get_pending_appends());

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              "payload");
  append_buffers = {append_buffer2};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(0U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, AppendFlushByAge) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 0, 0, 0.0005, -1);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();

  uint32_t offset  = 0;
  journal::AppendBuffer append_buffer2;
  while (!append_buffer1.first->is_flush_in_progress() &&
         !append_buffer1.first->is_complete()) {
    usleep(1000);

    append_buffer2 = create_append_buffer(234, 124 + offset, "payload");
    ++offset;
    append_buffers = {append_buffer2};

    lock.lock();
    ASSERT_FALSE(object->append(std::move(append_buffers)));
    lock.unlock();
  }

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, AppendFilledObject) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 0, 0, 0.0, -1);
  auto object = flusher.create_object(oid, 12, &lock);

  std::string payload(2048, '1');
  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              payload);
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();

  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              payload);
  append_buffers = {append_buffer2};
  lock.lock();
  ASSERT_TRUE(object->append(std::move(append_buffers)));
  lock.unlock();

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, Flush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 0, 10, 0, -1);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
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
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 0, 10, 0, -1);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer = create_append_buffer(234, 123,
                                                             "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(1U, object->get_pending_appends());

  C_SaferCond cond;
  append_buffer.first->wait(&cond);
  object->flush(append_buffer.first);
  ASSERT_TRUE(append_buffer.first->is_flush_in_progress() ||
              append_buffer.first->is_complete());
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, FlushDetachedFuture) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer = create_append_buffer(234, 123,
                                                             "payload");

  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer};

  object->flush(append_buffer.first);
  ASSERT_FALSE(append_buffer.first->is_flush_in_progress());
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();

  // should automatically flush once its attached to the object
  C_SaferCond cond;
  append_buffer.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestObjectRecorder, Close) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock = ceph::make_mutex("object_recorder_lock");
  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue, 2, 0, 0, -1);
  auto object = flusher.create_object(oid, 24, &lock);

  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              "payload");
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1};
  lock.lock();
  ASSERT_FALSE(object->append(std::move(append_buffers)));
  lock.unlock();
  ASSERT_EQ(1U, object->get_pending_appends());

  lock.lock();
  ASSERT_FALSE(object->close());
  ASSERT_TRUE(ceph_mutex_is_locked(lock));
  lock.unlock();

  ASSERT_TRUE(flusher.wait_for_closed());

  ASSERT_EQ(0U, object->get_pending_appends());
}

TEST_F(TestObjectRecorder, Overflow) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  ceph::mutex lock1 = ceph::make_mutex("object_recorder_lock_1");
  ceph::mutex lock2 = ceph::make_mutex("object_recorder_lock_2");

  ObjectRecorderFlusher flusher(m_ioctx, m_work_queue);
  auto object1 = flusher.create_object(oid, 12, &lock1);

  std::string payload(1 << 11, '1');
  journal::AppendBuffer append_buffer1 = create_append_buffer(234, 123,
                                                              payload);
  journal::AppendBuffer append_buffer2 = create_append_buffer(234, 124,
                                                              payload);
  journal::AppendBuffers append_buffers;
  append_buffers = {append_buffer1, append_buffer2};
  lock1.lock();
  ASSERT_TRUE(object1->append(std::move(append_buffers)));
  lock1.unlock();

  C_SaferCond cond;
  append_buffer2.first->wait(&cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, object1->get_pending_appends());

  auto object2 = flusher.create_object(oid, 12, &lock2);

  journal::AppendBuffer append_buffer3 = create_append_buffer(456, 123,
                                                              payload);
  append_buffers = {append_buffer3};
  lock2.lock();
  ASSERT_FALSE(object2->append(std::move(append_buffers)));
  lock2.unlock();
  append_buffer3.first->flush(NULL);

  ASSERT_TRUE(flusher.wait_for_overflow());
}
