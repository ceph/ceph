// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/FutureImpl.h"
#include "common/Cond.h"
#include "gtest/gtest.h"
#include "test/journal/RadosTestFixture.h"

class TestFutureImpl : public RadosTestFixture {
public:
  struct FlushHandler : public journal::FutureImpl::FlushHandler {
    uint64_t flushes = 0;
    void flush(const ceph::ref_t<journal::FutureImpl>& future) override {
      ++flushes;
    }
    FlushHandler() = default;
  };

  TestFutureImpl() {
    m_flush_handler = std::make_shared<FlushHandler>();
  }

  auto create_future(uint64_t tag_tid, uint64_t entry_tid,
                     uint64_t commit_tid,
                     ceph::ref_t<journal::FutureImpl> prev = nullptr) {
    auto future = ceph::make_ref<journal::FutureImpl>(tag_tid, entry_tid, commit_tid);
    future->init(prev);
    return future;
  }

  void flush(const ceph::ref_t<journal::FutureImpl>& future) {
  }

  std::shared_ptr<FlushHandler> m_flush_handler;
};

TEST_F(TestFutureImpl, Getters) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  ASSERT_EQ(234U, future->get_tag_tid());
  ASSERT_EQ(123U, future->get_entry_tid());
  ASSERT_EQ(456U, future->get_commit_tid());
}

TEST_F(TestFutureImpl, Attach) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  ASSERT_FALSE(future->attach(m_flush_handler));
  ASSERT_EQ(2U, m_flush_handler.use_count());
}

TEST_F(TestFutureImpl, AttachWithPendingFlush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  future->flush(NULL);

  ASSERT_TRUE(future->attach(m_flush_handler));
  ASSERT_EQ(2U, m_flush_handler.use_count());
}

TEST_F(TestFutureImpl, Detach) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  ASSERT_FALSE(future->attach(m_flush_handler));
  future->detach();
  ASSERT_EQ(1U, m_flush_handler.use_count());
}

TEST_F(TestFutureImpl, DetachImplicit) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  ASSERT_FALSE(future->attach(m_flush_handler));
  future.reset();
  ASSERT_EQ(1U, m_flush_handler.use_count());
}

TEST_F(TestFutureImpl, Flush) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  ASSERT_FALSE(future->attach(m_flush_handler));

  C_SaferCond cond;
  future->flush(&cond);

  ASSERT_EQ(1U, m_flush_handler->flushes);
  future->safe(-EIO);
  ASSERT_EQ(-EIO, cond.wait());
}

TEST_F(TestFutureImpl, FlushWithoutContext) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  ASSERT_FALSE(future->attach(m_flush_handler));

  future->flush(NULL);
  ASSERT_EQ(1U, m_flush_handler->flushes);
  future->safe(-EIO);
  ASSERT_TRUE(future->is_complete());
  ASSERT_EQ(-EIO, future->get_return_value());
}

TEST_F(TestFutureImpl, FlushChain) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future1 = create_future(234, 123, 456);
  auto future2 = create_future(234, 124, 457, future1);
  auto future3 = create_future(235, 1, 458, future2);

  auto flush_handler = std::make_shared<FlushHandler>();
  ASSERT_FALSE(future1->attach(m_flush_handler));
  ASSERT_FALSE(future2->attach(flush_handler));
  ASSERT_FALSE(future3->attach(m_flush_handler));

  C_SaferCond cond;
  future3->flush(&cond);

  ASSERT_EQ(1U, m_flush_handler->flushes);
  ASSERT_EQ(1U, flush_handler->flushes);

  future3->safe(0);
  ASSERT_FALSE(future3->is_complete());

  future1->safe(0);
  ASSERT_FALSE(future3->is_complete());

  future2->safe(-EIO);
  ASSERT_TRUE(future3->is_complete());
  ASSERT_EQ(-EIO, future3->get_return_value());
  ASSERT_EQ(-EIO, cond.wait());
  ASSERT_EQ(0, future1->get_return_value());
}

TEST_F(TestFutureImpl, FlushInProgress) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future1 = create_future(234, 123, 456);
  auto future2 = create_future(234, 124, 457, future1);
  ASSERT_FALSE(future1->attach(m_flush_handler));
  ASSERT_FALSE(future2->attach(m_flush_handler));

  future1->set_flush_in_progress();
  ASSERT_TRUE(future1->is_flush_in_progress());

  future1->flush(NULL);
  ASSERT_EQ(0U, m_flush_handler->flushes);

  future1->safe(0);
}

TEST_F(TestFutureImpl, FlushAlreadyComplete) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 123, 456);
  future->safe(-EIO);

  C_SaferCond cond;
  future->flush(&cond);
  ASSERT_EQ(-EIO, cond.wait());
}

TEST_F(TestFutureImpl, Wait) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 1, 456);

  C_SaferCond cond;
  future->wait(&cond);
  future->safe(-EEXIST);
  ASSERT_EQ(-EEXIST, cond.wait());
}

TEST_F(TestFutureImpl, WaitAlreadyComplete) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future = create_future(234, 1, 456);
  future->safe(-EEXIST);

  C_SaferCond cond;
  future->wait(&cond);
  ASSERT_EQ(-EEXIST, cond.wait());
}

TEST_F(TestFutureImpl, SafePreservesError) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future1 = create_future(234, 123, 456);
  auto future2 = create_future(234, 124, 457, future1);

  future1->safe(-EIO);
  future2->safe(-EEXIST);
  ASSERT_TRUE(future2->is_complete());
  ASSERT_EQ(-EIO, future2->get_return_value());
}

TEST_F(TestFutureImpl, ConsistentPreservesError) {
  std::string oid = get_temp_oid();
  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  auto metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  auto future1 = create_future(234, 123, 456);
  auto future2 = create_future(234, 124, 457, future1);

  future2->safe(-EEXIST);
  future1->safe(-EIO);
  ASSERT_TRUE(future2->is_complete());
  ASSERT_EQ(-EEXIST, future2->get_return_value());
}
