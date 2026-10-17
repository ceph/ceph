// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <gtest/gtest.h>

#include "osd/Coroutines.h"

namespace {

struct CountingStack {
  static inline int live = 0;
  boost::context::fixedsize_stack stack;

  boost::context::stack_context allocate() {
    ++live;
    return stack.allocate();
  }

  void deallocate(boost::context::stack_context& sctx) noexcept {
    --live;
    stack.deallocate(sctx);
  }
};

class CoroutinesTest : public ::testing::Test {
protected:
  std::shared_ptr<resume_token_t> resumer;

  void SetUp() override {
    CountingStack::live = 0;
  }
};

} // namespace

// The body resets its owner when it completes, as
// PrimaryLogPG::on_coroutine_complete() does.
TEST_F(CoroutinesTest, CompleteWithoutYieldFreesStack)
{
  bool ran = false;
  start_coroutine(resumer, CountingStack(), [&](yield_token_t&) {
    ran = true;
    resumer = nullptr;
  });
  EXPECT_TRUE(ran);
  EXPECT_EQ(nullptr, resumer);
  EXPECT_EQ(0, CountingStack::live);
}

TEST_F(CoroutinesTest, CompleteAfterYieldFreesStack)
{
  std::weak_ptr<resume_token_t> weak;
  start_coroutine(resumer, CountingStack(), [&](yield_token_t& yield) {
    weak = resumer;
    yield();
    resumer = nullptr;
  });
  EXPECT_EQ(1, CountingStack::live);

  // Resume as the ECBackend::objects_read_sync() completion does.
  if (auto locked = weak.lock(); locked) {
    (*locked)();
  }
  EXPECT_EQ(nullptr, resumer);
  EXPECT_EQ(0, CountingStack::live);
}

// PrimaryLogPG::on_change() drops a suspended coroutine.
TEST_F(CoroutinesTest, DestroyWhileSuspendedFreesStack)
{
  start_coroutine(resumer, CountingStack(), [&](yield_token_t& yield) {
    yield();
    ADD_FAILURE() << "resumed after being destroyed";
  });
  EXPECT_EQ(1, CountingStack::live);

  resumer = nullptr;
  EXPECT_EQ(0, CountingStack::live);
}
