// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/cache/ReplicatedWriteLog.h"

void register_test_write_log_map() {
}

namespace librbd {
namespace cache {
namespace rwl {

class TestWriteLogMap : public TestFixture {
public:
  static uint32_t s_index;

  /*

  struct Operation {
    uint32_t index;
    Operation() : index(++s_index) {
    }
    Operation(Operation &&rhs) : index(rhs.index) {
    }
    Operation(const Operation &) = delete;

    Operation& operator=(Operation &&rhs) {
      index = rhs.index;
      return *this;
    }

    bool operator==(const Operation &rhs) const {
      return index == rhs.index;
    }
  };

  typedef std::list<Operation> Operations;

  typedef BlockGuard<Operation> OpBlockGuard;
  */
  
  void SetUp() override {
    TestFixture::SetUp();
    m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  }

  CephContext *m_cct;
};

TEST_F(TestWriteLogMap, FirstTest) {
  /*
  OpBlockGuard op_block_guard(m_cct);

  Operation op1;
  BlockGuardCell *cell1;
  ASSERT_EQ(0, op_block_guard.detain({1, 3}, &op1, &cell1));

  Operation op2;
  BlockGuardCell *cell2;
  ASSERT_EQ(0, op_block_guard.detain({0, 1}, &op2, &cell2));

  Operation op3;
  BlockGuardCell *cell3;
  ASSERT_EQ(0, op_block_guard.detain({3, 6}, &op3, &cell3));

  Operations released_ops;
  op_block_guard.release(cell1, &released_ops);
  ASSERT_TRUE(released_ops.empty());

  op_block_guard.release(cell2, &released_ops);
  ASSERT_TRUE(released_ops.empty());

  op_block_guard.release(cell3, &released_ops);
  ASSERT_TRUE(released_ops.empty());
  */
}

uint32_t TestWriteLogMap::s_index = 0;

} // namespace rwl
} // namespace cache
} // namespace librbd

