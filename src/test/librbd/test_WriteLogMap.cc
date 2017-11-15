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

uint64_t BlockToBytes(int n) { return n * BLOCK_SIZE; }
    
TEST_F(TestWriteLogMap, AddOverlaps) {
  WriteLogMap map(m_cct);

  WriteLogEntry e0(BlockToBytes(0), BlockToBytes(1));
  map.add_entry(&e0);

  WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 0));

  cout << "found0=[" << found0.size() << ":";
  for (auto &map_entry : found0) {
    cout << map_entry;
  }
  cout << "]" << std::endl;

  WriteLogEntry e1(BlockToBytes(1), BlockToBytes(1));
  map.add_entry(&e1);
  
  found0 = map.find_map_entries(BlockExtent(1, 1));

  cout << "found0=[" << found0.size() << ":";
  for (auto &map_entry : found0) {
    cout << map_entry;
  }
  cout << "]" << std::endl;

  WriteLogEntry e2(BlockToBytes(2), BlockToBytes(1));
  map.add_entry(&e2);

  found0 = map.find_map_entries(BlockExtent(2, 2));

  cout << "found0=[" << found0.size() << ":";
  for (auto &map_entry : found0) {
    cout << map_entry;
  }
  cout << "]" << std::endl;

  WriteLogEntry e3(BlockToBytes(1), BlockToBytes(1));
  map.add_entry(&e3);

  found0 = map.find_map_entries(BlockExtent(99, 99));

  cout << "found0=[" << found0.size() << ":";
  for (auto &map_entry : found0) {
    cout << map_entry;
  }
  cout << "]" << std::endl;

  WriteLogMapEntries found1 = map.find_map_entries(BlockExtent(1, 1));

  cout << "found1=[" << found1.size() << ":";
  for (auto &map_entry : found1) {
    cout << map_entry;
  }
  cout << "]" << std::endl;

  //ASSERT_EQ(map.find_map_entries(BlockExtent(1, 1)), {{ &e1 }});
  
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

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
