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
  void SetUp() override {
    TestFixture::SetUp();
    m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  }

  CephContext *m_cct;
};

uint64_t BlockToBytes(int n) { return n * BLOCK_SIZE; }
    
TEST_F(TestWriteLogMap, Simple) {
  WriteLogMap map(m_cct);

  /* WriteLogEntry takes offset, length, in bytes */
  WriteLogEntry e1(BlockToBytes(4), BlockToBytes(8));
  map.add_log_entry(&e1);

  /* BlockExtent takes first, last, in blocks */
  WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  /* Written range includes the single write above */
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(&e1, found0.front().log_entry);

  /* Nothing before that */
  found0 = map.find_map_entries(BlockExtent(0, 3));
  numfound = found0.size();
  ASSERT_EQ(0, numfound);

  /* Nothing after that */
  found0 = map.find_map_entries(BlockExtent(12, 99));
  numfound = found0.size();
  ASSERT_EQ(0, numfound);

  /* 4-11 will be e1 */
  for (int i=4; i<12; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(&e1, found0.front().log_entry);
  }

  map.remove_log_entry(&e1);
  /* Nothing should be found */
  for (int i=4; i<12; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(0, numfound);
  }
}

TEST_F(TestWriteLogMap, OverlapFront) {
  WriteLogMap map(m_cct);

  WriteLogEntry e0(BlockToBytes(4), BlockToBytes(8));
  map.add_log_entry(&e0);
  /* replaces block 4-7 of e0 */
  WriteLogEntry e1(BlockToBytes(0), BlockToBytes(8));
  map.add_log_entry(&e1);

  /* Written range includes the two writes above */
  WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  ASSERT_EQ(2, numfound);
  ASSERT_EQ(&e1, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e0, found0.front().log_entry);

  /* 0-7 will be e1 */
  for (int i=0; i<8; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(&e1, found0.front().log_entry);
  }

  /* 8-11 will be e0 */
  for (int i=8; i<12; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(&e0, found0.front().log_entry);
  }
}

TEST_F(TestWriteLogMap, OverlapBack) {
  WriteLogMap map(m_cct);

  WriteLogEntry e0(BlockToBytes(0), BlockToBytes(8));
  map.add_log_entry(&e0);
  /* replaces block 4-7 of e0 */
  WriteLogEntry e1(BlockToBytes(4), BlockToBytes(8));
  map.add_log_entry(&e1);

  /* Written range includes the two writes above */
  WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  ASSERT_EQ(2, numfound);
  ASSERT_EQ(&e0, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e1, found0.front().log_entry);

  /* 0-3 will be e0 */
  for (int i=0; i<4; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(&e0, found0.front().log_entry);
  }

  /* 4-11 will be e1 */
  for (int i=4; i<12; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(&e1, found0.front().log_entry);
  }

  map.remove_log_entry(&e0);

  /* 0-3 will find nothing */
  for (int i=0; i<4; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(0, numfound);
  }

  /* 4-11 will still be e1 */
  for (int i=4; i<12; i++) {
    WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(&e1, found0.front().log_entry);
  }

}

TEST_F(TestWriteLogMap, OverlapMiddle) {
  WriteLogMap map(m_cct);

  WriteLogEntry e0(BlockToBytes(0), BlockToBytes(1));
  map.add_log_entry(&e0);

  WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 0));
  int numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(&e0, found0.front().log_entry);
  WriteLogEntries entries = map.find_log_entries(BlockExtent(0, 0));
  int entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(&e0, entries.front());
  
  WriteLogEntry e1(BlockToBytes(1), BlockToBytes(1));
  map.add_log_entry(&e1);
  
  found0 = map.find_map_entries(BlockExtent(1, 1));
  numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(&e1, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(1, 1));
  entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(&e1, entries.front());

  WriteLogEntry e2(BlockToBytes(2), BlockToBytes(1));
  map.add_log_entry(&e2);

  found0 = map.find_map_entries(BlockExtent(2, 2));
  numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(&e2, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(2, 2));
  entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(&e2, entries.front());

  /* replaces e1 */
  WriteLogEntry e3(BlockToBytes(1), BlockToBytes(1));
  map.add_log_entry(&e3);

  found0 = map.find_map_entries(BlockExtent(1, 1));
  numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(&e3, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(1, 1));
  entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(&e3, entries.front());

  found0 = map.find_map_entries(BlockExtent(0, 100));
  numfound = found0.size();
  ASSERT_EQ(3, numfound);
  ASSERT_EQ(&e0, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e3, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e2, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(0, 100));
  entriesfound = entries.size();
  ASSERT_EQ(3, entriesfound);
  ASSERT_EQ(&e0, entries.front());
  entries.pop_front();
  ASSERT_EQ(&e3, entries.front());
  entries.pop_front();
  ASSERT_EQ(&e2, entries.front());

  entries.clear();
  entries.emplace_back(&e0);
  entries.emplace_back(&e1);
  map.remove_log_entries(entries);

  found0 = map.find_map_entries(BlockExtent(0, 100));
  numfound = found0.size();
  ASSERT_EQ(2, numfound);
  ASSERT_EQ(&e3, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e2, found0.front().log_entry);
}

TEST_F(TestWriteLogMap, OverlapSplit) {
  WriteLogMap map(m_cct);

  WriteLogEntry e0(BlockToBytes(0), BlockToBytes(8));
  map.add_log_entry(&e0);

  /* Splits e0 at 1 */
  WriteLogEntry e1(BlockToBytes(1), BlockToBytes(1));
  map.add_log_entry(&e1);

  /* Splits e0 again at 4 */
  WriteLogEntry e2(BlockToBytes(4), BlockToBytes(2));
  map.add_log_entry(&e2);

  /* Replaces one block of e2, and one of e0 */
  WriteLogEntry e3(BlockToBytes(5), BlockToBytes(2));
  map.add_log_entry(&e3);

  /* Expecting: 0:e0, 1:e1, 2..3:e0, 4:e2, 5..6:e3, 7:e0 */
  WriteLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  ASSERT_EQ(6, numfound);
  ASSERT_EQ(&e0, found0.front().log_entry);
  ASSERT_EQ(uint64_t(0), found0.front().block_extent.block_start);
  ASSERT_EQ(uint64_t(0), found0.front().block_extent.block_end);
  found0.pop_front();
  ASSERT_EQ(&e1, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e0, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e2, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e3, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(&e0, found0.front().log_entry);
}

} // namespace rwl
} // namespace cache
} // namespace librbd

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
