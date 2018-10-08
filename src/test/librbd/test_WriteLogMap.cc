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

struct TestLogEntry {
  uint64_t image_offset_bytes;
  uint64_t write_bytes;
  uint32_t referring_map_entries = 0;
  TestLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : image_offset_bytes(image_offset_bytes), write_bytes(write_bytes) {
  }
  const uint64_t get_offset_bytes() { return image_offset_bytes; }
  const uint64_t get_write_bytes() { return write_bytes; }
  const BlockExtent block_extent() {
    return BlockExtent(image_offset_bytes,
		       image_offset_bytes + write_bytes -1);
  }
  bool is_writer() { return true; }
  uint32_t get_map_ref() { return(referring_map_entries); }
  void inc_map_ref() { referring_map_entries++; }
  void dec_map_ref() { referring_map_entries--; }
  friend std::ostream &operator<<(std::ostream &os,
				  const TestLogEntry &entry) {
    os << "referring_map_entries=" << entry.referring_map_entries << ", "
       << "image_offset_bytes=" << entry.image_offset_bytes << ", "
       << "write_bytes=" << entry.write_bytes;
    return os;
  };
};

typedef std::list<std::shared_ptr<TestLogEntry>> TestLogEntries;
typedef LogMapEntry<TestLogEntry> TestMapEntry;
typedef LogMapEntries<TestLogEntry> TestLogMapEntries;
typedef LogMap<TestLogEntry, TestLogEntries> TestLogMap;

class TestWriteLogMap : public TestFixture {
public:
  void SetUp() override {
    TestFixture::SetUp();
    m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  }

  CephContext *m_cct;
};

/* Block sie is 1 */
uint64_t BlockToBytes(int n) { return n; }

TEST_F(TestWriteLogMap, Simple) {
  TestLogEntry e(0,0);
  TestLogEntries es;
  TestMapEntry me(make_shared<TestLogEntry>(e));
  TestLogMapEntries lme;
  TestLogMap  map(m_cct);

  /* LogEntry takes offset, length, in bytes */
  auto e1 = make_shared<TestLogEntry>(BlockToBytes(4), BlockToBytes(8));
  TestLogEntry *e1_ptr = e1.get();
  ASSERT_EQ(BlockToBytes(4), e1_ptr->get_offset_bytes());
  ASSERT_EQ(BlockToBytes(8), e1_ptr->get_write_bytes());
  map.add_log_entry(e1);

  /* BlockExtent takes first, last, in blocks */
  TestLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  /* Written range includes the single write above */
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(e1, found0.front().log_entry);

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
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(e1, found0.front().log_entry);
  }

  map.remove_log_entry(e1);
  /* Nothing should be found */
  for (int i=4; i<12; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(0, numfound);
  }
}

TEST_F(TestWriteLogMap, OverlapFront) {
  TestLogMap map(m_cct);

  auto e0 = make_shared<TestLogEntry>(BlockToBytes(4), BlockToBytes(8));
  map.add_log_entry(e0);
  /* replaces block 4-7 of e0 */
  auto e1 = make_shared<TestLogEntry>(BlockToBytes(0), BlockToBytes(8));
  map.add_log_entry(e1);

  /* Written range includes the two writes above */
  TestLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  ASSERT_EQ(2, numfound);
  ASSERT_EQ(e1, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e0, found0.front().log_entry);

  /* 0-7 will be e1 */
  for (int i=0; i<8; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(e1, found0.front().log_entry);
  }

  /* 8-11 will be e0 */
  for (int i=8; i<12; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(e0, found0.front().log_entry);
  }
}

TEST_F(TestWriteLogMap, OverlapBack) {
  TestLogMap map(m_cct);

  auto e0 = make_shared<TestLogEntry>(BlockToBytes(0), BlockToBytes(8));
  map.add_log_entry(e0);
  /* replaces block 4-7 of e0 */
  auto e1 = make_shared<TestLogEntry>(BlockToBytes(4), BlockToBytes(8));
  map.add_log_entry(e1);

  /* Written range includes the two writes above */
  TestLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  ASSERT_EQ(2, numfound);
  ASSERT_EQ(e0, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e1, found0.front().log_entry);

  /* 0-3 will be e0 */
  for (int i=0; i<4; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(e0, found0.front().log_entry);
  }

  /* 4-11 will be e1 */
  for (int i=4; i<12; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(e1, found0.front().log_entry);
  }

  map.remove_log_entry(e0);

  /* 0-3 will find nothing */
  for (int i=0; i<4; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(0, numfound);
  }

  /* 4-11 will still be e1 */
  for (int i=4; i<12; i++) {
    TestLogMapEntries found0 = map.find_map_entries(BlockExtent(i, i));
    int numfound = found0.size();
    ASSERT_EQ(1, numfound);
    ASSERT_EQ(e1, found0.front().log_entry);
  }

}

TEST_F(TestWriteLogMap, OverlapMiddle) {
  TestLogMap map(m_cct);

  auto e0 = make_shared<TestLogEntry>(BlockToBytes(0), BlockToBytes(1));
  map.add_log_entry(e0);

  TestLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 0));
  int numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(e0, found0.front().log_entry);
  TestLogEntries entries = map.find_log_entries(BlockExtent(0, 0));
  int entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(e0, entries.front());

  auto e1 = make_shared<TestLogEntry>(BlockToBytes(1), BlockToBytes(1));
  map.add_log_entry(e1);

  found0 = map.find_map_entries(BlockExtent(1, 1));
  numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(e1, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(1, 1));
  entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(e1, entries.front());

  auto e2 = make_shared<TestLogEntry>(BlockToBytes(2), BlockToBytes(1));
  map.add_log_entry(e2);

  found0 = map.find_map_entries(BlockExtent(2, 2));
  numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(e2, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(2, 2));
  entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(e2, entries.front());

  /* replaces e1 */
  auto e3 = make_shared<TestLogEntry>(BlockToBytes(1), BlockToBytes(1));
  map.add_log_entry(e3);

  found0 = map.find_map_entries(BlockExtent(1, 1));
  numfound = found0.size();
  ASSERT_EQ(1, numfound);
  ASSERT_EQ(e3, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(1, 1));
  entriesfound = entries.size();
  ASSERT_EQ(1, entriesfound);
  ASSERT_EQ(e3, entries.front());

  found0 = map.find_map_entries(BlockExtent(0, 100));
  numfound = found0.size();
  ASSERT_EQ(3, numfound);
  ASSERT_EQ(e0, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e3, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e2, found0.front().log_entry);
  entries = map.find_log_entries(BlockExtent(0, 100));
  entriesfound = entries.size();
  ASSERT_EQ(3, entriesfound);
  ASSERT_EQ(e0, entries.front());
  entries.pop_front();
  ASSERT_EQ(e3, entries.front());
  entries.pop_front();
  ASSERT_EQ(e2, entries.front());

  entries.clear();
  entries.emplace_back(e0);
  entries.emplace_back(e1);
  map.remove_log_entries(entries);

  found0 = map.find_map_entries(BlockExtent(0, 100));
  numfound = found0.size();
  ASSERT_EQ(2, numfound);
  ASSERT_EQ(e3, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e2, found0.front().log_entry);
}

TEST_F(TestWriteLogMap, OverlapSplit) {
  TestLogMap map(m_cct);

  auto e0 = make_shared<TestLogEntry>(BlockToBytes(0), BlockToBytes(8));
  map.add_log_entry(e0);

  /* Splits e0 at 1 */
  auto e1 = make_shared<TestLogEntry>(BlockToBytes(1), BlockToBytes(1));
  map.add_log_entry(e1);

  /* Splits e0 again at 4 */
  auto e2 = make_shared<TestLogEntry>(BlockToBytes(4), BlockToBytes(2));
  map.add_log_entry(e2);

  /* Replaces one block of e2, and one of e0 */
  auto e3 = make_shared<TestLogEntry>(BlockToBytes(5), BlockToBytes(2));
  map.add_log_entry(e3);

  /* Expecting: 0:e0, 1:e1, 2..3:e0, 4:e2, 5..6:e3, 7:e0 */
  TestLogMapEntries found0 = map.find_map_entries(BlockExtent(0, 100));
  int numfound = found0.size();
  ASSERT_EQ(6, numfound);
  ASSERT_EQ(e0, found0.front().log_entry);
  ASSERT_EQ(uint64_t(0), found0.front().block_extent.block_start);
  ASSERT_EQ(uint64_t(0), found0.front().block_extent.block_end);
  found0.pop_front();
  ASSERT_EQ(e1, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e0, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e2, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e3, found0.front().log_entry);
  found0.pop_front();
  ASSERT_EQ(e0, found0.front().log_entry);
}

} // namespace rwl
} // namespace cache
} // namespace librbd

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
