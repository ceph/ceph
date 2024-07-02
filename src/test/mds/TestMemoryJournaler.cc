
#include "stubs/MemoryJournaler.h"
#include "gtest/gtest.h"

using namespace ceph;

TEST(MemoryJournaler, BasicOps) {
  MemoryJournaler mj;

  bufferlist entry_buf;
  file_layout_t layout = file_layout_t::get_default();
  mj.create(&layout, JOURNAL_FORMAT_RESILIENT);
  ASSERT_TRUE(mj.is_readable());

  ASSERT_TRUE(mj.is_readonly());
  mj.set_writeable();
  ASSERT_FALSE(mj.is_readonly());

  mj.write_head(nullptr);

  // can append entries
  encode((uint64_t)1, entry_buf);
  ASSERT_NE(0, entry_buf.length());
  mj.append_entry(entry_buf);
  ASSERT_EQ(0, entry_buf.length());
  encode((uint64_t)2, entry_buf);
  mj.append_entry(entry_buf);
  encode((uint64_t)3, entry_buf);
  mj.append_entry(entry_buf);
  encode((uint64_t)4, entry_buf);
  mj.append_entry(entry_buf);

  // can wait for flush
  bool did_flush = false;
  mj.wait_for_flush(new LambdaContext([&did_flush](int r){
    ASSERT_EQ(0, r);
    did_flush = true;
  }));

  // can read entries
  uint64_t entry_val = 0;
  ASSERT_TRUE(mj.try_read_entry(entry_buf));
  decode(entry_val, entry_buf);
  ASSERT_EQ(1, entry_val);
  ASSERT_TRUE(mj.try_read_entry(entry_buf));
  decode(entry_val, entry_buf);
  ASSERT_EQ(2, entry_val);

  // has dirty data
  ASSERT_NE(mj.get_write_pos(), mj.get_write_safe_pos());

  // can flush
  ASSERT_FALSE(did_flush);
  mj.flush();
  ASSERT_EQ(mj.get_write_pos(), mj.get_write_safe_pos());
  ASSERT_TRUE(did_flush);

  mj.set_expire_pos(mj.get_read_pos());

  // has stale header
  ASSERT_TRUE(mj.is_write_head_needed());

  // can update header
  mj.write_head();
  ASSERT_FALSE(mj.is_write_head_needed());

  // add more events before reverting to header
  encode((uint64_t)5, entry_buf);
  ASSERT_NE(0, entry_buf.length());
  mj.append_entry(entry_buf);
  ASSERT_EQ(0, entry_buf.length());
  encode((uint64_t)6, entry_buf);
  mj.append_entry(entry_buf);

  // can recover to head
  mj.recover(nullptr);

  ASSERT_TRUE(mj.try_read_entry(entry_buf));
  decode(entry_val, entry_buf);
  ASSERT_EQ(3, entry_val);
}
