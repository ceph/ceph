// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Entry.h"
#include "gtest/gtest.h"

class TestEntry : public ::testing::Test {
};

TEST_F(TestEntry, DefaultConstructor) {
  journal::Entry entry;
  ASSERT_EQ(0U, entry.get_entry_tid());
  ASSERT_EQ(0U, entry.get_tag_tid());

  bufferlist data(entry.get_data());
  bufferlist expected_data;
  ASSERT_TRUE(data.contents_equal(expected_data));
}

TEST_F(TestEntry, Constructor) {
  bufferlist data;
  data.append("data");
  journal::Entry entry(234, 123, data);

  data.clear();
  data = entry.get_data();

  bufferlist expected_data;
  expected_data.append("data");

  ASSERT_EQ(123U, entry.get_entry_tid());
  ASSERT_EQ(234U, entry.get_tag_tid());
  ASSERT_TRUE(data.contents_equal(expected_data));
}

TEST_F(TestEntry, IsReadable) {
  bufferlist data;
  data.append("data");
  journal::Entry entry(234, 123, data);

  bufferlist full_bl;
  ::encode(entry, full_bl);

  uint32_t bytes_needed;
  for (size_t i = 0; i < full_bl.length() - 1; ++i) {
    bufferlist partial_bl;
    if (i > 0) {
      partial_bl.substr_of(full_bl, 0, i);
    }
    ASSERT_FALSE(journal::Entry::is_readable(partial_bl.begin(),
                                             &bytes_needed));
    ASSERT_GT(bytes_needed, 0U);
  }
  ASSERT_TRUE(journal::Entry::is_readable(full_bl.begin(), &bytes_needed));
  ASSERT_EQ(0U, bytes_needed);
}

TEST_F(TestEntry, IsReadableBadPreamble) {
  bufferlist data;
  data.append("data");
  journal::Entry entry(234, 123, data);

  uint64_t stray_bytes = 0x1122334455667788;
  bufferlist full_bl;
  ::encode(stray_bytes, full_bl);
  ::encode(entry, full_bl);

  uint32_t bytes_needed;
  bufferlist::iterator it = full_bl.begin();
  ASSERT_FALSE(journal::Entry::is_readable(it, &bytes_needed));
  ASSERT_EQ(0U, bytes_needed);

  it.advance(sizeof(stray_bytes));
  ASSERT_TRUE(journal::Entry::is_readable(it, &bytes_needed));
  ASSERT_EQ(0U, bytes_needed);
}

TEST_F(TestEntry, IsReadableBadCRC) {
  bufferlist data;
  data.append("data");
  journal::Entry entry(234, 123, data);

  bufferlist full_bl;
  ::encode(entry, full_bl);

  bufferlist bad_bl;
  bad_bl.substr_of(full_bl, 0, full_bl.length() - 4);
  ::encode(full_bl.crc32c(1), bad_bl);

  uint32_t bytes_needed;
  ASSERT_FALSE(journal::Entry::is_readable(bad_bl.begin(), &bytes_needed));
  ASSERT_EQ(0U, bytes_needed);



}
