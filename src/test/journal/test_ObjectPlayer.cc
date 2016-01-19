// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectPlayer.h"
#include "journal/Entry.h"
#include "include/stringify.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "test/journal/RadosTestFixture.h"

class TestObjectPlayer : public RadosTestFixture {
public:
  journal::ObjectPlayerPtr create_object(const std::string &oid,
                                         uint8_t order) {
    journal::ObjectPlayerPtr object(new journal::ObjectPlayer(
      m_ioctx, oid + ".", 0, *m_timer, m_timer_lock, order));
    return object;
  }

  std::string get_object_name(const std::string &oid) {
    return oid + ".0";
  }
};

TEST_F(TestObjectPlayer, Fetch) {
  std::string oid = get_temp_oid();

  journal::Entry entry1("tag1", 123, create_payload(std::string(24, '1')));
  journal::Entry entry2("tag1", 124, create_payload(std::string(24, '1')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_LE(0, cond.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestObjectPlayer, FetchLarge) {
  std::string oid = get_temp_oid();

  journal::Entry entry1("tag1", 123,
                        create_payload(std::string(8192 - 33, '1')));
  journal::Entry entry2("tag1", 124, create_payload(""));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 12);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_LE(0, cond.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestObjectPlayer, FetchDeDup) {
  std::string oid = get_temp_oid();

  journal::Entry entry1("tag1", 123, create_payload(std::string(24, '1')));
  journal::Entry entry2("tag1", 123, create_payload(std::string(24, '2')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_LE(0, cond.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry2};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestObjectPlayer, FetchEmpty) {
  std::string oid = get_temp_oid();

  bufferlist bl;
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_EQ(-ENOENT, cond.wait());
  ASSERT_TRUE(object->empty());
}

TEST_F(TestObjectPlayer, FetchError) {
  std::string oid = get_temp_oid();

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_EQ(-ENOENT, cond.wait());
  ASSERT_TRUE(object->empty());
}

TEST_F(TestObjectPlayer, FetchCorrupt) {
  std::string oid = get_temp_oid();

  journal::Entry entry1("tag1", 123, create_payload(std::string(24, '1')));
  journal::Entry entry2("tag1", 124, create_payload(std::string(24, '2')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(create_payload("corruption"), bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_EQ(-EINVAL, cond.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestObjectPlayer, FetchAppend) {
  std::string oid = get_temp_oid();

  journal::Entry entry1("tag1", 123, create_payload(std::string(24, '1')));
  journal::Entry entry2("tag1", 124, create_payload(std::string(24, '2')));

  bufferlist bl;
  ::encode(entry1, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond1;
  object->fetch(&cond1);
  ASSERT_LE(0, cond1.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1};
  ASSERT_EQ(expected_entries, entries);

  bl.clear();
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  C_SaferCond cond2;
  object->fetch(&cond2);
  ASSERT_LE(0, cond2.wait());

  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestObjectPlayer, PopEntry) {
  std::string oid = get_temp_oid();

  journal::Entry entry1("tag1", 123, create_payload(std::string(24, '1')));
  journal::Entry entry2("tag1", 124, create_payload(std::string(24, '1')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond;
  object->fetch(&cond);
  ASSERT_LE(0, cond.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  journal::Entry entry;
  object->front(&entry);
  object->pop_front();
  ASSERT_EQ(entry1, entry);
  object->front(&entry);
  object->pop_front();
  ASSERT_EQ(entry2, entry);
  ASSERT_TRUE(object->empty());
}

TEST_F(TestObjectPlayer, Watch) {
  std::string oid = get_temp_oid();
  journal::ObjectPlayerPtr object = create_object(oid, 14);

  C_SaferCond cond1;
  object->watch(&cond1, 0.1);

  journal::Entry entry1("tag1", 123, create_payload(std::string(24, '1')));
  journal::Entry entry2("tag1", 124, create_payload(std::string(24, '1')));

  bufferlist bl;
  ::encode(entry1, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));
  ASSERT_LE(0, cond1.wait());

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries;
  expected_entries = {entry1};
  ASSERT_EQ(expected_entries, entries);

  C_SaferCond cond2;
  object->watch(&cond2, 0.1);

  bl.clear();
  ::encode(entry2, bl);
  ASSERT_EQ(0, append(get_object_name(oid), bl));
  ASSERT_LE(0, cond2.wait());

  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestObjectPlayer, Unwatch) {
  std::string oid = get_temp_oid();
  journal::ObjectPlayerPtr object = create_object(oid, 14);

  Mutex mutex("lock");
  Cond cond;
  bool done = false;
  int rval = 0;
  C_SafeCond *ctx = new C_SafeCond(&mutex, &cond, &done, &rval);
  object->watch(ctx, 0.1);

  usleep(200000);
  ASSERT_FALSE(done);
  object->unwatch();
}
