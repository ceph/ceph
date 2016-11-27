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

template <typename T>
class TestObjectPlayer : public RadosTestFixture {
public:
  static const uint32_t max_fetch_bytes = T::max_fetch_bytes;

  journal::ObjectPlayerPtr create_object(const std::string &oid,
                                         uint8_t order) {
    journal::ObjectPlayerPtr object(new journal::ObjectPlayer(
      m_ioctx, oid + ".", 0, *m_timer, m_timer_lock, order,
      max_fetch_bytes));
    return object;
  }

  int fetch(journal::ObjectPlayerPtr object_player) {
    while (true) {
      C_SaferCond ctx;
      object_player->set_refetch_state(
        journal::ObjectPlayer::REFETCH_STATE_NONE);
      object_player->fetch(&ctx);
      int r = ctx.wait();
      if (r < 0 || !object_player->refetch_required()) {
        return r;
      }
    }
    return 0;
  }

  int watch_and_wait_for_entries(journal::ObjectPlayerPtr object_player,
                                 journal::ObjectPlayer::Entries *entries,
                                 size_t count) {
    for (size_t i = 0; i < 50; ++i) {
      object_player->get_entries(entries);
      if (entries->size() == count) {
        break;
      }

      C_SaferCond ctx;
      object_player->watch(&ctx, 0.1);

      int r = ctx.wait();
      if (r < 0) {
        return r;
      }
    }
    return 0;
  }

  std::string get_object_name(const std::string &oid) {
    return oid + ".0";
  }
};

template <uint32_t _max_fetch_bytes>
struct TestObjectPlayerParams {
  static const uint32_t max_fetch_bytes = _max_fetch_bytes;
};

typedef ::testing::Types<TestObjectPlayerParams<0>,
                         TestObjectPlayerParams<10> > TestObjectPlayerTypes;
TYPED_TEST_CASE(TestObjectPlayer, TestObjectPlayerTypes);

TYPED_TEST(TestObjectPlayer, Fetch) {
  std::string oid = this->get_temp_oid();

  journal::Entry entry1(234, 123, this->create_payload(std::string(24, '1')));
  journal::Entry entry2(234, 124, this->create_payload(std::string(24, '1')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 14);
  ASSERT_LE(0, this->fetch(object));

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TYPED_TEST(TestObjectPlayer, FetchLarge) {
  std::string oid = this->get_temp_oid();

  journal::Entry entry1(234, 123,
                        this->create_payload(std::string(8192 - 32, '1')));
  journal::Entry entry2(234, 124, this->create_payload(""));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 12);
  ASSERT_LE(0, this->fetch(object));

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TYPED_TEST(TestObjectPlayer, FetchDeDup) {
  std::string oid = this->get_temp_oid();

  journal::Entry entry1(234, 123, this->create_payload(std::string(24, '1')));
  journal::Entry entry2(234, 123, this->create_payload(std::string(24, '2')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 14);
  ASSERT_LE(0, this->fetch(object));

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry2};
  ASSERT_EQ(expected_entries, entries);
}

TYPED_TEST(TestObjectPlayer, FetchEmpty) {
  std::string oid = this->get_temp_oid();

  bufferlist bl;
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 14);

  ASSERT_EQ(0, this->fetch(object));
  ASSERT_TRUE(object->empty());
}

TYPED_TEST(TestObjectPlayer, FetchCorrupt) {
  std::string oid = this->get_temp_oid();

  journal::Entry entry1(234, 123, this->create_payload(std::string(24, '1')));
  journal::Entry entry2(234, 124, this->create_payload(std::string(24, '2')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(this->create_payload("corruption"), bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 14);
  ASSERT_EQ(-EBADMSG, this->fetch(object));

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TYPED_TEST(TestObjectPlayer, FetchAppend) {
  std::string oid = this->get_temp_oid();

  journal::Entry entry1(234, 123, this->create_payload(std::string(24, '1')));
  journal::Entry entry2(234, 124, this->create_payload(std::string(24, '2')));

  bufferlist bl;
  ::encode(entry1, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 14);
  ASSERT_LE(0, this->fetch(object));

  journal::ObjectPlayer::Entries entries;
  object->get_entries(&entries);
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries = {entry1};
  ASSERT_EQ(expected_entries, entries);

  bl.clear();
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));
  ASSERT_LE(0, this->fetch(object));

  object->get_entries(&entries);
  ASSERT_EQ(2U, entries.size());

  expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TYPED_TEST(TestObjectPlayer, PopEntry) {
  std::string oid = this->get_temp_oid();

  journal::Entry entry1(234, 123, this->create_payload(std::string(24, '1')));
  journal::Entry entry2(234, 124, this->create_payload(std::string(24, '1')));

  bufferlist bl;
  ::encode(entry1, bl);
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));

  journal::ObjectPlayerPtr object = this->create_object(oid, 14);
  ASSERT_LE(0, this->fetch(object));

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

TYPED_TEST(TestObjectPlayer, Watch) {
  std::string oid = this->get_temp_oid();
  journal::ObjectPlayerPtr object = this->create_object(oid, 14);

  C_SaferCond cond1;
  object->watch(&cond1, 0.1);

  journal::Entry entry1(234, 123, this->create_payload(std::string(24, '1')));
  journal::Entry entry2(234, 124, this->create_payload(std::string(24, '1')));

  bufferlist bl;
  ::encode(entry1, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));
  ASSERT_LE(0, cond1.wait());

  journal::ObjectPlayer::Entries entries;
  ASSERT_EQ(0, this->watch_and_wait_for_entries(object, &entries, 1U));
  ASSERT_EQ(1U, entries.size());

  journal::ObjectPlayer::Entries expected_entries;
  expected_entries = {entry1};
  ASSERT_EQ(expected_entries, entries);

  C_SaferCond cond2;
  object->watch(&cond2, 0.1);

  bl.clear();
  ::encode(entry2, bl);
  ASSERT_EQ(0, this->append(this->get_object_name(oid), bl));
  ASSERT_LE(0, cond2.wait());

  ASSERT_EQ(0, this->watch_and_wait_for_entries(object, &entries, 2U));
  ASSERT_EQ(2U, entries.size());

  expected_entries = {entry1, entry2};
  ASSERT_EQ(expected_entries, entries);
}

TYPED_TEST(TestObjectPlayer, Unwatch) {
  std::string oid = this->get_temp_oid();
  journal::ObjectPlayerPtr object = this->create_object(oid, 14);

  C_SaferCond watch_ctx;
  object->watch(&watch_ctx, 600);

  usleep(200000);

  object->unwatch();
  ASSERT_EQ(-ECANCELED, watch_ctx.wait());
}
