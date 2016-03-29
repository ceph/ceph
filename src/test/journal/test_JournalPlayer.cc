// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalPlayer.h"
#include "journal/Entry.h"
#include "journal/JournalMetadata.h"
#include "journal/ReplayHandler.h"
#include "include/stringify.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "gtest/gtest.h"
#include "test/journal/RadosTestFixture.h"
#include <list>

class TestJournalPlayer : public RadosTestFixture {
public:
  typedef std::list<journal::JournalPlayer *> JournalPlayers;
  typedef std::list<journal::Entry> Entries;

  struct ReplayHandler : public journal::ReplayHandler {
    Mutex lock;
    Cond cond;
    bool entries_available;
    bool complete;
    int complete_result;

    ReplayHandler()
      : lock("lock"), entries_available(false), complete(false),
        complete_result(0) {}

    virtual void get() {}
    virtual void put() {}

    virtual void handle_entries_available() {
      Mutex::Locker locker(lock);
      entries_available = true;
      cond.Signal();
    }

    virtual void handle_complete(int r) {
      Mutex::Locker locker(lock);
      complete = true;
      complete_result = r;
      cond.Signal();
    }
  };

  virtual void TearDown() {
    for (JournalPlayers::iterator it = m_players.begin();
         it != m_players.end(); ++it) {
      delete *it;
    }
    RadosTestFixture::TearDown();
  }

  int client_commit(const std::string &oid,
                    journal::JournalPlayer::ObjectSetPosition position) {
    return RadosTestFixture::client_commit(oid, "client", position);
  }

  journal::Entry create_entry(uint64_t tag_tid, uint64_t entry_tid) {
    bufferlist payload_bl;
    payload_bl.append("playload");
    return journal::Entry(tag_tid, entry_tid, payload_bl);
  }

  journal::JournalPlayer *create_player(const std::string &oid,
                                          const journal::JournalMetadataPtr &metadata) {
    journal::JournalPlayer *player(new journal::JournalPlayer(
      m_ioctx, oid + ".", metadata, &m_replay_hander));
    m_players.push_back(player);
    return player;
  }

  bool wait_for_entries(journal::JournalPlayer *player, uint32_t count,
                        Entries *entries) {
    entries->clear();
    while (entries->size() < count) {
      journal::Entry entry;
      uint64_t commit_tid;
      while (entries->size() < count &&
             player->try_pop_front(&entry, &commit_tid)) {
        entries->push_back(entry);
      }
      if (entries->size() == count) {
        break;
      }

      Mutex::Locker locker(m_replay_hander.lock);
      if (m_replay_hander.entries_available) {
        m_replay_hander.entries_available = false;
      } else if (m_replay_hander.cond.WaitInterval(
          reinterpret_cast<CephContext*>(m_ioctx.cct()),
          m_replay_hander.lock, utime_t(10, 0)) != 0) {
        break;
      }
    }
    return entries->size() == count;
  }

  bool wait_for_complete(journal::JournalPlayer *player) {
    journal::Entry entry;
    uint64_t commit_tid;
    player->try_pop_front(&entry, &commit_tid);

    Mutex::Locker locker(m_replay_hander.lock);
    while (!m_replay_hander.complete) {
      if (m_replay_hander.cond.WaitInterval(
            reinterpret_cast<CephContext*>(m_ioctx.cct()),
            m_replay_hander.lock, utime_t(10, 0)) != 0) {
        return false;
      }
    }
    m_replay_hander.complete = false;
    return true;
  }

  int write_entry(const std::string &oid, uint64_t object_num,
                  uint64_t tag_tid, uint64_t entry_tid) {
    bufferlist bl;
    ::encode(create_entry(tag_tid, entry_tid), bl);
    return append(oid + "." + stringify(object_num), bl);
  }

  JournalPlayers m_players;
  ReplayHandler m_replay_hander;
};

TEST_F(TestJournalPlayer, Prefetch) {
  std::string oid = get_temp_oid();

  journal::JournalPlayer::ObjectPositions positions;
  positions = {
    cls::journal::ObjectPosition(0, 234, 122) };
  cls::journal::ObjectSetPosition commit_position(positions);

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 122));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 123));
  ASSERT_EQ(0, write_entry(oid, 0, 234, 124));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 125));

  player->prefetch();

  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 3, &entries));
  ASSERT_TRUE(wait_for_complete(player));

  Entries expected_entries;
  expected_entries = {
    create_entry(234, 123),
    create_entry(234, 124),
    create_entry(234, 125)};
  ASSERT_EQ(expected_entries, entries);

  uint64_t last_tid;
  ASSERT_TRUE(metadata->get_last_allocated_entry_tid(234, &last_tid));
  ASSERT_EQ(125U, last_tid);
}

TEST_F(TestJournalPlayer, PrefetchSkip) {
  std::string oid = get_temp_oid();

  journal::JournalPlayer::ObjectPositions positions;
  positions = {
    cls::journal::ObjectPosition(0, 234, 125),
    cls::journal::ObjectPosition(1, 234, 124) };
  cls::journal::ObjectSetPosition commit_position(positions);

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 122));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 123));
  ASSERT_EQ(0, write_entry(oid, 0, 234, 124));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 125));

  player->prefetch();

  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 0, &entries));
  ASSERT_TRUE(wait_for_complete(player));

  uint64_t last_tid;
  ASSERT_TRUE(metadata->get_last_allocated_entry_tid(234, &last_tid));
  ASSERT_EQ(125U, last_tid);
}

TEST_F(TestJournalPlayer, PrefetchWithoutCommit) {
  std::string oid = get_temp_oid();

  cls::journal::ObjectSetPosition commit_position;

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 122));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 123));

  player->prefetch();

  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 2, &entries));
  ASSERT_TRUE(wait_for_complete(player));

  Entries expected_entries;
  expected_entries = {
    create_entry(234, 122),
    create_entry(234, 123)};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestJournalPlayer, PrefetchMultipleTags) {
  std::string oid = get_temp_oid();

  journal::JournalPlayer::ObjectPositions positions;
  positions = {
    cls::journal::ObjectPosition(2, 234, 122),
    cls::journal::ObjectPosition(1, 234, 121),
    cls::journal::ObjectPosition(0, 234, 120)};
  cls::journal::ObjectSetPosition commit_position(positions);

  ASSERT_EQ(0, create(oid, 14, 3));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 120));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 121));
  ASSERT_EQ(0, write_entry(oid, 2, 234, 122));
  ASSERT_EQ(0, write_entry(oid, 0, 234, 123));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 124));
  ASSERT_EQ(0, write_entry(oid, 0, 236, 0)); // new tag allocated

  player->prefetch();

  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 3, &entries));
  ASSERT_TRUE(wait_for_complete(player));

  uint64_t last_tid;
  ASSERT_TRUE(metadata->get_last_allocated_entry_tid(234, &last_tid));
  ASSERT_EQ(124U, last_tid);
  ASSERT_TRUE(metadata->get_last_allocated_entry_tid(236, &last_tid));
  ASSERT_EQ(0U, last_tid);
}

TEST_F(TestJournalPlayer, PrefetchCorruptSequence) {
  std::string oid = get_temp_oid();

  cls::journal::ObjectSetPosition commit_position;

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 120));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 121));
  ASSERT_EQ(0, write_entry(oid, 0, 234, 124));

  player->prefetch();
  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 2, &entries));

  journal::Entry entry;
  uint64_t commit_tid;
  ASSERT_FALSE(player->try_pop_front(&entry, &commit_tid));
  ASSERT_TRUE(wait_for_complete(player));
  ASSERT_EQ(-ENOMSG, m_replay_hander.complete_result);
}

TEST_F(TestJournalPlayer, PrefetchUnexpectedTag) {
  std::string oid = get_temp_oid();

  cls::journal::ObjectSetPosition commit_position;

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 120));
  ASSERT_EQ(0, write_entry(oid, 1, 235, 121));
  ASSERT_EQ(0, write_entry(oid, 0, 234, 124));

  player->prefetch();
  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 1, &entries));

  journal::Entry entry;
  uint64_t commit_tid;
  ASSERT_FALSE(player->try_pop_front(&entry, &commit_tid));
  ASSERT_TRUE(wait_for_complete(player));
  ASSERT_EQ(-ENOMSG, m_replay_hander.complete_result);
}

TEST_F(TestJournalPlayer, PrefetchAndWatch) {
  std::string oid = get_temp_oid();

  journal::JournalPlayer::ObjectPositions positions;
  positions = {
    cls::journal::ObjectPosition(0, 234, 122)};
  cls::journal::ObjectSetPosition commit_position(positions);

  ASSERT_EQ(0, create(oid));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 122));

  player->prefetch_and_watch(0.25);

  Entries entries;
  ASSERT_EQ(0, write_entry(oid, 1, 234, 123));
  ASSERT_TRUE(wait_for_entries(player, 1, &entries));

  Entries expected_entries;
  expected_entries = {create_entry(234, 123)};
  ASSERT_EQ(expected_entries, entries);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 124));
  ASSERT_TRUE(wait_for_entries(player, 1, &entries));

  expected_entries = {create_entry(234, 124)};
  ASSERT_EQ(expected_entries, entries);
}

TEST_F(TestJournalPlayer, PrefetchSkippedObject) {
  std::string oid = get_temp_oid();

  cls::journal::ObjectSetPosition commit_position;

  ASSERT_EQ(0, create(oid, 14, 3));
  ASSERT_EQ(0, client_register(oid));
  ASSERT_EQ(0, client_commit(oid, commit_position));

  journal::JournalMetadataPtr metadata = create_metadata(oid);
  ASSERT_EQ(0, init_metadata(metadata));
  metadata->set_active_set(2);

  journal::JournalPlayer *player = create_player(oid, metadata);

  ASSERT_EQ(0, write_entry(oid, 0, 234, 122));
  ASSERT_EQ(0, write_entry(oid, 1, 234, 123));
  ASSERT_EQ(0, write_entry(oid, 5, 234, 124));
  ASSERT_EQ(0, write_entry(oid, 6, 234, 125));
  ASSERT_EQ(0, write_entry(oid, 7, 234, 126));

  player->prefetch();

  Entries entries;
  ASSERT_TRUE(wait_for_entries(player, 5, &entries));
  ASSERT_TRUE(wait_for_complete(player));

  Entries expected_entries;
  expected_entries = {
    create_entry(234, 122),
    create_entry(234, 123),
    create_entry(234, 124),
    create_entry(234, 125),
    create_entry(234, 126)};
  ASSERT_EQ(expected_entries, entries);

  uint64_t last_tid;
  ASSERT_TRUE(metadata->get_last_allocated_entry_tid(234, &last_tid));
  ASSERT_EQ(126U, last_tid);
}
