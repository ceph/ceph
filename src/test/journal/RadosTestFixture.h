// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados/test.h"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "journal/JournalMetadata.h"
#include "cls/journal/cls_journal_types.h"
#include "gtest/gtest.h"

class ThreadPool;

class RadosTestFixture : public ::testing::Test {
public:
  static void SetUpTestCase();
  static void TearDownTestCase();

  static std::string get_temp_oid();

  RadosTestFixture();
  void SetUp() override;
  void TearDown() override;

  int create(const std::string &oid, uint8_t order = 14,
             uint8_t splay_width = 2);
  ceph::ref_t<journal::JournalMetadata> create_metadata(const std::string &oid,
                                              const std::string &client_id = "client",
                                              double commit_internal = 0.1,
                                              int max_concurrent_object_sets = 0);
  int append(const std::string &oid, const bufferlist &bl);

  int client_register(const std::string &oid, const std::string &id = "client",
                      const std::string &description = "");
  int client_commit(const std::string &oid, const std::string &id,
                    const cls::journal::ObjectSetPosition &commit_position);

  bufferlist create_payload(const std::string &payload);

  struct Listener : public journal::JournalMetadataListener {
    RadosTestFixture *test_fixture;
    ceph::mutex mutex = ceph::make_mutex("mutex");
    ceph::condition_variable cond;
    std::map<journal::JournalMetadata*, uint32_t> updates;

    Listener(RadosTestFixture *_test_fixture)
      : test_fixture(_test_fixture) {}

    void handle_update(journal::JournalMetadata *metadata) override {
      std::lock_guard locker{mutex};
      ++updates[metadata];
      cond.notify_all();
    }
  };

  int init_metadata(const ceph::ref_t<journal::JournalMetadata>& metadata);

  bool wait_for_update(const ceph::ref_t<journal::JournalMetadata>& metadata);

  static std::string _pool_name;
  static librados::Rados _rados;
  static uint64_t _oid_number;
  static ThreadPool *_thread_pool;

  librados::IoCtx m_ioctx;

  ContextWQ *m_work_queue = nullptr;

  ceph::mutex m_timer_lock;
  SafeTimer *m_timer = nullptr;

  Listener m_listener;

  std::list<ceph::ref_t<journal::JournalMetadata>> m_metadatas;
};
