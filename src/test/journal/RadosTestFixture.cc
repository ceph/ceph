// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados/test_cxx.h"
#include "test/journal/RadosTestFixture.h"
#include "cls/journal/cls_journal_client.h"
#include "include/stringify.h"
#include "common/WorkQueue.h"
#include "journal/Settings.h"

using namespace std::chrono_literals;

RadosTestFixture::RadosTestFixture()
  : m_timer_lock(ceph::make_mutex("m_timer_lock")),
    m_listener(this) {
}

void RadosTestFixture::SetUpTestCase() {
  _pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));

  CephContext* cct = reinterpret_cast<CephContext*>(_rados.cct());
  _thread_pool = new ThreadPool(cct, "RadosTestFixture::_thread_pool",
                                 "tp_test", 1);
  _thread_pool->start();
}

void RadosTestFixture::TearDownTestCase() {
  _thread_pool->stop();
  delete _thread_pool;

  ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
}

std::string RadosTestFixture::get_temp_oid() {
  ++_oid_number;
  return "oid" + stringify(_oid_number);
}

void RadosTestFixture::SetUp() {
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), m_ioctx));

  CephContext* cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  m_work_queue = new ContextWQ("RadosTestFixture::m_work_queue",
                               ceph::make_timespan(60),
                               _thread_pool);

  m_timer = new SafeTimer(cct, m_timer_lock, true);
  m_timer->init();
}

void RadosTestFixture::TearDown() {
  for (auto metadata : m_metadatas) {
    C_SaferCond ctx;
    metadata->shut_down(&ctx);
    ASSERT_EQ(0, ctx.wait());
  }

  {
    std::lock_guard locker{m_timer_lock};
    m_timer->shutdown();
  }
  delete m_timer;

  m_work_queue->drain();
  delete m_work_queue;
}

int RadosTestFixture::create(const std::string &oid, uint8_t order,
                             uint8_t splay_width) {
  return cls::journal::client::create(m_ioctx, oid, order, splay_width, -1);
}

ceph::ref_t<journal::JournalMetadata> RadosTestFixture::create_metadata(
    const std::string &oid, const std::string &client_id,
    double commit_interval, int max_concurrent_object_sets) {
  journal::Settings settings;
  settings.commit_interval = commit_interval;
  settings.max_concurrent_object_sets = max_concurrent_object_sets;

  auto metadata = ceph::make_ref<journal::JournalMetadata>(
    m_work_queue, m_timer, &m_timer_lock, m_ioctx, oid, client_id, settings);
  m_metadatas.push_back(metadata);
  return metadata;
}

int RadosTestFixture::append(const std::string &oid, const bufferlist &bl) {
  librados::ObjectWriteOperation op;
  op.append(bl);
  return m_ioctx.operate(oid, &op);
}

int RadosTestFixture::client_register(const std::string &oid,
                                      const std::string &id,
                                      const std::string &description) {
  bufferlist data;
  data.append(description);
  return cls::journal::client::client_register(m_ioctx, oid, id, data);
}

int RadosTestFixture::client_commit(const std::string &oid,
                                    const std::string &id,
                                    const cls::journal::ObjectSetPosition &commit_position) {
  librados::ObjectWriteOperation op;
  cls::journal::client::client_commit(&op, id, commit_position);
  return m_ioctx.operate(oid, &op);
}

bufferlist RadosTestFixture::create_payload(const std::string &payload) {
  bufferlist bl;
  bl.append(payload);
  return bl;
}

int RadosTestFixture::init_metadata(const ceph::ref_t<journal::JournalMetadata>& metadata) {
  C_SaferCond cond;
  metadata->init(&cond);
  return cond.wait();
}

bool RadosTestFixture::wait_for_update(const ceph::ref_t<journal::JournalMetadata>& metadata) {
  std::unique_lock locker{m_listener.mutex};
  while (m_listener.updates[metadata.get()] == 0) {
    if (m_listener.cond.wait_for(locker, 10s) == std::cv_status::timeout) {
      return false;
    }
  }
  --m_listener.updates[metadata.get()];
  return true;
}

std::string RadosTestFixture::_pool_name;
librados::Rados RadosTestFixture::_rados;
uint64_t RadosTestFixture::_oid_number = 0;
ThreadPool *RadosTestFixture::_thread_pool = nullptr;
