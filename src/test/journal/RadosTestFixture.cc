// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/journal/RadosTestFixture.h"
#include "cls/journal/cls_journal_client.h"
#include "include/stringify.h"

RadosTestFixture::RadosTestFixture()
  : m_timer_lock("m_timer_lock"), m_timer(NULL), m_listener(this) {
}

void RadosTestFixture::SetUpTestCase() {
  _pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));
}

void RadosTestFixture::TearDownTestCase() {
  ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
}

std::string RadosTestFixture::get_temp_oid() {
  ++_oid_number;
  return "oid" + stringify(_oid_number);
}

void RadosTestFixture::SetUp() {
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), m_ioctx));
  m_timer = new SafeTimer(reinterpret_cast<CephContext*>(m_ioctx.cct()),
                          m_timer_lock, false);
  m_timer->init();
}

void RadosTestFixture::TearDown() {
  {
    Mutex::Locker locker(m_timer_lock);
    m_timer->shutdown();
  }
  delete m_timer;
}

int RadosTestFixture::create(const std::string &oid, uint8_t order,
                             uint8_t splay_width) {
  return cls::journal::client::create(m_ioctx, oid, order, splay_width);
}

int RadosTestFixture::append(const std::string &oid, const bufferlist &bl) {
  librados::ObjectWriteOperation op;
  op.append(bl);
  return m_ioctx.operate(oid, &op);
}

int RadosTestFixture::client_register(const std::string &oid,
                                      const std::string &id,
                                      const std::string &description) {
  return cls::journal::client::client_register(m_ioctx, oid, id, description);
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

int RadosTestFixture::init_metadata(journal::JournalMetadataPtr metadata) {
  C_SaferCond cond;
  metadata->init(&cond);
  return cond.wait();
}

bool RadosTestFixture::wait_for_update(journal::JournalMetadataPtr metadata) {
  Mutex::Locker locker(m_listener.mutex);
  while (m_listener.updates[metadata.get()] == 0) {
    if (m_listener.cond.WaitInterval(
          reinterpret_cast<CephContext*>(m_ioctx.cct()),
          m_listener.mutex, utime_t(10, 0)) != 0) {
      return false;
    }
  }
  --m_listener.updates[metadata.get()];
  return true;
}

std::string RadosTestFixture::_pool_name;
librados::Rados RadosTestFixture::_rados;
uint64_t RadosTestFixture::_oid_number = 0;
