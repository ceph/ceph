// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_client.h"
#include "include/stringify.h"
#include "common/Cond.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <errno.h>
#include <set>
#include <string>
#include <boost/assign/list_of.hpp>

using namespace cls::journal;

class TestClsJournal : public ::testing::Test {
public:

  static void SetUpTestCase() {
    _pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));
  }

  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
  }

  std::string get_temp_image_name() {
    ++_image_number;
    return "image" + stringify(_image_number);
  }

  static std::string _pool_name;
  static librados::Rados _rados;
  static uint64_t _image_number;

};

std::string TestClsJournal::_pool_name;
librados::Rados TestClsJournal::_rados;
uint64_t TestClsJournal::_image_number = 0;

TEST_F(TestClsJournal, Create) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  uint8_t order = 1;
  uint8_t splay_width = 2;
  ASSERT_EQ(0, client::create(ioctx, oid, order, splay_width));

  uint8_t read_order;
  uint8_t read_splay_width;
  C_SaferCond cond;
  client::get_immutable_metadata(ioctx, oid, &read_order, &read_splay_width,
                                 &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(order, read_order);
  ASSERT_EQ(splay_width, read_splay_width);
}

TEST_F(TestClsJournal, MinimumSet) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4));

  librados::ObjectWriteOperation op1;
  client::set_active_set(&op1, 300);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  uint64_t minimum_set = 123;
  librados::ObjectWriteOperation op2;
  client::set_minimum_set(&op2, minimum_set);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  C_SaferCond cond;
  uint64_t read_minimum_set;
  uint64_t read_active_set;
  std::set<cls::journal::Client> read_clients;
  client::get_mutable_metadata(ioctx, oid, &read_minimum_set, &read_active_set,
                               &read_clients, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(minimum_set, read_minimum_set);
}

TEST_F(TestClsJournal, MinimumSetStale) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4));

  librados::ObjectWriteOperation op1;
  client::set_active_set(&op1, 300);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  uint64_t minimum_set = 123;
  librados::ObjectWriteOperation op2;
  client::set_minimum_set(&op2, minimum_set);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  librados::ObjectWriteOperation op3;
  client::set_minimum_set(&op3, 1);
  ASSERT_EQ(-ESTALE, ioctx.operate(oid, &op3));

  C_SaferCond cond;
  uint64_t read_minimum_set;
  uint64_t read_active_set;
  std::set<cls::journal::Client> read_clients;
  client::get_mutable_metadata(ioctx, oid, &read_minimum_set, &read_active_set,
                               &read_clients, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(minimum_set, read_minimum_set);
}

TEST_F(TestClsJournal, MinimumSetOrderConstraint) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4));

  librados::ObjectWriteOperation op1;
  client::set_minimum_set(&op1, 123);
  ASSERT_EQ(-EINVAL, ioctx.operate(oid, &op1));

  C_SaferCond cond;
  uint64_t read_minimum_set;
  uint64_t read_active_set;
  std::set<cls::journal::Client> read_clients;
  client::get_mutable_metadata(ioctx, oid, &read_minimum_set, &read_active_set,
                               &read_clients, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(0U, read_minimum_set);
}

TEST_F(TestClsJournal, ActiveSet) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4));

  uint64_t active_set = 234;
  librados::ObjectWriteOperation op1;
  client::set_active_set(&op1, active_set);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  C_SaferCond cond;
  uint64_t read_minimum_set;
  uint64_t read_active_set;
  std::set<cls::journal::Client> read_clients;
  client::get_mutable_metadata(ioctx, oid, &read_minimum_set, &read_active_set,
                               &read_clients, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(active_set, read_active_set);
}

TEST_F(TestClsJournal, ActiveSetStale) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4));

  librados::ObjectWriteOperation op1;
  client::set_active_set(&op1, 345);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  client::set_active_set(&op2, 3);
  ASSERT_EQ(-ESTALE, ioctx.operate(oid, &op2));
}

TEST_F(TestClsJournal, CreateDuplicate) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4));
  ASSERT_EQ(-EEXIST, client::create(ioctx, oid, 3, 5));
}

TEST_F(TestClsJournal, ClientRegister) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", "desc1"));

  std::set<Client> clients;
  ASSERT_EQ(0, client::client_list(ioctx, oid, &clients));

  std::set<Client> expected_clients = boost::assign::list_of(
    Client("id1", "desc1"));
  ASSERT_EQ(expected_clients, clients);
}

TEST_F(TestClsJournal, ClientRegisterDuplicate) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", "desc1"));
  ASSERT_EQ(-EEXIST, client::client_register(ioctx, oid, "id1", "desc2"));
}

TEST_F(TestClsJournal, ClientUnregister) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", "desc1"));
  ASSERT_EQ(0, client::client_unregister(ioctx, oid, "id1"));
}

TEST_F(TestClsJournal, ClientUnregisterDNE) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", "desc1"));
  ASSERT_EQ(0, client::client_unregister(ioctx, oid, "id1"));
  ASSERT_EQ(-ENOENT, client::client_unregister(ioctx, oid, "id1"));
}

TEST_F(TestClsJournal, ClientCommit) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", "desc1"));

  cls::journal::EntryPositions entry_positions;
  entry_positions = boost::assign::list_of(
    cls::journal::EntryPosition("tag1", 120))(
    cls::journal::EntryPosition("tag2", 121));
  cls::journal::ObjectSetPosition object_set_position(
    1, entry_positions);

  librados::ObjectWriteOperation op2;
  client::client_commit(&op2, "id1", object_set_position);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  std::set<Client> clients;
  ASSERT_EQ(0, client::client_list(ioctx, oid, &clients));

  std::set<Client> expected_clients = boost::assign::list_of(
    Client("id1", "desc1", object_set_position));
  ASSERT_EQ(expected_clients, clients);
}

TEST_F(TestClsJournal, ClientCommitInvalid) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", "desc1"));

  cls::journal::EntryPositions entry_positions;
  entry_positions = boost::assign::list_of(
    cls::journal::EntryPosition("tag1", 120))(
    cls::journal::EntryPosition("tag1", 121))(
    cls::journal::EntryPosition("tag2", 121));
  cls::journal::ObjectSetPosition object_set_position(
    1, entry_positions);

  librados::ObjectWriteOperation op2;
  client::client_commit(&op2, "id1", object_set_position);
  ASSERT_EQ(-EINVAL, ioctx.operate(oid, &op2));
}

TEST_F(TestClsJournal, ClientCommitDNE) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  cls::journal::ObjectSetPosition object_set_position;

  librados::ObjectWriteOperation op1;
  client::client_commit(&op1, "id1", object_set_position);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, &op1));
}

TEST_F(TestClsJournal, ClientList) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 12, 5));

  std::set<Client> expected_clients;
  librados::ObjectWriteOperation op1;
  for (uint32_t i = 0; i < 512; ++i) {
    std::string id =  "id" + stringify(i + 1);
    expected_clients.insert(Client(id, ""));
    client::client_register(&op1, id, "");
  }
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  std::set<Client> clients;
  ASSERT_EQ(0, client::client_list(ioctx, oid, &clients));
  ASSERT_EQ(expected_clients, clients);

  C_SaferCond cond;
  uint64_t read_minimum_set;
  uint64_t read_active_set;
  std::set<cls::journal::Client> read_clients;
  client::get_mutable_metadata(ioctx, oid, &read_minimum_set, &read_active_set,
                               &read_clients, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(expected_clients, read_clients);
}

TEST_F(TestClsJournal, GuardAppend) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  bufferlist bl;
  bl.append("journal entry!");

  librados::ObjectWriteOperation op1;
  op1.append(bl);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  client::guard_append(&op2, 1024);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));
}

TEST_F(TestClsJournal, GuardAppendDNE) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  librados::ObjectWriteOperation op2;
  client::guard_append(&op2, 1024);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));
}

TEST_F(TestClsJournal, GuardAppendOverflow) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  bufferlist bl;
  bl.append("journal entry!");

  librados::ObjectWriteOperation op1;
  op1.append(bl);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  client::guard_append(&op2, 1);
  ASSERT_EQ(-EOVERFLOW, ioctx.operate(oid, &op2));
}
