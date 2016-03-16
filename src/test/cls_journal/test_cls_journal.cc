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
  int64_t pool_id = ioctx.get_id();
  ASSERT_EQ(0, client::create(ioctx, oid, order, splay_width, pool_id));

  uint8_t read_order;
  uint8_t read_splay_width;
  int64_t read_pool_id;
  C_SaferCond cond;
  client::get_immutable_metadata(ioctx, oid, &read_order, &read_splay_width,
                                 &read_pool_id, &cond);
  ASSERT_EQ(0, cond.wait());
  ASSERT_EQ(order, read_order);
  ASSERT_EQ(splay_width, read_splay_width);
  ASSERT_EQ(pool_id, read_pool_id);
}

TEST_F(TestClsJournal, MinimumSet) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4, ioctx.get_id()));

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

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4, ioctx.get_id()));

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

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4, ioctx.get_id()));

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

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4, ioctx.get_id()));

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

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4, ioctx.get_id()));

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

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 4, ioctx.get_id()));
  ASSERT_EQ(-EEXIST, client::create(ioctx, oid, 3, 5, ioctx.get_id()));
}

TEST_F(TestClsJournal, GetClient) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  Client client;
  ASSERT_EQ(-ENOENT, client::get_client(ioctx, oid, "id", &client));

  bufferlist data;
  data.append(std::string('1', 128));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", data));

  ASSERT_EQ(0, client::get_client(ioctx, oid, "id1", &client));
  Client expected_client("id1", data);
  ASSERT_EQ(expected_client, client);
}

TEST_F(TestClsJournal, ClientRegister) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  std::set<Client> clients;
  ASSERT_EQ(0, client::client_list(ioctx, oid, &clients));

  std::set<Client> expected_clients = {Client("id1", bufferlist())};
  ASSERT_EQ(expected_clients, clients);
}

TEST_F(TestClsJournal, ClientRegisterDuplicate) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));
  ASSERT_EQ(-EEXIST, client::client_register(ioctx, oid, "id1", bufferlist()));
}

TEST_F(TestClsJournal, ClientUpdateData) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(-ENOENT, client::client_update_data(ioctx, oid, "id1",
                                                bufferlist()));

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  bufferlist data;
  data.append(std::string('1', 128));
  ASSERT_EQ(0, client::client_update_data(ioctx, oid, "id1", data));

  Client client;
  ASSERT_EQ(0, client::get_client(ioctx, oid, "id1", &client));
  Client expected_client("id1", data);
  ASSERT_EQ(expected_client, client);
}

TEST_F(TestClsJournal, ClientUpdateState) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(-ENOENT, client::client_update_state(ioctx, oid, "id1",
                                                 CLIENT_STATE_DISCONNECTED));

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  bufferlist data;
  data.append(std::string('1', 128));
  ASSERT_EQ(0, client::client_update_state(ioctx, oid, "id1",
                                           CLIENT_STATE_DISCONNECTED));

  Client client;
  ASSERT_EQ(0, client::get_client(ioctx, oid, "id1", &client));
  Client expected_client;
  expected_client.id = "id1";
  expected_client.state = CLIENT_STATE_DISCONNECTED;
  ASSERT_EQ(expected_client, client);
}

TEST_F(TestClsJournal, ClientUnregister) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));
  ASSERT_EQ(0, client::client_unregister(ioctx, oid, "id1"));
}

TEST_F(TestClsJournal, ClientUnregisterDNE) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));
  ASSERT_EQ(0, client::client_unregister(ioctx, oid, "id1"));
  ASSERT_EQ(-ENOENT, client::client_unregister(ioctx, oid, "id1"));
}

TEST_F(TestClsJournal, ClientUnregisterPruneTags) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id2", bufferlist()));

  ASSERT_EQ(0, client::tag_create(ioctx, oid, 0, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(0, client::tag_create(ioctx, oid, 1, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(0, client::tag_create(ioctx, oid, 2, 1, bufferlist()));

  librados::ObjectWriteOperation op1;
  client::client_commit(&op1, "id1", {{{1, 2, 120}}});
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  ASSERT_EQ(0, client::client_unregister(ioctx, oid, "id2"));

  std::set<Tag> expected_tags = {{0, 0, {}}, {2, 1, {}}};
  std::set<Tag> tags;
  ASSERT_EQ(0, client::tag_list(ioctx, oid, "id1",
                                boost::optional<uint64_t>(), &tags));
  ASSERT_EQ(expected_tags, tags);
}

TEST_F(TestClsJournal, ClientCommit) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  cls::journal::ObjectPositions object_positions;
  object_positions = {
    cls::journal::ObjectPosition(0, 234, 120),
    cls::journal::ObjectPosition(3, 235, 121)};
  cls::journal::ObjectSetPosition object_set_position(
    object_positions);

  librados::ObjectWriteOperation op2;
  client::client_commit(&op2, "id1", object_set_position);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  std::set<Client> clients;
  ASSERT_EQ(0, client::client_list(ioctx, oid, &clients));

  std::set<Client> expected_clients = {
    Client("id1", bufferlist(), object_set_position)};
  ASSERT_EQ(expected_clients, clients);
}

TEST_F(TestClsJournal, ClientCommitInvalid) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  cls::journal::ObjectPositions object_positions;
  object_positions = {
    cls::journal::ObjectPosition(0, 234, 120),
    cls::journal::ObjectPosition(4, 234, 121),
    cls::journal::ObjectPosition(5, 235, 121)};
  cls::journal::ObjectSetPosition object_set_position(
    object_positions);

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

  ASSERT_EQ(0, client::create(ioctx, oid, 12, 5, ioctx.get_id()));

  std::set<Client> expected_clients;
  librados::ObjectWriteOperation op1;
  for (uint32_t i = 0; i < 512; ++i) {
    std::string id =  "id" + stringify(i + 1);
    expected_clients.insert(Client(id, bufferlist()));
    client::client_register(&op1, id, bufferlist());
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

TEST_F(TestClsJournal, GetNextTagTid) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  uint64_t tag_tid;
  ASSERT_EQ(-ENOENT, client::get_next_tag_tid(ioctx, oid, &tag_tid));

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  ASSERT_EQ(0, client::get_next_tag_tid(ioctx, oid, &tag_tid));
  ASSERT_EQ(0U, tag_tid);

  ASSERT_EQ(0, client::tag_create(ioctx, oid, 0, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(0, client::get_next_tag_tid(ioctx, oid, &tag_tid));
  ASSERT_EQ(1U, tag_tid);
}

TEST_F(TestClsJournal, TagCreate) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(-ENOENT, client::tag_create(ioctx, oid, 0, Tag::TAG_CLASS_NEW,
                                        bufferlist()));

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  ASSERT_EQ(-ESTALE, client::tag_create(ioctx, oid, 1, Tag::TAG_CLASS_NEW,
                                        bufferlist()));
  ASSERT_EQ(-EINVAL, client::tag_create(ioctx, oid, 0, 1, bufferlist()));

  ASSERT_EQ(0, client::tag_create(ioctx, oid, 0, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(-EEXIST, client::tag_create(ioctx, oid, 0, Tag::TAG_CLASS_NEW,
                                        bufferlist()));
  ASSERT_EQ(0, client::tag_create(ioctx, oid, 1, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(0, client::tag_create(ioctx, oid, 2, 1, bufferlist()));

  std::set<Tag> expected_tags = {
    {0, 0, {}}, {1, 1, {}}, {2, 1, {}}};
  std::set<Tag> tags;
  ASSERT_EQ(0, client::tag_list(ioctx, oid, "id1",
                                boost::optional<uint64_t>(), &tags));
  ASSERT_EQ(expected_tags, tags);
}

TEST_F(TestClsJournal, TagCreatePrunesTags) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  ASSERT_EQ(0, client::tag_create(ioctx, oid, 0, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(0, client::tag_create(ioctx, oid, 1, Tag::TAG_CLASS_NEW,
                                  bufferlist()));
  ASSERT_EQ(0, client::tag_create(ioctx, oid, 2, 1, bufferlist()));

  librados::ObjectWriteOperation op1;
  client::client_commit(&op1, "id1", {{{1, 2, 120}}});
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  ASSERT_EQ(0, client::tag_create(ioctx, oid, 3, 0, bufferlist()));

  std::set<Tag> expected_tags = {
    {0, 0, {}}, {2, 1, {}}, {3, 0, {}}};
  std::set<Tag> tags;
  ASSERT_EQ(0, client::tag_list(ioctx, oid, "id1",
                                boost::optional<uint64_t>(), &tags));
  ASSERT_EQ(expected_tags, tags);
}

TEST_F(TestClsJournal, TagList) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  std::string oid = get_temp_image_name();

  ASSERT_EQ(0, client::create(ioctx, oid, 2, 2, ioctx.get_id()));
  ASSERT_EQ(0, client::client_register(ioctx, oid, "id1", bufferlist()));

  std::set<Tag> expected_all_tags;
  std::set<Tag> expected_filtered_tags;
  for (uint32_t i = 0; i < 96; ++i) {
    uint64_t tag_class = Tag::TAG_CLASS_NEW;
    if (i > 1) {
      tag_class = i % 2 == 0 ? 0 : 1;
    }

    Tag tag(i, i % 2 == 0 ? 0 : 1, bufferlist());
    expected_all_tags.insert(tag);
    if (i % 2 == 0) {
      expected_filtered_tags.insert(tag);
    }
    ASSERT_EQ(0, client::tag_create(ioctx, oid, i, tag_class,
                                    bufferlist()));
  }

  std::set<Tag> tags;
  ASSERT_EQ(0, client::tag_list(ioctx, oid, "id1", boost::optional<uint64_t>(),
                                &tags));
  ASSERT_EQ(expected_all_tags, tags);

  ASSERT_EQ(0, client::tag_list(ioctx, oid, "id1", boost::optional<uint64_t>(0),
                                &tags));
  ASSERT_EQ(expected_filtered_tags, tags);
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
