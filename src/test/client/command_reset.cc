// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>

#include <list>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "gtest/gtest.h"

#include "client/Client.h"
#include "global/global_context.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "osdc/Objecter.h"

namespace {

const std::string version_cmd = "{\"prefix\": \"version\"}";

class RecordingConnection : public Connection {
public:
  std::vector<MessageRef> sent;

  RecordingConnection() : Connection(g_ceph_context, nullptr) {
    set_peer_type(CEPH_ENTITY_TYPE_MDS);
  }

  bool is_connected() override { return true; }
  void send_keepalive() override {}
  void mark_down() override {}
  void mark_disposable() override {}
  void shutdown() override {}
  entity_addr_t get_peer_socket_addr() const override { return {}; }

protected:
  int send_msg(MessageRef&& m) override {
    sent.push_back(std::move(m));
    return 0;
  }
};

class CommandClient : public Client {
public:
  using Client::Client;
  using Client::handle_command_reply;
  using Client::ms_handle_remote_reset;

  // The table update and send done by Client::mds_command().
  ceph_tid_t send_command(const ConnectionRef& con, bool one_shot,
                          Context *on_finish, bufferlist *outbl,
                          std::string *outs) {
    std::scoped_lock l(command_lock);
    auto& op = command_table.start_command();
    op.on_finish = on_finish;
    op.cmd = {version_cmd};
    op.outbl = outbl;
    op.outs = outs;
    op.mds_gid = mds_gid_t(1);
    op.con = con;
    op.one_shot = one_shot;
    con->send_message2(op.get_message(uuid_d()));
    return op.tid;
  }
};

struct Waiter {
  ceph_tid_t tid = 0;
  std::optional<int> result;
  bufferlist outbl;
  std::string outs;
};

class C_Result : public Context {
  Waiter *w;
public:
  explicit C_Result(Waiter *w) : w(w) {}
  void finish(int r) override { w->result = r; }
};

} // anonymous namespace

class ClientCommandReset : public ::testing::Test {
protected:
  boost::asio::io_context ioc;
  Messenger *messenger = nullptr;
  MonClient *monc = nullptr;
  Objecter *objecter = nullptr;
  CommandClient *client = nullptr;
  std::list<Waiter> waiters;

  void SetUp() override {
    messenger = Messenger::create_client_messenger(g_ceph_context, "client");
    ASSERT_EQ(0, messenger->start());
    monc = new MonClient(g_ceph_context, ioc);
    objecter = new Objecter(g_ceph_context, messenger, monc, ioc);
    client = new CommandClient(messenger, monc, objecter);
  }

  void TearDown() override {
    // CommandTable asserts it is empty on destruction.
    client->cancel_commands_if([](const MDSCommandOp&) { return -ECANCELED; });
    delete client;
    delete objecter;
    delete monc;
    messenger->shutdown();
    messenger->wait();
    delete messenger;
  }

  Waiter& send(const ConnectionRef& con, bool one_shot) {
    auto& w = waiters.emplace_back();
    w.tid = client->send_command(con, one_shot, new C_Result(&w),
                                 &w.outbl, &w.outs);
    return w;
  }

  // What the messenger does when the MDS answers a reconnect with
  // RESETSESSION: drop all unacked messages, then report the reset.
  void remote_reset(RecordingConnection& con) {
    con.sent.clear();
    client->ms_handle_remote_reset(&con);
  }
};

TEST_F(ClientCommandReset, CommandResentAfterRemoteReset)
{
  auto con = ceph::make_ref<RecordingConnection>();
  auto other = ceph::make_ref<RecordingConnection>();
  auto& w = send(con, false);
  auto& other_w = send(other, false);
  ASSERT_EQ(1u, con->sent.size());
  ASSERT_EQ(1u, other->sent.size());

  remote_reset(*con);

  ASSERT_EQ(1u, con->sent.size());
  ASSERT_EQ(MSG_COMMAND, con->sent[0]->get_type());
  auto resent = ceph::ref_cast<MCommand>(con->sent[0]);
  EXPECT_EQ(w.tid, resent->get_tid());
  EXPECT_EQ(std::vector<std::string>{version_cmd}, resent->cmd);
  EXPECT_EQ(1u, other->sent.size());
  EXPECT_FALSE(w.result.has_value());

  auto reply = ceph::make_message<MCommandReply>(0, "");
  reply->set_tid(w.tid);
  bufferlist data;
  data.append("{\"version\": \"x\"}");
  reply->set_data(data);
  ASSERT_TRUE(client->handle_command_reply(reply));
  ASSERT_TRUE(w.result.has_value());
  EXPECT_EQ(0, *w.result);
  EXPECT_EQ("{\"version\": \"x\"}", w.outbl.to_str());
  EXPECT_FALSE(other_w.result.has_value());
}

TEST_F(ClientCommandReset, OneShotCommandAbortedOnRemoteReset)
{
  auto con = ceph::make_ref<RecordingConnection>();
  auto& w = send(con, true);
  ASSERT_EQ(1u, con->sent.size());

  remote_reset(*con);

  EXPECT_TRUE(con->sent.empty());
  ASSERT_TRUE(w.result.has_value());
  EXPECT_EQ(-EPIPE, *w.result);
}
