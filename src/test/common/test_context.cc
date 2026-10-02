// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 *
 */

#include <vector>
#include <utility>
#include <iostream> // for std::cout
#include <iterator>

#include "gtest/gtest.h"
#include "include/Context.h"
#include "include/types.h"
#include "include/msgr.h"
#include "os/Transaction.h"
#include "common/Finisher.h"
#include "common/ceph_context.h"
#include "common/config_proxy.h"
#include "common/Formatter.h"
#include "log/Log.h"

using namespace std;

TEST(Context, finish_contexts_vector_reentrancy)
{
  vector<pair<int, int>> completed;
  vector<Context*> contexts;
  contexts.push_back(make_lambda_context([&](int result) {
    EXPECT_TRUE(std::empty(contexts));
    completed.emplace_back(1, result);
    contexts.push_back(make_lambda_context([&](int next_result) {
      completed.emplace_back(3, next_result);
    }));
  }));
  contexts.push_back(make_lambda_context([&](int result) {
    completed.emplace_back(2, result);
  }));

  finish_contexts(nullptr, contexts, 7);
  EXPECT_EQ((vector<pair<int, int>> {{1, 7}, {2, 7}}), completed);
  ASSERT_EQ(1, std::size(contexts));

  finish_contexts(nullptr, contexts, 8);
  EXPECT_EQ((vector<pair<int, int>> {{1, 7}, {2, 7}, {3, 8}}), completed);
  EXPECT_TRUE(std::empty(contexts));
}

TEST(Context, transaction_context_sequences_preserve_order)
{
  vector<int> completed;
  auto record = [&completed](int id) {
    return make_lambda_context([&completed, id](int) {
      completed.push_back(id);
    });
  };

  vector<ceph::os::Transaction> transactions(2);
  transactions[0].register_on_applied(record(1));
  transactions[0].register_on_commit(record(3));
  transactions[0].register_on_applied_sync(record(5));
  transactions[1].register_on_applied(record(2));
  transactions[1].register_on_commit(record(4));
  transactions[1].register_on_applied_sync(record(6));

  ceph::os::Transaction::context_sequence applied {record(0)};
  ceph::os::Transaction::context_sequence committed;
  ceph::os::Transaction::context_sequence applied_sync;
  ceph::os::Transaction::collect_contexts(
    transactions, applied, committed, applied_sync);

  EXPECT_FALSE(transactions[0].has_contexts());
  EXPECT_FALSE(transactions[1].has_contexts());

  finish_contexts(nullptr, applied);
  finish_contexts(nullptr, committed);
  finish_contexts(nullptr, applied_sync);
  EXPECT_EQ((vector<int> {0, 1, 2, 3, 4, 5, 6}), completed);
}

TEST(Context, transaction_context_aggregation_owns_the_batch)
{
  vector<int> completed;
  auto record = [&completed](int id) {
    return make_lambda_context([&completed, id](int) {
      completed.push_back(id);
    });
  };

  vector<ceph::os::Transaction> transactions(2);
  transactions[0].register_on_commit(record(1));
  transactions[1].register_on_commit(record(2));

  Context *applied = nullptr;
  Context *committed = nullptr;
  Context *applied_sync = nullptr;
  ceph::os::Transaction::collect_contexts(
    transactions, applied, committed, applied_sync);

  EXPECT_EQ(nullptr, applied);
  ASSERT_NE(nullptr, committed);
  EXPECT_EQ(nullptr, applied_sync);
  committed->complete(7);
  EXPECT_EQ((vector<int> {1, 2}), completed);
}

TEST(Context, transaction_append_transfers_contexts_in_order)
{
  vector<int> completed;
  auto record = [&completed](int id) {
    return make_lambda_context([&completed, id](int) {
      completed.push_back(id);
    });
  };

  ceph::os::Transaction first;
  first.register_on_applied(record(1));
  first.register_on_commit(record(3));
  ceph::os::Transaction second;
  second.register_on_applied(record(2));
  second.register_on_commit(record(4));
  first.append(second);

  EXPECT_FALSE(second.has_contexts());
  auto *applied = first.get_on_applied();
  auto *committed = first.get_on_commit();
  ASSERT_NE(nullptr, applied);
  ASSERT_NE(nullptr, committed);
  applied->complete(0);
  committed->complete(0);
  EXPECT_EQ((vector<int> {1, 2, 3, 4}), completed);
}

TEST(Context, context_queue_appends_and_detaches_batches)
{
  auto mutex = ceph::make_mutex("ContextQueue test");
  ceph::condition_variable condition;
  ContextQueue queue(mutex, condition);

  vector<Context *> first {new C_NoopContext};
  vector<Context *> second {new C_NoopContext, new C_NoopContext};
  const vector<Context *> expected {
    first[0], second[0], second[1]
  };
  queue.queue(first);
  queue.queue(second);

  EXPECT_TRUE(std::empty(first));
  EXPECT_TRUE(std::empty(second));
  EXPECT_FALSE(queue.empty());

  vector<Context *> detached;
  queue.move_to(detached);
  EXPECT_EQ(expected, detached);
  EXPECT_TRUE(queue.empty());

  for (auto *context : detached) {
    delete context;
  }
}

TEST(CephContext, do_command)
{
  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};

  cct->_conf->cluster = "ceph";

  string key("key");
  string value("value");
  cct->_conf.set_val(key.c_str(), value.c_str());
  cmdmap_t cmdmap;
  cmdmap["var"] = key;

  {
    stringstream ss;
    bufferlist out;
    std::unique_ptr<Formatter> f{Formatter::create_unique("xml", "xml")};
    cct->do_command("config get", cmdmap, f.get(), ss, &out);
    f->flush(out);
    string s(out.c_str(), out.length());
    EXPECT_EQ("<config_get><key>" + value + "</key></config_get>", s);
  }

  {
    stringstream ss;
    bufferlist out;
    cmdmap_t bad_cmdmap; // no 'var' field
    std::unique_ptr<Formatter> f{Formatter::create_unique("xml", "xml")};
    int r = cct->do_command("config get", bad_cmdmap, f.get(), ss, &out);
    if (r >= 0) {
      f->flush(out);
    }
    string s(out.c_str(), out.length());
    EXPECT_EQ(-EINVAL, r);
    EXPECT_EQ("", s);
    EXPECT_EQ("", ss.str()); // no error string :/
  }
  {
    stringstream ss;
    bufferlist out;
    cmdmap_t bad_cmdmap;
    bad_cmdmap["var"] = string("doesnotexist123");
    std::unique_ptr<Formatter> f{Formatter::create_unique("xml", "xml")};
    int r = cct->do_command("config help", bad_cmdmap, f.get(), ss, &out);
    if (r >= 0) {
      f->flush(out);
    }
    string s(out.c_str(), out.length());
    EXPECT_EQ(-ENOENT, r);
    EXPECT_EQ("", s);
    EXPECT_EQ("Setting not found: 'doesnotexist123'", ss.str());
  }

  {
    stringstream ss;
    bufferlist out;
    std::unique_ptr<Formatter> f{Formatter::create_unique("xml", "xml")};
    cct->do_command("config diff get", cmdmap, f.get(), ss, &out);
    f->flush(out);
    string s(out.c_str(), out.length());
    EXPECT_EQ("<config_diff_get><diff><key><default></default><override>" + value + "</override><final>value</final></key><rbd_default_features><default>61</default><final>61</final></rbd_default_features><rbd_qos_exclude_ops><default>0</default><final>0</final></rbd_qos_exclude_ops></diff></config_diff_get>", s);
  }
}

TEST(CephContext, experimental_features)
{
  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};

  cct->_conf->cluster = "ceph";

  ASSERT_FALSE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf.set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "foo,bar");
  cct->_conf.apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf.set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "foo bar");
  cct->_conf.apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf.set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "baz foo");
  cct->_conf.apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf.set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "*");
  cct->_conf.apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("baz"));

  cct->_log->flush();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_context &&
 *    valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *   ./unittest_context # --gtest_filter=CephContext.*
 * "
 * End:
 */
