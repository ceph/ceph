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

#include <iostream> // for std::cout
#include <list>
#include <vector>

#include "gtest/gtest.h"
#include "include/types.h"
#include "include/msgr.h"
#include "include/Context.h"
#include "common/ceph_context.h"
#include "common/config_proxy.h"
#include "common/Formatter.h"
#include "log/Log.h"

using namespace std;

namespace {

class SequenceContext final : public Context
{
  std::vector<int>& completed;
  int& destroyed;
  const int value;

public:
  SequenceContext(std::vector<int>& completed, int& destroyed, const int value)
    : completed(completed), destroyed(destroyed), value(value)
  {}

  ~SequenceContext() override {
    ++destroyed;
  }

  void finish(const int result) override {
    EXPECT_EQ(7, result);
    completed.push_back(value);
  }
};

template<class Sequence>
void check_context_factory()
{
  std::vector<int> completed;
  int destroyed = 0;
  Sequence contexts;
  EXPECT_EQ(nullptr, C_Contexts::to_context(contexts));

  contexts.push_back(new SequenceContext(completed, destroyed, 1));
  Context *single = C_Contexts::to_context(contexts);
  ASSERT_NE(nullptr, single);
  EXPECT_TRUE(contexts.empty());
  EXPECT_EQ(0, destroyed);
  single->complete(7);
  EXPECT_EQ(1, destroyed);

  contexts.push_back(new SequenceContext(completed, destroyed, 2));
  contexts.push_back(new SequenceContext(completed, destroyed, 3));
  Context *batch = C_Contexts::to_context(contexts);
  ASSERT_NE(nullptr, batch);
  EXPECT_TRUE(contexts.empty());
  EXPECT_EQ(1, destroyed);
  batch->complete(7);
  EXPECT_EQ((std::vector<int> {1, 2, 3}), completed);
  EXPECT_EQ(3, destroyed);
}

} // anonymous namespace

TEST(Context, factory_owns_list_sequences)
{
  check_context_factory<std::list<Context *>>();
}

TEST(Context, factory_owns_vector_sequences)
{
  check_context_factory<std::vector<Context *>>();
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
