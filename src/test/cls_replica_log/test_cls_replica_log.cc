/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include "cls/replica_log/cls_replica_log_client.h"
#include "cls/replica_log/cls_replica_log_types.h"

#define SETUP_DATA \
  librados::Rados rados; \
  librados::IoCtx ioctx; \
  string pool_name = get_temp_pool_name(); \
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados)); \
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx)); \
  string oid = "obj"; \
  ASSERT_EQ(0, ioctx.create(oid, true));

#define ADD_MARKER \
  string entity = "tester_entity"; \
  string marker = "tester_marker1"; \
  utime_t time; \
  time.set_from_double(10); \
  list<pair<string, utime_t> > entries; \
  entries.push_back(make_pair("tester_obj1", time)); \
  time.set_from_double(20); \
  cls_replica_log_progress_marker progress; \
  cls_replica_log_prepare_marker(progress, entity, marker, time, &entries); \
  librados::ObjectWriteOperation opw; \
  cls_replica_log_update_bound(opw, progress); \
  ASSERT_EQ(0, ioctx.operate(oid, &opw));

TEST(cls_replica_log, test_set_get_marker)
{
  SETUP_DATA

  ADD_MARKER

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(0, cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
                                          reply_time, return_progress_list));

  ASSERT_EQ(reply_position_marker, marker);
  ASSERT_EQ((double)10, (double)reply_time);
  string response_entity;
  string response_marker;
  utime_t response_time;
  list<pair<string, utime_t> > response_item_list;

  cls_replica_log_extract_marker(return_progress_list.front(),
                                 response_entity, response_marker,
                                 response_time, response_item_list);
  ASSERT_EQ(response_entity, entity);
  ASSERT_EQ(response_marker, marker);
  ASSERT_EQ(response_time, time);
  ASSERT_EQ((unsigned)1, response_item_list.size());
  ASSERT_EQ("tester_obj1", response_item_list.front().first);
}

TEST(cls_replica_log, test_bad_update)
{
  SETUP_DATA

  ADD_MARKER

  time.set_from_double(15);
  cls_replica_log_progress_marker bad_marker;
  cls_replica_log_prepare_marker(bad_marker, entity, marker, time, &entries);
  librados::ObjectWriteOperation badw;
  cls_replica_log_update_bound(badw, bad_marker);
  ASSERT_EQ(-EINVAL, ioctx.operate(oid, &badw));
}

TEST(cls_replica_log, test_bad_delete)
{
  SETUP_DATA

  ADD_MARKER

  librados::ObjectWriteOperation badd;
  cls_replica_log_delete_bound(badd, entity);
  ASSERT_EQ(-ENOTEMPTY, ioctx.operate(oid, &badd));
}

TEST(cls_replica_log, test_good_delete)
{
  SETUP_DATA

  ADD_MARKER

  librados::ObjectWriteOperation opc;
  progress.items.clear();
  cls_replica_log_update_bound(opc, progress);
  ASSERT_EQ(0, ioctx.operate(oid, &opc));
  librados::ObjectWriteOperation opd;
  cls_replica_log_delete_bound(opd, entity);
  ASSERT_EQ(0, ioctx.operate(oid, &opd));

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(0, cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
                                          reply_time, return_progress_list));
  ASSERT_EQ((unsigned)0, return_progress_list.size());
}

TEST(cls_replica_log, test_bad_get)
{
  SETUP_DATA

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(-ENOENT,
            cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
                                       reply_time, return_progress_list));
}

TEST(cls_replica_log, test_double_delete)
{
  SETUP_DATA

  ADD_MARKER

  librados::ObjectWriteOperation opc;
  progress.items.clear();
  cls_replica_log_update_bound(opc, progress);
  ASSERT_EQ(0, ioctx.operate(oid, &opc));
  librados::ObjectWriteOperation opd;
  cls_replica_log_delete_bound(opd, entity);
  ASSERT_EQ(0, ioctx.operate(oid, &opd));

  librados::ObjectWriteOperation opd2;
  cls_replica_log_delete_bound(opd2, entity);
  ASSERT_EQ(0, ioctx.operate(oid, &opd2));

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(0, cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
                                          reply_time, return_progress_list));
  ASSERT_EQ((unsigned)0, return_progress_list.size());

}
