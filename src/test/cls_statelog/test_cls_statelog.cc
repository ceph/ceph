// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/statelog/cls_statelog_types.h"
#include "cls/statelog/cls_statelog_client.h"

#include "include/utime.h"
#include "common/Clock.h"
#include "global/global_context.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

static librados::ObjectWriteOperation *new_op() {
  return new librados::ObjectWriteOperation();
}

static librados::ObjectReadOperation *new_rop() {
  return new librados::ObjectReadOperation();
}

static void reset_op(librados::ObjectWriteOperation **pop) {
  delete *pop;
  *pop = new_op();
}
static void reset_rop(librados::ObjectReadOperation **pop) {
  delete *pop;
  *pop = new_rop();
}

void add_log(librados::ObjectWriteOperation *op, const string& client_id, const string& op_id, string& obj, uint32_t state)
{
  bufferlist bl;
  ::encode(state, bl);

  utime_t ts = ceph_clock_now(g_ceph_context);

  cls_statelog_add(*op, client_id, op_id, obj, ts, state, bl);
}

void next_op_id(string& op_id, int *id)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%d", *id);
  op_id = buf;
  (*id)++;
}

static string get_obj_name(int num)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "obj-%d", num);
  return string(buf);
}

static void get_entries_by_object(librados::IoCtx& ioctx, string& oid,
                                  list<cls_statelog_entry>& entries, string& object, string& op_id, int expected)
{
  /* search everything */
  string empty_str, marker;

  librados::ObjectReadOperation *rop = new_rop();
  bufferlist obl;
  bool truncated;
  cls_statelog_list(*rop, empty_str, op_id, object, marker, 0, entries, &marker, &truncated);
  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));
  ASSERT_EQ(expected, (int)entries.size());
  delete rop;
}

static void get_entries_by_client_id(librados::IoCtx& ioctx, string& oid,
                                     list<cls_statelog_entry>& entries, string& client_id, string& op_id, int expected)
{
  /* search everything */
  string empty_str, marker;

  librados::ObjectReadOperation *rop = new_rop();
  bufferlist obl;
  bool truncated;
  cls_statelog_list(*rop, client_id, op_id, empty_str, marker, 0, entries, &marker, &truncated);
  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));
  ASSERT_EQ(expected, (int)entries.size());
  delete rop;
}

static void get_all_entries(librados::IoCtx& ioctx, string& oid, list<cls_statelog_entry>& entries, int expected)
{
  /* search everything */
  string object, op_id;
  get_entries_by_object(ioctx, oid, entries, object, op_id, expected);
}

TEST(cls_rgw, test_statelog_basic)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "obj";

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  int id = 0;
  string client_id[] = { "client-1", "client-2" };

  const int num_ops = 10;
  string op_ids[num_ops];

  librados::ObjectWriteOperation *op = new_op();

  for (int i = 0; i < num_ops; i++) {
    next_op_id(op_ids[i], &id);
    string obj = get_obj_name(i / 2);
    string cid = client_id[i / (num_ops / 2)];
    add_log(op, cid, op_ids[i], obj, i /* just for testing */); 
  }
  ASSERT_EQ(0, ioctx.operate(oid, op));

  librados::ObjectReadOperation *rop = new_rop();

  list<cls_statelog_entry> entries;
  bool truncated;

  /* check list by client_id */

  int total_count = 0;
  for (int j = 0; j < 2; j++) {
    string marker;
    string obj;
    string cid = client_id[j];
    string op_id;

    bufferlist obl;

    cls_statelog_list(*rop, cid, op_id, obj, marker, 1, entries, &marker, &truncated);
    ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));
    ASSERT_EQ(1, (int)entries.size());

    reset_rop(&rop);
    marker.clear();
    cls_statelog_list(*rop, cid, op_id, obj, marker, 0, entries, &marker, &truncated);
    obl.clear();
    ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

    ASSERT_EQ(5, (int)entries.size());
    ASSERT_EQ(0, (int)truncated);

    map<string, string> emap;
    for (list<cls_statelog_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
      ASSERT_EQ(cid, iter->client_id);
      emap[iter->op_id] = iter->object;
    }
    ASSERT_EQ(5, (int)emap.size());
    /* verify correct object */
    for (int i = 0; i < num_ops / 2; i++, total_count++) {
      string ret_obj = emap[op_ids[total_count]];
      string obj = get_obj_name(total_count / 2);
      ASSERT_EQ(0, ret_obj.compare(obj));
    }
  }

  entries.clear();
  /* now search by object */
  total_count = 0;
  for (int i = 0; i < num_ops; i++) {
    string marker;
    string obj = get_obj_name(i / 2);
    string cid;
    string op_id;
    bufferlist obl;

    reset_rop(&rop);
    cls_statelog_list(*rop, cid, op_id, obj, marker, 0, entries, &marker, &truncated);
    ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));
    ASSERT_EQ(2, (int)entries.size());
  }

  /* search everything */

  get_all_entries(ioctx, oid, entries, 10);

  /* now remove an entry */
  cls_statelog_entry e = entries.front();
  entries.pop_front();

  reset_op(&op);
  cls_statelog_remove_by_client(*op, e.client_id, e.op_id);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  get_all_entries(ioctx, oid, entries, 9);

  get_entries_by_object(ioctx, oid, entries, e.object, e.op_id, 0);
  get_entries_by_client_id(ioctx, oid, entries, e.client_id, e.op_id, 0);

  string empty_str;
  get_entries_by_client_id(ioctx, oid, entries, e.client_id, empty_str, 4);
  get_entries_by_object(ioctx, oid, entries, e.object, empty_str, 1);
  delete op;
  delete rop;
}

