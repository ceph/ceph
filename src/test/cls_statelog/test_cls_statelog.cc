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

static void reset_rop(librados::ObjectReadOperation **pop) {
  delete *pop;
  *pop = new_rop();
}

void add_log(librados::ObjectWriteOperation *op, const string& client_id, const string& op_id, string& obj, uint32_t state)
{
  bufferlist bl;
  ::encode(state, bl);

  cls_statelog_add(*op, client_id, op_id, obj, state, bl);
}

void next_op_id(string& op_id, int *id)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%d", *id);
  op_id = buf;
  (*id)++;
}

string get_name(int i)
{
  string name_prefix = "data-source";

  char buf[16];
  snprintf(buf, sizeof(buf), "%d", i);
  return name_prefix + buf;
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

  int num_ops = 10;
  string op_ids[num_ops];

  librados::ObjectWriteOperation *op = new_op();

  for (int i = 0; i < num_ops; i++) {
    next_op_id(op_ids[i], &id);
    char buf[16];
    snprintf(buf, sizeof(buf), "obj-%d", i / 2);

    string cid = client_id[i / (num_ops / 2)];

    string obj = buf;
    add_log(op, cid, op_ids[i], obj, i /* just for testing */); 
  }
  ASSERT_EQ(0, ioctx.operate(oid, op));

  librados::ObjectReadOperation *rop = new_rop();

  list<cls_statelog_entry> entries;
  bool truncated;

  /* check list by client_id */

  string marker;

  string obj;
  string cid = client_id[0];
  string op_id;

  cls_statelog_list(*rop, cid, op_id, obj, marker, 0, entries, &marker, &truncated);

  bufferlist obl;
  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

  ASSERT_EQ(5, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);
#if 0
  list<cls_log_entry>::iterator iter;

  /* need to sort returned entries, all were using the same time as key */
  map<int, cls_log_entry> check_ents;

  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    cls_log_entry& entry = *iter;

    int num;
    ASSERT_EQ(0, read_bl(entry.data, &num));

    check_ents[num] = entry;
  }

  ASSERT_EQ(10, (int)check_ents.size());

  map<int, cls_log_entry>::iterator ei;

  /* verify entries are as expected */

  int i;

  for (i = 0, ei = check_ents.begin(); i < 10; i++, ++ei) {
    cls_log_entry& entry = ei->second;

    ASSERT_EQ(i, ei->first);
    check_entry(entry, start_time, i, false);
  }

  reset_rop(&rop);

  /* check list again, now want to be truncated*/

  marker.clear();

  cls_log_list(*rop, start_time, to_time, marker, 1, entries, &marker, &truncated);

  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

  ASSERT_EQ(1, (int)entries.size());
  ASSERT_EQ(1, (int)truncated);

  delete rop;
#endif
}

