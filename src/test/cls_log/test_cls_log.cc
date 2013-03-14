// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/log/cls_log_types.h"
#include "cls/log/cls_log_client.h"

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

static int read_bl(bufferlist& bl, int *i)
{
  bufferlist::iterator iter = bl.begin();

  try {
    ::decode(*i, iter);
  } catch (buffer::error& err) {
    std::cout << "failed to decode buffer" << std::endl;
    return -EIO;
  }

  return 0;
}

void add_log(librados::ObjectWriteOperation *op, utime_t& timestamp, string& section, string&name, int i)
{
  bufferlist bl;
  ::encode(i, bl);

  cls_log_add(*op, timestamp, section, name, bl);
}


TEST(cls_rgw, test_log_add)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));


  librados::ObjectWriteOperation *op = new_op();

  utime_t now = ceph_clock_now(g_ceph_context);

  string section = "global";
  string name = "data-source";

  int i;

  for (i = 0; i < 10; i++) {
    add_log(op, now, section, name, i);
  }

  ASSERT_EQ(0, ioctx.operate(oid, op));

  delete op;

  librados::ObjectReadOperation *rop = new_rop();

  list<cls_log_entry> entries;
  bool truncated;

  cls_log_list(*rop, now, 1000, entries, &truncated);

  bufferlist obl;
  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

  list<cls_log_entry>::iterator iter;

  map<int, bool> check_ents;

  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    cls_log_entry& entry = *iter;

    ASSERT_EQ(section, entry.section);
    ASSERT_EQ(name, entry.name);

    int num;
    ASSERT_EQ(0, read_bl(entry.data, &num));

    check_ents[num] = true;

    ASSERT_EQ(now, entry.timestamp);
  }

  ASSERT_EQ(10, (int)check_ents.size());

  map<int, bool>::iterator ei;

  for (i = 0, ei = check_ents.begin(); i < 10; i++, ++ei) {
    ASSERT_EQ(i, ei->first);
  }

}


