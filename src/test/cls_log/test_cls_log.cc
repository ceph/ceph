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

static void reset_rop(librados::ObjectReadOperation **pop) {
  delete *pop;
  *pop = new_rop();
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


string get_name(int i)
{
  string name_prefix = "data-source";

  char buf[16];
  snprintf(buf, sizeof(buf), "%d", i);
  return name_prefix + buf;
}

void generate_log(librados::IoCtx& ioctx, string& oid, int max, utime_t& start_time, bool modify_time)
{
  string section = "global";

  librados::ObjectWriteOperation *op = new_op();

  int i;

  for (i = 0; i < max; i++) {
    uint32_t secs = start_time.sec();
    if (modify_time)
      secs += i;

    utime_t ts(secs, start_time.nsec());
    string name = get_name(i);

    add_log(op, ts, section, name, i);
  }

  ASSERT_EQ(0, ioctx.operate(oid, op));

  delete op;
}

utime_t get_time(utime_t& start_time, int i, bool modify_time)
{
  uint32_t secs = start_time.sec();
  if (modify_time)
    secs += i;
  return utime_t(secs, start_time.nsec());
}

void check_entry(cls_log_entry& entry, utime_t& start_time, int i, bool modified_time)
{
  string section = "global";
  string name = get_name(i);
  utime_t ts = get_time(start_time, i, modified_time);

  ASSERT_EQ(section, entry.section);
  ASSERT_EQ(name, entry.name);
  ASSERT_EQ(ts, entry.timestamp);
}


TEST(cls_rgw, test_log_add_same_time)
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

  /* generate log */
  utime_t start_time = ceph_clock_now(g_ceph_context);
  generate_log(ioctx, oid, 10, start_time, false);

  librados::ObjectReadOperation *rop = new_rop();

  list<cls_log_entry> entries;
  bool truncated;

  /* check list */

  utime_t to_time = get_time(start_time, 1, true);

  string marker;

  cls_log_list(*rop, start_time, to_time, marker, 0, entries, &marker, &truncated);

  bufferlist obl;
  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

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

  /* destroy pool */
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rgw, test_log_add_different_time)
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

  /* generate log */
  utime_t start_time = ceph_clock_now(g_ceph_context);
  generate_log(ioctx, oid, 10, start_time, true);

  librados::ObjectReadOperation *rop = new_rop();

  list<cls_log_entry> entries;
  bool truncated;

  utime_t to_time = utime_t(start_time.sec() + 10, start_time.nsec());

  string marker;

  /* check list */
  cls_log_list(*rop, start_time, to_time, marker, 0, entries, &marker, &truncated);

  bufferlist obl;
  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

  list<cls_log_entry>::iterator iter;

  /* returned entries should be sorted by time */
  map<int, cls_log_entry> check_ents;

  int i;

  for (i = 0, iter = entries.begin(); iter != entries.end(); ++iter, ++i) {
    cls_log_entry& entry = *iter;

    int num;

    ASSERT_EQ(0, read_bl(entry.data, &num));

    ASSERT_EQ(i, num);

    check_entry(entry, start_time, i, true);
  }

  reset_rop(&rop);

  /* check list again with shifted time */
  utime_t next_time = get_time(start_time, 1, true);

  marker.clear();

  cls_log_list(*rop, next_time, to_time, marker, 0, entries, &marker, &truncated);

  ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

  ASSERT_EQ(9, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

  reset_rop(&rop);

  marker.clear();

  i = 0;
  do {
    bufferlist obl;
    string old_marker = marker;
    cls_log_list(*rop, start_time, to_time, old_marker, 1, entries, &marker, &truncated);

    ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));
    ASSERT_NE(old_marker, marker);
    ASSERT_EQ(1, (int)entries.size());

    ++i;
    ASSERT_GE(10, i);
  } while (truncated);

  ASSERT_EQ(10, i);
  delete rop;

  /* destroy pool */
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rgw, test_log_trim)
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

  /* generate log */
  utime_t start_time = ceph_clock_now(g_ceph_context);
  generate_log(ioctx, oid, 10, start_time, true);

  librados::ObjectReadOperation *rop = new_rop();

  list<cls_log_entry> entries;
  bool truncated;

  /* check list */

  /* trim */
  utime_t to_time = get_time(start_time, 10, true);

  for (int i = 0; i < 10; i++) {
    utime_t trim_time = get_time(start_time, i, true);

    utime_t zero_time;
    string start_marker, end_marker;

    ASSERT_EQ(0, cls_log_trim(ioctx, oid, zero_time, trim_time, start_marker, end_marker));

    string marker;

    cls_log_list(*rop, start_time, to_time, marker, 0, entries, &marker, &truncated);

    bufferlist obl;
    ASSERT_EQ(0, ioctx.operate(oid, rop, &obl));

    ASSERT_EQ(9 - i, (int)entries.size());
    ASSERT_EQ(0, (int)truncated);
  }
  delete rop;

  /* destroy pool */
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}
