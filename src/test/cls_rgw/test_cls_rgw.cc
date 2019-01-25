// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_ops.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include <errno.h>
#include <string>
#include <vector>
#include <map>
#include <set>

using namespace librados;

librados::Rados rados;
librados::IoCtx ioctx;
string pool_name;


/* must be the first test! */
TEST(cls_rgw, init)
{
  pool_name = get_temp_pool_name();
  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
}


string str_int(string s, int i)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d", i);
  s.append(buf);

  return s;
}


class OpMgr {
  vector<ObjectOperation *> ops;

public:
  OpMgr() {}
  ~OpMgr() {
    vector<ObjectOperation *>::iterator iter;
    for (iter = ops.begin(); iter != ops.end(); ++iter) {
      ObjectOperation *op = *iter;
      delete op;
    }
  }

  ObjectReadOperation *read_op() {
    ObjectReadOperation *op = new ObjectReadOperation;
    ops.push_back(op);
    return op;
  }

  ObjectWriteOperation *write_op() {
    ObjectWriteOperation *op = new ObjectWriteOperation;
    ops.push_back(op);
    return op;
  }
};

void test_stats(librados::IoCtx& ioctx, string& oid, RGWObjCategory category, uint64_t num_entries, uint64_t total_size)
{
  map<int, struct rgw_cls_list_ret> results;
  map<int, string> oids;
  oids[0] = oid;
  ASSERT_EQ(0, CLSRGWIssueGetDirHeader(ioctx, oids, results, 8)());

  uint64_t entries = 0;
  uint64_t size = 0;
  map<int, struct rgw_cls_list_ret>::iterator iter = results.begin();
  for (; iter != results.end(); ++iter) {
    entries += (iter->second).dir.header.stats[category].num_entries;
    size += (iter->second).dir.header.stats[category].total_size;
  }
  ASSERT_EQ(total_size, size);
  ASSERT_EQ(num_entries, entries);
}

void index_prepare(OpMgr& mgr, librados::IoCtx& ioctx, string& oid, RGWModifyOp index_op, string& tag,
		   string& obj, string& loc, uint16_t bi_flags = 0)
{
  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_obj_key key(obj, string());
  rgw_zone_set zones_trace;
  cls_rgw_bucket_prepare_op(*op, index_op, tag, key, loc, true, bi_flags, zones_trace);
  ASSERT_EQ(0, ioctx.operate(oid, op));
}

void index_complete(OpMgr& mgr, librados::IoCtx& ioctx, string& oid, RGWModifyOp index_op, string& tag,
		    int epoch, string& obj, rgw_bucket_dir_entry_meta& meta, uint16_t bi_flags = 0)
{
  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_obj_key key(obj, string());
  rgw_bucket_entry_ver ver;
  ver.pool = ioctx.get_id();
  ver.epoch = epoch;
  meta.accounted_size = meta.size;
  cls_rgw_bucket_complete_op(*op, index_op, tag, ver, key, meta, nullptr, true, bi_flags, nullptr);
  ASSERT_EQ(0, ioctx.operate(oid, op));
}

TEST(cls_rgw, index_basic)
{
  string bucket_oid = str_int("bucket", 0);

  OpMgr mgr;

  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_bucket_init_index(*op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  uint64_t epoch = 1;

  uint64_t obj_size = 1024;

#define NUM_OBJS 10
  for (int i = 0; i < NUM_OBJS; i++) {
    string obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i, obj_size * i);

    op = mgr.write_op();
    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta);
  }

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS,
	     obj_size * NUM_OBJS);
}

TEST(cls_rgw, index_multiple_obj_writers)
{
  string bucket_oid = str_int("bucket", 1);

  OpMgr mgr;

  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_bucket_init_index(*op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  uint64_t obj_size = 1024;

  string obj = str_int("obj", 0);
  string loc = str_int("loc", 0);
  /* multi prepare on a single object */
  for (int i = 0; i < NUM_OBJS; i++) {
    string tag = str_int("tag", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, 0, 0);
  }

  for (int i = NUM_OBJS; i > 0; i--) {
    string tag = str_int("tag", i - 1);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size * i;

    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, i, obj, meta);

    /* verify that object size doesn't change, as we went back with epoch */
    test_stats(ioctx, bucket_oid, RGWObjCategory::None, 1,
	       obj_size * NUM_OBJS);
  }
}

TEST(cls_rgw, index_remove_object)
{
  string bucket_oid = str_int("bucket", 2);

  OpMgr mgr;

  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_bucket_init_index(*op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  uint64_t obj_size = 1024;
  uint64_t total_size = 0;

  int epoch = 0;

  /* prepare multiple objects */
  for (int i = 0; i < NUM_OBJS; i++) {
    string obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i, total_size);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = i * obj_size;
    total_size += i * obj_size;

    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, ++epoch, obj, meta);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i + 1, total_size);
  }

  int i = NUM_OBJS / 2;
  string tag_remove = "tag-rm";
  string tag_modify = "tag-mod";
  string obj = str_int("obj", i);
  string loc = str_int("loc", i);

  /* prepare both removal and modification on the same object */
  index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, obj, loc);
  index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag_modify, obj, loc);

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS, total_size);

  rgw_bucket_dir_entry_meta meta;

  /* complete object removal */
  index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, ++epoch, obj, meta);

  /* verify stats correct */
  total_size -= i * obj_size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS - 1, total_size);

  meta.size = 512;
  meta.category = RGWObjCategory::None;

  /* complete object modification */
  index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag_modify, ++epoch, obj, meta);

  /* verify stats correct */
  total_size += meta.size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS, total_size);


  /* prepare both removal and modification on the same object, this time we'll
   * first complete modification then remove*/
  index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, obj, loc);
  index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_modify, obj, loc);

  /* complete modification */
  total_size -= meta.size;
  meta.size = i * obj_size * 2;
  meta.category = RGWObjCategory::None;

  /* complete object modification */
  index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag_modify, ++epoch, obj, meta);

  /* verify stats correct */
  total_size += meta.size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS, total_size);

  /* complete object removal */
  index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, ++epoch, obj, meta);

  /* verify stats correct */
  total_size -= meta.size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS - 1,
	     total_size);
}

TEST(cls_rgw, index_suggest)
{
  string bucket_oid = str_int("bucket", 3);

  OpMgr mgr;

  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_bucket_init_index(*op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  uint64_t total_size = 0;

  int epoch = 0;

  int num_objs = 100;

  uint64_t obj_size = 1024;

  /* create multiple objects */
  for (int i = 0; i < num_objs; i++) {
    string obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i, total_size);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    total_size += meta.size;

    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, ++epoch, obj, meta);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i + 1, total_size);
  }

  /* prepare (without completion) some of the objects */
  for (int i = 0; i < num_objs; i += 2) {
    string obj = str_int("obj", i);
    string tag = str_int("tag-prepare", i);
    string loc = str_int("loc", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, num_objs, total_size);
  }

  int actual_num_objs = num_objs;
  /* remove half of the objects */
  for (int i = num_objs / 2; i < num_objs; i++) {
    string obj = str_int("obj", i);
    string tag = str_int("tag-rm", i);
    string loc = str_int("loc", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, actual_num_objs, total_size);

    rgw_bucket_dir_entry_meta meta;
    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_DEL, tag, ++epoch, obj, meta);

    total_size -= obj_size;
    actual_num_objs--;
    test_stats(ioctx, bucket_oid, RGWObjCategory::None, actual_num_objs, total_size);
  }

  bufferlist updates;

  for (int i = 0; i < num_objs; i += 2) { 
    string obj = str_int("obj", i);
    string tag = str_int("tag-rm", i);
    string loc = str_int("loc", i);

    rgw_bucket_dir_entry dirent;
    dirent.key.name = obj;
    dirent.locator = loc;
    dirent.exists = (i < num_objs / 2); // we removed half the objects
    dirent.meta.size = 1024;
    dirent.meta.accounted_size = 1024;

    char suggest_op = (i < num_objs / 2 ? CEPH_RGW_UPDATE : CEPH_RGW_REMOVE);
    cls_rgw_encode_suggestion(suggest_op, dirent, updates);
  }

  map<int, string> bucket_objs;
  bucket_objs[0] = bucket_oid;
  int r = CLSRGWIssueSetTagTimeout(ioctx, bucket_objs, 8 /* max aio */, 1)();
  ASSERT_EQ(0, r);

  sleep(1);

  /* suggest changes! */
  op = mgr.write_op();
  cls_rgw_suggest_changes(*op, updates);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  /* suggest changes twice! */
  op = mgr.write_op();
  cls_rgw_suggest_changes(*op, updates);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, num_objs / 2, total_size);
}

/*
 * This case is used to test whether get_obj_vals will
 * return all validate utf8 objnames and filter out those
 * in BI_PREFIX_CHAR private namespace.
 */
TEST(cls_rgw, index_list)
{
  string bucket_oid = str_int("bucket", 4);

  OpMgr mgr;

  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_bucket_init_index(*op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  uint64_t epoch = 1;
  uint64_t obj_size = 1024;
  const int num_objs = 5;
  const string keys[num_objs] = {
    /* single byte utf8 character */
    { static_cast<char>(0x41) },
    /* double byte utf8 character */
    { static_cast<char>(0xCF), static_cast<char>(0x8F) },
    /* treble byte utf8 character */
    { static_cast<char>(0xDF), static_cast<char>(0x8F), static_cast<char>(0x8F) },
    /* quadruble byte utf8 character */
    { static_cast<char>(0xF7), static_cast<char>(0x8F), static_cast<char>(0x8F), static_cast<char>(0x8F) },
    /* BI_PREFIX_CHAR private namespace, for test only */
    { static_cast<char>(0x80), static_cast<char>(0x41) }
  };

  for (int i = 0; i < num_objs; i++) {
    string obj = keys[i];
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    op = mgr.write_op();
    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta);
  }

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, num_objs, obj_size * num_objs);

  map<int, string> oids = { {0, bucket_oid} };
  map<int, struct rgw_cls_list_ret> list_results;
  cls_rgw_obj_key start_key("", "");
  int r = CLSRGWIssueBucketList(ioctx, start_key, "", 1000, true, oids, list_results, 1)();

  ASSERT_EQ(r, 0);
  ASSERT_EQ(1u, list_results.size());

  auto it = list_results.begin();
  auto m = (it->second).dir.m;

  ASSERT_EQ(4u, m.size());
  int i = 0;
  for(auto it2 = m.begin(); it2 != m.end(); it2++, i++)
    ASSERT_EQ(it2->first.compare(keys[i]), 0);
}


TEST(cls_rgw, bi_list)
{
  string bucket_oid = str_int("bucket", 5);

 CephContext *cct = reinterpret_cast<CephContext *>(ioctx.cct());

  OpMgr mgr;

  ObjectWriteOperation *op = mgr.write_op();
  cls_rgw_bucket_init_index(*op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, op));

  string name;
  string marker;
  uint64_t max = 10;
  list<rgw_cls_bi_entry> entries;
  bool is_truncated;

  int ret = cls_rgw_bi_list(ioctx, bucket_oid, name, marker, max, &entries,
			    &is_truncated);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entries.size(), 0u);
  ASSERT_EQ(is_truncated, false);

  uint64_t epoch = 1;
  uint64_t obj_size = 1024;
  uint64_t num_objs = 35;

  for (uint64_t i = 0; i < num_objs; i++) {
    string obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);
    index_prepare(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc, RGW_BILOG_FLAG_VERSIONED_OP);
    op = mgr.write_op();
    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    index_complete(mgr, ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta, RGW_BILOG_FLAG_VERSIONED_OP);
  }

  ret = cls_rgw_bi_list(ioctx, bucket_oid, name, marker, num_objs + 10, &entries,
			    &is_truncated);
  ASSERT_EQ(ret, 0);
  if (cct->_conf->osd_max_omap_entries_per_request < num_objs) {
    ASSERT_EQ(entries.size(), cct->_conf->osd_max_omap_entries_per_request);
  } else {
    ASSERT_EQ(entries.size(), num_objs);
  }

  uint64_t num_entries = 0;

  is_truncated = true;
  while(is_truncated) {
    ret = cls_rgw_bi_list(ioctx, bucket_oid, name, marker, max, &entries,
			  &is_truncated);
    ASSERT_EQ(ret, 0);
    if (is_truncated) {
      ASSERT_EQ(entries.size(), std::min(max, cct->_conf->osd_max_omap_entries_per_request));
    } else {
      ASSERT_EQ(entries.size(), num_objs - num_entries);
    }
    num_entries += entries.size();
    marker = entries.back().idx;
  }

  ret = cls_rgw_bi_list(ioctx, bucket_oid, name, marker, max, &entries,
			&is_truncated);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entries.size(), 0u);
  ASSERT_EQ(is_truncated, false);

  if (cct->_conf->osd_max_omap_entries_per_request < 15) {
    num_entries = 0;
    max = 15;
    is_truncated = true;
    marker.clear();
    while(is_truncated) {
      ret = cls_rgw_bi_list(ioctx, bucket_oid, name, marker, max, &entries,
			    &is_truncated);
      ASSERT_EQ(ret, 0);
      if (is_truncated) {
	ASSERT_EQ(entries.size(), cct->_conf->osd_max_omap_entries_per_request);
      } else {
	ASSERT_EQ(entries.size(), num_objs - num_entries);
      }
      num_entries += entries.size();
      marker = entries.back().idx;
    }
  }

  ret = cls_rgw_bi_list(ioctx, bucket_oid, name, marker, max, &entries,
			&is_truncated);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entries.size(), 0u);
  ASSERT_EQ(is_truncated, false);
}

/* test garbage collection */
static void create_obj(cls_rgw_obj& obj, int i, int j)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d.%d", i, j);
  obj.pool = "pool";
  obj.pool.append(buf);
  obj.key.name = "oid";
  obj.key.name.append(buf);
  obj.loc = "loc";
  obj.loc.append(buf);
}

static bool cmp_objs(cls_rgw_obj& obj1, cls_rgw_obj& obj2)
{
  return (obj1.pool == obj2.pool) &&
         (obj1.key == obj2.key) &&
         (obj1.loc == obj2.loc);
}


TEST(cls_rgw, gc_set)
{
  /* add chains */
  string oid = "obj";
  for (int i = 0; i < 10; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    op.create(false); // create object

    info.tag = tag;
    cls_rgw_gc_set_entry(op, 0, info);

    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }

  bool truncated;
  list<cls_rgw_gc_obj_info> entries;
  string marker;
  string next_marker;

  /* list chains, verify truncated */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 8, true, entries, &truncated, next_marker));
  ASSERT_EQ(8, (int)entries.size());
  ASSERT_EQ(1, truncated);

  entries.clear();
  next_marker.clear();

  /* list all chains, verify not truncated */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 10, true, entries, &truncated, next_marker));
  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, truncated);
 
  /* verify all chains are valid */
  list<cls_rgw_gc_obj_info>::iterator iter = entries.begin();
  for (int i = 0; i < 10; i++, ++iter) {
    cls_rgw_gc_obj_info& entry = *iter;

    /* create expected chain name */
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;

    /* verify chain name as expected */
    ASSERT_EQ(entry.tag, tag);

    /* verify expected num of objects in chain */
    ASSERT_EQ(2, (int)entry.chain.objs.size());

    list<cls_rgw_obj>::iterator oiter = entry.chain.objs.begin();
    cls_rgw_obj obj1, obj2;

    /* create expected objects */
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);

    /* assign returned object names */
    cls_rgw_obj& ret_obj1 = *oiter++;
    cls_rgw_obj& ret_obj2 = *oiter;

    /* verify objects are as expected */
    ASSERT_EQ(1, (int)cmp_objs(obj1, ret_obj1));
    ASSERT_EQ(1, (int)cmp_objs(obj2, ret_obj2));
  }
}

TEST(cls_rgw, gc_list)
{
  /* add chains */
  string oid = "obj";
  for (int i = 0; i < 10; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    op.create(false); // create object

    info.tag = tag;
    cls_rgw_gc_set_entry(op, 0, info);

    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }

  bool truncated;
  list<cls_rgw_gc_obj_info> entries;
  list<cls_rgw_gc_obj_info> entries2;
  string marker;
  string next_marker;

  /* list chains, verify truncated */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 8, true, entries, &truncated, next_marker));
  ASSERT_EQ(8, (int)entries.size());
  ASSERT_EQ(1, truncated);

  marker = next_marker;
  next_marker.clear();

  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 8, true, entries2, &truncated, next_marker));
  ASSERT_EQ(2, (int)entries2.size());
  ASSERT_EQ(0, truncated);

  entries.splice(entries.end(), entries2);

  /* verify all chains are valid */
  list<cls_rgw_gc_obj_info>::iterator iter = entries.begin();
  for (int i = 0; i < 10; i++, ++iter) {
    cls_rgw_gc_obj_info& entry = *iter;

    /* create expected chain name */
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;

    /* verify chain name as expected */
    ASSERT_EQ(entry.tag, tag);

    /* verify expected num of objects in chain */
    ASSERT_EQ(2, (int)entry.chain.objs.size());

    list<cls_rgw_obj>::iterator oiter = entry.chain.objs.begin();
    cls_rgw_obj obj1, obj2;

    /* create expected objects */
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);

    /* assign returned object names */
    cls_rgw_obj& ret_obj1 = *oiter++;
    cls_rgw_obj& ret_obj2 = *oiter;

    /* verify objects are as expected */
    ASSERT_EQ(1, (int)cmp_objs(obj1, ret_obj1));
    ASSERT_EQ(1, (int)cmp_objs(obj2, ret_obj2));
  }
}

TEST(cls_rgw, gc_defer)
{
  librados::IoCtx ioctx;
  librados::Rados rados;

  string gc_pool_name = get_temp_pool_name();
  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(gc_pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(gc_pool_name.c_str(), ioctx));

  string oid = "obj";
  string tag = "mychain";

  librados::ObjectWriteOperation op;
  cls_rgw_gc_obj_info info;

  op.create(false);

  info.tag = tag;

  /* create chain */
  cls_rgw_gc_set_entry(op, 0, info);

  ASSERT_EQ(0, ioctx.operate(oid, &op));

  bool truncated;
  list<cls_rgw_gc_obj_info> entries;
  string marker;
  string next_marker;

  /* list chains, verify num entries as expected */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, true, entries, &truncated, next_marker));
  ASSERT_EQ(1, (int)entries.size());
  ASSERT_EQ(0, truncated);

  librados::ObjectWriteOperation op2;

  /* defer chain */
  cls_rgw_gc_defer_entry(op2, 5, tag);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  entries.clear();
  next_marker.clear();

  /* verify list doesn't show deferred entry (this may fail if cluster is thrashing) */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, true, entries, &truncated, next_marker));
  ASSERT_EQ(0, (int)entries.size());
  ASSERT_EQ(0, truncated);

  /* wait enough */
  sleep(5);
  next_marker.clear();

  /* verify list shows deferred entry */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, true, entries, &truncated, next_marker));
  ASSERT_EQ(1, (int)entries.size());
  ASSERT_EQ(0, truncated);

  librados::ObjectWriteOperation op3;
  vector<string> tags;
  tags.push_back(tag);

  /* remove chain */
  cls_rgw_gc_remove(op3, tags);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));

  entries.clear();
  next_marker.clear();

  /* verify entry was removed */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, true, entries, &truncated, next_marker));
  ASSERT_EQ(0, (int)entries.size());
  ASSERT_EQ(0, truncated);

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(gc_pool_name, rados));
}

auto populate_usage_log_info(std::string user, std::string payer, int total_usage_entries)
{
  rgw_usage_log_info info;

  for (int i=0; i < total_usage_entries; i++){
    auto bucket = str_int("bucket", i);
    info.entries.emplace_back(rgw_usage_log_entry(user, payer, bucket));
  }

  return info;
}

auto gen_usage_log_info(std::string payer, std::string bucket, int total_usage_entries)
{
  rgw_usage_log_info info;
  for (int i=0; i < total_usage_entries; i++){
    auto user = str_int("user", i);
    info.entries.emplace_back(rgw_usage_log_entry(user, payer, bucket));
  }

  return info;
}

TEST(cls_rgw, usage_basic)
{
  string oid="usage.1";
  string user="user1";
  uint64_t start_epoch{0}, end_epoch{(uint64_t) -1};
  int total_usage_entries = 512;
  uint64_t max_entries = 2000;
  string payer;

  auto info = populate_usage_log_info(user, payer, total_usage_entries);
  ObjectWriteOperation op;
  cls_rgw_usage_log_add(op, info);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  string read_iter;
  map <rgw_user_bucket, rgw_usage_log_entry> usage, usage2;
  bool truncated;


  int ret = cls_rgw_usage_log_read(ioctx, oid, user, "", start_epoch, end_epoch,
				   max_entries, read_iter, usage, &truncated);
  // read the entries, and see that we have all the added entries
  ASSERT_EQ(0, ret);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(static_cast<uint64_t>(total_usage_entries), usage.size());

  // delete and read to assert that we've deleted all the values
  ASSERT_EQ(0, cls_rgw_usage_log_trim(ioctx, oid, user, "", start_epoch, end_epoch));


  ret = cls_rgw_usage_log_read(ioctx, oid, user, "", start_epoch, end_epoch,
			       max_entries, read_iter, usage2, &truncated);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0u, usage2.size());

  // add and read to assert that bucket option is valid for usage reading
  string bucket1 = "bucket-usage-1";
  string bucket2 = "bucket-usage-2";
  info = gen_usage_log_info(payer, bucket1, 100);
  cls_rgw_usage_log_add(op, info);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  info = gen_usage_log_info(payer, bucket2, 100);
  cls_rgw_usage_log_add(op, info);
  ASSERT_EQ(0, ioctx.operate(oid, &op));
  ret = cls_rgw_usage_log_read(ioctx, oid, "", bucket1, start_epoch, end_epoch,
                              max_entries, read_iter, usage2, &truncated);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(100u, usage2.size());

  // delete and read to assert that bucket option is valid for usage trim
  ASSERT_EQ(0, cls_rgw_usage_log_trim(ioctx, oid, "", bucket1, start_epoch, end_epoch));

  ret = cls_rgw_usage_log_read(ioctx, oid, "", bucket1, start_epoch, end_epoch,
                               max_entries, read_iter, usage2, &truncated);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0u, usage2.size());
  ASSERT_EQ(0, cls_rgw_usage_log_trim(ioctx, oid, "", bucket2, start_epoch, end_epoch));
}

TEST(cls_rgw, usage_clear_no_obj)
{
  string user="user1";
  string oid="usage.10";
  librados::ObjectWriteOperation op;
  cls_rgw_usage_log_clear(op);
  int ret = ioctx.operate(oid, &op);
  ASSERT_EQ(0, ret);

}

TEST(cls_rgw, usage_clear)
{
  string user="user1";
  string payer;
  string oid="usage.10";
  librados::ObjectWriteOperation op;
  int max_entries=2000;

  auto info = populate_usage_log_info(user, payer, max_entries);

  cls_rgw_usage_log_add(op, info);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  ObjectWriteOperation op2;
  cls_rgw_usage_log_clear(op2);
  int ret = ioctx.operate(oid, &op2);
  ASSERT_EQ(0, ret);

  map <rgw_user_bucket, rgw_usage_log_entry> usage;
  bool truncated;
  uint64_t start_epoch{0}, end_epoch{(uint64_t) -1};
  string read_iter;
  ret = cls_rgw_usage_log_read(ioctx, oid, user, "", start_epoch, end_epoch,
			       max_entries, read_iter, usage, &truncated);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0u, usage.size());

}


/* must be last test! */

TEST(cls_rgw, finalize)
{
  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}
