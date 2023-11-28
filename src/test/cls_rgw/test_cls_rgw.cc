// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_ops.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"
#include "common/ceph_context.h"

#include <errno.h>
#include <string>
#include <vector>
#include <map>
#include <set>

using namespace std;
using namespace librados;

// creates a temporary pool and initializes an IoCtx shared by all tests
class cls_rgw : public ::testing::Test {
  static librados::Rados rados;
  static std::string pool_name;
 protected:
  static librados::IoCtx ioctx;

  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    /* create pool */
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  static void TearDownTestCase() {
    /* remove pool */
    ioctx.close();
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
  }
};
librados::Rados cls_rgw::rados;
std::string cls_rgw::pool_name;
librados::IoCtx cls_rgw::ioctx;


string str_int(string s, int i)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d", i);
  s.append(buf);

  return s;
}

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

void index_prepare(librados::IoCtx& ioctx, string& oid, RGWModifyOp index_op,
                   string& tag, const cls_rgw_obj_key& key, string& loc,
                   uint16_t bi_flags = 0, bool log_op = true)
{
  ObjectWriteOperation op;
  rgw_zone_set zones_trace;
  cls_rgw_bucket_prepare_op(op, index_op, tag, key, loc, log_op, bi_flags, zones_trace);
  ASSERT_EQ(0, ioctx.operate(oid, &op));
}

void index_complete(librados::IoCtx& ioctx, string& oid, RGWModifyOp index_op,
                    string& tag, int epoch, const cls_rgw_obj_key& key,
                    rgw_bucket_dir_entry_meta& meta, uint16_t bi_flags = 0,
                    bool log_op = true)
{
  ObjectWriteOperation op;
  rgw_bucket_entry_ver ver;
  ver.pool = ioctx.get_id();
  ver.epoch = epoch;
  meta.accounted_size = meta.size;
  cls_rgw_bucket_complete_op(op, index_op, tag, ver, key, meta, nullptr, log_op, bi_flags, nullptr);
  ASSERT_EQ(0, ioctx.operate(oid, &op));
  if (!key.instance.empty()) {
    bufferlist olh_tag;
    olh_tag.append(tag);
    rgw_zone_set zone_set;
    ASSERT_EQ(0, cls_rgw_bucket_link_olh(ioctx, oid, key, olh_tag,
                                         false, tag, &meta, epoch,
                                         ceph::real_time{}, true, true, zone_set));
  }
}

TEST_F(cls_rgw, index_basic)
{
  string bucket_oid = str_int("bucket", 0);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  uint64_t epoch = 1;

  uint64_t obj_size = 1024;

#define NUM_OBJS 10
  for (int i = 0; i < NUM_OBJS; i++) {
    cls_rgw_obj_key obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i, obj_size * i);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta);
  }

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS,
	     obj_size * NUM_OBJS);
}

TEST_F(cls_rgw, index_multiple_obj_writers)
{
  string bucket_oid = str_int("bucket", 1);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  uint64_t obj_size = 1024;

  cls_rgw_obj_key obj = str_int("obj", 0);
  string loc = str_int("loc", 0);
  /* multi prepare on a single object */
  for (int i = 0; i < NUM_OBJS; i++) {
    string tag = str_int("tag", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, 0, 0);
  }

  for (int i = NUM_OBJS; i > 0; i--) {
    string tag = str_int("tag", i - 1);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size * i;

    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, i, obj, meta);

    /* verify that object size doesn't change, as we went back with epoch */
    test_stats(ioctx, bucket_oid, RGWObjCategory::None, 1,
	       obj_size * NUM_OBJS);
  }
}

TEST_F(cls_rgw, index_remove_object)
{
  string bucket_oid = str_int("bucket", 2);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  uint64_t obj_size = 1024;
  uint64_t total_size = 0;

  int epoch = 0;

  /* prepare multiple objects */
  for (int i = 0; i < NUM_OBJS; i++) {
    cls_rgw_obj_key obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i, total_size);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = i * obj_size;
    total_size += i * obj_size;

    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, ++epoch, obj, meta);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i + 1, total_size);
  }

  int i = NUM_OBJS / 2;
  string tag_remove = "tag-rm";
  string tag_modify = "tag-mod";
  cls_rgw_obj_key obj = str_int("obj", i);
  string loc = str_int("loc", i);

  /* prepare both removal and modification on the same object */
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, obj, loc);
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag_modify, obj, loc);

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS, total_size);

  rgw_bucket_dir_entry_meta meta;

  /* complete object removal */
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, ++epoch, obj, meta);

  /* verify stats correct */
  total_size -= i * obj_size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS - 1, total_size);

  meta.size = 512;
  meta.category = RGWObjCategory::None;

  /* complete object modification */
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag_modify, ++epoch, obj, meta);

  /* verify stats correct */
  total_size += meta.size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS, total_size);


  /* prepare both removal and modification on the same object, this time we'll
   * first complete modification then remove*/
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, obj, loc);
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_modify, obj, loc);

  /* complete modification */
  total_size -= meta.size;
  meta.size = i * obj_size * 2;
  meta.category = RGWObjCategory::None;

  /* complete object modification */
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag_modify, ++epoch, obj, meta);

  /* verify stats correct */
  total_size += meta.size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS, total_size);

  /* complete object removal */
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag_remove, ++epoch, obj, meta);

  /* verify stats correct */
  total_size -= meta.size;
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, NUM_OBJS - 1,
	     total_size);
}

TEST_F(cls_rgw, index_suggest)
{
  string bucket_oid = str_int("suggest", 1);
  {
    ObjectWriteOperation op;
    cls_rgw_bucket_init_index(op);
    ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));
  }
  uint64_t total_size = 0;

  int epoch = 0;

  int num_objs = 100;

  uint64_t obj_size = 1024;

  /* create multiple objects */
  for (int i = 0; i < num_objs; i++) {
    cls_rgw_obj_key obj = str_int("obj", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i, total_size);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    total_size += meta.size;

    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, ++epoch, obj, meta);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, i + 1, total_size);
  }

  /* prepare (without completion) some of the objects */
  for (int i = 0; i < num_objs; i += 2) {
    cls_rgw_obj_key obj = str_int("obj", i);
    string tag = str_int("tag-prepare", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, num_objs, total_size);
  }

  int actual_num_objs = num_objs;
  /* remove half of the objects */
  for (int i = num_objs / 2; i < num_objs; i++) {
    cls_rgw_obj_key obj = str_int("obj", i);
    string tag = str_int("tag-rm", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

    test_stats(ioctx, bucket_oid, RGWObjCategory::None, actual_num_objs, total_size);

    rgw_bucket_dir_entry_meta meta;
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag, ++epoch, obj, meta);

    total_size -= obj_size;
    actual_num_objs--;
    test_stats(ioctx, bucket_oid, RGWObjCategory::None, actual_num_objs, total_size);
  }

  bufferlist updates;

  for (int i = 0; i < num_objs; i += 2) { 
    cls_rgw_obj_key obj = str_int("obj", i);
    string tag = str_int("tag-rm", i);
    string loc = str_int("loc", i);

    rgw_bucket_dir_entry dirent;
    dirent.key.name = obj.name;
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
  {
    ObjectWriteOperation op;
    cls_rgw_suggest_changes(op, updates);
    ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));
  }
  /* suggest changes twice! */
  {
    ObjectWriteOperation op;
    cls_rgw_suggest_changes(op, updates);
    ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));
  }
  test_stats(ioctx, bucket_oid, RGWObjCategory::None, num_objs / 2, total_size);
}

static void list_entries(librados::IoCtx& ioctx,
                         const std::string& oid,
                         uint32_t num_entries,
                         std::map<int, rgw_cls_list_ret>& results)
{
  std::map<int, std::string> oids = { {0, oid} };
  cls_rgw_obj_key start_key;
  string empty_prefix;
  string empty_delimiter;
  ASSERT_EQ(0, CLSRGWIssueBucketList(ioctx, start_key, empty_prefix,
                                     empty_delimiter, num_entries,
                                     true, oids, results, 1)());
}

TEST_F(cls_rgw, index_suggest_complete)
{
  string bucket_oid = str_int("suggest", 2);
  {
    ObjectWriteOperation op;
    cls_rgw_bucket_init_index(op);
    ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));
  }

  cls_rgw_obj_key obj = str_int("obj", 0);
  string tag = str_int("tag-prepare", 0);
  string loc = str_int("loc", 0);

  // prepare entry
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);

  // list entry before completion
  rgw_bucket_dir_entry dirent;
  {
    std::map<int, rgw_cls_list_ret> listing;
    list_entries(ioctx, bucket_oid, 1, listing);
    ASSERT_EQ(1, listing.size());
    const auto& entries = listing.begin()->second.dir.m;
    ASSERT_EQ(1, entries.size());
    dirent = entries.begin()->second;
    ASSERT_EQ(obj, dirent.key);
  }
  // complete entry
  {
    rgw_bucket_dir_entry_meta meta;
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, 1, obj, meta);
  }
  // suggest removal of listed entry
  {
    bufferlist updates;
    cls_rgw_encode_suggestion(CEPH_RGW_REMOVE, dirent, updates);

    ObjectWriteOperation op;
    cls_rgw_suggest_changes(op, updates);
    ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));
  }
  // list entry again, verify that suggested removal was not applied
  {
    std::map<int, rgw_cls_list_ret> listing;
    list_entries(ioctx, bucket_oid, 1, listing);
    ASSERT_EQ(1, listing.size());
    const auto& entries = listing.begin()->second.dir.m;
    ASSERT_EQ(1, entries.size());
    EXPECT_TRUE(entries.begin()->second.exists);
  }
}

/*
 * This case is used to test whether get_obj_vals will
 * return all validate utf8 objnames and filter out those
 * in BI_PREFIX_CHAR private namespace.
 */
TEST_F(cls_rgw, index_list)
{
  string bucket_oid = str_int("bucket", 4);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  uint64_t epoch = 1;
  uint64_t obj_size = 1024;
  const int num_objs = 4;
  const string keys[num_objs] = {
    /* single byte utf8 character */
    { static_cast<char>(0x41) },
    /* double byte utf8 character */
    { static_cast<char>(0xCF), static_cast<char>(0x8F) },
    /* treble byte utf8 character */
    { static_cast<char>(0xDF), static_cast<char>(0x8F), static_cast<char>(0x8F) },
    /* quadruple byte utf8 character */
    { static_cast<char>(0xF7), static_cast<char>(0x8F), static_cast<char>(0x8F), static_cast<char>(0x8F) },
  };

  for (int i = 0; i < num_objs; i++) {
    string obj = keys[i];
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc,
		  0 /* bi_flags */, false /* log_op */);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta,
		   0 /* bi_flags */, false /* log_op */);
  }

  map<string, bufferlist> entries;
  /* insert 998 omap key starts with BI_PREFIX_CHAR,
   * so bucket list first time will get one key before 0x80 and one key after */
  for (int i = 0; i < 998; ++i) {
    char buf[10];
    snprintf(buf, sizeof(buf), "%c%s%d", 0x80, "1000_", i);
    entries.emplace(string{buf}, bufferlist{});
  }
  ioctx.omap_set(bucket_oid, entries);

  test_stats(ioctx, bucket_oid, RGWObjCategory::None,
	     num_objs, obj_size * num_objs);

  map<int, string> oids = { {0, bucket_oid} };
  map<int, struct rgw_cls_list_ret> list_results;
  cls_rgw_obj_key start_key("", "");
  string empty_prefix;
  string empty_delimiter;
  int r = CLSRGWIssueBucketList(ioctx, start_key,
				empty_prefix, empty_delimiter,
				1000, true, oids, list_results, 1)();
  ASSERT_EQ(r, 0);
  ASSERT_EQ(1u, list_results.size());

  auto it = list_results.begin();
  auto m = (it->second).dir.m;

  ASSERT_EQ(4u, m.size());
  int i = 0;
  for(auto it2 = m.cbegin(); it2 != m.cend(); it2++, i++) {
    ASSERT_EQ(it2->first.compare(keys[i]), 0);
  }
}


/*
 * This case is used to test when bucket index list that includes a
 * delimiter can handle the first chunk ending in a delimiter.
 */
TEST_F(cls_rgw, index_list_delimited)
{
  string bucket_oid = str_int("bucket", 7);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  uint64_t epoch = 1;
  uint64_t obj_size = 1024;
  const int file_num_objs = 5;
  const int dir_num_objs = 1005;

  std::vector<std::string> file_prefixes =
    { "a", "c", "e", "g", "i", "k", "m", "o", "q", "s", "u" };
  std::vector<std::string> dir_prefixes =
    { "b/", "d/", "f/", "h/", "j/", "l/", "n/", "p/", "r/", "t/" };

  rgw_bucket_dir_entry_meta meta;
  meta.category = RGWObjCategory::None;
  meta.size = obj_size;

  // create top-level files
  for (const auto& p : file_prefixes) {
    for (int i = 0; i < file_num_objs; i++) {
      string tag = str_int("tag", i);
      string loc = str_int("loc", i);
      const string obj = str_int(p, i);

      index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc,
		    0 /* bi_flags */, false /* log_op */);

      index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta,
		     0 /* bi_flags */, false /* log_op */);
    }
  }

  // create large directories
  for (const auto& p : dir_prefixes) {
    for (int i = 0; i < dir_num_objs; i++) {
      string tag = str_int("tag", i);
      string loc = str_int("loc", i);
      const string obj = p + str_int("f", i);

      index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc,
		    0 /* bi_flags */, false /* log_op */);

      index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta,
		     0 /* bi_flags */, false /* log_op */);
    }
  }

  map<int, string> oids = { {0, bucket_oid} };
  map<int, struct rgw_cls_list_ret> list_results;
  cls_rgw_obj_key start_key("", "");
  const string empty_prefix;
  const string delimiter = "/";
  int r = CLSRGWIssueBucketList(ioctx, start_key,
				empty_prefix, delimiter,
				1000, true, oids, list_results, 1)();
  ASSERT_EQ(r, 0);
  ASSERT_EQ(1u, list_results.size()) <<
    "Because we only have one bucket index shard, we should "
    "only get one list_result.";

  auto it = list_results.begin();
  auto id_entry_map = it->second.dir.m;
  bool truncated = it->second.is_truncated;

  // the cls code will make 4 tries to get 1000 entries; however
  // because each of the subdirectories is so large, each attempt will
  // only retrieve the first part of the subdirectory

  ASSERT_EQ(48u, id_entry_map.size()) <<
    "We should get 40 top-level entries and the tops of 8 \"subdirectories\".";
  ASSERT_EQ(true, truncated) << "We did not get all entries.";

  ASSERT_EQ("a-0", id_entry_map.cbegin()->first);
  ASSERT_EQ("p/", id_entry_map.crbegin()->first);

  // now let's get the rest of the entries

  list_results.clear();
  
  cls_rgw_obj_key start_key2("p/", "");
  r = CLSRGWIssueBucketList(ioctx, start_key2,
			    empty_prefix, delimiter,
			    1000, true, oids, list_results, 1)();
  ASSERT_EQ(r, 0);

  it = list_results.begin();
  id_entry_map = it->second.dir.m;
  truncated = it->second.is_truncated;

  ASSERT_EQ(17u, id_entry_map.size()) <<
    "We should get 15 top-level entries and the tops of 2 \"subdirectories\".";
  ASSERT_EQ(false, truncated) << "We now have all entries.";

  ASSERT_EQ("q-0", id_entry_map.cbegin()->first);
  ASSERT_EQ("u-4", id_entry_map.crbegin()->first);
}


TEST_F(cls_rgw, bi_list)
{
  string bucket_oid = str_int("bucket", 5);

  CephContext *cct = reinterpret_cast<CephContext *>(ioctx.cct());

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  const std::string empty_name_filter;
  uint64_t max = 10;
  std::list<rgw_cls_bi_entry> entries;
  bool is_truncated;
  std::string marker;

  int ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, max,
			    &entries, &is_truncated);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entries.size(), 0u) <<
    "The listing of an empty bucket as 0 entries.";
  ASSERT_EQ(is_truncated, false) <<
    "The listing of an empty bucket is not truncated.";

  uint64_t epoch = 1;
  uint64_t obj_size = 1024;
  const uint64_t num_objs = 35;

  for (uint64_t i = 0; i < num_objs; i++) {
    string obj = str_int(i % 4 ? "obj" : "об'єкт", i);
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);
    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc,
		  RGW_BILOG_FLAG_VERSIONED_OP);

    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::None;
    meta.size = obj_size;
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, epoch, obj, meta,
		   RGW_BILOG_FLAG_VERSIONED_OP);
  }

  ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, num_objs + 10,
			&entries, &is_truncated);
  ASSERT_EQ(ret, 0);
  if (is_truncated) {
    ASSERT_LT(entries.size(), num_objs);
  } else {
    ASSERT_EQ(entries.size(), num_objs);
  }

  uint64_t num_entries = 0;

  is_truncated = true;
  marker.clear();
  while(is_truncated) {
    ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, max,
			  &entries, &is_truncated);
    ASSERT_EQ(ret, 0);
    if (is_truncated) {
      ASSERT_LT(entries.size(), num_objs - num_entries);
    } else {
      ASSERT_EQ(entries.size(), num_objs - num_entries);
    }
    num_entries += entries.size();
    marker = entries.back().idx;
  }

  // try with marker as final entry
  ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, max,
			&entries, &is_truncated);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entries.size(), 0u);
  ASSERT_EQ(is_truncated, false);

  if (cct->_conf->osd_max_omap_entries_per_request < 15) {
    num_entries = 0;
    max = 15;
    is_truncated = true;
    marker.clear();
    while(is_truncated) {
      ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, max,
			    &entries, &is_truncated);
      ASSERT_EQ(ret, 0);
      if (is_truncated) {
	ASSERT_LT(entries.size(), num_objs - num_entries);
      } else {
	ASSERT_EQ(entries.size(), num_objs - num_entries);
      }
      num_entries += entries.size();
      marker = entries.back().idx;
    }

    // try with marker as final entry
    ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, max,
			  &entries, &is_truncated);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(entries.size(), 0u);
    ASSERT_EQ(is_truncated, false);
  }

  // test with name filters; pairs contain filter and expected number of elements returned
  const std::list<std::pair<const std::string,unsigned>> filters_results =
    { { str_int("obj", 9), 1 },
      { str_int("об'єкт", 8), 1 },
      { str_int("obj", 8), 0 } };
  for (const auto& filter_result : filters_results) {
    is_truncated = true;
    entries.clear();
    marker.clear();

    ret = cls_rgw_bi_list(ioctx, bucket_oid, filter_result.first, marker, max,
			  &entries, &is_truncated);

    ASSERT_EQ(ret, 0) << "bi list test with name filters should succeed";
    ASSERT_EQ(entries.size(), filter_result.second) <<
      "bi list test with filters should return the correct number of results";
    ASSERT_EQ(is_truncated, false) <<
      "bi list test with filters should return correct truncation indicator";
  }

  // test whether combined segment count is correct
  is_truncated = false;
  entries.clear();
  marker.clear();

  ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, num_objs - 1,
			&entries, &is_truncated);
  ASSERT_EQ(ret, 0) << "combined segment count should succeed";
  ASSERT_EQ(entries.size(), num_objs - 1) <<
    "combined segment count should return the correct number of results";
  ASSERT_EQ(is_truncated, true) <<
    "combined segment count should return correct truncation indicator";


  marker = entries.back().idx; // advance marker
  ret = cls_rgw_bi_list(ioctx, bucket_oid, empty_name_filter, marker, num_objs - 1,
			&entries, &is_truncated);
  ASSERT_EQ(ret, 0) << "combined segment count should succeed";
  ASSERT_EQ(entries.size(), 1) <<
    "combined segment count should return the correct number of results";
  ASSERT_EQ(is_truncated, false) <<
    "combined segment count should return correct truncation indicator";
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


TEST_F(cls_rgw, gc_set)
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

TEST_F(cls_rgw, gc_list)
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

TEST_F(cls_rgw, gc_defer)
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

TEST_F(cls_rgw, usage_basic)
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

TEST_F(cls_rgw, usage_clear_no_obj)
{
  string user="user1";
  string oid="usage.10";
  librados::ObjectWriteOperation op;
  cls_rgw_usage_log_clear(op);
  int ret = ioctx.operate(oid, &op);
  ASSERT_EQ(0, ret);

}

TEST_F(cls_rgw, usage_clear)
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

static int bilog_list(librados::IoCtx& ioctx, const std::string& oid,
                      cls_rgw_bi_log_list_ret *result)
{
  int retcode = 0;
  librados::ObjectReadOperation op;
  cls_rgw_bilog_list(op, "", 128, result, &retcode);
  int ret = ioctx.operate(oid, &op, nullptr);
  if (ret < 0) {
    return ret;
  }
  return retcode;
}

static int bilog_trim(librados::IoCtx& ioctx, const std::string& oid,
                      const std::string& start_marker,
                      const std::string& end_marker)
{
  librados::ObjectWriteOperation op;
  cls_rgw_bilog_trim(op, start_marker, end_marker);
  return ioctx.operate(oid, &op);
}

TEST_F(cls_rgw, bi_log_trim)
{
  string bucket_oid = str_int("bucket", 6);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  // create 10 versioned entries. this generates instance and olh bi entries,
  // allowing us to check that bilog trim doesn't remove any of those
  for (int i = 0; i < 10; i++) {
    cls_rgw_obj_key obj{str_int("obj", i), "inst"};
    string tag = str_int("tag", i);
    string loc = str_int("loc", i);

    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);
    rgw_bucket_dir_entry_meta meta;
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, 1, obj, meta);
  }
  // bi list
  {
    list<rgw_cls_bi_entry> entries;
    bool truncated{false};
    ASSERT_EQ(0, cls_rgw_bi_list(ioctx, bucket_oid, "", "", 128,
                                 &entries, &truncated));
    // prepare/complete/instance/olh entry for each
    EXPECT_EQ(40u, entries.size());
    EXPECT_FALSE(truncated);
  }
  // bilog list
  vector<rgw_bi_log_entry> bilog1;
  {
    cls_rgw_bi_log_list_ret bilog;
    ASSERT_EQ(0, bilog_list(ioctx, bucket_oid, &bilog));
    // complete/olh entry for each
    EXPECT_EQ(20u, bilog.entries.size());

    bilog1.assign(std::make_move_iterator(bilog.entries.begin()),
                  std::make_move_iterator(bilog.entries.end()));
  }
  // trim front of bilog
  {
    const std::string from = "";
    const std::string to = bilog1[0].id;
    ASSERT_EQ(0, bilog_trim(ioctx, bucket_oid, from, to));
    cls_rgw_bi_log_list_ret bilog;
    ASSERT_EQ(0, bilog_list(ioctx, bucket_oid, &bilog));
    EXPECT_EQ(19u, bilog.entries.size());
    EXPECT_EQ(bilog1[1].id, bilog.entries.begin()->id);
    ASSERT_EQ(-ENODATA, bilog_trim(ioctx, bucket_oid, from, to));
  }
  // trim back of bilog
  {
    const std::string from = bilog1[18].id;
    const std::string to = "9";
    ASSERT_EQ(0, bilog_trim(ioctx, bucket_oid, from, to));
    cls_rgw_bi_log_list_ret bilog;
    ASSERT_EQ(0, bilog_list(ioctx, bucket_oid, &bilog));
    EXPECT_EQ(18u, bilog.entries.size());
    EXPECT_EQ(bilog1[18].id, bilog.entries.rbegin()->id);
    ASSERT_EQ(-ENODATA, bilog_trim(ioctx, bucket_oid, from, to));
  }
  // trim middle of bilog
  {
    const std::string from = bilog1[13].id;
    const std::string to = bilog1[14].id;
    ASSERT_EQ(0, bilog_trim(ioctx, bucket_oid, from, to));
    cls_rgw_bi_log_list_ret bilog;
    ASSERT_EQ(0, bilog_list(ioctx, bucket_oid, &bilog));
    EXPECT_EQ(17u, bilog.entries.size());
    ASSERT_EQ(-ENODATA, bilog_trim(ioctx, bucket_oid, from, to));
  }
  // trim full bilog
  {
    const std::string from = "";
    const std::string to = "9";
    ASSERT_EQ(0, bilog_trim(ioctx, bucket_oid, from, to));
    cls_rgw_bi_log_list_ret bilog;
    ASSERT_EQ(0, bilog_list(ioctx, bucket_oid, &bilog));
    EXPECT_EQ(0u, bilog.entries.size());
    ASSERT_EQ(-ENODATA, bilog_trim(ioctx, bucket_oid, from, to));
  }
  // bi list should be the same
  {
    list<rgw_cls_bi_entry> entries;
    bool truncated{false};
    ASSERT_EQ(0, cls_rgw_bi_list(ioctx, bucket_oid, "", "", 128,
                                 &entries, &truncated));
    EXPECT_EQ(40u, entries.size());
    EXPECT_FALSE(truncated);
  }
}

TEST_F(cls_rgw, index_racing_removes)
{
  string bucket_oid = str_int("bucket", 8);

  ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  ASSERT_EQ(0, ioctx.operate(bucket_oid, &op));

  int epoch = 0;
  rgw_bucket_dir_entry dirent;
  rgw_bucket_dir_entry_meta meta;

  // prepare/complete add for single object
  const cls_rgw_obj_key obj{"obj"};
  std::string loc = "loc";
  {
    std::string tag = "tag-add";
    index_prepare(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, obj, loc);
    index_complete(ioctx, bucket_oid, CLS_RGW_OP_ADD, tag, ++epoch, obj, meta);
    test_stats(ioctx, bucket_oid, RGWObjCategory::None, 1, 0);
  }

  // list to verify no pending ops
  {
    std::map<int, rgw_cls_list_ret> results;
    list_entries(ioctx, bucket_oid, 1, results);
    ASSERT_EQ(1, results.size());
    const auto& entries = results.begin()->second.dir.m;
    ASSERT_EQ(1, entries.size());
    dirent = std::move(entries.begin()->second);
    ASSERT_EQ(obj, dirent.key);
    ASSERT_TRUE(dirent.exists);
    ASSERT_TRUE(dirent.pending_map.empty());
  }

  // prepare three racing removals
  std::string tag1 = "tag-rm1";
  std::string tag2 = "tag-rm2";
  std::string tag3 = "tag-rm3";
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag1, obj, loc);
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag2, obj, loc);
  index_prepare(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag3, obj, loc);

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, 1, 0);

  // complete on tag2
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_DEL, tag2, ++epoch, obj, meta);
  {
    std::map<int, rgw_cls_list_ret> results;
    list_entries(ioctx, bucket_oid, 1, results);
    ASSERT_EQ(1, results.size());
    const auto& entries = results.begin()->second.dir.m;
    ASSERT_EQ(1, entries.size());
    dirent = std::move(entries.begin()->second);
    ASSERT_EQ(obj, dirent.key);
    ASSERT_FALSE(dirent.exists);
    ASSERT_FALSE(dirent.pending_map.empty());
  }

  // cancel on tag1
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_CANCEL, tag1, ++epoch, obj, meta);
  {
    std::map<int, rgw_cls_list_ret> results;
    list_entries(ioctx, bucket_oid, 1, results);
    ASSERT_EQ(1, results.size());
    const auto& entries = results.begin()->second.dir.m;
    ASSERT_EQ(1, entries.size());
    dirent = std::move(entries.begin()->second);
    ASSERT_EQ(obj, dirent.key);
    ASSERT_FALSE(dirent.exists);
    ASSERT_FALSE(dirent.pending_map.empty());
  }

  // final cancel on tag3
  index_complete(ioctx, bucket_oid, CLS_RGW_OP_CANCEL, tag3, ++epoch, obj, meta);

  // verify that the key was removed
  {
    std::map<int, rgw_cls_list_ret> results;
    list_entries(ioctx, bucket_oid, 1, results);
    EXPECT_EQ(1, results.size());
    const auto& entries = results.begin()->second.dir.m;
    ASSERT_EQ(0, entries.size());
  }

  test_stats(ioctx, bucket_oid, RGWObjCategory::None, 0, 0);
}
