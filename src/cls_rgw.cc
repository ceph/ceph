#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "rgw/rgw_cls_api.h"
#include "common/Clock.h"

#include "global/global_context.h"

CLS_VER(1,0)
CLS_NAME(rgw)

cls_handle_t h_class;
cls_method_handle_t h_rgw_bucket_init_index;
cls_method_handle_t h_rgw_bucket_list;
cls_method_handle_t h_rgw_bucket_prepare_op;
cls_method_handle_t h_rgw_bucket_complete_op;


#define ROUND_BLOCK_SIZE 4096

static uint64_t get_rounded_size(uint64_t size)
{
  return (size + ROUND_BLOCK_SIZE - 1) & ~(ROUND_BLOCK_SIZE - 1);
}

static int read_bucket_dir(cls_method_context_t hctx, struct rgw_bucket_dir& dir)
{
  bufferlist bl;

  uint64_t size;
  int rc = cls_cxx_stat(hctx, &size, NULL);
  if (rc < 0)
    return rc;

  rc = cls_cxx_read(hctx, 0, size, &bl);
  if (rc < 0)
    return rc;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(dir, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_list(): failed to decode buffer\n");
    return -EIO;
  }

  return 0;
}

static int write_bucket_dir(cls_method_context_t hctx, struct rgw_bucket_dir& dir)
{
  bufferlist bl;

  ::encode(dir, bl);

  int rc = cls_cxx_write_full(hctx, &bl);
  return rc;
}

int rgw_bucket_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rgw_bucket_dir dir;
  int rc = read_bucket_dir(hctx, dir);
  if (rc < 0)
    return rc;

  bufferlist::iterator iter = in->begin();

  struct rgw_cls_list_op op;

  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_list(): failed to decode request\n");
    return -EINVAL;
  }

  struct rgw_cls_list_ret ret;
  struct rgw_bucket_dir& new_dir = ret.dir;
  new_dir.header = dir.header;
  std::map<string, struct rgw_bucket_dir_entry>& m = new_dir.m;
  std::map<string, struct rgw_bucket_dir_entry>::iterator miter = dir.m.lower_bound(op.start_obj);
  uint32_t i;
  for (i = 0; i != op.num_entries && miter != dir.m.end(); ++i, ++miter) {
    m[miter->first] = miter->second;
  }

  ret.is_truncated = (miter != dir.m.end());

  ::encode(ret, *out);

  return 0;
}

int rgw_bucket_init_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  bufferlist::iterator iter;

  uint64_t size;
  int rc = cls_cxx_stat(hctx, &size, NULL);
  if (rc < 0)
    return rc;
  if (size != 0) {
    CLS_LOG("ERROR: index already initialized\n");
    return -EINVAL;
  }

  rgw_bucket_dir dir;
  rc = write_bucket_dir(hctx, dir);

  return rc;
}

int rgw_bucket_prepare_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rgw_bucket_dir dir;
  int rc = read_bucket_dir(hctx, dir);
  if (rc < 0)
    return rc;

  rgw_cls_obj_prepare_op op;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_prepare_op(): failed to decode request\n");
    return -EINVAL;
  }
  CLS_LOG("rgw_bucket_prepare_op(): request: op=%d name=%s\n", op.op, op.name.c_str());

  std::map<string, struct rgw_bucket_dir_entry>::iterator miter = dir.m.find(op.name);
  struct rgw_bucket_dir_entry *entry = NULL;

  if (miter != dir.m.end()) {
    entry = &miter->second;
  } else {
    entry = &dir.m[op.name];
    entry->name = op.name;
    entry->epoch = 0;
    entry->exists = false;
  }

  if (op.tag.empty()) {
    CLS_LOG("ERROR: tag is empty\n");
    return -EINVAL;
  }

  struct rgw_bucket_pending_info& info = entry->pending_map[op.tag];
  info.timestamp = ceph_clock_now(g_ceph_context);
  info.state = CLS_RGW_STATE_PENDING_MODIFY;
  info.op = op.op;

  entry->pending_map[op.tag] = info;

  rc = write_bucket_dir(hctx, dir);

  return rc;
}

int rgw_bucket_complete_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rgw_bucket_dir dir;
  int rc = read_bucket_dir(hctx, dir);
  if (rc < 0)
    return rc;

  rgw_cls_obj_complete_op op;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_modify(): failed to decode request\n");
    return -EINVAL;
  }
  CLS_LOG("rgw_bucket_modify(): request: op=%d name=%s epoch=%lld\n", op.op, op.name.c_str(), op.epoch);

  std::map<string, struct rgw_bucket_dir_entry>::iterator miter = dir.m.find(op.name);
  struct rgw_bucket_dir_entry *entry = NULL;

  CLS_LOG("rgw_bucket_modify(): dir.m.size()=%lld", dir.m.size());

  if (miter != dir.m.end()) {
    entry = &miter->second;
    CLS_LOG("rgw_bucket_modify(): existing entry: epoch=%lld\n", entry->epoch);
    if (op.epoch <= entry->epoch) {
      CLS_LOG("rgw_bucket_modify(): skipping request, old epoch\n");
      return 0;
    }

    struct rgw_bucket_category_stats& stats = dir.header.stats[entry->meta.category];
    stats.num_entries--;
    stats.total_size -= entry->meta.size;
    stats.total_size_rounded -= get_rounded_size(entry->meta.size);
  } else {
    entry = &dir.m[op.name];
    entry->name = op.name;
    entry->epoch = op.epoch;
    entry->meta = op.meta;
  }

  if (op.tag.size()) {
    map<string, struct rgw_bucket_pending_info>::iterator pinter = entry->pending_map.find(op.tag);
    if (pinter == entry->pending_map.end()) {
      CLS_LOG("ERROR: couldn't find tag for pending operation\n");
      return -EINVAL;
    }
    entry->pending_map.erase(pinter);
  }

  switch (op.op) {
  case CLS_RGW_OP_DEL:
    if (miter != dir.m.end()) {
      if (!entry->pending_map.size())
        dir.m.erase(miter);
      else
        entry->exists = false;
    } else {
      return -ENOENT;
    }
    break;
  case CLS_RGW_OP_ADD:
    {
      struct rgw_bucket_dir_entry_meta& meta = op.meta;
      struct rgw_bucket_category_stats& stats = dir.header.stats[meta.category];
      entry->meta = meta;
      entry->name = op.name;
      entry->epoch = op.epoch;
      entry->exists = true;
      stats.num_entries++;
      stats.total_size += meta.size;
      stats.total_size_rounded += get_rounded_size(meta.size);
    }
    break;
  }

  rc = write_bucket_dir(hctx, dir);

  return rc;
}

void __cls_init()
{
  CLS_LOG("Loaded rgw class!");

  cls_register("rgw", &h_class);
  cls_register_cxx_method(h_class, "bucket_init_index", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_init_index, &h_rgw_bucket_init_index);
  cls_register_cxx_method(h_class, "bucket_list", CLS_METHOD_RD | CLS_METHOD_PUBLIC, rgw_bucket_list, &h_rgw_bucket_list);
  cls_register_cxx_method(h_class, "bucket_prepare_op", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_prepare_op, &h_rgw_bucket_prepare_op);
  cls_register_cxx_method(h_class, "bucket_complete_op", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_complete_op, &h_rgw_bucket_complete_op);

  return;
}

