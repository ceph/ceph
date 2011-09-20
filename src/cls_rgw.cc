#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "rgw/rgw_cls_api.h"

CLS_VER(1,0)
CLS_NAME(rgw)

cls_handle_t h_class;
cls_method_handle_t h_rgw_bucket_list;
cls_method_handle_t h_rgw_bucket_modify;

static int read_bucket_dir(cls_method_context_t hctx, struct rgw_bucket_dir& dir)
{
  bufferlist bl;
  bufferlist::iterator iter;

  int rc = cls_cxx_read(hctx, 0, 0, &bl);
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
  std::map<string, struct rgw_bucket_dir_entry>::iterator miter = dir.m.find(op.start_obj);
  uint32_t i;
  for (i = 0; i != op.num_entries && miter != dir.m.end(); ++i, ++miter) {
    m[miter->first] = miter->second;
  }
  ::encode(ret, *out);

  return 0;
}

int rgw_bucket_modify(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rgw_bucket_dir dir;
  int rc = read_bucket_dir(hctx, dir);
  if (rc < 0)
    return rc;

  rgw_cls_obj_op op;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_modify(): failed to decode request\n");
    return -EINVAL;
  }
  CLS_LOG("rgw_bucket_modify(): request: op=%d name=%s epoch=%lld\n", op.op, op.entry.name.c_str(), op.epoch);

  std::map<string, struct rgw_bucket_dir_entry>::iterator miter = dir.m.find(op.entry.name);

  if (miter != dir.m.end()) {
    struct rgw_bucket_dir_entry& entry = miter->second;
    CLS_LOG("rgw_bucket_modify(): existing entry: epoch=%lld\n", entry.epoch);
    if (op.entry.epoch >= entry.epoch) {
      CLS_LOG("rgw_bucket_modify(): skipping request, old epoch\n");
      return 0;
    }
  }

  switch (op.op) {
  case CLS_RGW_OP_DEL:
    if (miter != dir.m.end())
      dir.m.erase(miter);
    else
      return -ENOENT;
    break;
  case CLS_RGW_OP_ADD:
    dir.m[op.entry.name] = op.entry;
    break;
  }

  rc = write_bucket_dir(hctx, dir);

  return rc;
}

void __cls_init()
{
  CLS_LOG("Loaded rgw class!");

  cls_register("rgw", &h_class);
  cls_register_cxx_method(h_class, "rgw_bucket_list", CLS_METHOD_RD | CLS_METHOD_PUBLIC, rgw_bucket_list, &h_rgw_bucket_list);
  cls_register_cxx_method(h_class, "rgw_bucket_modify", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_modify, &h_rgw_bucket_modify);

  return;
}

