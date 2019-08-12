// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "objclass/objclass.h"

#include "cls/version/cls_version_ops.h"

#include "include/compat.h"

CLS_VER(1,0)
CLS_NAME(version)


#define VERSION_ATTR "ceph.objclass.version"

static int set_version(cls_method_context_t hctx, struct obj_version *objv)
{
  bufferlist bl;

  encode(*objv, bl);

  CLS_LOG(20, "cls_version: set_version %s:%d", objv->tag.c_str(), (int)objv->ver);

  int ret = cls_cxx_setxattr(hctx, VERSION_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int init_version(cls_method_context_t hctx, struct obj_version *objv)
{
#define TAG_LEN 24
  char buf[TAG_LEN + 1];

  int ret = cls_gen_rand_base64(buf, sizeof(buf));
  if (ret < 0)
    return ret;

  objv->ver = 1;
  objv->tag = buf;

  CLS_LOG(20, "cls_version: init_version %s:%d", objv->tag.c_str(), (int)objv->ver);

  return set_version(hctx, objv);
}

/* implicit create should be true only if called from a write operation (set, inc), never from a read operation (read, check) */
static int read_version(cls_method_context_t hctx, obj_version *objv, bool implicit_create)
{
  bufferlist bl;
  int ret = cls_cxx_getxattr(hctx, VERSION_ATTR, &bl);
  if (ret == -ENOENT || ret == -ENODATA) {
    objv->ver = 0;

    if (implicit_create) {
      return init_version(hctx, objv);
    }
    return 0;
  }
  if (ret < 0)
    return ret;

  try {
    auto iter = bl.cbegin();
    decode(*objv, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: read_version(): failed to decode version entry\n");
    return -EIO;
  }
  CLS_LOG(20, "cls_version: read_version %s:%d", objv->tag.c_str(), (int)objv->ver);

  return 0;
}

static int cls_version_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_version_set_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_version_get(): failed to decode entry\n");
    return -EINVAL;
  }

  int ret = set_version(hctx, &op.objv);
  if (ret < 0)
    return ret;

  return 0;
}

static bool check_conds(list<obj_version_cond>& conds, obj_version& objv)
{
  if (conds.empty())
    return true;

  for (list<obj_version_cond>::iterator iter = conds.begin(); iter != conds.end(); ++iter) {
    obj_version_cond& cond = *iter;
    obj_version& v = cond.ver;
    CLS_LOG(20, "cls_version: check_version %s:%d (cond=%d)", v.tag.c_str(), (int)v.ver, (int)cond.cond);

    switch (cond.cond) {
      case VER_COND_NONE:
	break;
      case VER_COND_EQ:
	if (!objv.compare(&v))
	  return false;
	break;
      case VER_COND_GT:
	if (!(objv.ver > v.ver))
	  return false;
	break;
      case VER_COND_GE:
	if (!(objv.ver >= v.ver))
	  return false;
	break;
      case VER_COND_LT:
	if (!(objv.ver < v.ver))
	  return false;
	break;
      case VER_COND_LE:
	if (!(objv.ver <= v.ver))
	  return false;
	break;
      case VER_COND_TAG_EQ:
	if (objv.tag.compare(v.tag) != 0)
	  return false;
	break;
      case VER_COND_TAG_NE:
	if (objv.tag.compare(v.tag) == 0)
	  return false;
	break;
    }
  }

  return true;
}

static int cls_version_inc(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_version_inc_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_version_get(): failed to decode entry\n");
    return -EINVAL;
  }

  obj_version objv;
  int ret = read_version(hctx, &objv, true);
  if (ret < 0)
    return ret;
  
  if (!check_conds(op.conds, objv)) {
    return -ECANCELED;
  }
  objv.inc();

  ret = set_version(hctx, &objv);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_version_check(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_version_check_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_version_get(): failed to decode entry\n");
    return -EINVAL;
  }

  obj_version objv;
  int ret = read_version(hctx, &objv, false);
  if (ret < 0)
    return ret;
  
  if (!check_conds(op.conds, objv)) {
    CLS_LOG(20, "cls_version: failed condition check");
    return -ECANCELED;
  }

  return 0;
}

static int cls_version_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  obj_version objv;

  cls_version_read_ret read_ret;
  int ret = read_version(hctx, &read_ret.objv, false);
  if (ret < 0)
    return ret;

  encode(read_ret, *out);

  return 0;
}

CLS_INIT(version)
{
  CLS_LOG(1, "Loaded version class!");

  cls_handle_t h_class;
  cls_method_handle_t h_version_set;
  cls_method_handle_t h_version_inc;
  cls_method_handle_t h_version_inc_conds;
  cls_method_handle_t h_version_read;
  cls_method_handle_t h_version_check_conds;

  cls_register("version", &h_class);

  /* version */
  cls_register_cxx_method(h_class, "set", CLS_METHOD_RD | CLS_METHOD_WR, cls_version_set, &h_version_set);
  cls_register_cxx_method(h_class, "inc", CLS_METHOD_RD | CLS_METHOD_WR, cls_version_inc, &h_version_inc);
  cls_register_cxx_method(h_class, "inc_conds", CLS_METHOD_RD | CLS_METHOD_WR, cls_version_inc, &h_version_inc_conds);
  cls_register_cxx_method(h_class, "read", CLS_METHOD_RD, cls_version_read, &h_version_read);
  cls_register_cxx_method(h_class, "check_conds", CLS_METHOD_RD, cls_version_check, &h_version_check_conds);

  return;
}

