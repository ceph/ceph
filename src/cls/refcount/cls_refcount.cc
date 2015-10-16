// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "common/Clock.h"

#include "global/global_context.h"
#include "include/compat.h"

CLS_VER(1,0)
CLS_NAME(refcount)

cls_handle_t h_class;
cls_method_handle_t h_refcount_get;
cls_method_handle_t h_refcount_put;
cls_method_handle_t h_refcount_set;
cls_method_handle_t h_refcount_read;


#define REFCOUNT_ATTR "refcount"

struct obj_refcount {
  map<string, bool> refs;

  obj_refcount() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(refs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(obj_refcount)

static string wildcard_tag;

static int read_refcount(cls_method_context_t hctx, bool implicit_ref, obj_refcount *objr)
{
  bufferlist bl;
  objr->refs.clear();
  int ret = cls_cxx_getxattr(hctx, REFCOUNT_ATTR, &bl);
  if (ret == -ENODATA) {
    if (implicit_ref) {
      objr->refs[wildcard_tag] = true;
    }
    return 0;
  }
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*objr, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: read_refcount(): failed to decode refcount entry\n");
    return -EIO;
  }

  return 0;
}

static int set_refcount(cls_method_context_t hctx, map<string, bool>& refs)
{
  bufferlist bl;
  struct obj_refcount objr;

  objr.refs = refs;

  ::encode(objr, bl);

  int ret = cls_cxx_setxattr(hctx, REFCOUNT_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_refcount_get_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_get(): failed to decode entry\n");
    return -EINVAL;
  }

  obj_refcount objr;
  int ret = read_refcount(hctx, op.implicit_ref, &objr);
  if (ret < 0)
    return ret;

  CLS_LOG(10, "cls_rc_refcount_get() tag=%s\n", op.tag.c_str());

  objr.refs[op.tag] = true;

  ret = set_refcount(hctx, objr.refs);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_put(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_refcount_put_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_put(): failed to decode entry\n");
    return -EINVAL;
  }

  obj_refcount objr;
  int ret = read_refcount(hctx, op.implicit_ref, &objr);
  if (ret < 0)
    return ret;

  if (objr.refs.empty()) {// shouldn't happen!
    CLS_LOG(0, "ERROR: cls_rc_refcount_put() was called without any references!\n");
    return -EINVAL;
  }

  CLS_LOG(10, "cls_rc_refcount_put() tag=%s\n", op.tag.c_str());

  bool found = false;
  map<string, bool>::iterator iter = objr.refs.find(op.tag);
  if (iter != objr.refs.end()) {
    found = true;
  } else if (op.implicit_ref) {
    iter = objr.refs.find(wildcard_tag);
    if (iter != objr.refs.end()) {
      found = true;
    }
  }

  if (!found)
    return 0;

  objr.refs.erase(iter);

  if (objr.refs.empty()) {
    return cls_cxx_remove(hctx);
  }

  ret = set_refcount(hctx, objr.refs);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_refcount_set_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_refcount_set(): failed to decode entry\n");
    return -EINVAL;
  }

  if (!op.refs.size()) {
    return cls_cxx_remove(hctx);
  }

  obj_refcount objr;
  list<string>::iterator iter;
  for (iter = op.refs.begin(); iter != op.refs.end(); ++iter) {
    objr.refs[*iter] = true;
  }

  int ret = set_refcount(hctx, objr.refs);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_refcount_read_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_read(): failed to decode entry\n");
    return -EINVAL;
  }

  obj_refcount objr;

  cls_refcount_read_ret read_ret;
  int ret = read_refcount(hctx, op.implicit_ref, &objr);
  if (ret < 0)
    return ret;

  map<string, bool>::iterator iter;
  for (iter = objr.refs.begin(); iter != objr.refs.end(); ++iter) {
    read_ret.refs.push_back(iter->first);
  }

  ::encode(read_ret, *out);

  return 0;
}

void __cls_init()
{
  CLS_LOG(1, "Loaded refcount class!");

  cls_register("refcount", &h_class);

  /* refcount */
  cls_register_cxx_method(h_class, "get", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_refcount_get, &h_refcount_get);
  cls_register_cxx_method(h_class, "put", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_refcount_put, &h_refcount_put);
  cls_register_cxx_method(h_class, "set", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_refcount_set, &h_refcount_set);
  cls_register_cxx_method(h_class, "read", CLS_METHOD_RD, cls_rc_refcount_read, &h_refcount_read);

  return;
}

