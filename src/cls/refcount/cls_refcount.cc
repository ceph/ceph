// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/refcount/cls_refcount_ops.h"

#include "include/compat.h"

using std::string;

using ceph::bufferlist;

CLS_VER(1,0)
CLS_NAME(refcount)

#define REFCOUNT_ATTR "refcount"

static string wildcard_tag;

static int read_refcount(cls_method_context_t hctx, const std::string& src_tag, obj_refcount *objr)
{
  bufferlist bl;
  objr->refs.clear();
  int ret = cls_cxx_getxattr(hctx, REFCOUNT_ATTR, &bl);
  if (ret == -ENODATA) {
    if (!src_tag.empty()) {
      objr->refs[src_tag] = true;
    }
    return 0;
  }
  if (ret < 0)
    return ret;

  try {
    auto iter = bl.cbegin();
    decode(*objr, iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: read_refcount(): failed to decode refcount entry\n");
    return -EIO;
  }

  return 0;
}

static int set_refcount(cls_method_context_t hctx, const struct obj_refcount& objr)
{
  bufferlist bl;

  encode(objr, bl);

  int ret = cls_cxx_setxattr(hctx, REFCOUNT_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_refcount_get_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_get(): failed to decode entry\n");
    return -EINVAL;
  }

  CLS_LOG(10, "cls_rc_refcount_get() tag=%s src_tag=%s\n",
          op.tag.c_str(), op.src_tag.c_str());
  // tag must be non-empty strings
  if (unlikely(op.tag.empty() || op.src_tag.empty())) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_get(): empty tag\n");
    return -EINVAL;
  }

  // tags must be different
  if (unlikely(op.tag == op.src_tag)) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_get(): equal tags\n");
    return -EINVAL;
  }

  obj_refcount objr;
  int ret = read_refcount(hctx, op.src_tag, &objr);
  if (ret < 0)
    return ret;

  // this is legal since refcount are idempotent, simply do nothing
  if (objr.refs[op.tag]) {
    CLS_LOG(10, "cls_rc_refcount_get() tag=%s already exists -> do nothing\n",
            op.tag.c_str());
    return 0;
  }

  objr.refs[op.tag] = true;

  ret = set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_put(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_refcount_put_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_put(): failed to decode entry\n");
    return -EINVAL;
  }
  CLS_LOG(10, "cls_rc_refcount_put() tag=%s\n", op.tag.c_str());

  // tag must be non-empty strings
  if (unlikely(op.tag.empty())) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_put(): empty tag\n");
    return -EINVAL;
  }

  obj_refcount objr;
  int ret = read_refcount(hctx, "", &objr);
  if (ret < 0)
    return ret;

  // if refcount was not set on this object it can remove with no further checks
  if (objr.refs.empty()) {
    CLS_LOG(10, "cls_rc_refcount_put() tag=%s was called without any references\n",
            op.tag.c_str());
    ret = cls_cxx_remove(hctx);
    if (unlikely(ret != 0)) {
      CLS_LOG(1, "ERROR: cls_rc_refcount_put::cls_cxx_remove() ret=%d\n", ret);
    }
    return ret;
  }

  auto iter = objr.refs.find(op.tag);
  bool active = (iter != objr.refs.end());
  bool retired = (objr.retired_refs.find(op.tag) != objr.retired_refs.end());

  if (!active && !retired) {
    CLS_LOG(0, "ERROR: cls_rc_refcount_put() tag=%s doesn't exist!\n",
	    op.tag.c_str());
    return -EINVAL;
  }
  else if (!active) {
    return 0;
  }

  objr.retired_refs.insert(op.tag);
  objr.refs.erase(iter);

  if (objr.refs.empty()) {
    return cls_cxx_remove(hctx);
  }

  ret = set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_refcount_set_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_refcount_set(): failed to decode entry\n");
    return -EINVAL;
  }

  if (!op.refs.size()) {
    return cls_cxx_remove(hctx);
  }

  obj_refcount objr;
  for (auto iter = op.refs.begin(); iter != op.refs.end(); ++iter) {
    // tag must be non-empty strings
    if (unlikely(iter->empty())) {
      CLS_LOG(1, "ERROR: cls_rc_refcount_set(): empty tag\n");
      return -EINVAL;
    }
    if (unlikely(objr.refs[*iter])) {
      CLS_LOG(1, "ERROR: cls_rc_refcount_set(): repeated tag %s\n", iter->c_str());
      return -EEXIST;
    }
    objr.refs[*iter] = true;
  }

  int ret = set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_refcount_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_refcount_read_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_read(): failed to decode entry\n");
    return -EINVAL;
  }

  obj_refcount objr;

  cls_refcount_read_ret read_ret;
  int ret = read_refcount(hctx, "", &objr);
  if (ret < 0)
    return ret;

  for (auto iter = objr.refs.begin(); iter != objr.refs.end(); ++iter) {
    read_ret.refs.push_back(iter->first);
  }

  encode(read_ret, *out);

  return 0;
}

CLS_INIT(refcount)
{
  CLS_LOG(1, "Loaded refcount class!");

  cls_handle_t h_class;
  cls_method_handle_t h_refcount_get;
  cls_method_handle_t h_refcount_put;
  cls_method_handle_t h_refcount_set;
  cls_method_handle_t h_refcount_read;

  cls_register("refcount", &h_class);

  /* refcount */
  cls_register_cxx_method(h_class, "get", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_refcount_get, &h_refcount_get);
  cls_register_cxx_method(h_class, "put", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_refcount_put, &h_refcount_put);
  cls_register_cxx_method(h_class, "set", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_refcount_set, &h_refcount_set);
  cls_register_cxx_method(h_class, "read", CLS_METHOD_RD, cls_rc_refcount_read, &h_refcount_read);

  return;
}

