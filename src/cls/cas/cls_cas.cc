// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "objclass/objclass.h"
#include "cls_cas_ops.h"

#include "include/compat.h"
#include "osd/osd_types.h"

CLS_VER(1,0)
CLS_NAME(cas)

struct chunk_obj_refcount;

static int chunk_read_refcount(cls_method_context_t hctx, chunk_obj_refcount *objr)
{
  bufferlist bl;
  objr->refs.clear();
  int ret = cls_cxx_getxattr(hctx, CHUNK_REFCOUNT_ATTR, &bl);
  if (ret == -ENODATA) {
    return 0;
  }
  if (ret < 0)
    return ret;

  try {
    auto iter = bl.cbegin();
    decode(*objr, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: chunk_read_refcount(): failed to decode refcount entry\n");
    return -EIO;
  }

  return 0;
}

static int chunk_set_refcount(cls_method_context_t hctx, const struct chunk_obj_refcount& objr)
{
  bufferlist bl;

  encode(objr, bl);

  int ret = cls_cxx_setxattr(hctx, CHUNK_REFCOUNT_ATTR, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_chunk_refcount_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_chunk_refcount_get_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_refcount_get(): failed to decode entry\n");
    return -EINVAL;
  }

  chunk_obj_refcount objr;
  int ret = chunk_read_refcount(hctx, &objr);
  if (ret < 0)
    return ret;

  CLS_LOG(10, "cls_rc_chunk_refcount_get() oid=%s\n", op.source.oid.name.c_str());

  objr.refs.insert(op.source);

  ret = chunk_set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_chunk_refcount_put(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_chunk_refcount_put_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rc_chunk_refcount_put(): failed to decode entry\n");
    return -EINVAL;
  }

  chunk_obj_refcount objr;
  int ret = chunk_read_refcount(hctx, &objr);
  if (ret < 0)
    return ret;

  if (objr.refs.empty()) {// shouldn't happen!
    CLS_LOG(0, "ERROR: cls_rc_chunk_refcount_put() was called without any references!\n");
    return -EINVAL;
  }

  CLS_LOG(10, "cls_rc_chunk_refcount_put() oid=%s\n", op.source.oid.name.c_str());

  bool found = false;
  for (auto &p : objr.refs) {
    if (p == op.source) {
      found = true;
      break;
    }
  }

  if (!found) {
    return 0;
  }

  auto p = objr.refs.find(op.source);
  objr.refs.erase(p);

  if (objr.refs.empty()) {
    return cls_cxx_remove(hctx);
  }

  ret = chunk_set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_chunk_refcount_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_chunk_refcount_set_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_chunk_refcount_set(): failed to decode entry\n");
    return -EINVAL;
  }

  if (!op.refs.size()) {
    return cls_cxx_remove(hctx);
  }

  chunk_obj_refcount objr;
  objr.refs = op.refs;

  int ret = chunk_set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_rc_chunk_refcount_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  chunk_obj_refcount objr;

  cls_chunk_refcount_read_ret read_ret;
  int ret = chunk_read_refcount(hctx, &objr);
  if (ret < 0)
    return ret;

  for (auto &p : objr.refs) {
    read_ret.refs.insert(p);
  }

  encode(read_ret, *out);

  return 0;
}

static int cls_rc_write_or_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  hobject_t src_obj; 
  bufferlist indata, outdata;
  ceph_osd_op op;
  try {
    decode (op, in_iter);
    decode(src_obj, in_iter);
    in_iter.copy(op.extent.length, indata);
  }
  catch (buffer::error& e) {
    return -EINVAL;
  }

  CLS_LOG(10, " offset: %llu length: %llu \n",
	  static_cast<long long unsigned>(op.extent.offset),
	  static_cast<long long unsigned>(op.extent.length));
  chunk_obj_refcount objr;
  int ret = chunk_read_refcount(hctx, &objr);
  if (ret == -ENOENT) {
    objr.refs.insert(src_obj);
    bufferlist set_bl;
    encode(objr, set_bl);
    ret = cls_cxx_chunk_write_and_set(hctx, op.extent.offset, op.extent.length, &indata, op.flags,
				      &set_bl, set_bl.length());
    if (ret < 0)
      return ret;

    return 0;
  }

  objr.refs.insert(src_obj);
  ret = chunk_set_refcount(hctx, objr);
  if (ret < 0)
    return ret;

  return 0;
}


static int cls_rc_has_chunk(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  string fp_oid;
  bufferlist indata, outdata;
  try {
    decode (fp_oid, in_iter);
  }
  catch (buffer::error& e) {
    return -EINVAL;
  }
  CLS_LOG(10, " fp_oid: %s \n", fp_oid.c_str());

  bool ret = cls_has_chunk(hctx, fp_oid);
  if (ret) {
    return 0;
  }
  return -ENOENT;
}

CLS_INIT(cas)
{
  CLS_LOG(1, "Loaded cas class!");

  cls_handle_t h_class;
  cls_method_handle_t h_cas_write_or_get;
  cls_method_handle_t h_chunk_refcount_get;
  cls_method_handle_t h_chunk_refcount_put;
  cls_method_handle_t h_chunk_refcount_set;
  cls_method_handle_t h_chunk_refcount_read;
  cls_method_handle_t h_chunk_has_chunk;

  cls_register("cas", &h_class);

  /* chunk refcount */
  cls_register_cxx_method(h_class, "chunk_get", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_chunk_refcount_get, 
			  &h_chunk_refcount_get);
  cls_register_cxx_method(h_class, "chunk_put", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_chunk_refcount_put, 
			  &h_chunk_refcount_put);
  cls_register_cxx_method(h_class, "chunk_set", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_chunk_refcount_set, 
			  &h_chunk_refcount_set);
  cls_register_cxx_method(h_class, "chunk_read", CLS_METHOD_RD, cls_rc_chunk_refcount_read, 
			  &h_chunk_refcount_read);
  cls_register_cxx_method(h_class, "cas_write_or_get", CLS_METHOD_RD | CLS_METHOD_WR, cls_rc_write_or_get, 
			  &h_cas_write_or_get);
  cls_register_cxx_method(h_class, "has_chunk", CLS_METHOD_RD, cls_rc_has_chunk, 
	      &h_chunk_has_chunk);

  return;
}

