// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdarg>
#include "common/ceph_context.h"
#include "common/ceph_releases.h"
#include "common/config.h"
#include "common/debug.h"

#include "objclass/objclass.h"
#include "osd/PrimaryLogPG.h"

#include "osd/ClassHandler.h"

#include "auth/Crypto.h"
#include "common/armor.h"

#define dout_context ClassHandler::get_instance().cct

using std::map;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::real_time;


int cls_call(cls_method_context_t hctx, const char *cls, const char *method,
	     char *indata, int datalen, char **outdata, int *outdatalen)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  bufferlist idata;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_CALL;
  op.op.cls.class_len = strlen(cls);
  op.op.cls.method_len = strlen(method);
  op.op.cls.indata_len = datalen;
  op.indata.append(cls, op.op.cls.class_len);
  op.indata.append(method, op.op.cls.method_len);
  op.indata.append(indata, datalen);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);
  if (r < 0)
    return r;

  *outdata = (char *)malloc(op.outdata.length());
  if (!*outdata)
    return -ENOMEM;
  memcpy(*outdata, op.outdata.c_str(), op.outdata.length());
  *outdatalen = op.outdata.length();

  return r;
}

int cls_getxattr(cls_method_context_t hctx, const char *name,
		 char **outdata, int *outdatalen)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_GETXATTR;
  op.op.xattr.name_len = strlen(name);
  op.indata.append(name, op.op.xattr.name_len);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);
  if (r < 0)
    return r;

  *outdata = (char *)malloc(op.outdata.length());
  if (!*outdata)
    return -ENOMEM;
  memcpy(*outdata, op.outdata.c_str(), op.outdata.length());
  *outdatalen = op.outdata.length();

  return r;
}

int cls_setxattr(cls_method_context_t hctx, const char *name,
		 const char *value, int val_len)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_SETXATTR;
  op.op.xattr.name_len = strlen(name);
  op.op.xattr.value_len = val_len;
  op.indata.append(name, op.op.xattr.name_len);
  op.indata.append(value, val_len);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);

  return r;
}

int cls_read(cls_method_context_t hctx, int ofs, int len,
	     char **outdata, int *outdatalen)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_SYNC_READ;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  int r = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (r < 0)
    return r;

  *outdata = (char *)malloc(ops[0].outdata.length());
  if (!*outdata)
    return -ENOMEM;
  memcpy(*outdata, ops[0].outdata.c_str(), ops[0].outdata.length());
  *outdatalen = ops[0].outdata.length();

  return *outdatalen;
}

int cls_get_request_origin(cls_method_context_t hctx, entity_inst_t *origin)
{
  PrimaryLogPG::OpContext **pctx = static_cast<PrimaryLogPG::OpContext **>(hctx);
  *origin = (*pctx)->op->get_req()->get_orig_source_inst();
  return 0;
}

int cls_cxx_create(cls_method_context_t hctx, bool exclusive)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_CREATE;
  ops[0].op.flags = (exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0);
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_remove(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_DELETE;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_STAT;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;
  auto iter = ops[0].outdata.cbegin();
  utime_t ut;
  uint64_t s;
  try {
    decode(s, iter);
    decode(ut, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  if (size)
    *size = s;
  if (mtime)
    *mtime = ut.sec();
  return 0;
}

int cls_cxx_stat2(cls_method_context_t hctx, uint64_t *size, ceph::real_time *mtime)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_STAT;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;
  auto iter = ops[0].outdata.cbegin();
  real_time ut;
  uint64_t s;
  try {
    decode(s, iter);
    decode(ut, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  if (size)
    *size = s;
  if (mtime)
    *mtime = ut;
  return 0;
}

int cls_cxx_read2(cls_method_context_t hctx, int ofs, int len,
                  bufferlist *outbl, uint32_t op_flags)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_SYNC_READ;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  ops[0].op.flags = op_flags;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;
  *outbl = std::move(ops[0].outdata);
  return outbl->length();
}

int cls_cxx_write2(cls_method_context_t hctx, int ofs, int len,
                   bufferlist *inbl, uint32_t op_flags)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_WRITE;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  ops[0].op.flags = op_flags;
  ops[0].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *inbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = inbl->length();
  ops[0].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len, bufferlist *inbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(2);
  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = 0;
  ops[1].op.op = CEPH_OSD_OP_WRITE;
  ops[1].op.extent.offset = ofs;
  ops[1].op.extent.length = len;
  ops[1].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_truncate(cls_method_context_t hctx, int ofs)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = 0;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_write_zero(cls_method_context_t hctx, int ofs, int len)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_ZERO;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_getxattr(cls_method_context_t hctx, const char *name,
                     bufferlist *outbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_GETXATTR;
  op.op.xattr.name_len = strlen(name);
  op.indata.append(name, op.op.xattr.name_len);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);
  if (r < 0)
    return r;

  *outbl = std::move(op.outdata);
  return outbl->length();
}

int cls_cxx_getxattrs(cls_method_context_t hctx, map<string, bufferlist> *attrset)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_GETXATTRS;
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);
  if (r < 0)
    return r;

  auto iter = op.outdata.cbegin();
  try {
    decode(*attrset, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  return 0;
}

int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                     bufferlist *inbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_SETXATTR;
  op.op.xattr.name_len = strlen(name);
  op.op.xattr.value_len = inbl->length();
  op.indata.append(name, op.op.xattr.name_len);
  op.indata.append(*inbl);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);

  return r;
}

int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_ROLLBACK;
  ops[0].op.snap.snapid = snapid;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_get_all_vals(cls_method_context_t hctx, map<string, bufferlist>* vals,
                             bool *more)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  int ret;

  string start_after;
  string filter_prefix;
  uint64_t max = (uint64_t)-1;

  encode(start_after, op.indata);
  encode(max, op.indata);
  encode(filter_prefix, op.indata);

  op.op.op = CEPH_OSD_OP_OMAPGETVALS;
  
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  auto iter = op.outdata.cbegin();
  try {
    decode(*vals, iter);
    decode(*more, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  return vals->size();
}

int cls_cxx_map_get_keys(cls_method_context_t hctx, const string &start_obj,
			 uint64_t max_to_get, set<string> *keys,
                         bool *more)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  int ret;

  encode(start_obj, op.indata);
  encode(max_to_get, op.indata);

  op.op.op = CEPH_OSD_OP_OMAPGETKEYS;

  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  auto iter = op.outdata.cbegin();
  try {
    decode(*keys, iter);
    decode(*more, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  return keys->size();
}

int cls_cxx_map_get_vals(cls_method_context_t hctx, const string &start_obj,
			 const string &filter_prefix, uint64_t max_to_get,
			 map<string, bufferlist> *vals, bool *more)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  int ret;

  encode(start_obj, op.indata);
  encode(max_to_get, op.indata);
  encode(filter_prefix, op.indata);

  op.op.op = CEPH_OSD_OP_OMAPGETVALS;
  
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  auto iter = op.outdata.cbegin();
  try {
    decode(*vals, iter);
    decode(*more, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  return vals->size();
}

int cls_cxx_map_read_header(cls_method_context_t hctx, bufferlist *outbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  int ret;
  op.op.op = CEPH_OSD_OP_OMAPGETHEADER;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  *outbl = std::move(op.outdata);

  return 0;
}

int cls_cxx_map_get_val(cls_method_context_t hctx, const string &key,
			bufferlist *outbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  int ret;

  set<string> k;
  k.insert(key);
  encode(k, op.indata);

  op.op.op = CEPH_OSD_OP_OMAPGETVALSBYKEYS;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  auto iter = op.outdata.cbegin();
  try {
    map<string, bufferlist> m;

    decode(m, iter);
    map<string, bufferlist>::iterator iter = m.begin();
    if (iter == m.end())
      return -ENOENT;

    *outbl = iter->second;
  } catch (ceph::buffer::error& e) {
    return -EIO;
  }
  return 0;
}

int cls_cxx_map_get_vals_by_keys(cls_method_context_t hctx,
                                 const std::set<std::string> &keys,
                                 std::map<std::string, bufferlist> *map)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  int ret;

  encode(keys, op.indata);

  op.op.op = CEPH_OSD_OP_OMAPGETVALSBYKEYS;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  auto iter = op.outdata.cbegin();
  try {
    decode(*map, iter);
  } catch (buffer::error& e) {
    return -EIO;
  }
  return 0;
}

int cls_cxx_map_set_val(cls_method_context_t hctx, const string &key,
			bufferlist *inbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  bufferlist& update_bl = op.indata;
  map<string, bufferlist> m;
  m[key] = *inbl;
  encode(m, update_bl);

  op.op.op = CEPH_OSD_OP_OMAPSETVALS;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_set_vals(cls_method_context_t hctx,
			 const std::map<string, bufferlist> *map)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  bufferlist& update_bl = op.indata;
  encode(*map, update_bl);

  op.op.op = CEPH_OSD_OP_OMAPSETVALS;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_clear(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];

  op.op.op = CEPH_OSD_OP_OMAPCLEAR;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_write_header(cls_method_context_t hctx, bufferlist *inbl)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  op.indata = std::move(*inbl);

  op.op.op = CEPH_OSD_OP_OMAPSETHEADER;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_remove_range(cls_method_context_t hctx,
                             const std::string& key_begin,
                             const std::string& key_end)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  bufferlist& update_bl = op.indata;

  ::encode(key_begin, update_bl);
  ::encode(key_end, update_bl);

  op.op.op = CEPH_OSD_OP_OMAPRMKEYRANGE;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_remove_key(cls_method_context_t hctx, const string &key)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];
  bufferlist& update_bl = op.indata;
  set<string> to_rm;
  to_rm.insert(key);

  encode(to_rm, update_bl);

  op.op.op = CEPH_OSD_OP_OMAPRMKEYS;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_list_watchers(cls_method_context_t hctx,
			  obj_list_watch_response_t *watchers)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_LIST_WATCHERS;
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);
  if (r < 0)
    return r;

  auto iter = op.outdata.cbegin();
  try {
    decode(*watchers, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  return 0;
}

uint64_t cls_current_version(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;

  return ctx->pg->get_last_user_version();
}


int cls_current_subop_num(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;

  return ctx->processed_subop_count;
}

uint64_t cls_get_features(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;
  return ctx->pg->get_osdmap()->get_up_osd_features();
}

uint64_t cls_get_client_features(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;
  return ctx->op->get_req()->get_connection()->get_features();
}

ceph_release_t cls_get_required_osd_release(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;
  return ctx->pg->get_osdmap()->require_osd_release;
}

ceph_release_t cls_get_min_compatible_client(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;
  return ctx->pg->get_osdmap()->get_require_min_compat_client();
}

int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq) {
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;
  if (!ctx->new_obs.exists || (ctx->new_obs.oi.is_whiteout() &&
                               ctx->obc->ssc->snapset.clones.empty())) {
    return -ENOENT;
  }
  *snap_seq = ctx->obc->ssc->snapset.seq;
  return 0;
}

int cls_cxx_chunk_write_and_set(cls_method_context_t hctx, int ofs, int len,
				bufferlist *write_inbl, uint32_t op_flags,
				bufferlist *set_inbl, int set_len)
{
  PrimaryLogPG::OpContext **pctx = (PrimaryLogPG::OpContext **)hctx;
  char cname[] = "cas";
  char method[] = "chunk_set";

  vector<OSDOp> ops(2);
  ops[0].op.op = CEPH_OSD_OP_WRITE;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  ops[0].op.flags = op_flags;
  ops[0].indata = *write_inbl;

  ops[1].op.op = CEPH_OSD_OP_CALL;
  ops[1].op.cls.class_len = strlen(cname);
  ops[1].op.cls.method_len = strlen(method);
  ops[1].op.cls.indata_len = set_len;
  ops[1].indata.append(cname, ops[1].op.cls.class_len);
  ops[1].indata.append(method, ops[1].op.cls.method_len);
  ops[1].indata.append(*set_inbl);

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

bool cls_has_chunk(cls_method_context_t hctx, string fp_oid)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;
  if (!ctx->obc->obs.oi.has_manifest()) {
    return false;
  }

  for (auto &p : ctx->obc->obs.oi.manifest.chunk_map) {
    if (p.second.oid.oid.name == fp_oid) {
      return true;
    }
  }

  return false;
}

uint64_t cls_get_osd_min_alloc_size(cls_method_context_t hctx) {
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;

  return ctx->pg->get_min_alloc_size();
}

uint64_t cls_get_pool_stripe_width(cls_method_context_t hctx)
{
  PrimaryLogPG::OpContext *ctx = *(PrimaryLogPG::OpContext **)hctx;

  return ctx->pg->get_pool().stripe_width;
}
