#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "rgw_op.h"
#include "include/rados/librados.hpp"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;
using namespace boost;

RGWPutMultipartCopy::RGWPutMultipartCopy()
{
  begin = 0;
  end = 0;
  have_read = 0;
  total = 0;
  ofs_start = 0;
  ofs_end = -1;
  copy_permission = false;
  copy_all = false;
  if_mod = NULL;
  if_unmod = NULL;
  if_match = NULL;
  if_nomatch = NULL;
  mod_ptr = NULL;
  unmod_ptr = NULL;
  store = NULL;
  s = NULL;
  op_target = NULL;
  read_op = NULL;
}

RGWPutMultipartCopy::~RGWPutMultipartCopy()
{
  if (read_op)
  {
    delete read_op;
  }
  if (op_target)
  {
    delete op_target;
  }
}

void RGWPutMultipartCopy::init(struct req_state *p_req, RGWRados *p_store)
{
  s = p_req;
  store = p_store;
}

bool RGWPutMultipartCopy::get_permission()
{  
  return copy_permission;
}

int RGWPutMultipartCopy::get_read_len(off_t ofs)
{
  size_t len = 0;
  uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  if (total)
  {
    len = total - ofs;
    if (len > chunk_size)
    {
      len = chunk_size;
    }
  }
  else
  {
    len = 0;
  }

  if (len <= 0)
  {
    string part_num = s->info.args.get("partNumber");
    ldout(s->cct, 0) << "NOTICE: get_read_len() partNum = " << part_num << " copy finish" << dendl;
    return 0;
  }

  return len;
}

int RGWPutMultipartCopy::copy_data(int len, bufferlist &bl)
{
  off_t read_begin = begin;
  off_t read_end = read_begin + len -1;
  int read_len = read_op->read(read_begin, read_end, bl);
  begin += read_len;
  have_read += read_len;
  return read_len;
}

int RGWPutMultipartCopy::get_data(off_t ofs, bufferlist &bl)
{
  int len = get_read_len(ofs);
  if (len == 0)
  {
    return 0;
  }

  return copy_data(len, bl);
}

int RGWPutMultipartCopy::get_data_ready()
{
  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  rgw_bucket src_bucket;
  RGWBucketInfo src_bucket_info;
  rgw_obj src_obj(src_bucket, src_object);
  obj_ctx.set_atomic(src_obj);

  map<string, bufferlist> src_attrs;
  int op_ret = store->get_bucket_info(obj_ctx, src_tenant_name, src_bucket_name, 
    src_bucket_info, NULL, &src_attrs);
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: read_op->get_bucket_info() returned op_ret = " << op_ret << dendl;
    return op_ret;
  }

  rgw_bucket bucket;
  bucket = src_bucket_info.bucket;
  rgw_obj obj(bucket, src_object.name); 

  string oid;
  string key;
  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);

  op_target = new RGWRados::Object(store, src_bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  read_op = new RGWRados::Object::Read(op_target);

  ceph::real_time delete_at;
  map<string, bufferlist> attrs;
  encode_delete_at_attr(delete_at, attrs);

  bool high_precision_time = (s->system_request);
  uint64_t total_len, obj_size;
  ceph::real_time src_mtime;

  read_op->conds.mod_ptr = mod_ptr;
  read_op->conds.unmod_ptr = unmod_ptr;
  read_op->conds.high_precision_time = high_precision_time;
  read_op->conds.if_match = if_match;
  read_op->conds.if_nomatch = if_nomatch;
  read_op->params.attrs = &attrs;
  read_op->params.lastmod = &src_mtime;
  read_op->params.read_size = &total_len;
  read_op->params.obj_size = &obj_size;
  read_op->params.perr = &s->err;

  op_ret = read_op->prepare(&ofs_start, &ofs_end);
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: read_op->prepare() returned op_ret = " << op_ret << dendl;
    return op_ret;
  }

  return 0;
}

int RGWPutMultipartCopy::get_range()
{
  const char *copy_range = NULL;
  copy_range = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE");
  if (!copy_range)
  {
    begin = ofs_start;
    end = ofs_end;
    total = end - begin + 1;
    ldout(s->cct, 0) << "NOTICE: get_range() begin = " << begin << dendl;
    ldout(s->cct, 0) << "NOTICE: get_range() end = " << end << dendl;
    ldout(s->cct, 0) << "NOTICE: get_range() total =" << total << dendl;
    return 0;
  }

  ldout(s->cct, 0) << "NOTICE: get_range() copy_range = " << copy_range << dendl;
  string range_str = copy_range;
  int pos_pre = range_str.find('=');
  if (pos_pre < 0)
  {
    return -EINVAL;
  }

  int pos = range_str.find('-');
  if (pos < 0)
  {
    return -EINVAL;
  }

  string range_begin = range_str.substr(pos_pre + 1, pos);
  if (range_begin.empty())
  {
    return -EINVAL;
  }
  begin = atoi(range_begin.c_str());
  
  string range_end = range_str.substr(pos + 1);
  if (range_end.empty())
  {
    end = ofs_end;
  }
  else
  {
    end = atoi(range_end.c_str());
  }

  if (begin > end)
  {
    return -EINVAL;
  }

  if (end >= ofs_end)
  {
    end = ofs_end;
  }

  if (begin <= ofs_start)
  {
    begin = ofs_start;
  }

  total = end - begin + 1;
  ldout(s->cct, 0) << "NOTICE: get_range() begin = " << begin << dendl;
  ldout(s->cct, 0) << "NOTICE: get_range() end = " << end << dendl;
  ldout(s->cct, 0) << "NOTICE: get_range() total =" << total << dendl;
  return 0;
}

int RGWPutMultipartCopy::get_source_bucket_info(string &src_tenant_name, 
  string &src_bucket_name,
  rgw_obj_key &src_object)
{
  const char *copy_source = NULL;
  copy_source = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");
  if (!copy_source)
  {
    return -1;
  }

  string prefix_str = copy_source + 1;
  int pos = prefix_str.find('/');
  if (pos < 0)
  {
    return -EINVAL;
  }

  src_tenant_name = s->src_tenant_name;
  src_bucket_name = prefix_str.substr(0, pos);
  src_object.name = prefix_str.substr(pos + 1);
  src_object.instance = s->info.args.get("versionId", NULL);

  return 0;
}

int RGWPutMultipartCopy::get_obj_bucket()
{
  int ret = get_source_bucket_info(src_tenant_name, src_bucket_name, src_object);
  if (ret < 0)
  {
    return ret;
  }

  if_mod = s->info.env->get("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_X_AMZ_COPY_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_X_AMZ_COPY_IF_NONE_MATCH");

  if (if_mod) 
  {
    if (parse_time(if_mod, &mod_time) < 0)
    {
      return -EINVAL;
    }
    mod_ptr = &mod_time;
  }

  if (if_unmod) 
  {
    if (parse_time(if_unmod, &unmod_time) < 0)
    {
      return -EINVAL;
    }
    unmod_ptr = &unmod_time;
  }

  return 0;
}

int RGWPutMultipartCopy::get_params()
{
  int op_ret = 0;
  op_ret = get_obj_bucket();
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: get_obj_bucket() op_ret = " << op_ret << dendl;
    return op_ret;
  }

  op_ret = get_data_ready();
  if (op_ret < 0)
  {
    return op_ret;
  }

  op_ret = get_range();
  if (op_ret < 0)
  {
    ldout(s->cct, 0) << "ERROR: get_range() op_ret = " << op_ret << dendl;
    return op_ret;
  }

  copy_permission = true;
  ldout(s->cct, 0) << "NOTICE: get_params() copy_permission = " << copy_permission << dendl;
  return 0;
}
