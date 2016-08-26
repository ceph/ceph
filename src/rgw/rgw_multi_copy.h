#ifndef CEPH_RGW_MULTI_COPY_H
#define CEPH_RGW_MULTI_COPY_H

#include <string>
#include <list>
#include <map>
#include <set>

#include "common/ceph_time.h"

#include "rgw_rados.h"
#include "rgw_common.h"

using namespace std;

struct req_state;
class RGWPutObj;

class RGWPutMultipartCopy
{
private:  
  off_t begin;
  off_t end;
  off_t have_read;
  off_t total;

  off_t ofs_start;
  off_t ofs_end;

  bool copy_permission;
  bool copy_all;

  ceph::real_time mod_time;
  ceph::real_time unmod_time;
  ceph::real_time *mod_ptr;
  ceph::real_time *unmod_ptr;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;

  string src_tenant_name;
  string src_bucket_name;
  rgw_obj_key src_object;

  struct req_state *s;
  RGWRados *store;
  RGWRados::Object *op_target;
  RGWRados::Object::Read *read_op;

private:
  int get_range();
  int get_read_len(off_t ofs);
  int get_obj_bucket();
  int get_data_ready();

public:
  RGWPutMultipartCopy();
  ~RGWPutMultipartCopy();

  void init(struct req_state *p_req, RGWRados *p_store);
  int copy_data(int len, bufferlist &bl);
  bool get_permission();
  int get_params();
  int get_data(off_t off, bufferlist &bl);
  int get_source_bucket_info(string &src_tenant_name, string &src_bucket_name,
    rgw_obj_key &src_object);
};
#endif /* CEPH_RGW_MULTI_COPY_H */
