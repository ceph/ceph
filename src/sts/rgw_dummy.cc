// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include <boost/bind.hpp>

#include "common/Clock.h"
#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rados.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_multi.h"
#include "rgw_multi_del.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"

#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using ceph::crypto::MD5;

/**
 * Return a callable that can invoke dump_access_control().
 */

boost::function<void()> RGWOp::dump_access_control_f()
{
  return boost::bind(dump_access_control, s, this);
}

RGWHandler::~RGWHandler()
{
}

int RGWHandler::init(RGWRados *_store, struct req_state *_s, RGWClientIO *cio)
{
  store = _store;
  s = _s;

  return 0;
}

int RGWHandler::do_read_permissions(RGWOp *op, bool only_bucket)
{
#if 0
  int ret = rgw_build_policies(store, s, only_bucket, op->prefetch_data());
#else
  int ret = -EDOM;
#endif   

  if (ret < 0) {
    ldout(s->cct, 10) << "read_permissions on " << s->bucket << ":" <<s->object << " only_bucket=" << only_bucket << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
  }

  return ret;
}


RGWOp *RGWHandler_REST::get_op(RGWRados *store)
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = op_get();
     break;
   case OP_PUT:
     op = op_put();
     break;
   case OP_DELETE:
     op = op_delete();
     break;
   case OP_HEAD:
     op = op_head();
     break;
   case OP_POST:
     op = op_post();
     break;
   case OP_COPY:
     op = op_copy();
     break;
   case OP_OPTIONS:
     op = op_options();
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(store, s, this);
  }
  return op;
}

void RGWHandler_REST::put_op(RGWOp *op)
{
  delete op;
}

void dump_bucket_from_state(struct req_state *s)
{
#if 0
  int expose_bucket = g_conf->rgw_expose_bucket;
  if (expose_bucket) {
    if (!s->bucket_name.empty()) {
      string b;
      url_encode(s->bucket_name, b);
      s->cio->print("Bucket: %s\r\n", b.c_str());
    }
  }
#endif
}

void dump_access_control(req_state *s, RGWOp *op)
{
#if 0
  string origin;
  string method;
  string header;
  string exp_header;
  unsigned max_age = CORS_MAX_AGE_INVALID;

  if (!op->generate_cors_headers(origin, method, header, exp_header, &max_age))
    return;

  dump_access_control(s, origin.c_str(), method.c_str(), header.c_str(), exp_header.c_str(), max_age);
#endif
// assert here?
}

int RGWAccessControlList::get_perm(rgw_user& id, int perm_mask) {
  ldout(cct, 5) << "Searching permissions for uid=" << id << " mask=" << perm_mask << dendl;
  map<string, int>::iterator iter = acl_user_map.find(id.to_str());
  if (iter != acl_user_map.end()) {
    ldout(cct, 5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  ldout(cct, 5) << "Permissions for user not found" << dendl;
  return 0;
}

int RGWAccessControlList::get_group_perm(ACLGroupTypeEnum group, int perm_mask) {
  ldout(cct, 5) << "Searching permissions for group=" << (int)group << " mask=" << perm_mask << dendl;
  map<uint32_t, int>::iterator iter = acl_group_map.find((uint32_t)group);
  if (iter != acl_group_map.end()) {
    ldout(cct, 5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  ldout(cct, 5) << "Permissions for group not found" << dendl;
  return 0;
}

int RGWAccessControlPolicy::get_perm(rgw_user& id, int perm_mask) {
  int perm = acl.get_perm(id, perm_mask);

  if (id.compare(owner.get_id()) == 0) {
    perm |= perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP);
  }

  if (perm == perm_mask)
    return perm;

  /* should we continue looking up? */
  if ((perm & perm_mask) != perm_mask) {
    perm |= acl.get_group_perm(ACL_GROUP_ALL_USERS, perm_mask);

    if (id.compare(RGW_USER_ANON_ID)) {
      /* this is not the anonymous user */
      perm |= acl.get_group_perm(ACL_GROUP_AUTHENTICATED_USERS, perm_mask);
    }
  }

  ldout(cct, 5) << "Getting permissions id=" << id << " owner=" << owner.get_id() << " perm=" << perm << dendl;

  return perm;
}

bool RGWAccessControlPolicy::verify_permission(rgw_user& uid, int user_perm_mask, int perm)
{
  int test_perm = perm | RGW_PERM_READ_OBJS | RGW_PERM_WRITE_OBJS;

  int policy_perm = get_perm(uid, test_perm);

  /* the swift WRITE_OBJS perm is equivalent to the WRITE obj, just
     convert those bits. Note that these bits will only be set on
     buckets, so the swift READ permission on bucket will allow listing
     the bucket content */
  if (policy_perm & RGW_PERM_WRITE_OBJS) {
    policy_perm |= (RGW_PERM_WRITE | RGW_PERM_WRITE_ACP);
  }
  if (policy_perm & RGW_PERM_READ_OBJS) {
    policy_perm |= (RGW_PERM_READ | RGW_PERM_READ_ACP);
  }
   
  int acl_perm = policy_perm & perm & user_perm_mask;

  ldout(cct, 10) << " uid=" << uid << " requested perm (type)=" << perm << ", policy perm=" << policy_perm << ", user_perm_mask=" << user_perm_mask << ", acl perm=" << acl_perm << dendl;

  return (perm == acl_perm);
}

int RGWOp::init_quota()
{
	/* if (s->system_request) */
  return 0;
}

int RGWOp::verify_op_mask()
{
//	can look at:	op_mask & s->user.op_mask
//	and return -EPERM	if not accepted.
	return 0;
}
