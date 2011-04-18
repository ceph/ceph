
#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "common/armor.h"
#include "common/Clock.h"

#include "rgw_access.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_log.h"

using namespace std;
using ceph::crypto::MD5;

static int parse_range(const char *range, off_t& ofs, off_t& end)
{
  int r = -ERANGE;
  string s(range);
  int pos = s.find("bytes=");
  string ofs_str;
  string end_str;

  if (pos < 0)
    goto done;

  s = s.substr(pos + 6); /* size of("bytes=")  */
  pos = s.find('-');
  if (pos < 0)
    goto done;

  ofs_str = s.substr(0, pos);
  end_str = s.substr(pos + 1);
  if (ofs_str.length())
    ofs = atoll(ofs_str.c_str());

  if (end_str.length())
  end = atoll(end_str.c_str());

  RGW_LOG(10) << "parse_range ofs=" << ofs << " end=" << end << endl;

  if (end < ofs)
    goto done;

  r = 0;
done:
  return r;
}

/**
 * Get the HTTP request metadata out of the req_state as a
 * map(<attr_name, attr_contents>, where attr_name is RGW_ATTR_PREFIX.HTTP_NAME)
 * s: The request state
 * attrs: will be filled up with attrs mapped as <attr_name, attr_contents>
 *
 */
void get_request_metadata(struct req_state *s, map<string, bufferlist>& attrs)
{
  map<string, string>::iterator iter;
  for (iter = s->x_amz_map.begin(); iter != s->x_amz_map.end(); ++iter) {
    string name = iter->first;
#define X_AMZ_META "x-amz-meta"
    if (name.find(X_AMZ_META) == 0) {
      RGW_LOG(10) << "x>> " << iter->first << ":" << iter->second << endl;
      string& val = iter->second;
      bufferlist bl;
      bl.append(val.c_str(), val.size() + 1);
      string attr_name = RGW_ATTR_PREFIX;
      attr_name.append(name);
      attrs[attr_name.c_str()] = bl;
    }
  }
}

/**
 * Get the AccessControlPolicy for an object off of disk.
 * policy: must point to a valid RGWACL, and will be filled upon return.
 * bucket: name of the bucket containing the object.
 * object: name of the object to get the ACL for.
 * Returns: 0 on success, -ERR# otherwise.
 */
static int get_policy_from_attr(RGWAccessControlPolicy *policy, string& bucket, string& object)
{
  bufferlist bl;
  int ret = 0;

  if (bucket.size()) {
    ret = rgwstore->get_attr(bucket, object,
                       RGW_ATTR_ACL, bl);

    if (ret >= 0) {
      bufferlist::iterator iter = bl.begin();
      policy->decode(iter);
      if (rgw_log_level >= 15) {
        RGW_LOG(15) << "Read AccessControlPolicy" << endl;
        policy->to_xml(cerr);
        RGW_LOG(15) << endl;
      }
    }
  }

  return ret;
}

int read_acls(struct req_state *s, RGWAccessControlPolicy *policy, string& bucket, string& object)
{
  int ret = get_policy_from_attr(policy, bucket, object);
  if (ret == -ENOENT && object.size()) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy;
    string no_object;
    ret = get_policy_from_attr(&bucket_policy, bucket, no_object);
    if (ret < 0)
      return ret;

    if (!verify_permission(&bucket_policy, s->user.user_id, RGW_PERM_READ))
      ret = -EACCES;
    else
      ret = -ENOENT;
  } else if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_BUCKET;
  }

  return ret;
}

/**
 * Get the AccessControlPolicy for a bucket or object off of disk.
 * s: The req_state to draw information from.
 * only_bucket: If true, reads the bucket ACL rather than the object ACL.
 * Returns: 0 on success, -ERR# otherwise.
 */
int read_acls(struct req_state *s, bool only_bucket)
{
  int ret = 0;
  string obj_str;

  if (!s->acl) {
     s->acl = new RGWAccessControlPolicy;
     if (!s->acl)
       return -ENOMEM;
  }

  /* we're passed only_bucket = true when we specifically need the bucket's
     acls, that happens on write operations */
  if (!only_bucket)
    obj_str = s->object_str;

  ret = read_acls(s, s->acl, s->bucket_str, obj_str);

  return ret;
}

void RGWGetObj::execute()
{
  void *handle = NULL;

  if (!verify_permission(s, RGW_PERM_READ)) {
    ret = -EACCES;
    goto done;
  }

  ret = get_params();
  if (ret < 0)
    goto done;

  init_common();

  ret = rgwstore->prepare_get_obj(s->bucket_str, s->object_str, ofs, &end, &attrs, mod_ptr,
                                  unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &handle, &s->err);
  if (ret < 0)
    goto done;

  if (!get_data || ofs > end)
    goto done;

  while (ofs <= end) {
    ret = rgwstore->get_obj(&handle, s->bucket_str, s->object_str, &data, ofs, end);
    if (ret < 0) {
      goto done;
    }
    len = ret;
    ofs += len;
    ret = 0;

    send_response(handle);
    free(data);
  }

  return;

done:
  send_response(handle);
  free(data);
  rgwstore->finish_get_obj(&handle);
}

int RGWGetObj::init_common()
{
  if (range_str) {
    int r = parse_range(range_str, ofs, end);
    if (r < 0)
      return r;
  }
  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0)
      return -EINVAL;
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0)
      return -EINVAL;
    unmod_ptr = &unmod_time;
  }

  return 0;
}

void RGWListBuckets::execute()
{
  ret = rgw_read_user_buckets(s->user.user_id, buckets, !!(s->prot_flags & RGW_REST_OPENSTACK));
  if (ret < 0) {
    /* hmm.. something wrong here.. the user was authenticated, so it
       should exist, just try to recreate */
    RGW_LOG(10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << endl;

    /*

    on a second thought, this is probably a bug and we should fail

    rgw_put_user_buckets(s->user.user_id, buckets);
    ret = 0;

    */
  }

  send_response();
}

void RGWStatBucket::execute()
{
  RGWUserBuckets buckets;
  bucket.name = s->bucket;
  buckets.add(bucket);
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  ret = rgwstore->update_containers_stats(m);
  if (!ret)
    ret = -EEXIST;
  if (ret > 0) {
    ret = 0;
    map<string, RGWBucketEnt>::iterator iter = m.find(bucket.name);
    if (iter != m.end()) {
      bucket = iter->second;
    } else {
      ret = -EINVAL;
    }
  }

  send_response();
}

void RGWListBucket::execute()
{
  if (!verify_permission(s, RGW_PERM_READ)) {
    ret = -EACCES;
    goto done;
  }

  url_decode(s->args.get("prefix"), prefix);
  marker = s->args.get("marker");
  max_keys = s->args.get(limit_opt_name);
  if (!max_keys.empty()) {
    max = atoi(max_keys.c_str());
  } else {
    max = default_max;
  }
  url_decode(s->args.get("delimiter"), delimiter);

  if (s->prot_flags & RGW_REST_OPENSTACK) {
    string path_args = s->args.get("path");
    if (!path_args.empty()) {
      if (!delimiter.empty() || !prefix.empty()) {
        ret = -EINVAL;
        goto done;
      }
      url_decode(path_args, prefix);
      delimiter="/";
    }
  }

  ret = rgwstore->list_objects(s->user.user_id, s->bucket_str, max, prefix, delimiter, marker, objs, common_prefixes,
                               !!(s->prot_flags & RGW_REST_OPENSTACK));
done:
  send_response();
}

void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy policy, old_policy;
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  bool existed;
  bool pol_ret;

  int r = get_policy_from_attr(&old_policy, rgw_root_bucket, s->bucket_str);
  if (r >= 0)  {
    if (old_policy.get_owner().get_id().compare(s->user.user_id) != 0) {
      ret = -EEXIST;
      goto done;
    }
  }

  pol_ret = policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!pol_ret) {
    ret = -EINVAL;
    goto done;
  }
  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  ret = rgw_add_bucket(s->user.user_id, s->bucket_str);
  /* continue if EEXIST and create_bucket will fail below.  this way we can recover
   * from a partial create by retrying it. */
  if (ret && ret != -EEXIST)   
    goto done;

  existed = (ret == -EEXIST);

  ret = rgwstore->create_bucket(s->user.user_id, s->bucket_str, attrs,
				s->user.auid);
  if (ret && !existed && ret != -EEXIST)   /* if it exists (or previously existed), don't remove it! */
    rgw_remove_bucket(s->user.user_id, s->bucket_str);

  if (ret == -EEXIST)
    ret = 0;

done:
  send_response();
}

void RGWDeleteBucket::execute()
{
  ret = -EINVAL;

  if (!verify_permission(s, RGW_PERM_WRITE)) {
    abort_early(s, -EACCES);
    return;
  }

  if (s->bucket) {
    ret = rgwstore->delete_bucket(s->user.user_id, s->bucket_str);

    if (ret == 0) {
      rgw_remove_bucket(s->user.user_id, s->bucket_str);
    }
  }

  send_response();
}

void RGWPutObj::execute()
{
  ret = -EINVAL;
  if (!s->object) {
    goto done;
  } else {
    ret = get_params();
    if (ret < 0)
      goto done;

    RGWAccessControlPolicy policy;

    if (!verify_permission(s, RGW_PERM_WRITE)) {
      ret = -EACCES;
      goto done;
    }

    ret = policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
    if (!ret) {
       ret = -EINVAL;
       goto done;
    }

    char supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1];
    char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];

    if (supplied_md5_b64) {
      RGW_LOG(15) << "supplied_md5_b64=" << supplied_md5_b64 << endl;
      ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
			     supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
      RGW_LOG(15) << "ceph_armor ret=" << ret << endl;
      if (ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        ret = -ERR_INVALID_DIGEST;
        goto done;
      }

      buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
      RGW_LOG(15) << "supplied_md5=" << supplied_md5 << endl;
    }

    MD5 hash;
    do {
      get_data();
      if (len > 0) {
        hash.Update((unsigned char *)data, len);
	// For the first call to put_obj_data, pass -1 as the offset to
	// do a write_full.
        ret = rgwstore->put_obj_data(s->user.user_id, s->bucket_str,
				     s->object_str, data,
				     ((ofs == 0) ? -1 : ofs), len, NULL);
        free(data);
        if (ret < 0)
          goto done;
        ofs += len;
      }
    } while ( len > 0);

    hash.Final(m);

    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

    if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
       ret = -ERR_BAD_DIGEST;
       goto done;
    }
    bufferlist aclbl;
    policy.encode(aclbl);

    etag = calc_md5;
    map<string, bufferlist> attrs;
    bufferlist bl;
    bl.append(etag.c_str(), etag.size() + 1);
    attrs[RGW_ATTR_ETAG] = bl;
    attrs[RGW_ATTR_ACL] = aclbl;

    if (s->content_type) {
      bl.clear();
      bl.append(s->content_type, strlen(s->content_type) + 1);
      attrs[RGW_ATTR_CONTENT_TYPE] = bl;
    }

    get_request_metadata(s, attrs);

    ret = rgwstore->put_obj_meta(s->user.user_id, s->bucket_str, s->object_str, NULL, attrs);
  }
done:
  send_response();
}

void RGWDeleteObj::execute()
{
  ret = -EINVAL;
  if (s->object) {
    ret = rgwstore->delete_obj(s->user.user_id, s->bucket_str, s->object_str);
  }

  send_response();
}

static bool parse_copy_source(const char *src, string& bucket, string& object)
{
  string url_src(src);
  string dec_src;

  url_decode(url_src, dec_src);
  src = dec_src.c_str();

  RGW_LOG(15) << "decoded src=" << src << endl;

  if (*src == '/') ++src;

  string str(src);

  int pos = str.find("/");
  if (pos <= 0)
    return false;

  bucket = str.substr(0, pos);
  object = str.substr(pos + 1);

  if (object.size() == 0)
    return false;

  return true;
}

int RGWCopyObj::init_common()
{
  RGWAccessControlPolicy dest_policy;
  bufferlist aclbl;
  bufferlist bl;
  RGWAccessControlPolicy src_policy;
  string empty_str;
  time_t mod_time;
  time_t unmod_time;
  time_t *mod_ptr = NULL;
  time_t *unmod_ptr = NULL;

  if (!verify_permission(s, RGW_PERM_WRITE)) {
    ret = -EACCES;
    return ret;
  }

  ret = dest_policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!ret) {
     ret = -EINVAL;
     return ret;
  }

  ret = parse_copy_source(s->copy_source, src_bucket, src_object);
  if (!ret) {
     ret = -EINVAL;
     return ret;
  }
  /* just checking the bucket's permission */
  ret = read_acls(s, &src_policy, src_bucket, empty_str);
  if (ret < 0)
    return ret;

  if (!verify_permission(&src_policy, s->user.user_id, RGW_PERM_READ)) {
    ret = -EACCES;
    return ret;
  }

  dest_policy.encode(aclbl);

  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0) {
      ret = -EINVAL;
      return ret;
    }
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0) {
      ret = -EINVAL;
      return ret;
    }
    unmod_ptr = &unmod_time;
  }

  attrs[RGW_ATTR_ACL] = aclbl;
  get_request_metadata(s, attrs);

  return 0;
}

void RGWCopyObj::execute()
{
  ret = get_params();
  if (ret < 0)
    goto done;

  if (init_common() < 0)
    goto done;

  ret = rgwstore->copy_obj(s->user.user_id,
                        s->bucket_str, s->object_str,
                        src_bucket, src_object,
                        &mtime,
                        mod_ptr,
                        unmod_ptr,
                        if_match,
                        if_nomatch,
                        attrs, &s->err);

done:
  send_response();
}

void RGWGetACLs::execute()
{
  if (!verify_permission(s, RGW_PERM_READ_ACP)) {
    abort_early(s, -EACCES);
    return;
  }

  ret = read_acls(s);

  if (ret < 0) {
    send_response();
    return;
  }

  stringstream ss;
  s->acl->to_xml(ss);
  acls = ss.str(); 

  send_response();
}

static int rebuild_policy(RGWAccessControlPolicy& src, RGWAccessControlPolicy& dest)
{
  ACLOwner *owner = (ACLOwner *)src.find_first("Owner");
  if (!owner)
    return -EINVAL;

  RGWUserInfo owner_info;
  if (rgw_get_user_info_by_uid(owner->get_id(), owner_info) < 0) {
    RGW_LOG(10) << "owner info does not exist" << endl;
    return -EINVAL;
  }
  ACLOwner& new_owner = dest.get_owner();
  new_owner.set_id(owner->get_id());
  new_owner.set_name(owner_info.display_name);

  RGWAccessControlList& src_acl = src.get_acl();
  RGWAccessControlList& acl = dest.get_acl();

  XMLObjIter iter = src_acl.find("Grant");
  ACLGrant *src_grant = (ACLGrant *)iter.get_next();
  while (src_grant) {
    ACLGranteeType& type = src_grant->get_type();
    ACLGrant new_grant;
    bool grant_ok = false;
    string id;
    RGWUserInfo grant_user;
    switch (type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      {
        string email = src_grant->get_id();
        RGW_LOG(10) << "grant user email=" << email << endl;
        if (rgw_get_user_info_by_email(email, grant_user) < 0) {
          RGW_LOG(10) << "grant user email not found or other error" << endl;
          return -ERR_UNRESOLVABLE_EMAIL;
        }
        id = grant_user.user_id;
      }
    case ACL_TYPE_CANON_USER:
      {
        if (type.get_type() == ACL_TYPE_CANON_USER)
          id = src_grant->get_id();
    
        if (grant_user.user_id.empty() && rgw_get_user_info_by_uid(id, grant_user) < 0) {
          RGW_LOG(10) << "grant user does not exist:" << id << endl;
        } else {
          ACLPermission& perm = src_grant->get_permission();
          new_grant.set_canon(id, grant_user.display_name, perm.get_permissions());
          grant_ok = true;
          RGW_LOG(10) << "new grant: " << new_grant.get_id() << ":" << grant_user.display_name << endl;
        }
      }
      break;
    case ACL_TYPE_GROUP:
      {
        string group = src_grant->get_id();
        if (group.compare(RGW_URI_ALL_USERS) == 0 ||
            group.compare(RGW_URI_AUTH_USERS) == 0) {
          new_grant = *src_grant;
          grant_ok = true;
          RGW_LOG(10) << "new grant: " << new_grant.get_id() << endl;
        }
      }
    default:
      break;
    }
    if (grant_ok) {
      acl.add_grant(&new_grant);
    }
    src_grant = (ACLGrant *)iter.get_next();
  }

  return 0; 
}

void RGWPutACLs::execute()
{
  bufferlist bl;

  RGWAccessControlPolicy *policy = NULL;
  RGWXMLParser parser;
  RGWAccessControlPolicy new_policy;
  stringstream ss;
  char *orig_data = data;
  char *new_data = NULL;
  ACLOwner owner;

  if (!verify_permission(s, RGW_PERM_WRITE_ACP)) {
    ret = -EACCES;
    goto done;
  }

  ret = 0;

  if (!parser.init()) {
    ret = -EINVAL;
    goto done;
  }

  if (!s->acl) {
     s->acl = new RGWAccessControlPolicy;
     if (!s->acl) {
       ret = -ENOMEM;
       goto done;
     }
     owner.set_id(s->user.user_id);
     owner.set_name(s->user.display_name);
  } else {
     owner = s->acl->get_owner();
  }

  if (get_params() < 0)
    goto done;

  RGW_LOG(15) << "read len=" << len << " data=" << (data ? data : "") << endl;

  if (!s->canned_acl.empty() && len) {
    ret = -EINVAL;
    goto done;
  }
  if (!s->canned_acl.empty()) {
    RGWAccessControlPolicy canned_policy;
    bool r = canned_policy.create_canned(owner.get_id(), owner.get_display_name(), s->canned_acl);
    if (!r) {
      ret = -EINVAL;
      goto done;
    }
    canned_policy.to_xml(ss);
    new_data = strdup(ss.str().c_str());
    data = new_data;
    len = ss.str().size();
  }


  if (!parser.parse(data, len, 1)) {
    ret = -EACCES;
    goto done;
  }
  policy = (RGWAccessControlPolicy *)parser.find_first("AccessControlPolicy");
  if (!policy) {
    ret = -EINVAL;
    goto done;
  }

  if (rgw_log_level >= 15) {
    RGW_LOG(15) << "Old AccessControlPolicy" << endl;
    policy->to_xml(cout);
    RGW_LOG(15) << endl;
  }

  ret = rebuild_policy(*policy, new_policy);
  if (ret < 0)
    goto done;

  if (rgw_log_level >= 15) {
    RGW_LOG(15) << "New AccessControlPolicy" << endl;
    new_policy.to_xml(cout);
    RGW_LOG(15) << endl;
  }

  new_policy.encode(bl);
  ret = rgwstore->set_attr(s->bucket_str, s->object_str,
                       RGW_ATTR_ACL, bl);

done:
  free(orig_data);
  free(new_data);

  send_response();
  return;
}


void RGWHandler::init_state(struct req_state *s, struct fcgx_state *fcgx)
{
  /* Retrieve the loglevel from the CGI envirioment (if set) */
  const char *cgi_env_level = FCGX_GetParam("RGW_LOG_LEVEL", fcgx->envp);
  if (cgi_env_level != NULL) {
    int level = atoi(cgi_env_level);
    if (level >= 0) {
      rgw_log_level = level;
    }
  }

  const char *cgi_should_log = FCGX_GetParam("RGW_SHOULD_LOG", fcgx->envp);
  s->should_log = rgw_str_to_bool(cgi_should_log, RGW_SHOULD_LOG_DEFAULT);

  if (rgw_log_level >= 20) {
    char *p;
    for (int i=0; (p = fcgx->envp[i]); ++i) {
      RGW_LOG(20) << p << endl;
    }
  }
  s->fcgx = fcgx;
  s->content_started = false;
  s->err.clear();
  s->format = 0;
  if (s->acl) {
     delete s->acl;
     s->acl = new RGWAccessControlPolicy;
  }
  s->canned_acl.clear();
  s->expect_cont = false;

  free(s->os_user);
  free(s->os_groups);
  s->os_auth_token = NULL;
  s->os_user = NULL;
  s->os_groups = NULL;
  s->time = g_clock.now();
  s->user.clear();
}

int RGWHandler::do_read_permissions(bool only_bucket)
{
  int ret = read_acls(s, only_bucket);

  if (ret < 0)
    RGW_LOG(10) << "read_permissions on " << s->bucket_str << ":" <<s->object_str << " only_bucket=" << only_bucket << " ret=" << ret << endl;

  return ret;
}

