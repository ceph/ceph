
#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "common/base64.h"

#include "rgw_access.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_user.h"

using namespace std;

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
int read_acls(RGWAccessControlPolicy *policy, string& bucket, string& object)
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

  ret = read_acls(s->acl, s->bucket_str, obj_str);

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
                                  unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &handle, &err);
  if (ret < 0)
    goto done;

  if (!get_data || ofs > end)
    goto done;

  while (ofs <= end) {
    len = rgwstore->get_obj(&handle, s->bucket_str, s->object_str, &data, ofs, end);
    if (len < 0) {
      ret = len;
      goto done;
    }

    send_response(handle);
    ofs += len;
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
  ret = rgw_get_user_buckets(s->user.user_id, buckets);
  if (ret < 0) {
    /* hmm.. something wrong here.. the user was authenticated, so it
       should exist, just try to recreate */
    RGW_LOG(10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << endl;
    rgw_put_user_buckets(s->user.user_id, buckets);
    ret = 0;
  }

  send_response();
}

void RGWListBucket::execute()
{
  if (!verify_permission(s, RGW_PERM_READ)) {
    ret = -EACCES;
    goto done;
  }

  prefix = s->args.get("prefix");
  marker = s->args.get("marker");
  max_keys = s->args.get("max-keys");
 if (!max_keys.empty()) {
    max = atoi(max_keys.c_str());
  } else {
    max = -1;
  }
  url_decode(s->args.get("delimiter"), delimiter);
  ret = rgwstore->list_objects(s->user.user_id, s->bucket_str, max, prefix, delimiter, marker, objs, common_prefixes);
done:
  send_response();
}

void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy policy;
  map<string, bufferlist> attrs;
  bufferlist aclbl;

  bool pol_ret = policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!pol_ret) {
    ret = -EINVAL;
    goto done;
  }
  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  ret = rgwstore->create_bucket(s->user.user_id, s->bucket_str, attrs,
				s->user.auid);

  if (ret == 0) {
    RGWUserBuckets buckets;

    int r = rgw_get_user_buckets(s->user.user_id, buckets);
    RGWObjEnt new_bucket;

    switch (r) {
    case 0:
    case -ENOENT:
    case -ENODATA:
      new_bucket.name = s->bucket_str;
      new_bucket.size = 0;
      time(&new_bucket.mtime);
      buckets.add(new_bucket);
      ret = rgw_put_user_buckets(s->user.user_id, buckets);
      break;
    default:
      RGW_LOG(10) << "rgw_get_user_buckets returned " << ret << endl;
      break;
    }
  }
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
      RGWUserBuckets buckets;

      int r = rgw_get_user_buckets(s->user.user_id, buckets);

      if (r == 0 || r == -ENOENT) {
        buckets.remove(s->bucket_str);
        ret = rgw_put_user_buckets(s->user.user_id, buckets);
      }
    }
  }

  send_response();
}

void RGWPutObj::execute()
{
  ret = -EINVAL;
  struct rgw_err err;
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

    bool ret = policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
    if (!ret) {
       err.code = "InvalidArgument";
       ret = -EINVAL;
       goto done;
    }

    char supplied_md5_bin[MD5_DIGEST_LENGTH + 1];
    char supplied_md5[MD5_DIGEST_LENGTH * 2 + 1];
    char calc_md5[MD5_DIGEST_LENGTH * 2 + 1];
    MD5_CTX c;
    unsigned char m[MD5_DIGEST_LENGTH];

    if (supplied_md5_b64) {
      RGW_LOG(15) << "supplied_md5_b64=" << supplied_md5_b64 << endl;
      int ret = decode_base64(supplied_md5_b64, strlen(supplied_md5_b64),
                                 supplied_md5_bin, sizeof(supplied_md5_bin));
      RGW_LOG(15) << "decode_base64 ret=" << ret << endl;
      if (ret != MD5_DIGEST_LENGTH) {
        err.code = "InvalidDigest";
        ret = -EINVAL;
        goto done;
      }

      buf_to_hex((const unsigned char *)supplied_md5_bin, MD5_DIGEST_LENGTH, supplied_md5);
      RGW_LOG(15) << "supplied_md5=" << supplied_md5 << endl;
    }

    MD5_Init(&c);
    do {
      get_data();
      if (len > 0) {
        MD5_Update(&c, data, (unsigned long)len);
        ret = rgwstore->put_obj_data(s->user.user_id, s->bucket_str, s->object_str, data, ofs, len, NULL);
        free(data);
        if (ret < 0)
          goto done;
        ofs += len;
      }
    } while ( len > 0);

    MD5_Final(m, &c);

    buf_to_hex(m, MD5_DIGEST_LENGTH, calc_md5);

    if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
       err.code = "BadDigest";
       ret = -EINVAL;
       goto done;
    }
    bufferlist aclbl;
    policy.encode(aclbl);

    string md5_str(calc_md5);
    map<string, bufferlist> attrs;
    bufferlist bl;
    bl.append(md5_str.c_str(), md5_str.size() + 1);
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
  struct rgw_err err;
  RGWAccessControlPolicy dest_policy;
  bool ret;
  bufferlist aclbl;
  map<string, bufferlist> attrs;
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
     err.code = "InvalidArgument";
     ret = -EINVAL;
     return ret;
  }

  ret = parse_copy_source(s->copy_source, src_bucket, src_object);
  if (!ret) {
     err.code = "InvalidArgument";
     ret = -EINVAL;
     return ret;
  }
  /* just checking the bucket's permission */
  ret = read_acls(&src_policy, src_bucket, empty_str);
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
                        attrs, &err);

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
  if (rgw_get_user_info(owner->get_id(), owner_info) < 0) {
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
    switch (type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      {
        string email = src_grant->get_id();
        RGW_LOG(10) << "grant user email=" << email << endl;
        if (rgw_get_uid_by_email(email, id) < 0) {
          RGW_LOG(10) << "grant user email not found or other error" << endl;
          break;
        }
      }
    case ACL_TYPE_CANON_USER:
      {
        if (type.get_type() == ACL_TYPE_CANON_USER)
          id = src_grant->get_id();
    
        RGWUserInfo grant_user;
        if (rgw_get_user_info(id, grant_user) < 0) {
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

  RGWAccessControlPolicy *policy;
  RGWXMLParser parser;
  RGWAccessControlPolicy new_policy;

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
  }

  if (get_params() < 0)
    goto done;

  RGW_LOG(15) << "read data=" << data << " len=" << len << endl;

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
    policy->to_xml(cerr);
    RGW_LOG(15) << endl;
  }

  ret = rebuild_policy(*policy, new_policy);
  if (ret < 0)
    goto done;

  if (rgw_log_level >= 15) {
    RGW_LOG(15) << "New AccessControlPolicy" << endl;
    new_policy.to_xml(cerr);
    RGW_LOG(15) << endl;
  }

  new_policy.encode(bl);
  ret = rgwstore->set_attr(s->bucket_str, s->object_str,
                       RGW_ATTR_ACL, bl);

done:
  free(data);

  send_response();
  return;
}


void RGWHandler::init_state(struct req_state *s, struct fcgx_state *fcgx)
{
  this->s = s;

  /* Retrieve the loglevel from the CGI envirioment (if set) */
  const char *cgi_env_level = FCGX_GetParam("RGW_LOG_LEVEL", fcgx->envp);
  if (cgi_env_level != NULL) {
    int level = atoi(cgi_env_level);
    if (level >= 0) {
      rgw_log_level = level;
    }
  }

  if (rgw_log_level >= 20) {
    char *p;
    for (int i=0; (p = fcgx->envp[i]); ++i) {
      RGW_LOG(20) << p << endl;
    }
  }
  s->fcgx = fcgx;
  s->content_started = false;
  s->indent = 0;
  s->err_exist = false;
  memset(&s->err, 0, sizeof(s->err));
  if (s->acl) {
     delete s->acl;
     s->acl = new RGWAccessControlPolicy;
  }
  s->canned_acl.clear();

  provider_init_state();
}

int RGWHandler::do_read_permissions(bool only_bucket)
{
  int ret = read_acls(s, only_bucket);

  if (ret < 0)
    RGW_LOG(10) << "read_permissions on " << s->bucket_str << ":" <<s->object_str << " only_bucket=" << only_bucket << " ret=" << ret << endl;

  return ret;
}

