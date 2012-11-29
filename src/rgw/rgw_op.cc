
#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "common/Clock.h"
#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"

#include "rgw_rados.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_user.h"
#include "rgw_log.h"
#include "rgw_multi.h"

#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif

#define dout_subsys ceph_subsys_rgw

using namespace std;
using ceph::crypto::MD5;

static string mp_ns = "multipart";
static string shadow_ns = "shadow";

class MultipartMetaFilter : public RGWAccessListFilter {
public:
  MultipartMetaFilter() {}
  bool filter(string& name, string& key) {
    int len = name.size();
    if (len < 6)
      return false;

    int pos = name.find(MP_META_SUFFIX, len - 5);
    if (pos <= 0)
      return false;

    pos = name.rfind('.', pos - 1);
    if (pos < 0)
      return false;

    key = name.substr(0, pos);

    return true;
  }
};

static MultipartMetaFilter mp_filter;

static int parse_range(const char *range, off_t& ofs, off_t& end, bool *partial_content)
{
  int r = -ERANGE;
  string s(range);
  string ofs_str;
  string end_str;

  *partial_content = false;

  int pos = s.find("bytes=");
  if (pos < 0) {
    pos = 0;
    while (isspace(s[pos]))
      pos++;
    int end = pos;
    while (isalpha(s[end]))
      end++;
    if (strncasecmp(s.c_str(), "bytes", end - pos) != 0)
      return 0;
    while (isspace(s[end]))
      end++;
    if (s[end] != '=')
      return 0;
    s = s.substr(end + 1);
  } else {
    s = s.substr(pos + 6); /* size of("bytes=")  */
  }
  pos = s.find('-');
  if (pos < 0)
    goto done;

  *partial_content = true;

  ofs_str = s.substr(0, pos);
  end_str = s.substr(pos + 1);
  if (end_str.length()) {
    end = atoll(end_str.c_str());
    if (end < 0)
      goto done;
  }

  if (ofs_str.length()) {
    ofs = atoll(ofs_str.c_str());
  } else { // RFC2616 suffix-byte-range-spec
    ofs = -end;
    end = -1;
  }

  if (end >= 0 && end < ofs)
    goto done;

  r = 0;
done:
  return r;
}

static void format_xattr(std::string &xattr)
{
  /* If the extended attribute is not valid UTF-8, we encode it using quoted-printable
   * encoding.
   */
  if ((check_utf8(xattr.c_str(), xattr.length()) != 0) ||
      (check_for_control_characters(xattr.c_str(), xattr.length()) != 0)) {
    static const char MIME_PREFIX_STR[] = "=?UTF-8?Q?";
    static const int MIME_PREFIX_LEN = sizeof(MIME_PREFIX_STR) - 1;
    static const char MIME_SUFFIX_STR[] = "?=";
    static const int MIME_SUFFIX_LEN = sizeof(MIME_SUFFIX_STR) - 1;
    int mlen = mime_encode_as_qp(xattr.c_str(), NULL, 0);
    char *mime = new char[MIME_PREFIX_LEN + mlen + MIME_SUFFIX_LEN + 1];
    strcpy(mime, MIME_PREFIX_STR);
    mime_encode_as_qp(xattr.c_str(), mime + MIME_PREFIX_LEN, mlen);
    strcpy(mime + MIME_PREFIX_LEN + (mlen - 1), MIME_SUFFIX_STR);
    xattr.assign(mime);
    delete [] mime;
  }
}

/**
 * Get the HTTP request metadata out of the req_state as a
 * map(<attr_name, attr_contents>, where attr_name is RGW_ATTR_PREFIX.HTTP_NAME)
 * s: The request state
 * attrs: will be filled up with attrs mapped as <attr_name, attr_contents>
 *
 */
void rgw_get_request_metadata(struct req_state *s, map<string, bufferlist>& attrs)
{
  map<string, string>::iterator iter;
  for (iter = s->x_meta_map.begin(); iter != s->x_meta_map.end(); ++iter) {
    const string &name(iter->first);
    string &xattr(iter->second);
    ldout(s->cct, 10) << "x>> " << name << ":" << xattr << dendl;
    format_xattr(xattr);
    string attr_name(RGW_ATTR_PREFIX);
    attr_name.append(name);
    map<string, bufferlist>::value_type v(attr_name, bufferlist());
    std::pair < map<string, bufferlist>::iterator, bool > rval(attrs.insert(v));
    bufferlist& bl(rval.first->second);
    bl.append(xattr.c_str(), xattr.size() + 1);
  }
}

/**
 * Get the AccessControlPolicy for an object off of disk.
 * policy: must point to a valid RGWACL, and will be filled upon return.
 * bucket: name of the bucket containing the object.
 * object: name of the object to get the ACL for.
 * Returns: 0 on success, -ERR# otherwise.
 */
static int get_policy_from_attr(CephContext *cct, void *ctx, RGWAccessControlPolicy *policy, rgw_obj& obj)
{
  bufferlist bl;
  int ret = 0;

  if (obj.bucket.name.size()) {
    ret = rgwstore->get_attr(ctx, obj, RGW_ATTR_ACL, bl);

    if (ret >= 0) {
      bufferlist::iterator iter = bl.begin();
      try {
        policy->decode(iter);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
        return -EIO;
      }
      if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
        RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
        ldout(cct, 15) << "Read AccessControlPolicy";
        s3policy->to_xml(*_dout);
        *_dout << dendl;
      }
    } else if (ret == -ENODATA) {
      /* object exists, but policy is broken */
      RGWBucketInfo info;
      RGWUserInfo uinfo;
      string name;
      int r = rgwstore->get_bucket_info(ctx, obj.bucket.name, info);
      if (r < 0)
        goto done;
      r = rgw_get_user_info_by_uid(info.owner, uinfo);
      if (r < 0)
        goto done;

      policy->create_default(info.owner, uinfo.display_name);
      ret = 0;
    }
  }
done:
  return ret;
}

static int get_obj_attrs(struct req_state *s, rgw_obj& obj, map<string, bufferlist>& attrs, uint64_t *obj_size)
{
  void *handle;
  int ret = rgwstore->prepare_get_obj(s->obj_ctx, obj, NULL, NULL, &attrs, NULL,
                                      NULL, NULL, NULL, NULL, NULL, obj_size, &handle, &s->err);
  rgwstore->finish_get_obj(&handle);
  return ret;
}

static int read_policy(struct req_state *s, RGWBucketInfo& bucket_info, RGWAccessControlPolicy *policy, rgw_bucket& bucket, string& object)
{
  string upload_id;
  upload_id = s->args.get("uploadId");
  string oid = object;
  rgw_obj obj;

  if (bucket_info.flags & BUCKET_SUSPENDED) {
    ldout(s->cct, 0) << "NOTICE: bucket " << bucket_info.bucket.name << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  if (!oid.empty() && !upload_id.empty()) {
    RGWMPObj mp(oid, upload_id);
    oid = mp.get_meta();
    obj.init_ns(bucket, oid, mp_ns);
  } else {
    obj.init(bucket, oid);
  }
  int ret = get_policy_from_attr(s->cct, s->obj_ctx, policy, obj);
  if (ret == -ENOENT && object.size()) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy(s->cct);
    string no_object;
    rgw_obj no_obj(bucket, no_object);
    ret = get_policy_from_attr(s->cct, s->obj_ctx, &bucket_policy, no_obj);
    if (ret < 0)
      return ret;
    string& owner = bucket_policy.get_owner().get_id();
    if (owner.compare(s->user.user_id) != 0 &&
        !bucket_policy.verify_permission(s->user.user_id, s->perm_mask, RGW_PERM_READ))
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
static int build_policies(struct req_state *s, bool only_bucket, bool prefetch_data)
{
  int ret = 0;
  string obj_str;

  s->bucket_acl = new RGWAccessControlPolicy(s->cct);

  RGWBucketInfo bucket_info;
  if (s->bucket_name_str.size()) {
    ret = rgwstore->get_bucket_info(s->obj_ctx, s->bucket_name_str, bucket_info);
    if (ret < 0) {
      ldout(s->cct, 0) << "NOTICE: couldn't get bucket from bucket_name (name=" << s->bucket_name_str << ")" << dendl;
      return ret;
    }
    s->bucket = bucket_info.bucket;
    s->bucket_owner = bucket_info.owner;

    string no_obj;
    RGWAccessControlPolicy bucket_acl(s->cct);
    ret = read_policy(s, bucket_info, s->bucket_acl, s->bucket, no_obj);
  }

  /* we're passed only_bucket = true when we specifically need the bucket's
     acls, that happens on write operations */
  if (!only_bucket) {
    s->object_acl = new RGWAccessControlPolicy(s->cct);

    obj_str = s->object_str;
    rgw_obj obj(s->bucket, obj_str);
    rgwstore->set_atomic(s->obj_ctx, obj);
    if (prefetch_data) {
      rgwstore->set_prefetch_data(s->obj_ctx, obj);
    }
    ret = read_policy(s, bucket_info, s->object_acl, s->bucket, obj_str);
  }

  return ret;
}

int RGWGetObj::verify_permission()
{
  obj.init(s->bucket, s->object_str);
  rgwstore->set_atomic(s->obj_ctx, obj);
  rgwstore->set_prefetch_data(s->obj_ctx, obj);

  if (!verify_object_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWGetObj::execute()
{
  void *handle = NULL;
  utime_t start_time = s->time;
  bufferlist bl;

  perfcounter->inc(l_rgw_get);

  ret = get_params();
  if (ret < 0)
    goto done;

  ret = init_common();
  if (ret < 0)
    goto done;

  ret = rgwstore->prepare_get_obj(s->obj_ctx, obj, &ofs, &end, &attrs, mod_ptr,
                                  unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &s->obj_size, &handle, &s->err);
  if (ret < 0)
    goto done;

  start = ofs;

  if (!get_data || ofs > end)
    goto done;

  perfcounter->inc(l_rgw_get_b, end - ofs);

  while (ofs <= end) {
    ret = rgwstore->get_obj(s->obj_ctx, &handle, obj, bl, ofs, end);
    if (ret < 0) {
      goto done;
    }
    len = ret;
    ofs += len;
    ret = 0;

    perfcounter->finc(l_rgw_get_lat,
                     (ceph_clock_now(s->cct) - start_time));
    ret = send_response(bl);
    bl.clear();
    if (ret < 0) {
      dout(0) << "NOTICE: failed to send response to client" << dendl;
      goto done;
    }

    start_time = ceph_clock_now(s->cct);
  }

  return;

done:
  send_response(bl);
  rgwstore->finish_get_obj(&handle);
}

int RGWGetObj::init_common()
{
  if (range_str) {
    int r = parse_range(range_str, ofs, end, &partial_content);
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

int RGWListBuckets::verify_permission()
{
  return 0;
}

void RGWListBuckets::execute()
{
  ret = get_params();
  if (ret < 0)
    goto done;

  ret = rgw_read_user_buckets(s->user.user_id, buckets, !!(s->prot_flags & RGW_REST_SWIFT));
  if (ret < 0) {
    /* hmm.. something wrong here.. the user was authenticated, so it
       should exist, just try to recreate */
    ldout(s->cct, 10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << dendl;

    /*

    on a second thought, this is probably a bug and we should fail

    rgw_put_user_buckets(s->user.user_id, buckets);
    ret = 0;

    */
  }

done:
  send_response();
}

int RGWStatAccount::verify_permission()
{
  return 0;
}

void RGWStatAccount::execute()
{
  RGWUserBuckets buckets;

  ret = rgw_read_user_buckets(s->user.user_id, buckets, true);
  if (ret < 0) {
    /* hmm.. something wrong here.. the user was authenticated, so it
       should exist, just try to recreate */
    ldout(s->cct, 10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << dendl;

    /*

    on a second thought, this is probably a bug and we should fail

    rgw_put_user_buckets(s->user.user_id, buckets);
    ret = 0;

    */
  } else {
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    map<string, RGWBucketEnt>::iterator iter;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      RGWBucketEnt& bucket = iter->second;
      buckets_size += bucket.size;
      buckets_size_rounded += bucket.size_rounded;
      buckets_objcount += bucket.count;
    }
    buckets_count = m.size();
  }

  send_response();
}

int RGWStatBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWStatBucket::execute()
{
  RGWUserBuckets buckets;
  bucket.bucket = s->bucket;
  buckets.add(bucket);
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  ret = rgwstore->update_containers_stats(m);
  if (!ret)
    ret = -EEXIST;
  if (ret > 0) {
    ret = 0;
    map<string, RGWBucketEnt>::iterator iter = m.find(bucket.bucket.name);
    if (iter != m.end()) {
      bucket = iter->second;
    } else {
      ret = -EINVAL;
    }
  }

  send_response();
}

int RGWListBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

int RGWListBucket::parse_max_keys()
{
  if (!max_keys.empty()) {
    char *endptr;
    max = strtol(max_keys.c_str(), &endptr, 10);
    if (endptr) {
      while (*endptr && isspace(*endptr)) // ignore white space
        endptr++;
      if (*endptr) {
        return -EINVAL;
      }
    }
  } else {
    max = default_max;
  }

  return 0;
}

void RGWListBucket::execute()
{
  string no_ns;

  ret = get_params();
  if (ret < 0)
    goto done;

  ret = rgwstore->list_objects(s->bucket, max, prefix, delimiter, marker, objs, common_prefixes,
                               !!(s->prot_flags & RGW_REST_SWIFT), no_ns, &is_truncated, NULL);

done:
  send_response();
}

int RGWCreateBucket::verify_permission()
{
  if (!rgw_user_is_authenticated(s->user))
    return -EACCES;

  if (s->user.max_buckets) {
    RGWUserBuckets buckets;
    int ret = rgw_read_user_buckets(s->user.user_id, buckets, false);
    if (ret < 0)
      return ret;

    if (buckets.count() >= s->user.max_buckets) {
      return -ERR_TOO_MANY_BUCKETS;
    }
  }

  return 0;
}

void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy old_policy(s->cct);
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  bool existed;
  int r;
  rgw_obj obj(rgw_root_bucket, s->bucket_name_str);

  ret = get_params();
  if (ret < 0)
    goto done;

  s->bucket_owner = s->user.user_id;
  r = get_policy_from_attr(s->cct, s->obj_ctx, &old_policy, obj);
  if (r >= 0)  {
    if (old_policy.get_owner().get_id().compare(s->user.user_id) != 0) {
      ret = -EEXIST;
      goto done;
    }
  }
  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  s->bucket.name = s->bucket_name_str;
  ret = rgwstore->create_bucket(s->user.user_id, s->bucket, attrs, false,
                                true, s->user.auid);
  /* continue if EEXIST and create_bucket will fail below.  this way we can recover
   * from a partial create by retrying it. */
  ldout(s->cct, 20) << "rgw_create_bucket returned ret=" << ret << " bucket=" << s->bucket << dendl;

  if (ret && ret != -EEXIST)   
    goto done;

  existed = (ret == -EEXIST);

  ret = rgw_add_bucket(s->user.user_id, s->bucket);
  if (ret && !existed && ret != -EEXIST)   /* if it exists (or previously existed), don't remove it! */
    rgw_remove_user_bucket_info(s->user.user_id, s->bucket);

  if (ret == -EEXIST)
    ret = -ERR_BUCKET_EXISTS;

done:
  send_response();
}

int RGWDeleteBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteBucket::execute()
{
  ret = -EINVAL;

  if (s->bucket_name) {
    ret = rgwstore->delete_bucket(s->bucket);

    if (ret == 0) {
      ret = rgw_remove_user_bucket_info(s->user.user_id, s->bucket);
      if (ret < 0) {
        ldout(s->cct, 0) << "WARNING: failed to remove bucket: ret=" << ret << dendl;
      }
    }
  }

  send_response();
}

struct put_obj_aio_info {
  void *handle;
};

int RGWPutObj::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

class RGWPutObjProcessor_Plain : public RGWPutObjProcessor
{
  bufferlist data;
  rgw_obj obj;
  off_t ofs;

protected:
  int prepare(struct req_state *s);
  int handle_data(bufferlist& bl, off_t ofs, void **phandle);
  int throttle_data(void *handle) { return 0; }
  int complete(string& etag, map<string, bufferlist>& attrs);

public:
  RGWPutObjProcessor_Plain() : ofs(0) {}
};

int RGWPutObjProcessor_Plain::prepare(struct req_state *s)
{
  RGWPutObjProcessor::prepare(s);

  obj.init(s->bucket, s->object_str);

  return 0;
};

int RGWPutObjProcessor_Plain::handle_data(bufferlist& bl, off_t _ofs, void **phandle)
{
  if (ofs != _ofs)
    return -EINVAL;

  data.append(bl);
  ofs += bl.length();

  return 0;
}

int RGWPutObjProcessor_Plain::complete(string& etag, map<string, bufferlist>& attrs)
{
  int r = rgwstore->put_obj_meta(s->obj_ctx, obj, data.length(), NULL, attrs,
                                 RGW_OBJ_CATEGORY_MAIN, false, NULL, &data, NULL);
  return r;
}


class RGWPutObjProcessor_Aio : public RGWPutObjProcessor
{
  list<struct put_obj_aio_info> pending;
  size_t max_chunks;

  struct put_obj_aio_info pop_pending();
  int wait_pending_front();
  bool pending_has_completed();
  int drain_pending();

protected:
  rgw_obj obj;
  uint64_t obj_len;

  int handle_data(bufferlist& bl, off_t ofs, void **phandle);
  int throttle_data(void *handle);

  RGWPutObjProcessor_Aio() : max_chunks(RGW_MAX_PENDING_CHUNKS), obj_len(0) {}
  virtual ~RGWPutObjProcessor_Aio() {
    drain_pending();
  }
};

int RGWPutObjProcessor_Aio::handle_data(bufferlist& bl, off_t ofs, void **phandle)
{
  if ((uint64_t)ofs + bl.length() > obj_len)
    obj_len = ofs + bl.length();

  // For the first call pass -1 as the offset to
  // do a write_full.
  int r = rgwstore->aio_put_obj_data(NULL, obj,
                                     bl,
                                     ((ofs != 0) ? ofs : -1),
                                     false, phandle);

  return r;
}

struct put_obj_aio_info RGWPutObjProcessor_Aio::pop_pending()
{
  struct put_obj_aio_info info;
  info = pending.front();
  pending.pop_front();
  return info;
}

int RGWPutObjProcessor_Aio::wait_pending_front()
{
  struct put_obj_aio_info info = pop_pending();
  int ret = rgwstore->aio_wait(info.handle);
  return ret;
}

bool RGWPutObjProcessor_Aio::pending_has_completed()
{
  if (pending.size() == 0)
    return false;

  struct put_obj_aio_info& info = pending.front();
  return rgwstore->aio_completed(info.handle);
}

int RGWPutObjProcessor_Aio::drain_pending()
{
  int ret = 0;
  while (!pending.empty()) {
    int r = wait_pending_front();
    if (r < 0)
      ret = r;
  }
  return ret;
}

int RGWPutObjProcessor_Aio::throttle_data(void *handle)
{
  if (handle) {
    struct put_obj_aio_info info;
    info.handle = handle;
    pending.push_back(info);
  }
  size_t orig_size = pending.size();
  while (pending_has_completed()) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }

  /* resize window in case messages are draining too fast */
  if (orig_size - pending.size() >= max_chunks) {
    max_chunks++;
  }

  if (pending.size() > max_chunks) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }
  return 0;
}

class RGWPutObjProcessor_Atomic : public RGWPutObjProcessor_Aio
{
  bufferlist first_chunk;
  rgw_obj head_obj;
protected:
  int prepare(struct req_state *s);
  int complete(string& etag, map<string, bufferlist>& attrs);

public:
  ~RGWPutObjProcessor_Atomic() {}
  RGWPutObjProcessor_Atomic() {}
  int handle_data(bufferlist& bl, off_t ofs, void **phandle) {
    if (!ofs) {
      first_chunk.claim(bl);
      *phandle = NULL;
      return 0;
    }
    assert (ofs >= RGW_MAX_CHUNK_SIZE);
    int r = RGWPutObjProcessor_Aio::handle_data(bl, ofs, phandle);

    return r;
  }
};

int RGWPutObjProcessor_Atomic::prepare(struct req_state *s)
{
  RGWPutObjProcessor::prepare(s);

  string oid = s->object_str;
  head_obj.init(s->bucket, s->object_str);

  char buf[33];
  gen_rand_alphanumeric(s->cct, buf, sizeof(buf) - 1);
  oid.append("_");
  oid.append(buf);
  obj.init_ns(s->bucket, oid, shadow_ns);

  return 0;
}

int RGWPutObjProcessor_Atomic::complete(string& etag, map<string, bufferlist>& attrs)
{
  uint64_t head_chunk_len = first_chunk.length();
  RGWObjManifest manifest;
  manifest.objs[0].loc = head_obj;
  manifest.objs[0].loc_ofs = 0;
  manifest.objs[0].size = head_chunk_len;
  if (obj_len > RGW_MAX_CHUNK_SIZE) {
    manifest.objs[RGW_MAX_CHUNK_SIZE].loc = obj;
    manifest.objs[RGW_MAX_CHUNK_SIZE].loc_ofs = RGW_MAX_CHUNK_SIZE;
    manifest.objs[RGW_MAX_CHUNK_SIZE].size = obj_len - head_chunk_len;
  }

  manifest.obj_size = obj_len;

  rgwstore->set_atomic(s->obj_ctx, head_obj);

  int r = rgwstore->put_obj_meta(s->obj_ctx, head_obj, obj_len, NULL, attrs,
                                 RGW_OBJ_CATEGORY_MAIN, false, NULL, &first_chunk, &manifest);

  return r;
}

class RGWPutObjProcessor_Multipart : public RGWPutObjProcessor_Aio
{
  string part_num;
  RGWMPObj mp;
protected:
  int prepare(struct req_state *s);
  int complete(string& etag, map<string, bufferlist>& attrs);

public:
  RGWPutObjProcessor_Multipart() {}
};

int RGWPutObjProcessor_Multipart::prepare(struct req_state *s)
{
  RGWPutObjProcessor::prepare(s);

  string oid = s->object_str;
  string upload_id;
  upload_id = s->args.get("uploadId");
  mp.init(oid, upload_id);

  part_num = s->args.get("partNumber");
  if (part_num.empty()) {
    return -EINVAL;
  }
  oid = mp.get_part(part_num);

  obj.init_ns(s->bucket, oid, mp_ns);
  return 0;
}

int RGWPutObjProcessor_Multipart::complete(string& etag, map<string, bufferlist>& attrs)
{
  int r = rgwstore->put_obj_meta(s->obj_ctx, obj, s->obj_size, NULL, attrs, RGW_OBJ_CATEGORY_MAIN, false, NULL, NULL, NULL);
  if (r < 0)
    return r;

  bufferlist bl;
  RGWUploadPartInfo info;
  string p = "part.";
  p.append(part_num);
  info.num = atoi(part_num.c_str());
  info.etag = etag;
  info.size = s->obj_size;
  info.modified = ceph_clock_now(s->cct);
  ::encode(info, bl);

  string multipart_meta_obj = mp.get_meta();

  rgw_obj meta_obj;
  meta_obj.init_ns(s->bucket, multipart_meta_obj, mp_ns);

  r = rgwstore->omap_set(meta_obj, p, bl);

  return r;
}


RGWPutObjProcessor *RGWPutObj::select_processor()
{
  RGWPutObjProcessor *processor;

  bool multipart = s->args.exists("uploadId");

  if (!multipart) {
    if (s->content_length <= RGW_MAX_CHUNK_SIZE && !chunked_upload)
      processor = new RGWPutObjProcessor_Plain();
    else
      processor = new RGWPutObjProcessor_Atomic();
  } else {
    processor = new RGWPutObjProcessor_Multipart();
  }

  return processor;
}

void RGWPutObj::dispose_processor(RGWPutObjProcessor *processor)
{
  delete processor;
}

void RGWPutObj::execute()
{
  RGWPutObjProcessor *processor = NULL;
  char supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1];
  char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  bufferlist bl, aclbl;
  map<string, bufferlist> attrs;
  int len;


  perfcounter->inc(l_rgw_put);
  ret = -EINVAL;
  if (!s->object) {
    goto done;
  }

  ret = get_params();
  if (ret < 0)
    goto done;

  if (supplied_md5_b64) {
    ldout(s->cct, 15) << "supplied_md5_b64=" << supplied_md5_b64 << dendl;
    ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
                       supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
    ldout(s->cct, 15) << "ceph_armor ret=" << ret << dendl;
    if (ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      ret = -ERR_INVALID_DIGEST;
      goto done;
    }

    buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
    ldout(s->cct, 15) << "supplied_md5=" << supplied_md5 << dendl;
  }

  if (supplied_etag) {
    strncpy(supplied_md5, supplied_etag, sizeof(supplied_md5));
  }

  processor = select_processor();

  ret = processor->prepare(s);
  if (ret < 0)
    goto done;

  do {
    bufferlist data;
    len = get_data(data);
    if (len < 0) {
      ret = len;
      goto done;
    }
    if (!len)
      break;

    void *handle;
    const unsigned char *data_ptr = (const unsigned char *)data.c_str();

    ret = processor->handle_data(data, ofs, &handle);
    if (ret < 0)
      goto done;

    hash.Update(data_ptr, len);

    ret = processor->throttle_data(handle);
    if (ret < 0)
      goto done;

    ofs += len;
  } while (len > 0);

  if (!chunked_upload && (uint64_t)ofs != s->content_length) {
    ret = -ERR_REQUEST_TIMEOUT;
    goto done;
  }
  s->obj_size = ofs;
  perfcounter->inc(l_rgw_put_b, s->obj_size);

  hash.Final(m);

  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

  if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
     ret = -ERR_BAD_DIGEST;
     goto done;
  }
  policy.encode(aclbl);

  etag = calc_md5;

  if (supplied_etag && etag.compare(supplied_etag) != 0) {
    ret = -ERR_UNPROCESSABLE_ENTITY;
    goto done;
  }
  bl.append(etag.c_str(), etag.size() + 1);
  attrs[RGW_ATTR_ETAG] = bl;
  attrs[RGW_ATTR_ACL] = aclbl;

  if (s->content_type) {
    bl.clear();
    bl.append(s->content_type, strlen(s->content_type) + 1);
    attrs[RGW_ATTR_CONTENT_TYPE] = bl;
  }

  rgw_get_request_metadata(s, attrs);

  ret = processor->complete(etag, attrs);
done:
  dispose_processor(processor);
  perfcounter->finc(l_rgw_put_lat,
                   (ceph_clock_now(s->cct) - s->time));
  send_response();
  return;
}

int RGWPutMetadata::verify_permission()
{
  if (!verify_object_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWPutMetadata::execute()
{
  const char *meta_prefix = RGW_ATTR_META_PREFIX;
  int meta_prefix_len = sizeof(RGW_ATTR_META_PREFIX) - 1;
  map<string, bufferlist> attrs, orig_attrs, rmattrs;
  map<string, bufferlist>::iterator iter;
  bufferlist bl;

  rgw_obj obj(s->bucket, s->object_str);

  rgwstore->set_atomic(s->obj_ctx, obj);

  ret = get_params();
  if (ret < 0)
    goto done;

  rgw_get_request_metadata(s, attrs);

  /* check if obj exists, read orig attrs */
  ret = get_obj_attrs(s, obj, orig_attrs, NULL);
  if (ret < 0)
    goto done;

  /* only remove meta attrs */
  for (iter = orig_attrs.begin(); iter != orig_attrs.end(); ++iter) {
    const string& name = iter->first;
    if (name.compare(0, meta_prefix_len, meta_prefix) == 0) {
      rmattrs[name] = iter->second;
    } else if (attrs.find(name) == attrs.end()) {
      attrs[name] = iter->second;
    }
  }

  if (has_policy) {
    policy.encode(bl);
    attrs[RGW_ATTR_ACL] = bl;
  }
  ret = rgwstore->set_attrs(s->obj_ctx, obj, attrs, &rmattrs);

done:
  send_response();
}

int RGWDeleteObj::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteObj::execute()
{
  ret = -EINVAL;
  rgw_obj obj(s->bucket, s->object_str);
  if (s->object) {
    rgwstore->set_atomic(s->obj_ctx, obj);
    ret = rgwstore->delete_obj(s->obj_ctx, obj);
  }

  send_response();
}

bool RGWCopyObj::parse_copy_location(const char *src, string& bucket_name, string& object)
{
  string url_src(src);
  string dec_src;

  url_decode(url_src, dec_src);
  src = dec_src.c_str();

  ldout(s->cct, 15) << "decoded obj=" << src << dendl;

  if (*src == '/') ++src;

  string str(src);

  int pos = str.find("/");
  if (pos <= 0)
    return false;

  bucket_name = str.substr(0, pos);
  object = str.substr(pos + 1);

  if (object.size() == 0)
    return false;

  return true;
}

int RGWCopyObj::verify_permission()
{
  string empty_str;
  RGWAccessControlPolicy src_policy(s->cct);
  ret = get_params();
  if (ret < 0)
    return ret;

  RGWBucketInfo src_bucket_info, dest_bucket_info;

  /* get buckets info (source and dest) */

  ret = rgwstore->get_bucket_info(s->obj_ctx, src_bucket_name, src_bucket_info);
  if (ret < 0)
    return ret;

  src_bucket = src_bucket_info.bucket;

  if (src_bucket_name.compare(dest_bucket_name) == 0) {
    dest_bucket_info = src_bucket_info;
  } else {
    ret = rgwstore->get_bucket_info(s->obj_ctx, dest_bucket_name, dest_bucket_info);
    if (ret < 0)
      return ret;
  }

  dest_bucket = dest_bucket_info.bucket;

  /* check source object permissions */
  ret = read_policy(s, src_bucket_info, &src_policy, src_bucket, src_object);
  if (ret < 0)
    return ret;

  if (!src_policy.verify_permission(s->user.user_id, s->perm_mask, RGW_PERM_READ))
    return -EACCES;

  RGWAccessControlPolicy dest_bucket_policy(s->cct);

  /* check dest bucket permissions */
  ret = read_policy(s, dest_bucket_info, &dest_bucket_policy, dest_bucket, empty_str);
  if (ret < 0)
    return ret;

  if (!dest_bucket_policy.verify_permission(s->user.user_id, s->perm_mask, RGW_PERM_WRITE))
    return -EACCES;

  ret = init_dest_policy();
  if (ret < 0)
    return ret;

  return 0;
}


int RGWCopyObj::init_common()
{
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

  bufferlist aclbl;
  dest_policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  rgw_get_request_metadata(s, attrs);

  if (s->content_type) {
    bufferlist bl;
    bl.append(s->content_type, strlen(s->content_type) + 1);
    attrs[RGW_ATTR_CONTENT_TYPE] = bl;
  }

  return 0;
}

void RGWCopyObj::execute()
{
  rgw_obj src_obj, dst_obj;

  if (init_common() < 0)
    goto done;

  src_obj.init(src_bucket, src_object);
  dst_obj.init(dest_bucket, dest_object);
  rgwstore->set_atomic(s->obj_ctx, src_obj);
  rgwstore->set_atomic(s->obj_ctx, dst_obj);

  ret = rgwstore->copy_obj(s->obj_ctx,
                        dst_obj,
                        src_obj,
                        &mtime,
                        mod_ptr,
                        unmod_ptr,
                        if_match,
                        if_nomatch,
                        replace_attrs,
                        attrs, RGW_OBJ_CATEGORY_MAIN, &s->err);

done:
  send_response();
}

int RGWGetACLs::verify_permission()
{
  bool perm;
  if (s->object) {
    perm = verify_object_permission(s, RGW_PERM_READ_ACP);
  } else {
    perm = verify_bucket_permission(s, RGW_PERM_READ_ACP);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWGetACLs::execute()
{
  stringstream ss;
  RGWAccessControlPolicy *acl = (s->object ? s->object_acl : s->bucket_acl);
  RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(acl);
  s3policy->to_xml(ss);
  acls = ss.str(); 
  send_response();
}



int RGWPutACLs::verify_permission()
{
  bool perm;
  if (s->object) {
    perm = verify_object_permission(s, RGW_PERM_WRITE_ACP);
  } else {
    perm = verify_bucket_permission(s, RGW_PERM_WRITE_ACP);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWPutACLs::execute()
{
  bufferlist bl;

  RGWAccessControlPolicy_S3 *policy = NULL;
  RGWACLXMLParser_S3 parser(s->cct);
  RGWAccessControlPolicy_S3 new_policy(s->cct);
  stringstream ss;
  char *new_data = NULL;
  ACLOwner owner;
  rgw_obj obj;

  ret = 0;

  if (!parser.init()) {
    ret = -EINVAL;
    goto done;
  }

  owner.set_id(s->user.user_id);
  owner.set_name(s->user.display_name);

  ret = get_params();
  if (ret < 0)
    goto done;

  ldout(s->cct, 15) << "read len=" << len << " data=" << (data ? data : "") << dendl;

  if (!s->canned_acl.empty() && len) {
    ret = -EINVAL;
    goto done;
  }
  if (!s->canned_acl.empty()) {
    ret = get_canned_policy(owner, ss);
    if (ret < 0)
      goto done;

    new_data = strdup(ss.str().c_str());
    free(data);
    data = new_data;
    len = ss.str().size();
  }


  if (!parser.parse(data, len, 1)) {
    ret = -EACCES;
    goto done;
  }
  policy = (RGWAccessControlPolicy_S3 *)parser.find_first("AccessControlPolicy");
  if (!policy) {
    ret = -EINVAL;
    goto done;
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "Old AccessControlPolicy";
    policy->to_xml(*_dout);
    *_dout << dendl;
  }

  ret = policy->rebuild(&owner, new_policy);
  if (ret < 0)
    goto done;

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "New AccessControlPolicy:";
    new_policy.to_xml(*_dout);
    *_dout << dendl;
  }

  new_policy.encode(bl);
  obj.init(s->bucket, s->object_str);
  rgwstore->set_atomic(s->obj_ctx, obj);
  ret = rgwstore->set_attr(s->obj_ctx, obj, RGW_ATTR_ACL, bl);

done:
  send_response();
}

int RGWInitMultipart::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWInitMultipart::execute()
{
  bufferlist aclbl;
  map<string, bufferlist> attrs;
  rgw_obj obj;

  if (get_params() < 0)
    goto done;
  ret = -EINVAL;
  if (!s->object)
    goto done;

  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  if (s->content_type) {
    bufferlist bl;
    bl.append(s->content_type, strlen(s->content_type) + 1);
    attrs[RGW_ATTR_CONTENT_TYPE] = bl;
  }

  rgw_get_request_metadata(s, attrs);

  do {
    char buf[33];
    gen_rand_alphanumeric(s->cct, buf, sizeof(buf) - 1);
    upload_id = buf;

    string tmp_obj_name;
    RGWMPObj mp(s->object_str, upload_id);
    tmp_obj_name = mp.get_meta();

    obj.init_ns(s->bucket, tmp_obj_name, mp_ns);
    // the meta object will be indexed with 0 size, we c
    ret = rgwstore->put_obj_meta(s->obj_ctx, obj, 0, NULL, attrs, RGW_OBJ_CATEGORY_MULTIMETA, true, NULL, NULL, NULL);
  } while (ret == -EEXIST);
done:
  send_response();
}

static int get_multiparts_info(struct req_state *s, string& meta_oid, map<uint32_t, RGWUploadPartInfo>& parts,
                               RGWAccessControlPolicy& policy, map<string, bufferlist>& attrs)
{
  map<string, bufferlist> parts_map;
  map<string, bufferlist>::iterator iter;
  bufferlist header;

  rgw_obj obj;
  obj.init_ns(s->bucket, meta_oid, mp_ns);

  int ret = get_obj_attrs(s, obj, attrs, NULL);
  if (ret < 0)
    return ret;

  ret = rgwstore->omap_get_all(obj, header, parts_map);
  if (ret < 0)
    return ret;

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    string name = iter->first;
    if (name.compare(RGW_ATTR_ACL) == 0) {
      bufferlist& bl = iter->second;
      bufferlist::iterator bli = bl.begin();
      try {
        ::decode(policy, bli);
      } catch (buffer::error& err) {
        ldout(s->cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
        return -EIO;
      }
      break;
    }
  }


  for (iter = parts_map.begin(); iter != parts_map.end(); ++iter) {
    bufferlist& bl = iter->second;
    bufferlist::iterator bli = bl.begin();
    RGWUploadPartInfo info;
    try {
      ::decode(info, bli);
    } catch (buffer::error& err) {
      ldout(s->cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    }
    parts[info.num] = info;
  }
  return 0;
}

int RGWCompleteMultipart::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

static string string_unquote(const string& s)
{
  if (s[0] != '"' || s.size() < 2)
    return s;

  int len;
  for (len = s.size(); len > 2; --len) {
    if (s[len - 1] != ' ')
      break;
  }

  if (s[len-1] != '"')
    return s;

  return s.substr(1, len - 2);
}

void RGWCompleteMultipart::execute()
{
  RGWMultiCompleteUpload *parts;
  map<int, string>::iterator iter;
  RGWMultiXMLParser parser;
  string meta_oid;
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  RGWAccessControlPolicy policy(s->cct);
  map<string, bufferlist> attrs;
  off_t ofs = 0;
  MD5 hash;
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  bufferlist etag_bl;
  rgw_obj meta_obj;
  rgw_obj target_obj;
  RGWMPObj mp;
  RGWObjManifest manifest;

  ret = get_params();
  if (ret < 0)
    goto done;

  if (!data) {
    ret = -EINVAL;
    goto done;
  }

  if (!parser.init()) {
    ret = -EINVAL;
    goto done;
  }

  if (!parser.parse(data, len, 1)) {
    ret = -EINVAL;
    goto done;
  }

  parts = (RGWMultiCompleteUpload *)parser.find_first("CompleteMultipartUpload");
  if (!parts) {
    ret = -EINVAL;
    goto done;
  }

  mp.init(s->object_str, upload_id);
  meta_oid = mp.get_meta();

  ret = get_multiparts_info(s, meta_oid, obj_parts, policy, attrs);
  if (ret == -ENOENT)
    ret = -ERR_NO_SUCH_UPLOAD;
  if (parts->parts.size() != obj_parts.size())
    ret = -ERR_INVALID_PART;
  if (ret < 0)
    goto done;

  for (iter = parts->parts.begin(), obj_iter = obj_parts.begin();
       iter != parts->parts.end() && obj_iter != obj_parts.end();
       ++iter, ++obj_iter) {
    char etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    if (iter->first != (int)obj_iter->first) {
      ldout(s->cct, 0) << "NOTICE: parts num mismatch: next requested: " << iter->first << " next uploaded: " << obj_iter->first << dendl;
      ret = -ERR_INVALID_PART;
      goto done;
    }
    string part_etag = string_unquote(iter->second);
    if (part_etag.compare(obj_iter->second.etag) != 0) {
      ldout(s->cct, 0) << "NOTICE: etag mismatch: part: " << iter->first << " etag: " << iter->second << dendl;
      ret = -ERR_INVALID_PART;
      goto done;
    }

    hex_to_buf(obj_iter->second.etag.c_str(), etag, CEPH_CRYPTO_MD5_DIGESTSIZE);
    hash.Update((const byte *)etag, sizeof(etag));
  }
  hash.Final((byte *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],  sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)parts->parts.size());
  ldout(s->cct, 10) << "calculated etag: " << final_etag_str << dendl;

  etag_bl.append(final_etag_str, strlen(final_etag_str) + 1);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  target_obj.init(s->bucket, s->object_str);
  
  for (obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
    string oid = mp.get_part(obj_iter->second.num);
    rgw_obj src_obj;
    src_obj.init_ns(s->bucket, oid, mp_ns);

    RGWObjManifestPart& part = manifest.objs[ofs];

    part.loc = src_obj;
    part.loc_ofs = 0;
    part.size = obj_iter->second.size;

    ofs += part.size;
  }

  manifest.obj_size = ofs;

  rgwstore->set_atomic(s->obj_ctx, target_obj);

  ret = rgwstore->put_obj_meta(s->obj_ctx, target_obj, ofs, NULL, attrs,
                               RGW_OBJ_CATEGORY_MAIN, false, NULL, NULL, &manifest);
  if (ret < 0)
    goto done;

  // remove the upload obj
  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  rgwstore->delete_obj(s->obj_ctx, meta_obj);

done:
  send_response();
}

int RGWAbortMultipart::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWAbortMultipart::execute()
{
  ret = -EINVAL;
  string upload_id;
  string meta_oid;
  string prefix;
  upload_id = s->args.get("uploadId");
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  RGWAccessControlPolicy policy(s->cct);
  map<string, bufferlist> attrs;
  rgw_obj meta_obj;
  RGWMPObj mp;

  if (upload_id.empty() || s->object_str.empty())
    goto done;

  mp.init(s->object_str, upload_id); 
  meta_oid = mp.get_meta();

  ret = get_multiparts_info(s, meta_oid, obj_parts, policy, attrs);
  if (ret < 0)
    goto done;

  for (obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
    string oid = mp.get_part(obj_iter->second.num);
    rgw_obj obj;
    obj.init_ns(s->bucket, oid, mp_ns);
    ret = rgwstore->delete_obj(s->obj_ctx, obj);
    if (ret < 0 && ret != -ENOENT)
      goto done;
  }
  // and also remove the metadata obj
  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  ret = rgwstore->delete_obj(s->obj_ctx, meta_obj);
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_BUCKET;
  }
done:

  send_response();
}

int RGWListMultipart::verify_permission()
{
  if (!verify_object_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWListMultipart::execute()
{
  map<string, bufferlist> xattrs;
  string meta_oid;
  RGWMPObj mp;

  ret = get_params();
  if (ret < 0)
    goto done;

  mp.init(s->object_str, upload_id);
  meta_oid = mp.get_meta();

  ret = get_multiparts_info(s, meta_oid, parts, policy, xattrs);

done:
  send_response();
}

int RGWListBucketMultiparts::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWListBucketMultiparts::execute()
{
  vector<RGWObjEnt> objs;
  string marker_meta;

  ret = get_params();
  if (ret < 0)
    goto done;

  if (s->prot_flags & RGW_REST_SWIFT) {
    string path_args;
    path_args = s->args.get("path");
    if (!path_args.empty()) {
      if (!delimiter.empty() || !prefix.empty()) {
        ret = -EINVAL;
        goto done;
      }
      prefix = path_args;
      delimiter="/";
    }
  }
  marker_meta = marker.get_meta();
  ret = rgwstore->list_objects(s->bucket, max_uploads, prefix, delimiter, marker_meta, objs, common_prefixes,
                               !!(s->prot_flags & RGW_REST_SWIFT), mp_ns, &is_truncated, &mp_filter);
  if (objs.size()) {
    vector<RGWObjEnt>::iterator iter;
    RGWMultipartUploadEntry entry;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      string name = iter->name;
      if (!entry.mp.from_meta(name))
        continue;
      entry.obj = *iter;
      uploads.push_back(entry);
    }
    next_marker = entry;
  }
done:
  send_response();
}

int RGWHandler::init(struct req_state *_s, FCGX_Request *fcgx)
{
  s = _s;

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    char *p;
    for (int i=0; (p = fcgx->envp[i]); ++i) {
      ldout(s->cct, 20) << p << dendl;
    }
  }
  return 0;
}

int RGWHandler::do_read_permissions(RGWOp *op, bool only_bucket)
{
  int ret = build_policies(s, only_bucket, op->prefetch_data());

  if (ret < 0) {
    ldout(s->cct, 10) << "read_permissions on " << s->bucket << ":" <<s->object_str << " only_bucket=" << only_bucket << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
  }

  return ret;
}

