
#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "common/Clock.h"
#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"

#include "rgw_access.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_multi.h"

using namespace std;
using ceph::crypto::MD5;

static string mp_ns = "multipart";
static string tmp_ns = "tmp";

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

  RGW_LOG(10) << "parse_range ofs=" << ofs << " end=" << end << dendl;

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
    RGW_LOG(10) << "format_xattr: formatted as '" << xattr << "'" << dendl;
  }
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
  for (iter = s->x_meta_map.begin(); iter != s->x_meta_map.end(); ++iter) {
    const string &name(iter->first);
    string &xattr(iter->second);
    RGW_LOG(10) << "x>> " << name << ":" << xattr << dendl;
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
static int get_policy_from_attr(void *ctx, RGWAccessControlPolicy *policy, rgw_obj& obj)
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
        RGW_LOG(0) << "error: could not decode policy, caught buffer::error" << dendl;
        return -EIO;
      }
      if (g_conf->rgw_log >= 15) {
        RGW_LOG(15) << "Read AccessControlPolicy" << dendl;
        policy->to_xml(cerr);
        RGW_LOG(15) << dendl;
      }
    }
  }

  return ret;
}

int read_acls(struct req_state *s, RGWAccessControlPolicy *policy, rgw_bucket& bucket, string& object)
{
  string upload_id;
  url_decode(s->args.get("uploadId"), upload_id);
  string oid = object;
  rgw_obj obj;

  if (!oid.empty()) {
    bool suspended;
    int ret = rgwstore->bucket_suspended(bucket, &suspended);
    if (ret < 0)
      return ret;

    if (suspended)
      return -ERR_USER_SUSPENDED;
  }

  if (!oid.empty() && !upload_id.empty()) {
    RGWMPObj mp(oid, upload_id);
    oid = mp.get_meta();
    obj.set_ns(mp_ns);
  }
  obj.init(bucket, oid, object);

  int ret = get_policy_from_attr(s->obj_ctx, policy, obj);
  if (ret == -ENOENT && object.size()) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy;
    string no_object;
    rgw_obj no_obj(bucket, no_object);
    ret = get_policy_from_attr(s->obj_ctx, &bucket_policy, no_obj);
    if (ret < 0)
      return ret;

    if (!verify_permission(&bucket_policy, s->user.user_id, s->perm_mask, RGW_PERM_READ))
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
  if (!only_bucket) {
    obj_str = s->object_str;
    rgw_obj obj(s->bucket, obj_str);
    rgwstore->set_atomic(s->obj_ctx, obj);
  }

  if (s->bucket_name_str.size()) {
    RGWBucketInfo bucket_info;
    ret = rgw_get_bucket_info(s->bucket_name_str, bucket_info);
    if (ret < 0) {
      RGW_LOG(0) << "couldn't get bucket from bucket_name (name=" << s->bucket_name_str << ")" << dendl;
      return ret;
    }
    s->bucket = bucket_info.bucket;
    s->bucket_owner = bucket_info.owner;
  }

  ret = read_acls(s, s->acl, s->bucket, obj_str);

  return ret;
}

int RGWGetObj::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWGetObj::execute()
{
  void *handle = NULL;
  rgw_obj obj;

  ret = get_params();
  if (ret < 0)
    goto done;

  init_common();

  obj.init(s->bucket, s->object_str);
  rgwstore->set_atomic(s->obj_ctx, obj);
  ret = rgwstore->prepare_get_obj(s->obj_ctx, obj, ofs, &end, &attrs, mod_ptr,
                                  unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &s->obj_size, &handle, &s->err);
  if (ret < 0)
    goto done;

  if (!get_data || ofs > end)
    goto done;

  while (ofs <= end) {
    data = NULL;
    ret = rgwstore->get_obj(s->obj_ctx, &handle, obj, &data, ofs, end);
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
    start = ofs;
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
  ret = rgw_read_user_buckets(s->user.user_id, buckets, !!(s->prot_flags & RGW_REST_SWIFT));
  if (ret < 0) {
    /* hmm.. something wrong here.. the user was authenticated, so it
       should exist, just try to recreate */
    RGW_LOG(10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << dendl;

    /*

    on a second thought, this is probably a bug and we should fail

    rgw_put_user_buckets(s->user.user_id, buckets);
    ret = 0;

    */
  }

  send_response();
}

int RGWStatBucket::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_READ))
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
  if (!::verify_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWListBucket::execute()
{
  string no_ns;

  url_decode(s->args.get("prefix"), prefix);
  url_decode(s->args.get("marker"), marker);
  url_decode(s->args.get(limit_opt_name), max_keys);
  if (!max_keys.empty()) {
    char *endptr;
    max = strtol(max_keys.c_str(), &endptr, 10);
    if (endptr) {
      while (*endptr && isspace(*endptr)) // ignore white space
        endptr++;
      if (*endptr) {
        ret = -EINVAL;
        goto done;
      }
    }
  } else {
    max = default_max;
  }
  url_decode(s->args.get("delimiter"), delimiter);

  if (s->prot_flags & RGW_REST_SWIFT) {
    string path_args;
    url_decode(s->args.get("path"), path_args);
    if (!path_args.empty()) {
      if (!delimiter.empty() || !prefix.empty()) {
        ret = -EINVAL;
        goto done;
      }
      url_decode(path_args, prefix);
      delimiter="/";
    }
  }

  ret = rgwstore->list_objects(s->user.user_id, s->bucket, max, prefix, delimiter, marker, objs, common_prefixes,
                               !!(s->prot_flags & RGW_REST_SWIFT), no_ns, &is_truncated, NULL);

done:
  send_response();
}

int RGWCreateBucket::verify_permission()
{
  if (!rgw_user_is_authenticated(s->user))
    return -EACCES;

  return 0;
}

void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy policy, old_policy;
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  bool existed;
  bool pol_ret;

  rgw_obj obj(rgw_root_bucket, s->bucket_name_str);

  s->bucket_owner = s->user.user_id;

  int r = get_policy_from_attr(s->obj_ctx, &old_policy, obj);
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

  s->bucket.name = s->bucket_name_str;
  ret = rgwstore->create_bucket(s->user.user_id, s->bucket, attrs, false,
                                true, s->user.auid);
  /* continue if EEXIST and create_bucket will fail below.  this way we can recover
   * from a partial create by retrying it. */
  RGW_LOG(0) << "rgw_create_bucket returned ret=" << ret << " bucket=" << s->bucket << dendl;

  if (ret && ret != -EEXIST)   
    goto done;

  existed = (ret == -EEXIST);

  ret = rgw_add_bucket(s->user.user_id, s->bucket);
  if (ret && !existed && ret != -EEXIST)   /* if it exists (or previously existed), don't remove it! */
    rgw_remove_user_bucket_info(s->user.user_id, s->bucket, false);

  if (ret == -EEXIST)
    ret = 0;

done:
  send_response();
}

int RGWDeleteBucket::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteBucket::execute()
{
  ret = -EINVAL;

  if (s->bucket_name) {
    ret = rgwstore->delete_bucket(s->user.user_id, s->bucket, false);

    if (ret == 0) {
      ret = rgw_remove_user_bucket_info(s->user.user_id, s->bucket, false);
      if (ret < 0) {
        RGW_LOG(0) << "WARNING: failed to remove bucket: ret=" << ret << dendl;
      }

      string oid;
      rgw_obj obj(s->bucket, oid);
      RGWIntentEvent intent = DEL_POOL;
      int r = rgw_log_intent(s, obj, intent);
      if (r < 0) {
        RGW_LOG(0) << "WARNING: failed to log intent for bucket removal bucket=" << s->bucket << dendl;
      }
    }
  }

  send_response();
}

struct put_obj_aio_info {
  void *data;
  void *handle;
};

static struct put_obj_aio_info pop_pending(std::list<struct put_obj_aio_info>& pending)
{
  struct put_obj_aio_info info;
  info = pending.front();
  pending.pop_front();
  return info;
}

static int wait_pending_front(std::list<struct put_obj_aio_info>& pending)
{
  struct put_obj_aio_info info = pop_pending(pending);
  int ret = rgwstore->aio_wait(info.handle);
  free(info.data);
  return ret;
}

static bool pending_has_completed(std::list<struct put_obj_aio_info>& pending)
{
  if (pending.size() == 0)
    return false;

  struct put_obj_aio_info& info = pending.front();
  return rgwstore->aio_completed(info.handle);
}

static int drain_pending(std::list<struct put_obj_aio_info>& pending)
{
  int ret = 0;
  while (!pending.empty()) {
    int r = wait_pending_front(pending);
    if (r < 0)
      ret = r;
  }
  return ret;
}

int RGWPutObj::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWPutObj::execute()
{
  bool multipart;
  string multipart_meta_obj;
  string part_num;
  list<struct put_obj_aio_info> pending;
  size_t max_chunks = RGW_MAX_PENDING_CHUNKS;
  bool created_obj = false;
  rgw_obj obj;

  ret = -EINVAL;
  if (!s->object) {
    goto done;
  } else {
    ret = get_params();
    if (ret < 0)
      goto done;

    RGWAccessControlPolicy policy;

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
      RGW_LOG(15) << "supplied_md5_b64=" << supplied_md5_b64 << dendl;
      ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
			     supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
      RGW_LOG(15) << "ceph_armor ret=" << ret << dendl;
      if (ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        ret = -ERR_INVALID_DIGEST;
        goto done;
      }

      buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
      RGW_LOG(15) << "supplied_md5=" << supplied_md5 << dendl;
    }

    MD5 hash;
    string oid;
    multipart = s->args.exists("uploadId");
    if (!multipart) {
      oid = s->object_str;
      obj.set_ns(tmp_ns);

      char buf[33];
      gen_rand_alphanumeric(buf, sizeof(buf) - 1);
      oid.append("_");
      oid.append(buf);
    } else {
      oid = s->object_str;
      string upload_id;
      url_decode(s->args.get("uploadId"), upload_id);
      RGWMPObj mp(oid, upload_id);
      multipart_meta_obj = mp.get_meta();

      url_decode(s->args.get("partNumber"), part_num);
      if (part_num.empty()) {
        ret = -EINVAL;
        goto done;
      }
      oid = mp.get_part(part_num);

      obj.set_ns(mp_ns);
    }
    obj.init(s->bucket, oid, s->object_str);
    int len;
    do {
      len = get_data();
      if (len < 0) {
        ret = len;
        goto done;
      }
      if (len > 0) {
        struct put_obj_aio_info info;
        size_t orig_size;
	// For the first call to put_obj_data, pass -1 as the offset to
	// do a write_full.
        void *handle;
        ret = rgwstore->aio_put_obj_data(s->obj_ctx, s->user.user_id, obj,
				     data,
				     ((ofs == 0) ? -1 : ofs), len, &handle);
        if (ret < 0)
          goto done_err;

        created_obj = true;

        hash.Update((unsigned char *)data, len);
        info.handle = handle;
        info.data = data;
        pending.push_back(info);
        orig_size = pending.size();
        while (pending_has_completed(pending)) {
          ret = wait_pending_front(pending);
          if (ret < 0)
            goto done_err;

        }

        /* resize window in case messages are draining too fast */
        if (orig_size - pending.size() >= max_chunks)
          max_chunks++;

        if (pending.size() > max_chunks) {
          ret = wait_pending_front(pending);
          if (ret < 0)
            goto done_err;
        }
        ofs += len;
      }
    } while ( len > 0);
    drain_pending(pending);

    if ((uint64_t)ofs != s->content_length) {
      ret = -ERR_REQUEST_TIMEOUT;
      goto done_err;
    }
    s->obj_size = ofs;

    hash.Final(m);

    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

    if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
       ret = -ERR_BAD_DIGEST;
       goto done_err;
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

    if (!multipart) {
      rgw_obj dst_obj(s->bucket, s->object_str);
      rgwstore->set_atomic(s->obj_ctx, dst_obj);
      ret = rgwstore->clone_obj(s->obj_ctx, dst_obj, 0, obj, 0, s->obj_size, NULL, attrs, RGW_OBJ_CATEGORY_MAIN);
      if (ret < 0)
        goto done_err;
      if (created_obj) {
        ret = rgwstore->delete_obj(NULL, s->user.user_id, obj, false);
        if (ret < 0)
          goto done;
      }
    } else {
      ret = rgwstore->put_obj_meta(s->obj_ctx, s->user.user_id, obj, s->obj_size, NULL, attrs, RGW_OBJ_CATEGORY_MAIN, false);
      if (ret < 0)
        goto done_err;

      bl.clear();
      RGWUploadPartInfo info;
      string p = "part.";
      p.append(part_num);
      info.num = atoi(part_num.c_str());
      info.etag = etag;
      info.size = s->obj_size;
      info.modified = ceph_clock_now(g_ceph_context);
      ::encode(info, bl);

      rgw_obj meta_obj(s->bucket, multipart_meta_obj, s->object_str, mp_ns);

      ret = rgwstore->tmap_set(meta_obj, p, bl);
    }
  }
done:
  drain_pending(pending);
  send_response();
  return;

done_err:
  if (created_obj)
    rgwstore->delete_obj(s->obj_ctx, s->user.user_id, obj);
  drain_pending(pending);
  send_response();
}

int RGWDeleteObj::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteObj::execute()
{
  ret = -EINVAL;
  rgw_obj obj(s->bucket, s->object_str);
  if (s->object) {
    rgwstore->set_atomic(s->obj_ctx, obj);
    ret = rgwstore->delete_obj(s->obj_ctx, s->user.user_id, obj);
  }

  send_response();
}

static bool parse_copy_source(const char *src, string& bucket_name, string& object)
{
  string url_src(src);
  string dec_src;

  url_decode(url_src, dec_src);
  src = dec_src.c_str();

  RGW_LOG(15) << "decoded src=" << src << dendl;

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
  RGWAccessControlPolicy src_policy;
  RGWAccessControlPolicy dest_policy;

  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  ret = dest_policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!ret)
     return -EINVAL;

  string src_bucket_name;

  ret = parse_copy_source(s->copy_source, src_bucket_name, src_object);
  if (!ret)
     return -EINVAL;

  RGWBucketInfo bucket_info;

  ret = rgw_get_bucket_info(src_bucket_name, bucket_info);
  if (ret < 0)
    return ret;

  src_bucket = bucket_info.bucket;

  /* just checking the bucket's permission */
  ret = read_acls(s, &src_policy, src_bucket, empty_str);
  if (ret < 0)
    return ret;

  if (!::verify_permission(&src_policy, s->user.user_id, s->perm_mask, RGW_PERM_READ))
    return -EACCES;

  dest_policy.encode(aclbl);

  return 0;
}


int RGWCopyObj::init_common()
{
  time_t mod_time;
  time_t unmod_time;
  time_t *mod_ptr = NULL;
  time_t *unmod_ptr = NULL;

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
  rgw_obj src_obj, dst_obj;

  ret = get_params();
  if (ret < 0)
    goto done;

  if (init_common() < 0)
    goto done;

  src_obj.init(src_bucket, src_object);
  dst_obj.init(s->bucket, s->object_str);
  rgwstore->set_atomic(s->obj_ctx, src_obj);
  rgwstore->set_atomic(s->obj_ctx, dst_obj);

  ret = rgwstore->copy_obj(s->obj_ctx, s->user.user_id,
                        dst_obj,
                        src_obj,
                        &mtime,
                        mod_ptr,
                        unmod_ptr,
                        if_match,
                        if_nomatch,
                        attrs, RGW_OBJ_CATEGORY_MAIN, &s->err);

done:
  send_response();
}

int RGWGetACLs::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_READ_ACP))
    return -EACCES;

  return 0;
}

void RGWGetACLs::execute()
{
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

static int rebuild_policy(ACLOwner *owner, RGWAccessControlPolicy& src, RGWAccessControlPolicy& dest)
{
  if (!owner)
    return -EINVAL;

  ACLOwner *requested_owner = (ACLOwner *)src.find_first("Owner");
  if (requested_owner && requested_owner->get_id().compare(owner->get_id()) != 0) {
    return -EPERM;
  }

  RGWUserInfo owner_info;
  if (rgw_get_user_info_by_uid(owner->get_id(), owner_info) < 0) {
    RGW_LOG(10) << "owner info does not exist" << dendl;
    return -EINVAL;
  }
  ACLOwner& dest_owner = dest.get_owner();
  dest_owner.set_id(owner->get_id());
  dest_owner.set_name(owner_info.display_name);

  RGW_LOG(20) << "owner id=" << owner->get_id() << dendl;
  RGW_LOG(20) << "dest owner id=" << dest.get_owner().get_id() << dendl;

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
        RGW_LOG(10) << "grant user email=" << email << dendl;
        if (rgw_get_user_info_by_email(email, grant_user) < 0) {
          RGW_LOG(10) << "grant user email not found or other error" << dendl;
          return -ERR_UNRESOLVABLE_EMAIL;
        }
        id = grant_user.user_id;
      }
    case ACL_TYPE_CANON_USER:
      {
        if (type.get_type() == ACL_TYPE_CANON_USER)
          id = src_grant->get_id();
    
        if (grant_user.user_id.empty() && rgw_get_user_info_by_uid(id, grant_user) < 0) {
          RGW_LOG(10) << "grant user does not exist:" << id << dendl;
          return -EINVAL;
        } else {
          ACLPermission& perm = src_grant->get_permission();
          new_grant.set_canon(id, grant_user.display_name, perm.get_permissions());
          grant_ok = true;
          RGW_LOG(10) << "new grant: " << new_grant.get_id() << ":" << grant_user.display_name << dendl;
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
          RGW_LOG(10) << "new grant: " << new_grant.get_id() << dendl;
        } else {
          RGW_LOG(10) << "grant group does not exist:" << group << dendl;
          return -EINVAL;
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

int RGWPutACLs::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE_ACP))
    return -EACCES;

  return 0;
}

void RGWPutACLs::execute()
{
  bufferlist bl;

  RGWAccessControlPolicy *policy = NULL;
  RGWACLXMLParser parser;
  RGWAccessControlPolicy new_policy;
  stringstream ss;
  char *orig_data = data;
  char *new_data = NULL;
  ACLOwner owner;
  rgw_obj obj;

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

  RGW_LOG(15) << "read len=" << len << " data=" << (data ? data : "") << dendl;

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

  if (g_conf->rgw_log >= 15) {
    RGW_LOG(15) << "Old AccessControlPolicy" << dendl;
    policy->to_xml(cout);
    RGW_LOG(15) << dendl;
  }

  ret = rebuild_policy(&owner, *policy, new_policy);
  if (ret < 0)
    goto done;

  if (g_conf->rgw_log >= 15) {
    RGW_LOG(15) << "New AccessControlPolicy" << dendl;
    new_policy.to_xml(cout);
    RGW_LOG(15) << dendl;
  }

  new_policy.encode(bl);
  obj.init(s->bucket, s->object_str);
  rgwstore->set_atomic(s->obj_ctx, obj);
  ret = rgwstore->set_attr(s->obj_ctx, obj, RGW_ATTR_ACL, bl);

done:
  free(orig_data);
  free(new_data);

  send_response();
}

int RGWInitMultipart::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWInitMultipart::execute()
{
  bufferlist bl;
  bufferlist aclbl;
  RGWAccessControlPolicy policy;
  map<string, bufferlist> attrs;
  rgw_obj obj;

  if (get_params() < 0)
    goto done;
  ret = -EINVAL;
  if (!s->object)
    goto done;

  ret = policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!ret) {
     ret = -EINVAL;
     goto done;
  }

  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  if (s->content_type) {
    bl.append(s->content_type, strlen(s->content_type) + 1);
    attrs[RGW_ATTR_CONTENT_TYPE] = bl;
  }

  get_request_metadata(s, attrs);

  do {
    char buf[33];
    gen_rand_alphanumeric(buf, sizeof(buf) - 1);
    upload_id = buf;

    string tmp_obj_name;
    RGWMPObj mp(s->object_str, upload_id);
    tmp_obj_name = mp.get_meta();

    obj.init(s->bucket, tmp_obj_name, s->object_str, mp_ns);
    // the meta object will be indexed with 0 size, we c
    ret = rgwstore->put_obj_meta(s->obj_ctx, s->user.user_id, obj, 0, NULL, attrs, RGW_OBJ_CATEGORY_MULTIMETA, true);
  } while (ret == -EEXIST);
done:
  send_response();
}

static int get_multiparts_info(struct req_state *s, string& meta_oid, map<uint32_t, RGWUploadPartInfo>& parts,
                               RGWAccessControlPolicy& policy, map<string, bufferlist>& attrs)
{
  void *handle;
  map<string, bufferlist> parts_map;
  map<string, bufferlist>::iterator iter;
  bufferlist header;

  rgw_obj obj(s->bucket, meta_oid, s->object_str, mp_ns);

  int ret = rgwstore->prepare_get_obj(s->obj_ctx, obj, 0, NULL, &attrs, NULL,
                                      NULL, NULL, NULL, NULL, NULL, NULL, &handle, &s->err);
  rgwstore->finish_get_obj(&handle);

  if (ret < 0)
    return ret;

  ret = rgwstore->tmap_get(obj, header, parts_map);
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
        RGW_LOG(0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
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
      RGW_LOG(0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    }
    parts[info.num] = info;
  }
  return 0;
}

int RGWCompleteMultipart::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWCompleteMultipart::execute()
{
  RGWMultiCompleteUpload *parts;
  map<int, string>::iterator iter;
  RGWMultiXMLParser parser;
  string meta_oid;
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  RGWAccessControlPolicy policy;
  map<string, bufferlist> attrs;
  off_t ofs = 0;
  MD5 hash;
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  bufferlist etag_bl;
  rgw_obj meta_obj;
  rgw_obj target_obj;
  RGWMPObj mp;
  vector<RGWCloneRangeInfo> ranges;


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
      RGW_LOG(0) << "parts num mismatch: next requested: " << iter->first << " next uploaded: " << obj_iter->first << dendl;
      ret = -ERR_INVALID_PART;
      goto done;
    }
    if (iter->second.compare(obj_iter->second.etag) != 0) {
      RGW_LOG(0) << "etag mismatch: part: " << iter->first << " etag: " << iter->second << dendl;
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
  RGW_LOG(10) << "calculated etag: " << final_etag_str << dendl;

  etag_bl.append(final_etag_str, strlen(final_etag_str) + 1);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  target_obj.init(s->bucket, s->object_str);
  rgwstore->set_atomic(s->obj_ctx, target_obj);
  ret = rgwstore->put_obj_meta(s->obj_ctx, s->user.user_id, target_obj, 0, NULL, attrs, RGW_OBJ_CATEGORY_MAIN, false);
  if (ret < 0)
    goto done;
  
  for (obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
    string oid = mp.get_part(obj_iter->second.num);
    rgw_obj src_obj(s->bucket, oid, s->object_str, mp_ns);

    RGWCloneRangeInfo range;
    range.src = src_obj;
    range.src_ofs = 0;
    range.dst_ofs = ofs;
    range.len = obj_iter->second.size;
    ranges.push_back(range);

    ofs += obj_iter->second.size;
  }
  ret = rgwstore->clone_objs(s->obj_ctx, target_obj, ranges, attrs, RGW_OBJ_CATEGORY_MAIN, NULL, true, false);
  if (ret < 0)
    goto done;

  // now erase all parts
  for (obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
    string oid = mp.get_part(obj_iter->second.num);
    rgw_obj obj(s->bucket, oid, s->object_str, mp_ns);
    rgwstore->delete_obj(s->obj_ctx, s->user.user_id, obj);
  }
  // and also remove the metadata obj
  meta_obj.init(s->bucket, meta_oid, s->object_str, mp_ns);
  rgwstore->delete_obj(s->obj_ctx, s->user.user_id, meta_obj);

done:
  send_response();
}

int RGWAbortMultipart::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWAbortMultipart::execute()
{
  ret = -EINVAL;
  string upload_id;
  string meta_oid;
  string prefix;
  url_decode(s->args.get("uploadId"), upload_id);
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  RGWAccessControlPolicy policy;
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
    rgw_obj obj(s->bucket, oid, s->object_str, mp_ns);
    ret = rgwstore->delete_obj(s->obj_ctx, s->user.user_id, obj);
    if (ret < 0 && ret != -ENOENT)
      goto done;
  }
  // and also remove the metadata obj
  meta_obj.init(s->bucket, meta_oid, s->object_str, mp_ns);
  ret = rgwstore->delete_obj(s->obj_ctx, s->user.user_id, meta_obj);
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_BUCKET;
  }
done:

  send_response();
}

int RGWListMultipart::verify_permission()
{
  if (!::verify_permission(s, RGW_PERM_READ))
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
  if (!::verify_permission(s, RGW_PERM_READ))
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
    url_decode(s->args.get("path"), path_args);
    if (!path_args.empty()) {
      if (!delimiter.empty() || !prefix.empty()) {
        ret = -EINVAL;
        goto done;
      }
      url_decode(path_args, prefix);
      delimiter="/";
    }
  }
  marker_meta = marker.get_meta();
  ret = rgwstore->list_objects(s->user.user_id, s->bucket, max_uploads, prefix, delimiter, marker_meta, objs, common_prefixes,
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

  RGWConf *conf = s->env->conf;

  if (conf->log_level >= 0) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", conf->log_level);
    g_conf->set_val("rgw_log", buf);
    g_conf->apply_changes(NULL);
  }
  if (g_conf->rgw_log >= 20) {
    char *p;
    for (int i=0; (p = fcgx->envp[i]); ++i) {
      RGW_LOG(20) << p << dendl;
    }
  }
  return 0;
}

int RGWHandler::do_read_permissions(bool only_bucket)
{
  int ret = read_acls(s, only_bucket);

  if (ret < 0) {
    RGW_LOG(10) << "read_permissions on " << s->bucket << ":" <<s->object_str << " only_bucket=" << only_bucket << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
  }

  return ret;
}

