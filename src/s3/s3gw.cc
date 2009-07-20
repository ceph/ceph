#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/md5.h>

#include "fcgiapp.h"

#include "s3access.h"
#include "s3acl.h"
#include "s3user.h"
#include "s3op.h"
#include "s3rest.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "include/base64.h"
#include "common/BackTrace.h"

using namespace std;


#define CGI_PRINTF(stream, format, ...) do { \
   fprintf(stderr, format, __VA_ARGS__); \
   FCGX_FPrintF(stream, format, __VA_ARGS__); \
} while (0)

static void get_canon_amz_hdr(struct req_state *s, string& dest)
{
  dest = "";
  map<string, string>::iterator iter;
  for (iter = s->x_amz_map.begin(); iter != s->x_amz_map.end(); ++iter) {
    dest.append(iter->first);
    dest.append(":");
    dest.append(iter->second);
    dest.append("\n");
  }
}

static void get_canon_resource(struct req_state *s, string& dest)
{
  if (s->host_bucket) {
    dest = "/";
    dest.append(s->host_bucket);
  }

  dest.append(s->path_name_url.c_str());

  string& sub = s->args.get_sub_resource();
  if (sub.size() > 0) {
    dest.append("?");
    dest.append(sub);
  }
}

static void get_auth_header(struct req_state *s, string& dest, bool qsr)
{
  dest = "";
  if (s->method)
    dest = s->method;
  dest.append("\n");
  
  const char *md5 = FCGX_GetParam("HTTP_CONTENT_MD5", s->fcgx->envp);
  if (md5)
    dest.append(md5);
  dest.append("\n");

  const char *type = FCGX_GetParam("CONTENT_TYPE", s->fcgx->envp);
  if (type)
    dest.append(type);
  dest.append("\n");

  string date;
  if (qsr) {
    date = s->args.get("Expires");
  } else {
    const char *str = FCGX_GetParam("HTTP_DATE", s->fcgx->envp);
    if (str)
      date = str;
  }

  if (date.size())
      dest.append(date);
  dest.append("\n");

  string canon_amz_hdr;
  get_canon_amz_hdr(s, canon_amz_hdr);
  dest.append(canon_amz_hdr);

  string canon_resource;
  get_canon_resource(s, canon_resource);
  dest.append(canon_resource);
}

static void calc_hmac_sha1(const char *key, int key_len,
                           const char *msg, int msg_len,
                           char *dest, int *len) /* dest should be large enough to hold result */
{
  char hex_str[128];
  unsigned char *result = HMAC(EVP_sha1(), key, key_len, (const unsigned char *)msg,
                               msg_len, (unsigned char *)dest, (unsigned int *)len);

  buf_to_hex(result, *len, hex_str);

  cerr << "hmac=" << hex_str << std::endl;
}

static void do_list_buckets(struct req_state *s)
{
  S3ListBuckets_REST op(s);
  op.execute();
}

static bool verify_signature(struct req_state *s)
{
  bool qsr = false;
  string auth_id;
  string auth_sign;

  if (!s->http_auth || !(*s->http_auth)) {
    auth_id = s->args.get("AWSAccessKeyId");
    if (auth_id.size()) {
      url_decode(s->args.get("Signature"), auth_sign);

      string date = s->args.get("Expires");
      time_t exp = atoll(date.c_str());
      time_t now;
      time(&now);
      if (now >= exp)
        return false;

      qsr = true;
    } else {
      /* anonymous access */
      s3_get_anon_user(s->user);
      return true;
    }
  } else {
    if (strncmp(s->http_auth, "AWS ", 4))
      return false;
    string auth_str(s->http_auth + 4);
    int pos = auth_str.find(':');
    if (pos < 0)
      return false;

    auth_id = auth_str.substr(0, pos);
    auth_sign = auth_str.substr(pos + 1);
  }

  /* first get the user info */
  if (s3_get_user_info(auth_id, s->user) < 0) {
    cerr << "error reading user info, uid=" << auth_id << " can't authenticate" << std::endl;
    return false;
  }

  /* now verify signature */
   
  string auth_hdr;
  get_auth_header(s, auth_hdr, qsr);
  cerr << "auth_hdr:" << std::endl << auth_hdr << std::endl;

  const char *key = s->user.secret_key.c_str();
  int key_len = strlen(key);

  char hmac_sha1[EVP_MAX_MD_SIZE];
  int len;
  calc_hmac_sha1(key, key_len, auth_hdr.c_str(), auth_hdr.size(), hmac_sha1, &len);

  char b64[64]; /* 64 is really enough */
  int ret = encode_base64(hmac_sha1, len, b64, sizeof(b64));
  if (ret < 0) {
    cerr << "encode_base64 failed" << std::endl;
    return false;
  }

  cerr << "b64=" << b64 << std::endl;
  cerr << "auth_sign=" << auth_sign << std::endl;
  cerr << "compare=" << auth_sign.compare(b64) << std::endl;
  return (auth_sign.compare(b64) == 0);
}

static void get_object(struct req_state *s, bool get_data)
{
  S3GetObj_REST op(s, get_data);

  op.execute();
}

static void do_write_acls(struct req_state *s)
{
  S3PutACLs_REST op(s);
  op.execute();
}

static void get_acls(struct req_state *s)
{
  S3GetACLs_REST op(s);
  op.execute();
}

static bool is_acl_op(struct req_state *s)
{
  return s->args.exists("acl");
}

static void do_retrieve_objects(struct req_state *s, bool get_data)
{
  if (is_acl_op(s)) {
    get_acls(s);
    return;
  }

  if (s->object) {
    get_object(s, get_data);
    return;
  } else if (!s->bucket) {
    return;
  }

  S3ListBucket_REST op(s);
  op.execute();
}

static void do_create_bucket(struct req_state *s)
{
  S3CreateBucket_REST op(s);
  op.execute();
}

static void do_copy_object(struct req_state *s)
{
  S3CopyObj_REST op(s);
  op.execute();
}

static void do_create_object(struct req_state *s)
{
  S3PutObj_REST op(s);
  op.execute();
}

static void do_delete_bucket(struct req_state *s)
{
  S3DeleteBucket_REST op(s);
  op.execute();
}

static void do_delete_object(struct req_state *s)
{
  S3DeleteObj_REST op(s);
  op.execute();
}

static void do_retrieve(struct req_state *s, bool get_data)
{
  if (s->bucket) {
    if (is_acl_op(s)) {
      get_acls(s);
      return;
    }

    do_retrieve_objects(s, get_data);
  } else {
    do_list_buckets(s);
  }
}

static void do_create(struct req_state *s)
{
  if (is_acl_op(s)) {
    do_write_acls(s);
  } else if (s->object) {
    if (!s->copy_source)
      do_create_object(s);
    else
      do_copy_object(s);
  } else if (s->bucket) {
    do_create_bucket(s);
  } else
    return;
}

static void do_delete(struct req_state *s)
{
  if (s->object)
    do_delete_object(s);
  else if (s->bucket)
    do_delete_bucket(s);
  else
    return;
}

static sighandler_t sighandler;

static void godown(int signum)
{
  BackTrace bt(0);
  bt.print(cerr);

  signal(SIGSEGV, sighandler);
}

int read_permissions(struct req_state *s)
{
  bool only_bucket;

  switch (s->op) {
  case OP_HEAD:
  case OP_GET:
    only_bucket = false;
    break;
  case OP_PUT:
    /* is it a 'create bucket' request? */
    if (s->object_str.size() == 0)
      return 0;
    if (is_acl_op(s)) {
      only_bucket = false;
      break;
    }
  case OP_DELETE:
    only_bucket = true;
    break;
  default:
    return -EINVAL;
  }

  int ret = read_acls(s, only_bucket);

  if (ret < 0)
    cerr << "read_permissions on " << s->bucket_str << ":" <<s->object_str << " only_bucket=" << only_bucket << " ret=" << ret << std::endl;

  return ret;
}

int main(int argc, char *argv[])
{
  struct req_state s;
  struct fcgx_state fcgx;
  S3Handler_REST s3handler;

  if (!S3Access::init_storage_provider("rados", argc, argv)) {
    cerr << "couldn't init storage provider" << std::endl;
  }

  sighandler = signal(SIGSEGV, godown);

  while (FCGX_Accept(&fcgx.in, &fcgx.out, &fcgx.err, &fcgx.envp) >= 0) 
  {
    s3handler.init_state(&s, &fcgx);

    int ret = read_acls(&s);
    if (ret < 0) {
      switch (ret) {
      case -ENOENT:
        break;
      default:
        cerr << "could not read acls" << " ret=" << ret << std::endl;
        abort_early(&s, -EPERM);
        continue;
      }
    }
    ret = verify_signature(&s);
    if (!ret) {
      cerr << "signature DOESN'T match" << std::endl;
      abort_early(&s, -EPERM);
      continue;
    }

    ret = read_permissions(&s);
    if (ret < 0) {
      abort_early(&s, ret);
      continue;
    }

    switch (s.op) {
    case OP_GET:
      do_retrieve(&s, true);
      break;
    case OP_PUT:
      do_create(&s);
      break;
    case OP_DELETE:
      do_delete(&s);
      break;
    case OP_HEAD:
      do_retrieve(&s, false);
      break;
    default:
      abort_early(&s, -EACCES);
      break;
    }
  }
  return 0;
}

