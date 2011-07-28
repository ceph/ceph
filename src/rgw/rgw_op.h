/**
 * All operations via the rados gateway are carried out by
 * small classes known as RGWOps. This class contains a req_state
 * and each possible command is a subclass of this with a defined
 * execute() method that does whatever the subclass name implies.
 * These subclasses must be further subclassed (by interface type)
 * to provide additional virtual methods such as send_response or get_params.
 */
#ifndef CEPH_RGW_OP_H
#define CEPH_RGW_OP_H

#include <string>

#include "rgw_common.h"
#include "rgw_access.h"
#include "rgw_user.h"
#include "rgw_acl.h"

using namespace std;

struct req_state;

/** Get the HTTP request metadata */
extern void get_request_metadata(struct req_state *s, map<string, bufferlist>& attrs);
/**
 * Get the ACL for an object off of disk. If you hold the req_state, use next
 * method.
 */
extern int read_acls(RGWAccessControlPolicy *policy, string& bucket, string& object);
/** Get the ACL needed for a request off of disk.*/
extern int read_acls(struct req_state *s, bool only_bucket = false);

/**
 * Provide the base class for all ops.
 */
class RGWOp {
protected:
  struct req_state *s;
public:
  RGWOp() {}
  virtual ~RGWOp() {}

  virtual void init(struct req_state *s) {
    this->s = s;
  }
  virtual int verify_permission() = 0;
  virtual void execute() = 0;
};

class RGWGetObj : public RGWOp {
protected:
  const char *range_str;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  off_t ofs;
  uint64_t len;
  uint64_t total_len;
  off_t start;
  off_t end;
  time_t mod_time;
  time_t lastmod;
  time_t unmod_time;
  time_t *mod_ptr;
  time_t *unmod_ptr;
  map<string, bufferlist> attrs;
  char *data;
  int ret;
  bool get_data;

  int init_common();
public:
  RGWGetObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    range_str = NULL;
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    start = 0;
    ofs = 0;
    len = 0;
    total_len = 0;
    end = -1;
    mod_time = 0;
    lastmod = 0;
    unmod_time = 0;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    attrs.clear();
    data = NULL;
    ret = 0;

    /* get_data should not be initialized here! */
  }
  void set_get_data(bool get_data) {
    this->get_data = get_data;
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual int send_response(void *handle) = 0;
};

class RGWListBuckets : public RGWOp {
protected:
  int ret;
  RGWUserBuckets buckets;

public:
  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    buckets.clear();
  }
  RGWListBuckets() {}

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWListBucket : public RGWOp {
protected:
  string prefix;
  string marker; 
  string max_keys;
  string delimiter;
  int max;
  int ret;
  vector<RGWObjEnt> objs;
  map<string, bool> common_prefixes;

  string limit_opt_name;
  int default_max;
  bool is_truncated;

public:
  RGWListBucket() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    prefix.clear();
    marker.clear();
    max_keys.clear();
    delimiter.clear();
    max = 0;
    ret = 0;
    objs.clear();
    common_prefixes.clear();
    is_truncated = false;
  }
  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWStatBucket : public RGWOp {
protected:
  int ret;
  RGWBucketEnt bucket;

public:
  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    bucket.clear();
  }
  RGWStatBucket() {}
  ~RGWStatBucket() {}

  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWCreateBucket : public RGWOp {
protected:
  int ret;

public:
  RGWCreateBucket() {}

  int verify_permission();
  void execute();
  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  virtual void send_response() = 0;
};

class RGWDeleteBucket : public RGWOp {
protected:
  int ret;

public:
  RGWDeleteBucket() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWPutObj : public RGWOp {
protected:
  int ret;
  off_t ofs;
  char *data;
  const char *supplied_md5_b64;
  string etag;

public:
  RGWPutObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    ofs = 0;
    data = NULL;
    supplied_md5_b64 = NULL;
    etag = "";
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual int get_data() = 0;
  virtual void send_response() = 0;
};

class RGWDeleteObj : public RGWOp {
protected:
  int ret;

public:
  RGWDeleteObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWCopyObj : public RGWOp {
  bufferlist aclbl;
protected:
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  off_t ofs;
  off_t len;
  off_t end;
  time_t *mod_ptr;
  time_t *unmod_ptr;
  int ret;
  map<string, bufferlist> attrs;
  string src_bucket;
  string src_object;
  time_t mtime;

  int init_common();
public:
  RGWCopyObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    ofs = 0;
    len = 0;
    end = -1;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    ret = 0;
    attrs.clear();
    src_bucket.clear();
    src_object.clear();
    mtime = 0;
    aclbl.clear();
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWGetACLs : public RGWOp {
protected:
  int ret;
  string acls;

public:
  RGWGetACLs() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    acls.clear();
  }
  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWPutACLs : public RGWOp {
protected:
  int ret;
  size_t len;
  char *data;

public:
  RGWPutACLs() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    len = 0;
    data = NULL;
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWInitMultipart : public RGWOp {
protected:
  int ret;
  string upload_id;

public:
  RGWInitMultipart() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    upload_id = "";
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWCompleteMultipart : public RGWOp {
protected:
  int ret;
  string upload_id;
  string etag;
  char *data;
  int len;

public:
  RGWCompleteMultipart() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    upload_id = "";
    etag="";
    data = NULL;
    len = 0;
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWAbortMultipart : public RGWOp {
protected:
  int ret;

public:
  RGWAbortMultipart() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  int verify_permission();
  void execute();

  virtual void send_response() = 0;
};

class RGWListMultipart : public RGWOp {
protected:
  int ret;
  string upload_id;
  map<uint32_t, RGWUploadPartInfo> parts;
  int max_parts;
  int marker;
  RGWAccessControlPolicy policy;

public:
  RGWListMultipart() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    upload_id = "";
    parts.clear();
    max_parts = 1000;
    marker = 0;
    policy = RGWAccessControlPolicy();
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

#define MP_META_SUFFIX ".meta"

class RGWMPObj {
  string oid;
  string prefix;
  string meta;
  string upload_id;
public:
  RGWMPObj() {}
  RGWMPObj(string& _oid, string& _upload_id) {
    init(_oid, _upload_id);
  }
  void init(string& _oid, string& _upload_id) {
    if (_oid.empty()) {
      clear();
      return;
    }
    oid = _oid;
    upload_id = _upload_id;
    prefix = oid;
    prefix.append(".");
    prefix.append(upload_id);
    meta = prefix;
    meta.append(MP_META_SUFFIX);
  }
  string& get_meta() { return meta; }
  string get_part(int num) {
    char buf[16];
    snprintf(buf, 16, ".%d", num);
    string s = prefix;
    s.append(buf);
    return s;
  }
  string get_part(string& part) {
    string s = prefix;
    s.append(".");
    s.append(part);
    return s;
  }
  string& get_upload_id() {
    return upload_id;
  }
  string& get_key() {
    return oid;
  }
  bool from_meta(string& meta) {
    int end_pos = meta.rfind('.'); // search for ".meta"
    if (end_pos < 0)
      return false;
    int mid_pos = meta.rfind('.', end_pos - 1); // <key>.<upload_id>
    if (mid_pos < 0)
      return false;
    oid = meta.substr(0, mid_pos);
    upload_id = meta.substr(mid_pos + 1, end_pos - mid_pos - 1);
    init(oid, upload_id);
    return true;
  }
  void clear() {
    oid = "";
    prefix = "";
    meta = "";
    upload_id = "";
  }
};

struct RGWMultipartUploadEntry {
  RGWObjEnt obj;
  RGWMPObj mp;

  void clear() {
    obj.clear();
    string empty;
    mp.init(empty, empty);
  }
};

class RGWListBucketMultiparts : public RGWOp {
protected:
  string prefix;
  RGWMPObj marker; 
  RGWMultipartUploadEntry next_marker; 
  int max_uploads;
  string delimiter;
  int ret;
  vector<RGWMultipartUploadEntry> uploads;
  map<string, bool> common_prefixes;
  bool is_truncated;
  int default_max;

public:
  RGWListBucketMultiparts() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    prefix.clear();
    marker.clear();
    next_marker.clear();
    max_uploads = default_max;
    delimiter.clear();
    max_uploads = default_max;
    ret = 0;
    uploads.clear();
    is_truncated = false;
    common_prefixes.clear();
  }
  int verify_permission();
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWHandler {
protected:
  struct req_state *s;

  int do_read_permissions(bool only_bucket);
public:
  RGWHandler() {}
  virtual ~RGWHandler() {}
  virtual int init(struct req_state *_s, FCGX_Request *fcgx);

  virtual RGWOp *get_op() = 0;
  virtual void put_op(RGWOp *op) = 0;
  virtual int read_permissions() = 0;
  virtual int authorize() = 0;
};

#endif
