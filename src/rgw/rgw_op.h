// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/**
 * All operations via the rados gateway are carried out by
 * small classes known as RGWOps. This class contains a req_state
 * and each possible command is a subclass of this with a defined
 * execute() method that does whatever the subclass name implies.
 * These subclasses must be further subclassed (by interface type)
 * to provide additional virtual methods such as send_response or get_params.
 */

#pragma once

#include <limits.h>

#include <array>
#include <memory>
#include <span>
#include <string>
#include <set>
#include <map>
#include <vector>

#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/function.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/asio/deadline_timer.hpp>

#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"

#include "rgw_cksum.h"
#include "rgw_common.h"
#include "rgw_dmclock.h"
#include "rgw_sal.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_acl.h"
#include "rgw_cors.h"
#include "rgw_quota.h"
#include "rgw_putobj.h"
#include "rgw_sal.h"
#include "rgw_compression_types.h"
#include "rgw_log.h"

#include "rgw_lc.h"
#include "rgw_tag.h"
#include "rgw_object_lock.h"
#include "cls/rgw/cls_rgw_client.h"
#include "rgw_public_access.h"
#include "rgw_bucket_encryption.h"
#include "rgw_tracer.h"

#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"

#include "include/ceph_assert.h"

struct req_state;
class RGWOp;
class RGWRados;
class RGWMultiCompleteUpload;
class RGWPutObj_Torrent;

namespace rgw::auth::registry { class StrategyRegistry; }

int rgw_forward_request_to_master(const DoutPrefixProvider* dpp,
                                  const rgw::SiteConfig& site,
                                  const rgw_owner& effective_owner,
                                  bufferlist* indata, JSONParser* jp,
                                  req_info& req, optional_yield y);

int rgw_op_get_bucket_policy_from_attr(const DoutPrefixProvider *dpp,
                                       CephContext *cct,
                                       rgw::sal::Driver* driver,
                                       const rgw_user& bucket_owner,
                                       std::map<std::string, bufferlist>& bucket_attrs,
                                       RGWAccessControlPolicy& policy,
                                       optional_yield y);

class RGWHandler {
protected:
  rgw::sal::Driver* driver{nullptr};
  req_state *s{nullptr};

  int do_init_permissions(const DoutPrefixProvider *dpp, optional_yield y);
  int do_read_permissions(RGWOp* op, bool only_bucket, optional_yield y);

public:
  RGWHandler() {}
  virtual ~RGWHandler();

  virtual int init(rgw::sal::Driver* driver,
                   req_state* _s,
                   rgw::io::BasicClient* cio);

  virtual int init_permissions(RGWOp*, optional_yield y) {
    return 0;
  }

  virtual int retarget(RGWOp* op, RGWOp** new_op, optional_yield) {
    *new_op = op;
    return 0;
  }

  virtual int read_permissions(RGWOp* op, optional_yield y) = 0;
  virtual int authorize(const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int postauth_init(optional_yield y) = 0;
  virtual int error_handler(int err_no, std::string* error_content, optional_yield y);
  virtual void dump(const std::string& code, const std::string& message) const {}

  virtual bool supports_quota() {
    return true;
  }
};



void rgw_bucket_object_pre_exec(req_state *s);

namespace dmc = rgw::dmclock;

std::tuple<int, bufferlist > rgw_rest_read_all_input(req_state *s,
                                        const uint64_t max_len,
                                        const bool allow_chunked=true);

template <class T>
int rgw_rest_get_json_input(CephContext *cct, req_state *s, T& out,
			    uint64_t max_len, bool *empty)
{
  if (empty)
    *empty = false;

  int rv = 0;
  bufferlist data;
  std::tie(rv, data) = rgw_rest_read_all_input(s, max_len);
  if (rv < 0) {
    return rv;
  }

  if (!data.length()) {
    if (empty) {
      *empty = true;
    }

    return -EINVAL;
  }

  JSONParser parser;

  if (!parser.parse(data.c_str(), data.length())) {
    return -EINVAL;
  }

  try {
      decode_json_obj(out, &parser);
  } catch (JSONDecoder::err& e) {
      return -EINVAL;
  }

  return 0;
}

// So! Now and then when we try to update bucket information, the
// bucket has changed during the course of the operation. (Or we have
// a cache consistency problem that Watch/Notify isn't ruling out
// completely.)
//
// When this happens, we need to update the bucket info and try
// again. We have, however, to try the right *part* again.  We can't
// simply re-send, since that will obliterate the previous update.
//
// Thus, callers of this function should include everything that
// merges information to be changed into the bucket information as
// well as the call to set it.
//
// The called function must return an integer, negative on error. In
// general, they should just return op_ret.
template<typename F>
int retry_raced_bucket_write(const DoutPrefixProvider *dpp,
                             rgw::sal::Bucket *b,
                             const F &f,
                             optional_yield y) {
  auto r = f();
  for (auto i = 0u; i < 15u && r == -ECANCELED; ++i) {
    r = b->try_refresh_info(dpp, nullptr, y);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}

/**
 * Provide the base class for all ops.
 */
class RGWOp : public DoutPrefixProvider {
protected:
  req_state *s;
  RGWHandler *dialect_handler;
  rgw::sal::Driver* driver;
  RGWCORSConfiguration bucket_cors;
  bool cors_exist;
  RGWQuota quota;
  int op_ret;
  int do_aws4_auth_completion();
  bool init_called = false;

  virtual int init_quota();

  std::tuple<int, bufferlist> read_all_input(req_state *s,
                                             const uint64_t max_len,
                                             const bool allow_chunked=true) {

    int rv = 0;
    bufferlist data;
    std::tie(rv, data) = rgw_rest_read_all_input(s, max_len);
    if (rv >= 0) {
      do_aws4_auth_completion();
    }

    return std::make_tuple(rv, std::move(data));
  }

  template <class T>
  int get_json_input(CephContext *cct, req_state *s, T& out,
                     uint64_t max_len, bool *empty) {
    int r = rgw_rest_get_json_input(cct, s, out, max_len, empty);
    if (r >= 0) {
      do_aws4_auth_completion();
    }
    return r;
  }

public:
  RGWOp()
    : s(nullptr),
      dialect_handler(nullptr),
      driver(nullptr),
      cors_exist(false),
      op_ret(0) {
  }

  virtual ~RGWOp() override;

  int get_ret() const { return op_ret; }

  virtual int init_processing(optional_yield y) {
    if (dialect_handler->supports_quota()) {
      op_ret = init_quota();
      if (op_ret < 0)
        return op_ret;
    }

    return 0;
  }

  virtual void init(rgw::sal::Driver* driver, req_state *s, RGWHandler *dialect_handler) {
    if (init_called) return;
    this->driver = driver;
    init_called = true;
    this->s = s;
    this->dialect_handler = dialect_handler;
  }
  int read_bucket_cors();
  bool generate_cors_headers(std::string& origin, std::string& method, std::string& headers, std::string& exp_headers, unsigned *max_age);

  virtual int verify_params() { return 0; }
  virtual bool prefetch_data() { return false; }

  /* Authenticate requester -- verify its identity.
   *
   * NOTE: typically the procedure is common across all operations of the same
   * dialect (S3, Swift API). However, there are significant exceptions in
   * both APIs: browser uploads, /info and OPTIONS handlers. All of them use
   * different, specific authentication schema driving the need for per-op
   * authentication. The alternative is to duplicate parts of the method-
   * dispatch logic in RGWHandler::authorize() and pollute it with a lot
   * of special cases. */
  virtual int verify_requester(const rgw::auth::StrategyRegistry& auth_registry, optional_yield y) {
    /* TODO(rzarzynski): rename RGWHandler::authorize to generic_authenticate. */
    return dialect_handler->authorize(this, y);
  }
  virtual int verify_permission(optional_yield y) = 0;
  virtual int verify_op_mask();
  virtual void pre_exec() {}
  virtual void execute(optional_yield y) = 0;
  virtual void send_response() {}
  virtual void complete() {
    send_response();
  }
  virtual const char* name() const = 0;
  virtual RGWOpType get_type() { return RGW_OP_UNKNOWN; }

  virtual uint32_t op_mask() { return 0; }

  virtual int error_handler(int err_no, std::string *error_content, optional_yield y);

  // implements DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext* get_cct() const override { return s->cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }

  virtual dmc::client_id dmclock_client() { return dmc::client_id::metadata; }
  virtual dmc::Cost dmclock_cost() { return 1; }
  virtual void write_ops_log_entry(rgw_log_entry& entry) const {};
};

class RGWDefaultResponseOp : public RGWOp {
public:
  void send_response() override;
};

class RGWGetObj_Filter : public RGWGetDataCB
{
protected:
  RGWGetObj_Filter *next{nullptr};
public:
  RGWGetObj_Filter() {}
  explicit RGWGetObj_Filter(RGWGetObj_Filter *next): next(next) {}
  ~RGWGetObj_Filter() override {}
  /**
   * Passes data through filter.
   * Filter can modify content of bl.
   * When bl_len == 0 , it means 'flush
   */
  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    if (next) {
      return next->handle_data(bl, bl_ofs, bl_len);
    }
    return 0;
  }
  /**
   * Flushes any cached data. Used by RGWGetObjFilter.
   * Return logic same as handle_data.
   */
  virtual int flush() {
    if (next) {
      return next->flush();
    }
    return 0;
  }
  /**
   * Allows filter to extend range required for successful filtering
   */
  virtual int fixup_range(off_t& ofs, off_t& end) {
    if (next) {
      return next->fixup_range(ofs, end);
    }
    return 0;
  }
};

class RGWGetObj : public RGWOp {
protected:
  const char *range_str;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  uint32_t mod_zone_id;
  uint64_t mod_pg_ver;
  off_t ofs;
  uint64_t total_len;
  off_t start;
  off_t end;
  ceph::real_time mod_time;
  ceph::real_time lastmod;
  ceph::real_time unmod_time;
  ceph::real_time *mod_ptr;
  ceph::real_time *unmod_ptr;
  rgw::sal::Attrs attrs;
  bool get_torrent = false;
  bool get_data;
  bool partial_content;
  bool ignore_invalid_range;
  bool range_parsed;
  bool skip_manifest;
  bool skip_decrypt{false};
  bool sync_cloudtiered{false};
  utime_t gc_invalidate_time;
  bool is_slo;
  std::string lo_etag;
  bool rgwx_stat; /* extended rgw stat operation */
  std::string version_id;
  rgw_zone_set_entry dst_zone_trace;

  // compression attrs
  RGWCompressionInfo cs_info;
  off_t first_block, last_block;
  off_t q_ofs, q_len;
  bool first_data;
  uint64_t cur_ofs;
  bufferlist waiting;
  uint64_t action = 0;

  bool get_retention;
  bool get_legal_hold;

  // optional partNumber param for s3
  std::optional<int> multipart_part_num;
  // PartsCount response when partNumber is specified
  std::optional<int> multipart_parts_count;

  int init_common();
public:
  RGWGetObj() {
    range_str = NULL;
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    mod_zone_id = 0;
    mod_pg_ver = 0;
    start = 0;
    ofs = 0;
    total_len = 0;
    end = -1;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    get_data = false;
    partial_content = false;
    range_parsed = false;
    skip_manifest = false;
    is_slo = false;
    first_block = 0;
    last_block = 0;
    q_ofs = 0;
    q_len = 0;
    first_data = true;
    cur_ofs = 0;
    get_retention = false;
    get_legal_hold = false;
 }

  bool prefetch_data() override;

  void set_get_data(bool get_data) {
    this->get_data = get_data;
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  int parse_range();
  int read_user_manifest_part(
    rgw::sal::Bucket* bucket,
    const rgw_bucket_dir_entry& ent,
    const RGWAccessControlPolicy& bucket_acl,
    const boost::optional<rgw::IAM::Policy>& bucket_policy,
    const off_t start_ofs,
    const off_t end_ofs,
    bool swift_slo);
  int handle_user_manifest(const char *prefix, optional_yield y);
  int handle_slo_manifest(bufferlist& bl, optional_yield y);

  int get_data_cb(bufferlist& bl, off_t ofs, off_t len);

  virtual int get_params(optional_yield y) = 0;
  virtual int send_response_data_error(optional_yield y) = 0;
  virtual int send_response_data(bufferlist& bl, off_t ofs, off_t len) = 0;

  const char* name() const override { return "get_obj"; }
  RGWOpType get_type() override { return RGW_OP_GET_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual bool need_object_expiration() { return false; }
  /**
   * calculates filter used to decrypt RGW objects data
   */
  virtual int get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter>* filter, RGWGetObj_Filter* cb, bufferlist* manifest_bl) {
    *filter = nullptr;
    return 0;
  }

  // get lua script to run as a "get object" filter
  int get_lua_filter(std::unique_ptr<RGWGetObj_Filter>* filter,
      RGWGetObj_Filter* cb);

  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

class RGWGetObj_CB : public RGWGetObj_Filter
{
  RGWGetObj *op;
public:
  explicit RGWGetObj_CB(RGWGetObj *_op) : op(_op) {}
  ~RGWGetObj_CB() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    return op->get_data_cb(bl, bl_ofs, bl_len);
  }
};

class RGWGetObjTags : public RGWOp {
 protected:
  bufferlist tags_bl;
  bool has_tags{false};
 public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void pre_exec() override;

  virtual void send_response_data(bufferlist& bl) = 0;
  const char* name() const override { return "get_obj_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  RGWOpType get_type() override { return RGW_OP_GET_OBJ_TAGGING; }

};

class RGWPutObjTags : public RGWOp {
 protected:
  bufferlist tags_bl;
 public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  virtual void send_response() override = 0;
  virtual int get_params(optional_yield y) = 0;
  const char* name() const override { return "put_obj_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ_TAGGING; }

};

class RGWDeleteObjTags: public RGWOp {
 public:
  void pre_exec() override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  const char* name() const override { return "delete_obj_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OBJ_TAGGING;}
};

class RGWGetBucketTags : public RGWOp {
protected:
  bufferlist tags_bl;
  bool has_tags{false};
public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void pre_exec() override;

  virtual void send_response_data(bufferlist& bl) = 0;
  const char* name() const override { return "get_bucket_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_TAGGING; }
};

class RGWPutBucketTags : public RGWOp {
protected:
  bufferlist tags_bl;
  bufferlist in_data;
public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  virtual void send_response() override = 0;
  virtual int get_params(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  const char* name() const override { return "put_bucket_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_TAGGING; }
};

class RGWDeleteBucketTags : public RGWOp {
public:
  void pre_exec() override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  const char* name() const override { return "delete_bucket_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_TAGGING;}
};

struct rgw_sync_policy_group;

class RGWGetBucketReplication : public RGWOp {
public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void pre_exec() override;

  virtual void send_response_data() = 0;
  const char* name() const override { return "get_bucket_replication"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_REPLICATION; }
};

class RGWPutBucketReplication : public RGWOp {
protected:
  bufferlist in_data;
  std::vector<rgw_sync_policy_group> sync_policy_groups;
public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  virtual void send_response() override = 0;
  virtual int get_params(optional_yield y) = 0;
  const char* name() const override { return "put_bucket_replication"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_REPLICATION; }
};

class RGWDeleteBucketReplication : public RGWOp {
protected:
  virtual void update_sync_policy(rgw_sync_policy_info *policy) = 0;
public:
  void pre_exec() override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  const char* name() const override { return "delete_bucket_replication"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_REPLICATION;}
};

class RGWBulkDelete : public RGWOp {
public:
  struct acct_path_t {
    std::string bucket_name;
    rgw_obj_key obj_key;
  };

  struct fail_desc_t {
    int err;
    acct_path_t path;
  };

  class Deleter {
  protected:
    const DoutPrefixProvider * dpp;
    unsigned int num_deleted;
    unsigned int num_unfound;
    std::list<fail_desc_t> failures;

    rgw::sal::Driver*  const driver;
    req_state * const s;

  public:
    Deleter(const DoutPrefixProvider* dpp, rgw::sal::Driver*  const str, req_state * const s)
      : dpp(dpp),
        num_deleted(0),
        num_unfound(0),
        driver(str),
        s(s) {
    }

    unsigned int get_num_deleted() const {
      return num_deleted;
    }

    unsigned int get_num_unfound() const {
      return num_unfound;
    }

    const std::list<fail_desc_t> get_failures() const {
      return failures;
    }

    bool verify_permission(RGWBucketInfo& binfo,
                           std::map<std::string, bufferlist>& battrs,
                           ACLOwner& bucket_owner /* out */,
			   optional_yield y);
    bool delete_single(const acct_path_t& path, optional_yield y);
    bool delete_chunk(const std::list<acct_path_t>& paths, optional_yield y);
  };
  /* End of Deleter subclass */

  static const size_t MAX_CHUNK_ENTRIES = 1024;

protected:
  std::unique_ptr<Deleter> deleter;

public:
  RGWBulkDelete()
    : deleter(nullptr) {
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_data(std::list<acct_path_t>& items,
                       bool * is_truncated) = 0;
  void send_response() override = 0;

  const char* name() const override { return "bulk_delete"; }
  RGWOpType get_type() override { return RGW_OP_BULK_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

inline std::ostream& operator<<(std::ostream& out, const RGWBulkDelete::acct_path_t &o) {
  return out << o.bucket_name << "/" << o.obj_key;
}


class RGWBulkUploadOp : public RGWOp {
protected:
  class fail_desc_t {
  public:
    fail_desc_t(const int err, std::string path)
      : err(err),
        path(std::move(path)) {
    }

    const int err;
    const std::string path;
  };

  static constexpr std::array<int, 2> terminal_errors = {
    { -EACCES, -EPERM }
  };

  /* FIXME:  boost::container::small_vector<fail_desc_t, 4> failures; */
  std::vector<fail_desc_t> failures;
  size_t num_created;

  class StreamGetter;
  class DecoratedStreamGetter;
  class AlignedStreamGetter;

  virtual std::unique_ptr<StreamGetter> create_stream() = 0;
  virtual void send_response() override = 0;

  boost::optional<std::pair<std::string, rgw_obj_key>>
  parse_path(const std::string_view& path);

  std::pair<std::string, std::string>
  handle_upload_path(req_state *s);

  bool handle_file_verify_permission(RGWBucketInfo& binfo,
				     const rgw_obj& obj,
				     std::map<std::string, ceph::bufferlist>& battrs,
                                     ACLOwner& bucket_owner /* out */,
				     optional_yield y);
  int handle_file(std::string_view path,
                  size_t size,
                  AlignedStreamGetter& body,
		  optional_yield y);

  int handle_dir_verify_permission(optional_yield y);
  int handle_dir(std::string_view path, optional_yield y);

public:
  RGWBulkUploadOp()
    : num_created(0) {
  }

  void init(rgw::sal::Driver* const driver,
            req_state* const s,
            RGWHandler* const h) override;

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  const char* name() const override { return "bulk_upload"; }

  RGWOpType get_type() override {
    return RGW_OP_BULK_UPLOAD;
  }

  uint32_t op_mask() override {
    return RGW_OP_TYPE_WRITE;
  }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
}; /* RGWBulkUploadOp */


class RGWBulkUploadOp::StreamGetter {
public:
  StreamGetter() = default;
  virtual ~StreamGetter() = default;

  virtual ssize_t get_at_most(size_t want, ceph::bufferlist& dst) = 0;
  virtual ssize_t get_exactly(size_t want, ceph::bufferlist& dst) = 0;
}; /* End of nested subclass StreamGetter */


class RGWBulkUploadOp::DecoratedStreamGetter : public StreamGetter {
  StreamGetter& decoratee;

protected:
  StreamGetter& get_decoratee() {
    return decoratee;
  }

public:
  explicit DecoratedStreamGetter(StreamGetter& decoratee)
    : decoratee(decoratee) {
  }
  virtual ~DecoratedStreamGetter() = default;

  ssize_t get_at_most(const size_t want, ceph::bufferlist& dst) override {
    return get_decoratee().get_at_most(want, dst);
  }

  ssize_t get_exactly(const size_t want, ceph::bufferlist& dst) override {
    return get_decoratee().get_exactly(want, dst);
  }
}; /* RGWBulkUploadOp::DecoratedStreamGetter */


class RGWBulkUploadOp::AlignedStreamGetter
  : public RGWBulkUploadOp::DecoratedStreamGetter {
  size_t position;
  size_t length;
  size_t alignment;

public:
  template <typename U>
  AlignedStreamGetter(const size_t position,
                      const size_t length,
                      const size_t alignment,
                      U&& decoratee)
    : DecoratedStreamGetter(std::forward<U>(decoratee)),
      position(position),
      length(length),
      alignment(alignment) {
  }
  virtual ~AlignedStreamGetter();
  ssize_t get_at_most(size_t want, ceph::bufferlist& dst) override;
  ssize_t get_exactly(size_t want, ceph::bufferlist& dst) override;
}; /* RGWBulkUploadOp::AlignedStreamGetter */


struct RGWUsageStats {
  uint64_t bytes_used = 0;
  uint64_t bytes_used_rounded = 0;
  uint64_t buckets_count = 0;
  uint64_t objects_count = 0;
};

#define RGW_LIST_BUCKETS_LIMIT_MAX 10000

class RGWListBuckets : public RGWOp {
protected:
  bool sent_data;
  std::string marker;
  std::string end_marker;
  int64_t limit;
  uint64_t limit_max;
  bool is_truncated;

  RGWUsageStats global_stats;
  std::map<std::string, RGWUsageStats> policies_stats;

  virtual uint64_t get_default_max() const {
    return 1000;
  }

public:
  RGWListBuckets()
    : sent_data(false),
      limit(RGW_LIST_BUCKETS_LIMIT_MAX),
      limit_max(RGW_LIST_BUCKETS_LIMIT_MAX),
      is_truncated(false) {
  }

  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  virtual void handle_listing_chunk(std::span<RGWBucketEnt> buckets) {
    /* The default implementation, used by e.g. S3, just generates a new
     * part of listing and sends it client immediately. Swift can behave
     * differently: when the reverse option is requested, all incoming
     * instances of RGWBucketList are buffered and finally reversed. */
    return send_response_data(buckets);
  }
  virtual void send_response_begin(bool has_buckets) = 0;
  virtual void send_response_data(std::span<const RGWBucketEnt> buckets) = 0;
  virtual void send_response_end() = 0;
  void send_response() override {}

  virtual bool should_get_stats() { return false; }
  virtual bool supports_account_metadata() { return false; }

  const char* name() const override { return "list_buckets"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKETS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
}; // class RGWListBuckets

class RGWGetUsage : public RGWOp {
protected:
  bool sent_data;
  std::string start_date;
  std::string end_date;
  int show_log_entries;
  int show_log_sum;
  std::map<std::string, bool> categories;
  std::map<rgw_user_bucket, rgw_usage_log_entry> usage;
  std::map<std::string, rgw_usage_log_entry> summary_map;
  std::map<std::string, bucket_meta_entry> buckets_usage;
  cls_user_header header;
  RGWStorageStats stats;
public:
  RGWGetUsage() : sent_data(false), show_log_entries(true), show_log_sum(true){
  }

  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override {}

  virtual bool should_get_stats() { return false; }

  const char* name() const override { return "get_self_usage"; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWStatAccount : public RGWOp {
protected:
  RGWUsageStats global_stats;
  std::map<std::string, RGWUsageStats> policies_stats;

public:
  RGWStatAccount() = default;

  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "stat_account"; }
  RGWOpType get_type() override { return RGW_OP_STAT_ACCOUNT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWListBucket : public RGWOp {
protected:
  std::string prefix;
  rgw_obj_key marker;
  rgw_obj_key next_marker;
  rgw_obj_key end_marker;
  std::string max_keys;
  std::string delimiter;
  std::string encoding_type;
  bool list_versions{false};
  int max{0};
  std::vector<rgw_bucket_dir_entry> objs;
  std::map<std::string, bool> common_prefixes;
  std::optional<RGWStorageStats> stats; // initialized if need_container_stats()

  int default_max{0};
  bool is_truncated{false};
  bool allow_unordered{false};

  int shard_id{-1};

  int parse_max_keys();

public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void init(rgw::sal::Driver* driver, req_state *s, RGWHandler *h) override {
    RGWOp::init(driver, s, h);
  }
  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "list_bucket"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual bool need_container_stats() { return false; }
};

class RGWGetBucketLogging : public RGWOp {
public:
  RGWGetBucketLogging() {}
  int verify_permission(optional_yield y) override;
  void execute(optional_yield) override { }

  void send_response() override = 0;
  const char* name() const override { return "get_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWGetBucketLocation : public RGWOp {
public:
  RGWGetBucketLocation() {}
  ~RGWGetBucketLocation() override {}
  int verify_permission(optional_yield y) override;
  void execute(optional_yield) override { }

  void send_response() override = 0;
  const char* name() const override { return "get_bucket_location"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_LOCATION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWGetBucketVersioning : public RGWOp {
protected:
  bool versioned{false};
  bool versioning_enabled{false};
  bool mfa_enabled{false};
public:
  RGWGetBucketVersioning() = default;

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "get_bucket_versioning"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_VERSIONING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

enum BucketVersionStatus {
  VersioningStatusInvalid = -1,
  VersioningNotChanged = 0,
  VersioningEnabled = 1,
  VersioningSuspended =2,
};

class RGWSetBucketVersioning : public RGWOp {
protected:
  int versioning_status;
  bool mfa_set_status{false};
  bool mfa_status{false};
  bufferlist in_data;
public:
  RGWSetBucketVersioning() : versioning_status(VersioningNotChanged) {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) { return 0; }

  void send_response() override = 0;
  const char* name() const override { return "set_bucket_versioning"; }
  RGWOpType get_type() override { return RGW_OP_SET_BUCKET_VERSIONING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketWebsite : public RGWOp {
public:
  RGWGetBucketWebsite() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "get_bucket_website"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_WEBSITE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWSetBucketWebsite : public RGWOp {
protected:
  bufferlist in_data;
  RGWBucketWebsiteConf website_conf;
public:
  RGWSetBucketWebsite() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) { return 0; }

  void send_response() override = 0;
  const char* name() const override { return "set_bucket_website"; }
  RGWOpType get_type() override { return RGW_OP_SET_BUCKET_WEBSITE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteBucketWebsite : public RGWOp {
public:
  RGWDeleteBucketWebsite() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "delete_bucket_website"; }
  RGWOpType get_type() override { return RGW_OP_SET_BUCKET_WEBSITE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWStatBucket : public RGWOp {
protected:
  std::unique_ptr<rgw::sal::Bucket> bucket;
  RGWStorageStats stats;

public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "stat_bucket"; }
  RGWOpType get_type() override { return RGW_OP_STAT_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWCreateBucket : public RGWOp {
 protected:
  rgw::sal::Bucket::CreateParams createparams;
  RGWAccessControlPolicy policy;
  std::string location_constraint;
  bool has_cors = false;
  bool relaxed_region_enforcement = false;
  RGWCORSConfiguration cors_config;
  std::set<std::string> rmattr_names;

  virtual bool need_metadata_upload() const { return false; }

 public:
  void emplace_attr(std::string&& key, buffer::list&& bl) {
    createparams.attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  void init(rgw::sal::Driver* driver, req_state *s, RGWHandler *h) override {
    RGWOp::init(driver, s, h);
    relaxed_region_enforcement =
	s->cct->_conf.get_val<bool>("rgw_relaxed_region_enforcement");
  }
  virtual int get_params(optional_yield y) { return 0; }
  void send_response() override = 0;
  const char* name() const override { return "create_bucket"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteBucket : public RGWOp {
protected:
  RGWObjVersionTracker objv_tracker;

public:
  RGWDeleteBucket() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "delete_bucket"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

struct rgw_slo_entry {
  std::string path;
  std::string etag;
  uint64_t size_bytes;

  rgw_slo_entry() : size_bytes(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(path, bl);
    encode(etag, bl);
    encode(size_bytes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(path, bl);
     decode(etag, bl);
     decode(size_bytes, bl);
     DECODE_FINISH(bl);
  }

  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_slo_entry)

struct RGWSLOInfo {
  std::vector<rgw_slo_entry> entries;
  uint64_t total_size;

  /* in memory only */
  bufferlist raw_data;

  RGWSLOInfo() : total_size(0) {}
  ~RGWSLOInfo() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(total_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(entries, bl);
     decode(total_size, bl);
     DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(RGWSLOInfo)

class RGWPutObj : public RGWOp {
protected:
  off_t ofs;
  const char *supplied_md5_b64;
  const char *supplied_etag;
  const char *if_match;
  const char *if_nomatch;
  std::string copy_source;
  const char *copy_source_range;
  RGWBucketInfo copy_source_bucket_info;
  rgw::sal::Attrs copy_source_bucket_attrs;
  std::string copy_source_tenant_name;
  std::string copy_source_bucket_name;
  std::string copy_source_object_name;
  std::string copy_source_version_id;
  off_t copy_source_range_fst;
  off_t copy_source_range_lst;
  std::string etag;
  bool chunked_upload;
  RGWAccessControlPolicy policy;
  RGWObjTags obj_tags;
  const char *dlo_manifest;
  RGWSLOInfo *slo_info;
  rgw::sal::Attrs attrs;
  ceph::real_time mtime;
  uint64_t olh_epoch;
  std::string version_id;
  bufferlist bl_aux;
  std::map<std::string, std::string> crypt_http_responses;
  std::string user_data;

  std::string multipart_upload_id;
  std::string multipart_part_str;
  int multipart_part_num = 0;
  jspan_ptr multipart_trace;

  boost::optional<ceph::real_time> delete_at;
  //append obj
  bool append;
  uint64_t position;
  uint64_t cur_accounted_size;

  //object lock
  RGWObjectRetention *obj_retention;
  RGWObjectLegalHold *obj_legal_hold;

  std::optional<rgw::cksum::Cksum> cksum;

public:
  RGWPutObj() : ofs(0),
                supplied_md5_b64(NULL),
                supplied_etag(NULL),
                if_match(NULL),
                if_nomatch(NULL),
                copy_source_range(NULL),
                copy_source_range_fst(0),
                copy_source_range_lst(0),
                chunked_upload(0),
                dlo_manifest(NULL),
                slo_info(NULL),
                olh_epoch(0),
                append(false),
                position(0),
                cur_accounted_size(0),
                obj_retention(nullptr),
                obj_legal_hold(nullptr) {}

  ~RGWPutObj() override {
    delete slo_info;
    delete obj_retention;
    delete obj_legal_hold;
  }

  virtual int init_processing(optional_yield y) override;

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  /* this is for cases when copying data from other object */
  virtual int get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter>* filter,
                                 RGWGetObj_Filter* cb,
                                 std::map<std::string, bufferlist>& attrs,
                                 bufferlist* manifest_bl) {
    *filter = nullptr;
    return 0;
  }
  virtual int get_encrypt_filter(std::unique_ptr<rgw::sal::DataProcessor> *filter,
                                 rgw::sal::DataProcessor *cb) {
    return 0;
  }
  // if configured, construct a filter to generate torrent metadata
  auto get_torrent_filter(rgw::sal::DataProcessor *cb)
      -> std::optional<RGWPutObj_Torrent>;

  // get lua script to run as a "put object" filter
  int get_lua_filter(std::unique_ptr<rgw::sal::DataProcessor>* filter,
      rgw::sal::DataProcessor* cb);

  int get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len);
  int get_data(const off_t fst, const off_t lst, bufferlist& bl);

  virtual int get_params(optional_yield y) = 0;
  virtual int get_data(bufferlist& bl) = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_obj"; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

class RGWPostObj : public RGWOp {
protected:
  off_t min_len;
  off_t max_len;
  int len;
  off_t ofs;
  const char *supplied_md5_b64;
  const char *supplied_etag;
  std::string etag;
  RGWAccessControlPolicy policy;
  std::map<std::string, bufferlist> attrs;
  boost::optional<ceph::real_time> delete_at;
  std::optional<rgw::cksum::Cksum> cksum;

  /* Must be called after get_data() or the result is undefined. */
  virtual std::string get_current_filename() const = 0;
  virtual std::string get_current_content_type() const = 0;
  virtual bool is_next_file_to_upload() {
     return false;
  }
public:
  RGWPostObj() : min_len(0),
                 max_len(LLONG_MAX),
                 len(0),
                 ofs(0),
                 supplied_md5_b64(nullptr),
                 supplied_etag(nullptr) {
  }

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_encrypt_filter(std::unique_ptr<rgw::sal::DataProcessor> *filter,
                                 rgw::sal::DataProcessor *cb) {
    return 0;
  }
  virtual int get_params(optional_yield y) = 0;
  virtual int get_data(ceph::bufferlist& bl, bool& again) = 0;
  void send_response() override = 0;
  const char* name() const override { return "post_obj"; }
  RGWOpType get_type() override { return RGW_OP_POST_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

class RGWPutMetadataAccount : public RGWOp {
protected:
  std::set<std::string> rmattr_names;
  std::map<std::string, bufferlist> attrs, orig_attrs;
  std::map<int, std::string> temp_url_keys;
  RGWQuotaInfo new_quota;
  bool new_quota_extracted;

  RGWAccessControlPolicy policy;
  bool has_policy;

public:
  RGWPutMetadataAccount()
    : new_quota_extracted(false),
      has_policy(false) {
  }

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void pre_exec() override { }
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  virtual void filter_out_temp_url(std::map<std::string, bufferlist>& add_attrs,
                                   const std::set<std::string>& rmattr_names,
                                   std::map<int, std::string>& temp_url_keys);
  const char* name() const override { return "put_account_metadata"; }
  RGWOpType get_type() override { return RGW_OP_PUT_METADATA_ACCOUNT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWPutMetadataBucket : public RGWOp {
protected:
  rgw::sal::Attrs attrs;
  std::set<std::string> rmattr_names;
  bool has_policy, has_cors;
  uint32_t policy_rw_mask;
  RGWAccessControlPolicy policy;
  RGWCORSConfiguration cors_config;
  rgw_placement_rule placement_rule;
  std::optional<std::string> swift_ver_location;

public:
  RGWPutMetadataBucket()
    : has_policy(false), has_cors(false), policy_rw_mask(0)
  {}

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_bucket_metadata"; }
  RGWOpType get_type() override { return RGW_OP_PUT_METADATA_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWPutMetadataObject : public RGWOp {
protected:
  RGWAccessControlPolicy policy;
  boost::optional<ceph::real_time> delete_at;
  const char *dlo_manifest;

public:
  RGWPutMetadataObject()
    : dlo_manifest(NULL)
  {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_obj_metadata"; }
  RGWOpType get_type() override { return RGW_OP_PUT_METADATA_OBJECT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual bool need_object_expiration() { return false; }
};

class RGWRestoreObj : public RGWOp {
protected:
  std::optional<uint64_t> expiry_days;
public:
  RGWRestoreObj() {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual int get_params(optional_yield y) {return 0;}

  void send_response() override = 0;
  const char* name() const override { return "restore_obj"; }
  RGWOpType get_type() override { return RGW_OP_RESTORE_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteObj : public RGWOp {
protected:
  bool delete_marker;
  bool multipart_delete;
  std::string version_id;
  ceph::real_time unmod_since; /* if unmodified since */
  bool no_precondition_error;
  std::unique_ptr<RGWBulkDelete::Deleter> deleter;
  bool bypass_perm;
  bool bypass_governance_mode;

public:
  RGWDeleteObj()
    : delete_marker(false),
      multipart_delete(false),
      no_precondition_error(false),
      deleter(nullptr),
      bypass_perm(true),
      bypass_governance_mode(false) {
  }

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  int handle_slo_manifest(bufferlist& bl, optional_yield y);

  virtual int get_params(optional_yield y) { return 0; }
  void send_response() override = 0;
  const char* name() const override { return "delete_obj"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  virtual bool need_object_expiration() { return false; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

class RGWCopyObj : public RGWOp {
protected:
  RGWAccessControlPolicy dest_policy;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  // Required or it is not a copy operation
  std::string_view copy_source;
  // Not actually required
  std::optional<std::string_view> md_directive;

  off_t ofs;
  off_t len;
  off_t end;
  ceph::real_time mod_time;
  ceph::real_time unmod_time;
  ceph::real_time *mod_ptr;
  ceph::real_time *unmod_ptr;
  rgw::sal::Attrs attrs;
  std::unique_ptr<rgw::sal::Bucket> src_bucket;
  ceph::real_time src_mtime;
  ceph::real_time mtime;
  rgw::sal::AttrsMod attrs_mod;
  std::string source_zone;
  std::string etag;

  off_t last_ofs;

  std::string version_id;
  uint64_t olh_epoch;

  boost::optional<ceph::real_time> delete_at;
  bool copy_if_newer;

  bool need_to_check_storage_class = false;

  //object lock
  RGWObjectRetention *obj_retention;
  RGWObjectLegalHold *obj_legal_hold;

  int init_common();

public:
  RGWCopyObj() {
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    ofs = 0;
    len = 0;
    end = -1;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    attrs_mod = rgw::sal::ATTRSMOD_NONE;
    last_ofs = 0;
    olh_epoch = 0;
    copy_if_newer = false;
    obj_retention = nullptr;
    obj_legal_hold = nullptr;
  }

  ~RGWCopyObj() override {
    delete obj_retention;
    delete obj_legal_hold;
  }

  static bool parse_copy_location(const std::string_view& src,
                                  std::string& bucket_name,
                                  rgw_obj_key& object,
                                  req_state *s);

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl));
  }

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  void progress_cb(off_t ofs);

  virtual int check_storage_class(const rgw_placement_rule& src_placement) {
    return 0;
  }

  virtual int init_dest_policy() { return 0; }
  virtual int get_params(optional_yield y) = 0;
  virtual void send_partial_response(off_t ofs) {}
  void send_response() override = 0;
  const char* name() const override { return "copy_obj"; }
  RGWOpType get_type() override { return RGW_OP_COPY_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

class RGWGetACLs : public RGWOp {
protected:
  std::string acls;

public:
  RGWGetACLs() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "get_acls"; }
  RGWOpType get_type() override { return RGW_OP_GET_ACLS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutACLs : public RGWOp {
protected:
  bufferlist data;

public:
  RGWPutACLs() {}
  ~RGWPutACLs() override {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_policy_from_state(const ACLOwner& owner,
                                    RGWAccessControlPolicy& p) { return 0; }
  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_acls"; }
  RGWOpType get_type() override { return RGW_OP_PUT_ACLS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetLC : public RGWOp {
protected:

public:
  RGWGetLC() { }
  ~RGWGetLC() override { }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield) override = 0;

  void send_response() override = 0;
  const char* name() const override { return "get_lifecycle"; }
  RGWOpType get_type() override { return RGW_OP_GET_LC; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutLC : public RGWOp {
protected:
  bufferlist data;
  const char *content_md5;
  std::string cookie;

public:
  RGWPutLC() {
    content_md5 = nullptr;
  }
  ~RGWPutLC() override {}

  void init(rgw::sal::Driver* driver, req_state *s, RGWHandler *dialect_handler) override {
    static constexpr std::size_t COOKIE_LEN = 16;
    char buf[COOKIE_LEN + 1];

    RGWOp::init(driver, s, dialect_handler);
    gen_rand_alphanumeric(s->cct, buf, sizeof(buf) - 1);
    cookie = buf;
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_lifecycle"; }
  RGWOpType get_type() override { return RGW_OP_PUT_LC; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteLC : public RGWOp {
public:
  RGWDeleteLC() = default;
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "delete_lifecycle"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_LC; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetCORS : public RGWOp {
protected:

public:
  RGWGetCORS() {}

  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "get_cors"; }
  RGWOpType get_type() override { return RGW_OP_GET_CORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutCORS : public RGWOp {
protected:
  bufferlist cors_bl;
  bufferlist in_data;

public:
  RGWPutCORS() {}
  ~RGWPutCORS() override {}

  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_cors"; }
  RGWOpType get_type() override { return RGW_OP_PUT_CORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteCORS : public RGWOp {
protected:

public:
  RGWDeleteCORS() {}

  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "delete_cors"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_CORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWOptionsCORS : public RGWOp {
protected:
  RGWCORSRule *rule;
  const char *origin, *req_hdrs, *req_meth;

public:
  RGWOptionsCORS() : rule(NULL), origin(NULL),
                     req_hdrs(NULL), req_meth(NULL) {
  }

  int verify_permission(optional_yield y) override {return 0;}
  int validate_cors_request(RGWCORSConfiguration *cc);
  void execute(optional_yield y) override;
  void get_response_params(std::string& allowed_hdrs, std::string& exp_hdrs, unsigned *max_age);
  void send_response() override = 0;
  const char* name() const override { return "options_cors"; }
  RGWOpType get_type() override { return RGW_OP_OPTIONS_CORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutBucketEncryption : public RGWOp {
protected:
  RGWBucketEncryptionConfig bucket_encryption_conf;
  bufferlist data;
public:
  RGWPutBucketEncryption() = default;
  ~RGWPutBucketEncryption() {}

  int get_params(optional_yield y);
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "put_bucket_encryption"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_ENCRYPTION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketEncryption : public RGWOp {
protected:
  RGWBucketEncryptionConfig bucket_encryption_conf;
public:
  RGWGetBucketEncryption() {}

  int get_params(optional_yield y);
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_bucket_encryption"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_ENCRYPTION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWDeleteBucketEncryption : public RGWOp {
protected:
  RGWBucketEncryptionConfig bucket_encryption_conf;
public:
  RGWDeleteBucketEncryption() {}

  int get_params(optional_yield y);
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_bucket_encryption"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_ENCRYPTION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetRequestPayment : public RGWOp {
protected:
  bool requester_pays;

public:
  RGWGetRequestPayment() : requester_pays(0) {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "get_request_payment"; }
  RGWOpType get_type() override { return RGW_OP_GET_REQUEST_PAYMENT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWSetRequestPayment : public RGWOp {
protected:
  bool requester_pays;
  bufferlist in_data;
public:
 RGWSetRequestPayment() : requester_pays(false) {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) { return 0; }

  void send_response() override = 0;
  const char* name() const override { return "set_request_payment"; }
  RGWOpType get_type() override { return RGW_OP_SET_REQUEST_PAYMENT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWInitMultipart : public RGWOp {
protected:
  std::string upload_id;
  RGWAccessControlPolicy policy;
  ceph::real_time mtime;
  jspan_ptr multipart_trace;
  //object lock
  std::optional<RGWObjectRetention> obj_retention = std::nullopt;
  std::optional<RGWObjectLegalHold> obj_legal_hold = std::nullopt;
  rgw::sal::Attrs attrs;
  rgw::cksum::Type cksum_algo{rgw::cksum::Type::none};

public:
  RGWInitMultipart() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "init_multipart"; }
  RGWOpType get_type() override { return RGW_OP_INIT_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual int prepare_encryption(std::map<std::string, bufferlist>& attrs) { return 0; }
};

class RGWCompleteMultipart : public RGWOp {
protected:
  std::string upload_id;
  std::string etag;
  std::string version_id;
  bufferlist data;
  std::unique_ptr<rgw::sal::MPSerializer> serializer;
  jspan_ptr multipart_trace;
  ceph::real_time upload_time;
  std::unique_ptr<rgw::sal::Notification> res;
  std::unique_ptr<rgw::sal::Object> meta_obj;
  std::optional<rgw::cksum::Cksum> cksum;
  std::optional<std::string> armored_cksum;
  off_t ofs = 0;

public:
  RGWCompleteMultipart() {}
  ~RGWCompleteMultipart() = default;

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  bool check_previously_completed(const RGWMultiCompleteUpload* parts);
  void complete() override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "complete_multipart"; }
  RGWOpType get_type() override { return RGW_OP_COMPLETE_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWAbortMultipart : public RGWOp {
protected:
  jspan_ptr multipart_trace;
public:
  RGWAbortMultipart() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  void send_response() override = 0;
  const char* name() const override { return "abort_multipart"; }
  RGWOpType get_type() override { return RGW_OP_ABORT_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

class RGWListMultipart : public RGWOp {
protected:
  std::string upload_id;
  std::unique_ptr<rgw::sal::MultipartUpload> upload;
  int max_parts;
  int marker;
  RGWAccessControlPolicy policy;
  bool truncated;
  rgw_placement_rule* placement;
  std::optional<rgw::cksum::Cksum> cksum;

public:
  RGWListMultipart() {
    max_parts = 1000;
    marker = 0;
    truncated = false;
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "list_multipart"; }
  RGWOpType get_type() override { return RGW_OP_LIST_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWListBucketMultiparts : public RGWOp {
protected:
  std::string prefix;
  std::string marker_meta;
  std::string marker_key;
  std::string marker_upload_id;
  std::string next_marker_key;
  std::string next_marker_upload_id;
  int max_uploads;
  std::string delimiter;
  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::map<std::string, bool> common_prefixes;
  bool is_truncated;
  int default_max;
  bool encode_url {false};

public:
  RGWListBucketMultiparts() {
    max_uploads = 0;
    is_truncated = false;
    default_max = 0;
  }

  void init(rgw::sal::Driver* driver, req_state *s, RGWHandler *h) override {
    RGWOp::init(driver, s, h);
    max_uploads = default_max;
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "list_bucket_multiparts"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET_MULTIPARTS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};


class RGWGetCrossDomainPolicy : public RGWOp {
public:
  RGWGetCrossDomainPolicy() = default;
  ~RGWGetCrossDomainPolicy() override = default;

  int verify_permission(optional_yield) override {
    return 0;
  }

  void execute(optional_yield) override {
    op_ret = 0;
  }

  const char* name() const override { return "get_crossdomain_policy"; }

  RGWOpType get_type() override {
    return RGW_OP_GET_CROSS_DOMAIN_POLICY;
  }

  uint32_t op_mask() override {
    return RGW_OP_TYPE_READ;
  }
};


class RGWGetHealthCheck : public RGWOp {
public:
  RGWGetHealthCheck() = default;
  ~RGWGetHealthCheck() override = default;

  int verify_permission(optional_yield) override {
    return 0;
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_health_check"; }

  RGWOpType get_type() override {
    return RGW_OP_GET_HEALTH_CHECK;
  }

  uint32_t op_mask() override {
    return RGW_OP_TYPE_READ;
  }
};


class RGWDeleteMultiObj : public RGWOp {
  /**
   * Handles the deletion of an individual object and uses
   * set_partial_response to record the outcome.
   */
  void handle_individual_object(const rgw_obj_key& o, optional_yield y);

protected:
  std::vector<delete_multi_obj_entry> ops_log_entries;
  bufferlist data;
  rgw::sal::Bucket* bucket;
  bool quiet;
  bool status_dumped;
  bool bypass_perm;
  bool bypass_governance_mode;

public:
  RGWDeleteMultiObj() {
    quiet = false;
    status_dumped = false;
    bypass_perm = true;
    bypass_governance_mode = false;
  }

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  void send_response() override;

  virtual int get_params(optional_yield y) = 0;
  virtual void send_status() = 0;
  virtual void begin_response() = 0;
  virtual void send_partial_response(const rgw_obj_key& key, bool delete_marker,
                                     const std::string& marker_version_id,
                                     int ret) = 0;
  virtual void end_response() = 0;
  const char* name() const override { return "multi_object_delete"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_MULTI_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

  void write_ops_log_entry(rgw_log_entry& entry) const override;
};

class RGWInfo: public RGWOp {
public:
  RGWInfo() = default;
  ~RGWInfo() override = default;

  int verify_permission(optional_yield) override { return 0; }
  const char* name() const override { return "get info"; }
  RGWOpType get_type() override { return RGW_OP_GET_INFO; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

extern int rgw_build_bucket_policies(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
				     req_state* s, optional_yield y);
extern int rgw_build_object_policies(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
				     req_state *s, bool prefetch_data, optional_yield y);
extern void rgw_build_iam_environment(rgw::sal::Driver* driver,
				      req_state* s);

inline int get_system_versioning_params(req_state *s,
					uint64_t *olh_epoch,
					std::string *version_id)
{
  if (!s->system_request) {
    return 0;
  }

  if (olh_epoch) {
    std::string epoch_str = s->info.args.get(RGW_SYS_PARAM_PREFIX "versioned-epoch");
    if (!epoch_str.empty()) {
      std::string err;
      *olh_epoch = strict_strtol(epoch_str.c_str(), 10, &err);
      if (!err.empty()) {
        ldpp_subdout(s, rgw, 0) << "failed to parse versioned-epoch param"
				 << dendl;
        return -EINVAL;
      }
    }
  }

  if (version_id) {
    *version_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "version-id");
  }

  return 0;
} /* get_system_versioning_params */

static inline void format_xattr(std::string &xattr)
{
  /* If the extended attribute is not valid UTF-8, we encode it using
   * quoted-printable encoding.
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
} /* format_xattr */

/**
 * Get the HTTP request metadata out of the req_state as a
 * map(<attr_name, attr_contents>, where attr_name is RGW_ATTR_PREFIX.HTTP_NAME)
 * s: The request state
 * attrs: will be filled up with attrs mapped as <attr_name, attr_contents>
 * On success returns 0.
 * On failure returns a negative error code.
 *
 */
inline int rgw_get_request_metadata(const DoutPrefixProvider *dpp,
                                    CephContext* const cct,
				    struct req_info& info,
				    std::map<std::string, ceph::bufferlist>& attrs,
				    const bool allow_empty_attrs = true)
{
  static const std::set<std::string> blocklisted_headers = {
      "x-amz-server-side-encryption-customer-algorithm",
      "x-amz-server-side-encryption-customer-key",
      "x-amz-server-side-encryption-customer-key-md5",
      /* XXX agreed w/cbodley that probably a cleanup is needed here--we probably
       * don't want to store these, esp. under user.rgw */
      "x-amz-storage-class",
      "x-amz-content-sha256",
      "x-amz-checksum-algorithm",
      "x-amz-date"
  };

  size_t valid_meta_count = 0;
  for (auto& kv : info.x_meta_map) {
    const std::string& name = kv.first;
    std::string& xattr = kv.second;

    if (blocklisted_headers.count(name) == 1) {
      ldpp_subdout(dpp, rgw, 10) << "skipping x>> " << name << dendl;
      continue;
    } else if (allow_empty_attrs || !xattr.empty()) {
      ldpp_subdout(dpp, rgw, 10) << "x>> " << name << ":" << xattr << dendl;
      format_xattr(xattr);

      std::string attr_name(RGW_ATTR_PREFIX);
      attr_name.append(name);

      /* Check roughly whether we aren't going behind the limit on attribute
       * name. Passing here doesn't guarantee that an OSD will accept that
       * as ObjectStore::get_max_attr_name_length() can set the limit even
       * lower than the "osd_max_attr_name_len" configurable.  */
      const auto max_attr_name_len = cct->_conf->rgw_max_attr_name_len;
      if (max_attr_name_len && attr_name.length() > max_attr_name_len) {
        return -ENAMETOOLONG;
      }

      /* Similar remarks apply to the check for value size. We're verifying
       * it early at the RGW's side as it's being claimed in /info. */
      const auto max_attr_size = cct->_conf->rgw_max_attr_size;
      if (max_attr_size && xattr.length() > max_attr_size) {
        return -EFBIG;
      }

      /* Swift allows administrators to limit the number of metadats items
       * send _in a single request_. */
      const auto max_attrs_num_in_req = cct->_conf->rgw_max_attrs_num_in_req;
      if (max_attrs_num_in_req &&
          ++valid_meta_count > max_attrs_num_in_req) {
        return -E2BIG;
      }

      auto rval = attrs.emplace(std::move(attr_name), ceph::bufferlist());
      /* At the moment the value of the freshly created attribute key-value
       * pair is an empty bufferlist. */

      ceph::bufferlist& bl = rval.first->second;
      bl.append(xattr.c_str(), xattr.size() + 1);
    }
  }

  return 0;
} /* rgw_get_request_metadata */

inline void encode_delete_at_attr(boost::optional<ceph::real_time> delete_at,
				  std::map<std::string, bufferlist>& attrs)
{
  if (delete_at == boost::none) {
    return;
  }

  bufferlist delatbl;
  encode(*delete_at, delatbl);
  attrs[RGW_ATTR_DELETE_AT] = delatbl;
} /* encode_delete_at_attr */

inline void encode_obj_tags_attr(const RGWObjTags& obj_tags, std::map<std::string, bufferlist>& attrs)
{
  if (obj_tags.empty()) {
    return;
  }
  bufferlist tagsbl;
  obj_tags.encode(tagsbl);
  attrs[RGW_ATTR_TAGS] = std::move(tagsbl);
}

inline int encode_dlo_manifest_attr(const char * const dlo_manifest,
				    std::map<std::string, bufferlist>& attrs)
{
  std::string dm = dlo_manifest;

  if (dm.find('/') == std::string::npos) {
    return -EINVAL;
  }

  bufferlist manifest_bl;
  manifest_bl.append(dlo_manifest, strlen(dlo_manifest) + 1);
  attrs[RGW_ATTR_USER_MANIFEST] = manifest_bl;

  return 0;
} /* encode_dlo_manifest_attr */

inline void complete_etag(MD5& hash, std::string *etag)
{
  char etag_buf[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char etag_buf_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];

  hash.Final((unsigned char *)etag_buf);
  buf_to_hex((const unsigned char *)etag_buf, CEPH_CRYPTO_MD5_DIGESTSIZE,
	    etag_buf_str);

  *etag = etag_buf_str;
} /* complete_etag */

using boost::container::flat_map;

class RGWGetAttrs : public RGWOp {
public:
    using get_attrs_t = flat_map<std::string, std::optional<buffer::list>>;
protected:
  get_attrs_t attrs;

public:
  RGWGetAttrs()
  {}

  virtual ~RGWGetAttrs() {}

  void emplace_key(std::string&& key) {
    attrs.emplace(std::move(key), std::nullopt);
  }

  int verify_permission(optional_yield y);
  void pre_exec();
  void execute(optional_yield y);

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const char* name() const { return "get_attrs"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_ATTRS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
}; /* RGWGetAttrs */

class RGWSetAttrs : public RGWOp {
protected:
  std::map<std::string, buffer::list> attrs;

public:
  RGWSetAttrs() {}
  ~RGWSetAttrs() override {}

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl));
  }

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  void send_response() override = 0;
  const char* name() const override { return "set_attrs"; }
  RGWOpType get_type() override { return RGW_OP_SET_ATTRS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWRMAttrs : public RGWOp {
protected:
  rgw::sal::Attrs attrs;

public:
  RGWRMAttrs()
  {}

  virtual ~RGWRMAttrs() {}

  void emplace_key(std::string&& key) {
    attrs.emplace(std::move(key), buffer::list());
  }

  int verify_permission(optional_yield y);
  void pre_exec();
  void execute(optional_yield y);

  virtual int get_params() = 0;
  virtual void send_response() = 0;
  virtual const char* name() const { return "rm_attrs"; }
  virtual RGWOpType get_type() { return RGW_OP_DELETE_ATTRS; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_DELETE; }
}; /* RGWRMAttrs */

class RGWGetObjLayout : public RGWOp {
public:
  RGWGetObjLayout() {
  }

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("admin", RGW_CAP_READ);
  }
  int verify_permission(optional_yield) override {
    return check_caps(s->user->get_info().caps);
  }
  void pre_exec() override;
  void execute(optional_yield y) override;

  const char* name() const override { return "get_obj_layout"; }
  virtual RGWOpType get_type() override { return RGW_OP_GET_OBJ_LAYOUT; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutBucketPolicy : public RGWOp {
  bufferlist data;
public:
  RGWPutBucketPolicy() = default;
  ~RGWPutBucketPolicy() {
  }
  void send_response() override;
  int verify_permission(optional_yield y) override;
  uint32_t op_mask() override {
    return RGW_OP_TYPE_WRITE;
  }
  void execute(optional_yield y) override;
  int get_params(optional_yield y);
  const char* name() const override { return "put_bucket_policy"; }
  RGWOpType get_type() override {
    return RGW_OP_PUT_BUCKET_POLICY;
  }
};

class RGWGetBucketPolicy : public RGWOp {
  buffer::list policy;
public:
  RGWGetBucketPolicy() = default;
  void send_response() override;
  int verify_permission(optional_yield y) override;
  uint32_t op_mask() override {
    return RGW_OP_TYPE_READ;
  }
  void execute(optional_yield y) override;
  const char* name() const override { return "get_bucket_policy"; }
  RGWOpType get_type() override {
    return RGW_OP_GET_BUCKET_POLICY;
  }
};

class RGWDeleteBucketPolicy : public RGWOp {
public:
  RGWDeleteBucketPolicy() = default;
  void send_response() override;
  int verify_permission(optional_yield y) override;
  uint32_t op_mask() override {
    return RGW_OP_TYPE_WRITE;
  }
  void execute(optional_yield y) override;
  int get_params(optional_yield y);
  const char* name() const override { return "delete_bucket_policy"; }
  RGWOpType get_type() override {
    return RGW_OP_DELETE_BUCKET_POLICY;
  }
};

class RGWPutBucketObjectLock : public RGWOp {
protected:
  bufferlist data;
  bufferlist obj_lock_bl;
  RGWObjectLock obj_lock;
public:
  RGWPutBucketObjectLock() = default;
  ~RGWPutBucketObjectLock() {}
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual void send_response() override = 0;
  virtual int get_params(optional_yield y) = 0;
  const char* name() const override { return "put_bucket_object_lock"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_OBJ_LOCK; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketObjectLock : public RGWOp {
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual void send_response() override = 0;
  const char* name() const override {return "get_bucket_object_lock"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_OBJ_LOCK; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutObjRetention : public RGWOp {
protected:
  bufferlist data;
  RGWObjectRetention obj_retention;
  bool bypass_perm;
  bool bypass_governance_mode;
public:
  RGWPutObjRetention():bypass_perm(true), bypass_governance_mode(false) {}
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual void send_response() override = 0;
  virtual int get_params(optional_yield y) = 0;
  const char* name() const override { return "put_obj_retention"; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ_RETENTION; }
};

class RGWGetObjRetention : public RGWOp {
protected:
  RGWObjectRetention obj_retention;
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual void send_response() override = 0;
  const char* name() const override {return "get_obj_retention"; }
  RGWOpType get_type() override { return RGW_OP_GET_OBJ_RETENTION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutObjLegalHold : public RGWOp {
protected:
  bufferlist data;
  RGWObjectLegalHold obj_legal_hold;
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual void send_response() override = 0;
  virtual int get_params(optional_yield y) = 0;
  const char* name() const override { return "put_obj_legal_hold"; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ_LEGAL_HOLD; }
};

class RGWGetObjLegalHold : public RGWOp {
protected:
  RGWObjectLegalHold obj_legal_hold;
public:
  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;
  virtual void send_response() override = 0;
  const char* name() const override {return "get_obj_legal_hold"; }
  RGWOpType get_type() override { return RGW_OP_GET_OBJ_LEGAL_HOLD; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};


class RGWConfigBucketMetaSearch : public RGWOp {
protected:
  std::map<std::string, uint32_t> mdsearch_config;
public:
  RGWConfigBucketMetaSearch() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  virtual int get_params(optional_yield y) = 0;
  const char* name() const override { return "config_bucket_meta_search"; }
  virtual RGWOpType get_type() override { return RGW_OP_CONFIG_BUCKET_META_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketMetaSearch : public RGWOp {
public:
  RGWGetBucketMetaSearch() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield) override {}

  const char* name() const override { return "get_bucket_meta_search"; }
  virtual RGWOpType get_type() override { return RGW_OP_GET_BUCKET_META_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWDelBucketMetaSearch : public RGWOp {
public:
  RGWDelBucketMetaSearch() {}

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  void execute(optional_yield y) override;

  const char* name() const override { return "delete_bucket_meta_search"; }
  virtual RGWOpType delete_type() { return RGW_OP_DEL_BUCKET_META_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetClusterStat : public RGWOp {
protected:
  RGWClusterStat stats_op;
public:
  RGWGetClusterStat() {}

  void init(rgw::sal::Driver* driver, req_state *s, RGWHandler *h) override {
    RGWOp::init(driver, s, h);
  }
  int verify_permission(optional_yield) override {return 0;}
  virtual void send_response() override = 0;
  virtual int get_params(optional_yield y) = 0;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_cluster_stat"; }
  dmc::client_id dmclock_client() override { return dmc::client_id::admin; }
};

class RGWGetBucketPolicyStatus : public RGWOp {
protected:
  bool isPublic {false};
public:
  int verify_permission(optional_yield y) override;
  const char* name() const override { return "get_bucket_policy_status"; }
  virtual RGWOpType get_type() override { return RGW_OP_GET_BUCKET_POLICY_STATUS; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  void execute(optional_yield y) override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

class RGWPutBucketPublicAccessBlock : public RGWOp {
protected:
  bufferlist data;
  PublicAccessBlockConfiguration access_conf;
public:
  int verify_permission(optional_yield y) override;
  const char* name() const override { return "put_bucket_public_access_block";}
  virtual RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_PUBLIC_ACCESS_BLOCK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  int get_params(optional_yield y);
  void execute(optional_yield y) override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

class RGWGetBucketPublicAccessBlock : public RGWOp {
protected:
  PublicAccessBlockConfiguration access_conf;
public:
  int verify_permission(optional_yield y) override;
  const char* name() const override { return "get_bucket_public_access_block";}
  virtual RGWOpType get_type() override { return RGW_OP_GET_BUCKET_PUBLIC_ACCESS_BLOCK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  int get_params(optional_yield y);
  void execute(optional_yield y) override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

class RGWDeleteBucketPublicAccessBlock : public RGWOp {
protected:
  PublicAccessBlockConfiguration access_conf;
public:
  int verify_permission(optional_yield y) override;
  const char* name() const override { return "delete_bucket_public_access_block";}
  virtual RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_PUBLIC_ACCESS_BLOCK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  int get_params(optional_yield y);
  void execute(optional_yield y) override;
  void send_response() override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

inline int parse_value_and_bound(
    const std::string &input,
    int &output,
    const long lower_bound,
    const long upper_bound,
    const long default_val)
{
  if (!input.empty()) {
    char *endptr;
    output = strtol(input.c_str(), &endptr, 10);
    if (endptr) {
      if (endptr == input.c_str()) return -EINVAL;
      while (*endptr && isspace(*endptr)) // ignore white space
        endptr++;
      if (*endptr) {
        return -EINVAL;
      }
    }
    if(output > upper_bound) {
      output = upper_bound;
    }
    if(output < lower_bound) {
      output = lower_bound;
    }
  } else {
    output = default_val;
  }

  return 0;
}

int rgw_policy_from_attrset(const DoutPrefixProvider *dpp,
                            CephContext *cct,
                            std::map<std::string, bufferlist>& attrset,
                            RGWAccessControlPolicy *policy);
