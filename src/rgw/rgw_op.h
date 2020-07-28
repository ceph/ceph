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

#ifndef CEPH_RGW_OP_H
#define CEPH_RGW_OP_H

#include <limits.h>

#include <array>
#include <memory>
#include <string>
#include <set>
#include <map>
#include <vector>

#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/function.hpp>

#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"

#include "rgw_common.h"
#include "rgw_dmclock.h"
#include "rgw_sal.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_acl.h"
#include "rgw_cors.h"
#include "rgw_quota.h"
#include "rgw_putobj.h"
#include "rgw_multi.h"
#include "rgw_sal.h"

#include "rgw_lc.h"
#include "rgw_torrent.h"
#include "rgw_tag.h"
#include "rgw_object_lock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"
#include "rgw_public_access.h"

#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"

#include "include/ceph_assert.h"

using ceph::crypto::SHA1;

struct req_state;
class RGWOp;
class RGWRados;


namespace rgw {
namespace auth {
namespace registry {

class StrategyRegistry;

}
}
}

int rgw_op_get_bucket_policy_from_attr(CephContext *cct,
				       rgw::sal::RGWRadosStore *store,
                                       RGWBucketInfo& bucket_info,
                                       map<string, bufferlist>& bucket_attrs,
                                       RGWAccessControlPolicy *policy);

class RGWHandler {
protected:
  rgw::sal::RGWRadosStore* store{nullptr};
  struct req_state *s{nullptr};

  int do_init_permissions();
  int do_read_permissions(RGWOp* op, bool only_bucket);

public:
  RGWHandler() {}
  virtual ~RGWHandler();

  virtual int init(rgw::sal::RGWRadosStore* store,
                   struct req_state* _s,
                   rgw::io::BasicClient* cio);

  virtual int init_permissions(RGWOp*) {
    return 0;
  }

  virtual int retarget(RGWOp* op, RGWOp** new_op) {
    *new_op = op;
    return 0;
  }

  virtual int read_permissions(RGWOp* op) = 0;
  virtual int authorize(const DoutPrefixProvider* dpp) = 0;
  virtual int postauth_init() = 0;
  virtual int error_handler(int err_no, std::string* error_content);
  virtual void dump(const string& code, const string& message) const {}

  virtual bool supports_quota() {
    return true;
  }
};



void rgw_bucket_object_pre_exec(struct req_state *s);

namespace dmc = rgw::dmclock;

/**
 * Provide the base class for all ops.
 */
class RGWOp : public DoutPrefixProvider {
protected:
  struct req_state *s;
  RGWHandler *dialect_handler;
  rgw::sal::RGWRadosStore *store;
  RGWCORSConfiguration bucket_cors;
  bool cors_exist;
  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;
  int op_ret;
  int do_aws4_auth_completion();

  virtual int init_quota();

public:
  RGWOp()
    : s(nullptr),
      dialect_handler(nullptr),
      store(nullptr),
      cors_exist(false),
      op_ret(0) {
  }

  virtual ~RGWOp() = default;

  int get_ret() const { return op_ret; }

  virtual int init_processing() {
    if (dialect_handler->supports_quota()) {
      op_ret = init_quota();
      if (op_ret < 0)
        return op_ret;
    }

    return 0;
  }

  virtual void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *dialect_handler) {
    this->store = store;
    this->s = s;
    this->dialect_handler = dialect_handler;
  }
  int read_bucket_cors();
  bool generate_cors_headers(string& origin, string& method, string& headers, string& exp_headers, unsigned *max_age);

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
  virtual int verify_requester(const rgw::auth::StrategyRegistry& auth_registry) {
    /* TODO(rzarzynski): rename RGWHandler::authorize to generic_authenticate. */
    return dialect_handler->authorize(this);
  }
  virtual int verify_permission() = 0;
  virtual int verify_op_mask();
  virtual void pre_exec() {}
  virtual void execute() = 0;
  virtual void send_response() {}
  virtual void complete() {
    send_response();
  }
  virtual const char* name() const = 0;
  virtual RGWOpType get_type() { return RGW_OP_UNKNOWN; }

  virtual uint32_t op_mask() { return 0; }

  virtual int error_handler(int err_no, string *error_content);

  // implements DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext* get_cct() const override { return s->cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }

  virtual dmc::client_id dmclock_client() { return dmc::client_id::metadata; }
  virtual dmc::Cost dmclock_cost() { return 1; }
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
  seed torrent; // get torrent
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
  map<string, bufferlist> attrs;
  bool get_data;
  bool partial_content;
  bool ignore_invalid_range;
  bool range_parsed;
  bool skip_manifest;
  bool skip_decrypt{false};
  utime_t gc_invalidate_time;
  bool is_slo;
  string lo_etag;
  bool rgwx_stat; /* extended rgw stat operation */
  string version_id;

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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  int parse_range();
  int read_user_manifest_part(
    rgw_bucket& bucket,
    const rgw_bucket_dir_entry& ent,
    RGWAccessControlPolicy * const bucket_acl,
    const boost::optional<rgw::IAM::Policy>& bucket_policy,
    const off_t start_ofs,
    const off_t end_ofs,
    bool swift_slo);
  int handle_user_manifest(const char *prefix);
  int handle_slo_manifest(bufferlist& bl);

  int get_data_cb(bufferlist& bl, off_t ofs, off_t len);

  virtual int get_params() = 0;
  virtual int send_response_data_error() = 0;
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
  int verify_permission() override;
  void execute() override;
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
  int verify_permission() override;
  void execute() override;

  virtual void send_response() override = 0;
  virtual int get_params() = 0;
  const char* name() const override { return "put_obj_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ_TAGGING; }

};

class RGWDeleteObjTags: public RGWOp {
 public:
  void pre_exec() override;
  int verify_permission() override;
  void execute() override;

  const char* name() const override { return "delete_obj_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OBJ_TAGGING;}
};

class RGWGetBucketTags : public RGWOp {
protected:
  bufferlist tags_bl;
  bool has_tags{false};
public:
  int verify_permission() override;
  void execute() override;
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
  int verify_permission() override;
  void execute() override;

  virtual void send_response() override = 0;
  virtual int get_params() = 0;
  const char* name() const override { return "put_bucket_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_TAGGING; }
};

class RGWDeleteBucketTags : public RGWOp {
public:
  void pre_exec() override;
  int verify_permission() override;
  void execute() override;

  const char* name() const override { return "delete_bucket_tags"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_TAGGING;}
};

struct rgw_sync_policy_group;

class RGWGetBucketReplication : public RGWOp {
public:
  int verify_permission() override;
  void execute() override;
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
  int verify_permission() override;
  void execute() override;

  virtual void send_response() override = 0;
  virtual int get_params() = 0;
  const char* name() const override { return "put_bucket_replication"; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_REPLICATION; }
};

class RGWDeleteBucketReplication : public RGWOp {
protected:
  virtual void update_sync_policy(rgw_sync_policy_info *policy) = 0;
public:
  void pre_exec() override;
  int verify_permission() override;
  void execute() override;

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

    rgw::sal::RGWRadosStore * const store;
    req_state * const s;

  public:
    Deleter(const DoutPrefixProvider* dpp, rgw::sal::RGWRadosStore * const str, req_state * const s)
      : dpp(dpp),
        num_deleted(0),
        num_unfound(0),
        store(str),
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
                           map<string, bufferlist>& battrs,
                           ACLOwner& bucket_owner /* out */);
    bool delete_single(const acct_path_t& path);
    bool delete_chunk(const std::list<acct_path_t>& paths);
  };
  /* End of Deleter subclass */

  static const size_t MAX_CHUNK_ENTRIES = 1024;

protected:
  std::unique_ptr<Deleter> deleter;

public:
  RGWBulkDelete()
    : deleter(nullptr) {
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_data(std::list<acct_path_t>& items,
                       bool * is_truncated) = 0;
  void send_response() override = 0;

  const char* name() const override { return "bulk_delete"; }
  RGWOpType get_type() override { return RGW_OP_BULK_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

inline ostream& operator<<(ostream& out, const RGWBulkDelete::acct_path_t &o) {
  return out << o.bucket_name << "/" << o.obj_key;
}


class RGWBulkUploadOp : public RGWOp {
  boost::optional<RGWSysObjectCtx> dir_ctx;

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
  handle_upload_path(struct req_state *s);

  bool handle_file_verify_permission(RGWBucketInfo& binfo,
				     const rgw_obj& obj,
				     std::map<std::string, ceph::bufferlist>& battrs,
                                     ACLOwner& bucket_owner /* out */);
  int handle_file(std::string_view path,
                  size_t size,
                  AlignedStreamGetter& body);

  int handle_dir_verify_permission();
  int handle_dir(std::string_view path);

public:
  RGWBulkUploadOp()
    : num_created(0) {
  }

  void init(rgw::sal::RGWRadosStore* const store,
            struct req_state* const s,
            RGWHandler* const h) override;

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

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
  std::map<std::string, ceph::bufferlist> attrs;
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

  int verify_permission() override;
  void execute() override;

  virtual int get_params() = 0;
  virtual void handle_listing_chunk(rgw::sal::RGWBucketList&& buckets) {
    /* The default implementation, used by e.g. S3, just generates a new
     * part of listing and sends it client immediately. Swift can behave
     * differently: when the reverse option is requested, all incoming
     * instances of RGWBucketList are buffered and finally reversed. */
    return send_response_data(buckets);
  }
  virtual void send_response_begin(bool has_buckets) = 0;
  virtual void send_response_data(rgw::sal::RGWBucketList& buckets) = 0;
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
  string start_date;
  string end_date;
  int show_log_entries;
  int show_log_sum;
  map<string, bool> categories;
  map<rgw_user_bucket, rgw_usage_log_entry> usage;
  map<string, rgw_usage_log_entry> summary_map;
  map<string, cls_user_bucket_entry> buckets_usage;
  cls_user_header header;
  RGWStorageStats stats;
public:
  RGWGetUsage() : sent_data(false), show_log_entries(true), show_log_sum(true){
  }

  int verify_permission() override;
  void execute() override;

  virtual int get_params() = 0;
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

  int verify_permission() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "stat_account"; }
  RGWOpType get_type() override { return RGW_OP_STAT_ACCOUNT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWListBucket : public RGWOp {
protected:
  string prefix;
  rgw_obj_key marker; 
  rgw_obj_key next_marker; 
  rgw_obj_key end_marker;
  string max_keys;
  string delimiter;
  string encoding_type;
  bool list_versions;
  int max;
  vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;

  int default_max;
  bool is_truncated;
  bool allow_unordered;

  int shard_id;

  int parse_max_keys();

public:
  RGWListBucket() : list_versions(false), max(0),
                    default_max(0), is_truncated(false),
		    allow_unordered(false), shard_id(-1) {}
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
  }
  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "list_bucket"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual bool need_container_stats() { return false; }
};

class RGWGetBucketLogging : public RGWOp {
public:
  RGWGetBucketLogging() {}
  int verify_permission() override;
  void execute() override { }

  void send_response() override = 0;
  const char* name() const override { return "get_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWGetBucketLocation : public RGWOp {
public:
  RGWGetBucketLocation() {}
  ~RGWGetBucketLocation() override {}
  int verify_permission() override;
  void execute() override { }

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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() { return 0; }

  void send_response() override = 0;
  const char* name() const override { return "set_bucket_versioning"; }
  RGWOpType get_type() override { return RGW_OP_SET_BUCKET_VERSIONING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketWebsite : public RGWOp {
public:
  RGWGetBucketWebsite() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() { return 0; }

  void send_response() override = 0;
  const char* name() const override { return "set_bucket_website"; }
  RGWOpType get_type() override { return RGW_OP_SET_BUCKET_WEBSITE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteBucketWebsite : public RGWOp {
public:
  RGWDeleteBucketWebsite() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "delete_bucket_website"; }
  RGWOpType get_type() override { return RGW_OP_SET_BUCKET_WEBSITE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWStatBucket : public RGWOp {
protected:
  std::unique_ptr<rgw::sal::RGWBucket> bucket;

public:
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "stat_bucket"; }
  RGWOpType get_type() override { return RGW_OP_STAT_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWCreateBucket : public RGWOp {
protected:
  RGWAccessControlPolicy policy;
  string location_constraint;
  rgw_placement_rule placement_rule;
  RGWBucketInfo info;
  obj_version ep_objv;
  bool has_cors;
  bool relaxed_region_enforcement;
  bool obj_lock_enabled;
  RGWCORSConfiguration cors_config;
  boost::optional<std::string> swift_ver_location;
  map<string, buffer::list> attrs;
  set<string> rmattr_names;

  bufferlist in_data;

  virtual bool need_metadata_upload() const { return false; }

public:
  RGWCreateBucket() : has_cors(false), relaxed_region_enforcement(false), obj_lock_enabled(false) {}

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
    relaxed_region_enforcement =
	s->cct->_conf.get_val<bool>("rgw_relaxed_region_enforcement");
  }
  virtual int get_params() { return 0; }
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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "delete_bucket"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

struct rgw_slo_entry {
  string path;
  string etag;
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
  vector<rgw_slo_entry> entries;
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
  seed torrent;
  off_t ofs;
  const char *supplied_md5_b64;
  const char *supplied_etag;
  const char *if_match;
  const char *if_nomatch;
  std::string copy_source;
  const char *copy_source_range;
  RGWBucketInfo copy_source_bucket_info;
  string copy_source_tenant_name;
  string copy_source_bucket_name;
  string copy_source_object_name;
  string copy_source_version_id;
  off_t copy_source_range_fst;
  off_t copy_source_range_lst;
  string etag;
  bool chunked_upload;
  RGWAccessControlPolicy policy;
  std::unique_ptr <RGWObjTags> obj_tags;
  const char *dlo_manifest;
  RGWSLOInfo *slo_info;
  map<string, bufferlist> attrs;
  ceph::real_time mtime;
  uint64_t olh_epoch;
  string version_id;
  bufferlist bl_aux;
  map<string, string> crypt_http_responses;
  string user_data;

  std::string multipart_upload_id;
  std::string multipart_part_str;
  int multipart_part_num = 0;

  boost::optional<ceph::real_time> delete_at;
  //append obj
  bool append;
  uint64_t position;
  uint64_t cur_accounted_size;

  //object lock
  RGWObjectRetention *obj_retention;
  RGWObjectLegalHold *obj_legal_hold;

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

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  /* this is for cases when copying data from other object */
  virtual int get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter>* filter,
                                 RGWGetObj_Filter* cb,
                                 map<string, bufferlist>& attrs,
                                 bufferlist* manifest_bl) {
    *filter = nullptr;
    return 0;
  }
  virtual int get_encrypt_filter(std::unique_ptr<rgw::putobj::DataProcessor> *filter,
                                 rgw::putobj::DataProcessor *cb) {
    return 0;
  }

  int get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len);
  int get_data(const off_t fst, const off_t lst, bufferlist& bl);

  virtual int get_params() = 0;
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
  string etag;
  RGWAccessControlPolicy policy;
  map<string, bufferlist> attrs;
  boost::optional<ceph::real_time> delete_at;

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

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_encrypt_filter(std::unique_ptr<rgw::putobj::DataProcessor> *filter,
                                 rgw::putobj::DataProcessor *cb) {
    return 0;
  }
  virtual int get_params() = 0;
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

  RGWObjVersionTracker acct_op_tracker;

  RGWAccessControlPolicy policy;
  bool has_policy;

public:
  RGWPutMetadataAccount()
    : new_quota_extracted(false),
      has_policy(false) {
  }

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int init_processing() override;
  int verify_permission() override;
  void pre_exec() override { }
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  virtual void filter_out_temp_url(map<string, bufferlist>& add_attrs,
                                   const set<string>& rmattr_names,
                                   map<int, string>& temp_url_keys);
  const char* name() const override { return "put_account_metadata"; }
  RGWOpType get_type() override { return RGW_OP_PUT_METADATA_ACCOUNT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWPutMetadataBucket : public RGWOp {
protected:
  map<string, buffer::list> attrs;
  set<string> rmattr_names;
  bool has_policy, has_cors;
  uint32_t policy_rw_mask;
  RGWAccessControlPolicy policy;
  RGWCORSConfiguration cors_config;
  rgw_placement_rule placement_rule;
  boost::optional<std::string> swift_ver_location;

public:
  RGWPutMetadataBucket()
    : has_policy(false), has_cors(false), policy_rw_mask(0)
  {}

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */
  }

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
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

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_obj_metadata"; }
  RGWOpType get_type() override { return RGW_OP_PUT_METADATA_OBJECT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual bool need_object_expiration() { return false; }
};

class RGWDeleteObj : public RGWOp {
protected:
  bool delete_marker;
  bool multipart_delete;
  string version_id;
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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  int handle_slo_manifest(bufferlist& bl);

  virtual int get_params() { return 0; }
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
  map<string, buffer::list> attrs;
  string src_tenant_name, src_bucket_name, src_obj_name;
  std::unique_ptr<rgw::sal::RGWBucket> src_bucket;
  std::unique_ptr<rgw::sal::RGWObject> src_object;
  string dest_tenant_name, dest_bucket_name, dest_obj_name;
  std::unique_ptr<rgw::sal::RGWBucket> dest_bucket;
  std::unique_ptr<rgw::sal::RGWObject> dest_object;
  ceph::real_time src_mtime;
  ceph::real_time mtime;
  RGWRados::AttrsMod attrs_mod;
  string source_zone;
  string etag;

  off_t last_ofs;

  string version_id;
  uint64_t olh_epoch;

  boost::optional<ceph::real_time> delete_at;
  bool copy_if_newer;

  bool need_to_check_storage_class = false;

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
    attrs_mod = RGWRados::ATTRSMOD_NONE;
    last_ofs = 0;
    olh_epoch = 0;
    copy_if_newer = false;
  }

  static bool parse_copy_location(const std::string_view& src,
                                  string& bucket_name,
                                  rgw_obj_key& object);

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl));
  }

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    dest_policy.set_ctx(s->cct);
  }
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  void progress_cb(off_t ofs);

  virtual int check_storage_class(const rgw_placement_rule& src_placement) {
    return 0;
  }

  virtual int init_dest_policy() { return 0; }
  virtual int get_params() = 0;
  virtual void send_partial_response(off_t ofs) {}
  void send_response() override = 0;
  const char* name() const override { return "copy_obj"; }
  RGWOpType get_type() override { return RGW_OP_COPY_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  dmc::client_id dmclock_client() override { return dmc::client_id::data; }
};

class RGWGetACLs : public RGWOp {
protected:
  string acls;

public:
  RGWGetACLs() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "get_acls"; }
  RGWOpType get_type() override { return RGW_OP_GET_ACLS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutACLs : public RGWOp {
protected:
  bufferlist data;
  ACLOwner owner;

public:
  RGWPutACLs() {}
  ~RGWPutACLs() override {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_policy_from_state(rgw::sal::RGWRadosStore *store, struct req_state *s, stringstream& ss) { return 0; }
  virtual int get_params() = 0;
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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override = 0;

  void send_response() override = 0;
  const char* name() const override { return "get_lifecycle"; }
  RGWOpType get_type() override { return RGW_OP_GET_LC; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutLC : public RGWOp {
protected:
  bufferlist data;
  const char *content_md5;
  string cookie;

public:
  RGWPutLC() {
    content_md5 = nullptr;
  }
  ~RGWPutLC() override {}

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *dialect_handler) override {
#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    RGWOp::init(store, s, dialect_handler);
    gen_rand_alphanumeric(s->cct, buf, sizeof(buf) - 1);
    cookie = buf;
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

//  virtual int get_policy_from_state(RGWRados *store, struct req_state *s, stringstream& ss) { return 0; }
  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_lifecycle"; }
  RGWOpType get_type() override { return RGW_OP_PUT_LC; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteLC : public RGWOp {
public:
  RGWDeleteLC() = default;
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "delete_lifecycle"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_LC; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetCORS : public RGWOp {
protected:

public:
  RGWGetCORS() {}

  int verify_permission() override;
  void execute() override;

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

  int verify_permission() override;
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "put_cors"; }
  RGWOpType get_type() override { return RGW_OP_PUT_CORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWDeleteCORS : public RGWOp {
protected:

public:
  RGWDeleteCORS() {}

  int verify_permission() override;
  void execute() override;

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

  int verify_permission() override {return 0;}
  int validate_cors_request(RGWCORSConfiguration *cc);
  void execute() override;
  void get_response_params(string& allowed_hdrs, string& exp_hdrs, unsigned *max_age);
  void send_response() override = 0;
  const char* name() const override { return "options_cors"; }
  RGWOpType get_type() override { return RGW_OP_OPTIONS_CORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWGetRequestPayment : public RGWOp {
protected:
  bool requester_pays;

public:
  RGWGetRequestPayment() : requester_pays(0) {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

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

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() { return 0; }

  void send_response() override = 0;
  const char* name() const override { return "set_request_payment"; }
  RGWOpType get_type() override { return RGW_OP_SET_REQUEST_PAYMENT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWInitMultipart : public RGWOp {
protected:
  string upload_id;
  RGWAccessControlPolicy policy;
  ceph::real_time mtime;

public:
  RGWInitMultipart() {}

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy.set_ctx(s->cct);
  }
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "init_multipart"; }
  RGWOpType get_type() override { return RGW_OP_INIT_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual int prepare_encryption(map<string, bufferlist>& attrs) { return 0; }
};

class RGWCompleteMultipart : public RGWOp {
protected:
  string upload_id;
  string etag;
  string version_id;
  bufferlist data;

  struct MPSerializer {
    librados::IoCtx ioctx;
    rados::cls::lock::Lock lock;
    librados::ObjectWriteOperation op;
    std::string oid;
    bool locked;

    MPSerializer() : lock("RGWCompleteMultipart"), locked(false)
      {}

    int try_lock(const std::string& oid, utime_t dur);

    int unlock() {
      return lock.unlock(&ioctx, oid);
    }

    void clear_locked() {
      locked = false;
    }
  } serializer;

public:
  RGWCompleteMultipart() {}
  ~RGWCompleteMultipart() override {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  void complete() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "complete_multipart"; }
  RGWOpType get_type() override { return RGW_OP_COMPLETE_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWAbortMultipart : public RGWOp {
public:
  RGWAbortMultipart() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  void send_response() override = 0;
  const char* name() const override { return "abort_multipart"; }
  RGWOpType get_type() override { return RGW_OP_ABORT_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

class RGWListMultipart : public RGWOp {
protected:
  string upload_id;
  map<uint32_t, RGWUploadPartInfo> parts;
  int max_parts;
  int marker;
  RGWAccessControlPolicy policy;
  bool truncated;

public:
  RGWListMultipart() {
    max_parts = 1000;
    marker = 0;
    truncated = false;
  }

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    policy = RGWAccessControlPolicy(s->cct);
  }
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "list_multipart"; }
  RGWOpType get_type() override { return RGW_OP_LIST_MULTIPART; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

struct RGWMultipartUploadEntry {
  rgw_bucket_dir_entry obj;
  RGWMPObj mp;

  friend std::ostream& operator<<(std::ostream& out,
				  const RGWMultipartUploadEntry& e) {
    constexpr char quote = '"';
    return out << "RGWMultipartUploadEntry{ obj.key=" <<
      quote << e.obj.key << quote << " mp=" << e.mp << " }";
  }
};

class RGWListBucketMultiparts : public RGWOp {
protected:
  string prefix;
  RGWMPObj marker; 
  RGWMultipartUploadEntry next_marker; 
  int max_uploads;
  string delimiter;
  vector<RGWMultipartUploadEntry> uploads;
  map<string, bool> common_prefixes;
  bool is_truncated;
  int default_max;
  bool encode_url {false};

public:
  RGWListBucketMultiparts() {
    max_uploads = 0;
    is_truncated = false;
    default_max = 0;
  }

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
    max_uploads = default_max;
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "list_bucket_multiparts"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET_MULTIPARTS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};


class RGWGetCrossDomainPolicy : public RGWOp {
public:
  RGWGetCrossDomainPolicy() = default;
  ~RGWGetCrossDomainPolicy() override = default;

  int verify_permission() override {
    return 0;
  }

  void execute() override {
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

  int verify_permission() override {
    return 0;
  }

  void execute() override;

  const char* name() const override { return "get_health_check"; }

  RGWOpType get_type() override {
    return RGW_OP_GET_HEALTH_CHECK;
  }

  uint32_t op_mask() override {
    return RGW_OP_TYPE_READ;
  }
};


class RGWDeleteMultiObj : public RGWOp {
protected:
  bufferlist data;
  rgw::sal::RGWBucket* bucket;
  bool quiet;
  bool status_dumped;
  bool acl_allowed = false;

public:
  RGWDeleteMultiObj() {
    quiet = false;
    status_dumped = false;
  }
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  virtual void send_status() = 0;
  virtual void begin_response() = 0;
  virtual void send_partial_response(rgw_obj_key& key, bool delete_marker,
                                     const string& marker_version_id, int ret) = 0;
  virtual void end_response() = 0;
  const char* name() const override { return "multi_object_delete"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_MULTI_OBJ; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

class RGWInfo: public RGWOp {
public:
  RGWInfo() = default;
  ~RGWInfo() override = default;

  int verify_permission() override { return 0; }
  const char* name() const override { return "get info"; }
  RGWOpType get_type() override { return RGW_OP_GET_INFO; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

extern int rgw_build_bucket_policies(rgw::sal::RGWRadosStore* store, struct req_state* s);
extern int rgw_build_object_policies(rgw::sal::RGWRadosStore *store, struct req_state *s,
				     bool prefetch_data);
extern void rgw_build_iam_environment(rgw::sal::RGWRadosStore* store,
						                          struct req_state* s);
extern vector<rgw::IAM::Policy> get_iam_user_policy_from_attr(CephContext* cct,
                        rgw::sal::RGWRadosStore* store,
                        map<string, bufferlist>& attrs,
                        const string& tenant);

static inline int get_system_versioning_params(req_state *s,
					      uint64_t *olh_epoch,
					      string *version_id)
{
  if (!s->system_request) {
    return 0;
  }

  if (olh_epoch) {
    string epoch_str = s->info.args.get(RGW_SYS_PARAM_PREFIX "versioned-epoch");
    if (!epoch_str.empty()) {
      string err;
      *olh_epoch = strict_strtol(epoch_str.c_str(), 10, &err);
      if (!err.empty()) {
        lsubdout(s->cct, rgw, 0) << "failed to parse versioned-epoch param"
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
static inline int rgw_get_request_metadata(CephContext* const cct,
                                           struct req_info& info,
                                           std::map<std::string, ceph::bufferlist>& attrs,
                                           const bool allow_empty_attrs = true)
{
  static const std::set<std::string> blacklisted_headers = {
      "x-amz-server-side-encryption-customer-algorithm",
      "x-amz-server-side-encryption-customer-key",
      "x-amz-server-side-encryption-customer-key-md5",
      "x-amz-storage-class"
  };

  size_t valid_meta_count = 0;
  for (auto& kv : info.x_meta_map) {
    const std::string& name = kv.first;
    std::string& xattr = kv.second;

    if (blacklisted_headers.count(name) == 1) {
      lsubdout(cct, rgw, 10) << "skipping x>> " << name << dendl;
      continue;
    } else if (allow_empty_attrs || !xattr.empty()) {
      lsubdout(cct, rgw, 10) << "x>> " << name << ":" << xattr << dendl;
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

      /* Similar remarks apply to the check for value size. We're veryfing
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

static inline void encode_delete_at_attr(boost::optional<ceph::real_time> delete_at,
					map<string, bufferlist>& attrs)
{
  if (delete_at == boost::none) {
    return;
  } 

  bufferlist delatbl;
  encode(*delete_at, delatbl);
  attrs[RGW_ATTR_DELETE_AT] = delatbl;
} /* encode_delete_at_attr */

static inline void encode_obj_tags_attr(RGWObjTags* obj_tags, map<string, bufferlist>& attrs)
{
  if (obj_tags == nullptr){
    // we assume the user submitted a tag format which we couldn't parse since
    // this wouldn't be parsed later by get/put obj tags, lets delete if the
    // attr was populated
    return;
  }

  bufferlist tagsbl;
  obj_tags->encode(tagsbl);
  attrs[RGW_ATTR_TAGS] = tagsbl;
}

static inline int encode_dlo_manifest_attr(const char * const dlo_manifest,
					  map<string, bufferlist>& attrs)
{
  string dm = dlo_manifest;

  if (dm.find('/') == string::npos) {
    return -EINVAL;
  }

  bufferlist manifest_bl;
  manifest_bl.append(dlo_manifest, strlen(dlo_manifest) + 1);
  attrs[RGW_ATTR_USER_MANIFEST] = manifest_bl;

  return 0;
} /* encode_dlo_manifest_attr */

static inline void complete_etag(MD5& hash, string *etag)
{
  char etag_buf[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char etag_buf_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];

  hash.Final((unsigned char *)etag_buf);
  buf_to_hex((const unsigned char *)etag_buf, CEPH_CRYPTO_MD5_DIGESTSIZE,
	    etag_buf_str);

  *etag = etag_buf_str;
} /* complete_etag */

class RGWSetAttrs : public RGWOp {
protected:
  map<string, buffer::list> attrs;

public:
  RGWSetAttrs() {}
  ~RGWSetAttrs() override {}

  void emplace_attr(std::string&& key, buffer::list&& bl) {
    attrs.emplace(std::move(key), std::move(bl));
  }

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  void send_response() override = 0;
  const char* name() const override { return "set_attrs"; }
  RGWOpType get_type() override { return RGW_OP_SET_ATTRS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetObjLayout : public RGWOp {
protected:
  RGWObjManifest *manifest{nullptr};
  rgw_raw_obj head_obj;

public:
  RGWGetObjLayout() {
  }

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("admin", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->get_info().caps);
  }
  void pre_exec() override;
  void execute() override;

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
  int verify_permission() override;
  uint32_t op_mask() override {
    return RGW_OP_TYPE_WRITE;
  }
  void execute() override;
  int get_params();
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
  int verify_permission() override;
  uint32_t op_mask() override {
    return RGW_OP_TYPE_READ;
  }
  void execute() override;
  const char* name() const override { return "get_bucket_policy"; }
  RGWOpType get_type() override {
    return RGW_OP_GET_BUCKET_POLICY;
  }
};

class RGWDeleteBucketPolicy : public RGWOp {
public:
  RGWDeleteBucketPolicy() = default;
  void send_response() override;
  int verify_permission() override;
  uint32_t op_mask() override {
    return RGW_OP_TYPE_WRITE;
  }
  void execute() override;
  int get_params();
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
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  virtual void send_response() = 0;
  virtual int get_params() = 0;
  const char* name() const override { return "put_bucket_object_lock"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_OBJ_LOCK; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketObjectLock : public RGWOp {
public:
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  virtual void send_response() = 0;
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
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  virtual void send_response() override = 0;
  virtual int get_params() = 0;
  const char* name() const override { return "put_obj_retention"; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ_RETENTION; }
};

class RGWGetObjRetention : public RGWOp {
protected:
  RGWObjectRetention obj_retention;
public:
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  virtual void send_response() = 0;
  const char* name() const override {return "get_obj_retention"; }
  RGWOpType get_type() override { return RGW_OP_GET_OBJ_RETENTION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWPutObjLegalHold : public RGWOp {
protected:
  bufferlist data;
  RGWObjectLegalHold obj_legal_hold;
public:
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  virtual void send_response() override = 0;
  virtual int get_params() = 0;
  const char* name() const override { return "put_obj_legal_hold"; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ_LEGAL_HOLD; }
};

class RGWGetObjLegalHold : public RGWOp {
protected:
  RGWObjectLegalHold obj_legal_hold;
public:
  int verify_permission() override;
  void pre_exec() override;
  void execute() override;
  virtual void send_response() = 0;
  const char* name() const override {return "get_obj_legal_hold"; }
  RGWOpType get_type() override { return RGW_OP_GET_OBJ_LEGAL_HOLD; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};


class RGWConfigBucketMetaSearch : public RGWOp {
protected:
  std::map<std::string, uint32_t> mdsearch_config;
public:
  RGWConfigBucketMetaSearch() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  virtual int get_params() = 0;
  const char* name() const override { return "config_bucket_meta_search"; }
  virtual RGWOpType get_type() override { return RGW_OP_CONFIG_BUCKET_META_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetBucketMetaSearch : public RGWOp {
public:
  RGWGetBucketMetaSearch() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override {}

  const char* name() const override { return "get_bucket_meta_search"; }
  virtual RGWOpType get_type() override { return RGW_OP_GET_BUCKET_META_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

class RGWDelBucketMetaSearch : public RGWOp {
public:
  RGWDelBucketMetaSearch() {}

  int verify_permission() override;
  void pre_exec() override;
  void execute() override;

  const char* name() const override { return "delete_bucket_meta_search"; }
  virtual RGWOpType delete_type() { return RGW_OP_DEL_BUCKET_META_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

class RGWGetClusterStat : public RGWOp {
protected:
  struct rados_cluster_stat_t stats_op;
public:
  RGWGetClusterStat() {}

  void init(rgw::sal::RGWRadosStore *store, struct req_state *s, RGWHandler *h) override {
    RGWOp::init(store, s, h);
  }
  int verify_permission() override {return 0;}
  virtual void send_response() override = 0;
  virtual int get_params() = 0;
  void execute() override;
  const char* name() const override { return "get_cluster_stat"; }
  dmc::client_id dmclock_client() override { return dmc::client_id::admin; }
};

class RGWGetBucketPolicyStatus : public RGWOp {
protected:
  bool isPublic {false};
public:
  int verify_permission() override;
  const char* name() const override { return "get_bucket_policy_status"; }
  virtual RGWOpType get_type() override { return RGW_OP_GET_BUCKET_POLICY_STATUS; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  void execute() override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

class RGWPutBucketPublicAccessBlock : public RGWOp {
protected:
  bufferlist data;
  PublicAccessBlockConfiguration access_conf;
public:
  int verify_permission() override;
  const char* name() const override { return "put_bucket_public_access_block";}
  virtual RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_PUBLIC_ACCESS_BLOCK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  int get_params();
  void execute() override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

class RGWGetBucketPublicAccessBlock : public RGWOp {
protected:
  PublicAccessBlockConfiguration access_conf;
public:
  int verify_permission() override;
  const char* name() const override { return "get_bucket_public_access_block";}
  virtual RGWOpType get_type() override { return RGW_OP_GET_BUCKET_PUBLIC_ACCESS_BLOCK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  int get_params();
  void execute() override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

class RGWDeleteBucketPublicAccessBlock : public RGWOp {
protected:
  PublicAccessBlockConfiguration access_conf;
public:
  int verify_permission() override;
  const char* name() const override { return "delete_bucket_public_access_block";}
  virtual RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_PUBLIC_ACCESS_BLOCK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  int get_params();
  void execute() override;
  void send_response() override;
  dmc::client_id dmclock_client() override { return dmc::client_id::metadata; }
};

static inline int parse_value_and_bound(
    const string &input,
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

int forward_request_to_master(struct req_state *s, obj_version *objv, rgw::sal::RGWRadosStore *store,
                              bufferlist& in_data, JSONParser *jp, req_info *forward_info = nullptr);

#endif /* CEPH_RGW_OP_H */
