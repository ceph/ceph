// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>
#include <iostream>
#include <map>

#include "common/Formatter.h"
#include <common/errno.h>
#include "rgw_lc.h"
#include "rgw_lc_tier.h"
#include "rgw_string.h"
#include "rgw_zone.h"
#include "rgw_common.h"
#include "rgw_rest.h"
#include "svc_zone.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

struct rgw_lc_multipart_part_info {
  int part_num{0};
  uint64_t ofs{0};
  uint64_t size{0};
  std::string etag;
};

struct rgw_lc_obj_properties {
  ceph::real_time mtime;
  std::string etag;
  uint64_t versioned_epoch{0};
  std::map<std::string, RGWTierACLMapping>& target_acl_mappings;
  std::string target_storage_class;

  rgw_lc_obj_properties(ceph::real_time _mtime, std::string _etag,
      uint64_t _versioned_epoch, std::map<std::string,
      RGWTierACLMapping>& _t_acl_mappings,
      std::string _t_storage_class) :
    mtime(_mtime), etag(_etag),
    versioned_epoch(_versioned_epoch),
    target_acl_mappings(_t_acl_mappings),
    target_storage_class(_t_storage_class) {}
};

struct rgw_lc_multipart_upload_info {
  std::string upload_id;
  uint64_t obj_size;
  ceph::real_time mtime;
  std::string etag;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(upload_id, bl);
    encode(obj_size, bl);
    encode(mtime, bl);
    encode(etag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(upload_id, bl);
    decode(obj_size, bl);
    decode(mtime, bl);
    decode(etag, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_multipart_upload_info)

static inline string get_key_instance(const rgw_obj_key& key)
{
  if (!key.instance.empty() &&
      !key.have_null_instance()) {
    return "-" + key.instance;
  }
  return "";
}

static inline string get_key_oid(const rgw_obj_key& key)
{
  string oid = key.name;
  if (!key.instance.empty() &&
      !key.have_null_instance()) {
    oid += string("-") + key.instance;
  }
  return oid;
}

static inline string obj_to_aws_path(const rgw_obj& obj)
{
  string path = obj.bucket.name + "/" + get_key_oid(obj.key);
  return path;
}

static int read_upload_status(const DoutPrefixProvider *dpp, rgw::sal::Driver *driver,
    const rgw_raw_obj *status_obj, rgw_lc_multipart_upload_info *status)
{
  int ret = 0;
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(driver);

  if (!rados) {
    ldpp_dout(dpp, 0) << "ERROR: Not a RadosStore. Cannot be transitioned to cloud." << dendl;
    return -1;
  }

  auto& pool = status_obj->pool;
  const auto oid = status_obj->oid;
  auto sysobj = rados->svc()->sysobj;
  bufferlist bl;

  ret = rgw_get_system_obj(sysobj, pool, oid, bl, nullptr, nullptr,
      null_yield, dpp);

  if (ret < 0) {
    return ret;
  }

  if (bl.length() > 0) {
    try {
      auto p = bl.cbegin();
      status->decode(p);
    } catch (buffer::error& e) {
      ldpp_dout(dpp, 10) << "failed to decode status obj: "
        << e.what() << dendl;
      return -EIO;
    }
  } else {
    return -EIO;
  }

  return 0;
}

static int put_upload_status(const DoutPrefixProvider *dpp, rgw::sal::Driver *driver,
    const rgw_raw_obj *status_obj, rgw_lc_multipart_upload_info *status)
{
  int ret = 0;
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(driver);

  if (!rados) {
    ldpp_dout(dpp, 0) << "ERROR: Not a RadosStore. Cannot be transitioned to cloud." << dendl;
    return -1;
  }

  auto& pool = status_obj->pool;
  const auto oid = status_obj->oid;
  auto sysobj = rados->svc()->sysobj;
  bufferlist bl;
  status->encode(bl);

  ret = rgw_put_system_obj(dpp, sysobj, pool, oid, bl, true, nullptr,
      real_time{}, null_yield);

  return ret;
}

static int delete_upload_status(const DoutPrefixProvider *dpp, rgw::sal::Driver *driver,
    const rgw_raw_obj *status_obj)
{
  int ret = 0;
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(driver);

  if (!rados) {
    ldpp_dout(dpp, 0) << "ERROR: Not a RadosStore. Cannot be transitioned to cloud." << dendl;
    return -1;
  }

  auto& pool = status_obj->pool;
  const auto oid = status_obj->oid;
  auto sysobj = rados->svc()->sysobj;

  ret = rgw_delete_system_obj(dpp, sysobj, pool, oid, nullptr, null_yield);

  return ret;
}

static std::set<string> keep_headers = { "CONTENT_TYPE",
                                         "CONTENT_ENCODING",
                                         "CONTENT_DISPOSITION",
                                         "CONTENT_LANGUAGE" };

/*
 * mapping between rgw object attrs and output http fields
 *
 static const struct rgw_http_attr base_rgw_to_http_attrs[] = {
 { RGW_ATTR_CONTENT_LANG,      "Content-Language" },
 { RGW_ATTR_EXPIRES,           "Expires" },
 { RGW_ATTR_CACHE_CONTROL,     "Cache-Control" },
 { RGW_ATTR_CONTENT_DISP,      "Content-Disposition" },
 { RGW_ATTR_CONTENT_ENC,       "Content-Encoding" },
 { RGW_ATTR_USER_MANIFEST,     "X-Object-Manifest" },
 { RGW_ATTR_X_ROBOTS_TAG ,     "X-Robots-Tag" },
 { RGW_ATTR_STORAGE_CLASS ,    "X-Amz-Storage-Class" },
// RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION header depends on access mode:
// S3 endpoint: x-amz-website-redirect-location
// S3Website endpoint: Location
{ RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION, "x-amz-website-redirect-location" },
}; */

static void init_headers(map<string, bufferlist>& attrs,
    map<string, string>& headers)
{
  for (auto& kv : attrs) {
    const char * name = kv.first.c_str();
    const auto aiter = rgw_to_http_attrs.find(name);

    if (aiter != std::end(rgw_to_http_attrs)) {
      headers[aiter->second] = rgw_bl_str(kv.second);
    } else if (strncmp(name, RGW_ATTR_META_PREFIX,
          sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
      name += sizeof(RGW_ATTR_META_PREFIX) - 1;
      string sname(name);
      string name_prefix = RGW_ATTR_META_PREFIX;
      char full_name_buf[name_prefix.size() + sname.size() + 1];
      snprintf(full_name_buf, sizeof(full_name_buf), "%.*s%.*s",
          static_cast<int>(name_prefix.length()),
          name_prefix.data(),
          static_cast<int>(sname.length()),
          sname.data());
      headers[full_name_buf] = rgw_bl_str(kv.second);
    } else if (strcmp(name,RGW_ATTR_CONTENT_TYPE) == 0) {
      headers["CONTENT_TYPE"] = rgw_bl_str(kv.second);
    }
  }
}

/* Read object or just head from remote endpoint. For now initializes only headers,
 * but can be extended to fetch etag, mtime etc if needed.
 */
static int cloud_tier_get_object(RGWLCCloudTierCtx& tier_ctx, bool head,
                         std::map<std::string, std::string>& headers) {
  RGWRESTConn::get_obj_params req_params;
  std::string target_obj_name;
  int ret = 0;
  rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
        tier_ctx.target_storage_class);
  std::string etag;
  RGWRESTStreamRWRequest *in_req;

  rgw_bucket dest_bucket;
  dest_bucket.name = tier_ctx.target_bucket_name;
  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
                    tier_ctx.obj->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance(tier_ctx.obj->get_key());
  }

  rgw_obj dest_obj(dest_bucket, rgw_obj_key(target_obj_name));

  /* init input connection */
  req_params.get_op = !head;
  req_params.prepend_metadata = true;
  req_params.rgwx_stat = true;
  req_params.sync_manifest = true;
  req_params.skip_decrypt = true;

  ret = tier_ctx.conn.get_obj(tier_ctx.dpp, dest_obj, req_params, true /* send */, &in_req);
  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: " << __func__ << "(): conn.get_obj() returned ret=" << ret << dendl;
    return ret;
  }

  /* fetch headers */
  ret = tier_ctx.conn.complete_request(in_req, nullptr, nullptr, nullptr, nullptr, &headers, null_yield);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(tier_ctx.dpp, 20) << "ERROR: " << __func__ << "(): conn.complete_request() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

static bool is_already_tiered(const DoutPrefixProvider *dpp,
                             std::map<std::string, std::string>& headers,
                             ceph::real_time& mtime) {
  char buf[32];
  map<string, string> attrs = headers;

  for (const auto& a : attrs) {
    ldpp_dout(dpp, 20) << "GetCrf attr[" << a.first << "] = " << a.second <<dendl;
  }
  utime_t ut(mtime);
  snprintf(buf, sizeof(buf), "%lld.%09lld",
      (long long)ut.sec(),
      (long long)ut.nsec());

  string s = attrs["X_AMZ_META_RGWX_SOURCE_MTIME"];

  if (s.empty())
    s = attrs["x_amz_meta_rgwx_source_mtime"];

  ldpp_dout(dpp, 20) << "is_already_tiered attrs[X_AMZ_META_RGWX_SOURCE_MTIME] = " << s <<dendl;
  ldpp_dout(dpp, 20) << "is_already_tiered mtime buf = " << buf <<dendl;

  if (!s.empty() && !strcmp(s.c_str(), buf)){
    return 1;
  }
  return 0;
}

/* Read object locally & also initialize dest rest obj based on read attrs */
class RGWLCStreamRead
{
  CephContext *cct;
  const DoutPrefixProvider *dpp;
  std::map<std::string, bufferlist> attrs;
  uint64_t obj_size;
  rgw::sal::Object *obj;
  const real_time &mtime;

  bool multipart{false};
  uint64_t m_part_size{0};
  off_t m_part_off{0};
  off_t m_part_end{0};

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op;
  off_t ofs{0};
  off_t end{0};
  rgw_rest_obj rest_obj;

  int retcode{0};

  public:
  RGWLCStreamRead(CephContext *_cct, const DoutPrefixProvider *_dpp,
      rgw::sal::Object *_obj, const real_time &_mtime) :
    cct(_cct), dpp(_dpp), obj(_obj), mtime(_mtime),
    read_op(obj->get_read_op()) {}

  ~RGWLCStreamRead() {};
  int set_range(off_t _ofs, off_t _end);
  int get_range(off_t &_ofs, off_t &_end);
  rgw_rest_obj& get_rest_obj();
  void set_multipart(uint64_t part_size, off_t part_off, off_t part_end);
  int init();
  int init_rest_obj();
  int read(off_t ofs, off_t end, RGWGetDataCB *out_cb);
};

/* Send PUT op to remote endpoint */
class RGWLCCloudStreamPut
{
  const DoutPrefixProvider *dpp;
  rgw_lc_obj_properties obj_properties;
  RGWRESTConn& conn;
  const rgw_obj& dest_obj;
  std::string etag;
  RGWRESTStreamS3PutObj *out_req{nullptr};

  struct multipart_info {
    bool is_multipart{false};
    std::string upload_id;
    int part_num{0};
    uint64_t part_size;
  } multipart;

  int retcode;

  public:
  RGWLCCloudStreamPut(const DoutPrefixProvider *_dpp,
      const rgw_lc_obj_properties&  _obj_properties,
      RGWRESTConn& _conn,
      const rgw_obj& _dest_obj) :
    dpp(_dpp), obj_properties(_obj_properties), conn(_conn), dest_obj(_dest_obj) {
    }
  int init();
  static bool keep_attr(const std::string& h);
  static void init_send_attrs(const DoutPrefixProvider *dpp, const rgw_rest_obj& rest_obj,
      const rgw_lc_obj_properties& obj_properties,
      std::map<std::string, std::string>& attrs);
  void send_ready(const DoutPrefixProvider *dpp, const rgw_rest_obj& rest_obj);
  void handle_headers(const std::map<std::string, std::string>& headers);
  bool get_etag(std::string *petag);
  void set_multipart(const std::string& upload_id, int part_num, uint64_t part_size);
  int send();
  RGWGetDataCB *get_cb();
  int complete_request();
};

int RGWLCStreamRead::set_range(off_t _ofs, off_t _end) {
  ofs = _ofs;
  end = _end;

  return 0;
}

int RGWLCStreamRead::get_range(off_t &_ofs, off_t &_end) {
  _ofs = ofs;
  _end = end;

  return 0;
}

rgw_rest_obj& RGWLCStreamRead::get_rest_obj() {
  return rest_obj;
}

void RGWLCStreamRead::set_multipart(uint64_t part_size, off_t part_off, off_t part_end) {
  multipart = true;
  m_part_size = part_size;
  m_part_off = part_off;
  m_part_end = part_end;
}

int RGWLCStreamRead::init() {
  optional_yield y = null_yield;
  real_time read_mtime;

  read_op->params.lastmod = &read_mtime;

  int ret = read_op->prepare(y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fail to prepare read_op, ret = " << ret << dendl;
    return ret;
  }

  if (read_mtime != mtime) {
    /* raced */
    return -ECANCELED;
  }

  attrs = obj->get_attrs();
  obj_size = obj->get_obj_size();

  ret = init_rest_obj();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fail to initialize rest_obj, ret = " << ret << dendl;
    return ret;
  }

  if (!multipart) {
    set_range(0, obj_size - 1);
  } else {
    set_range(m_part_off, m_part_end);
  }
  return 0;
}

int RGWLCStreamRead::init_rest_obj() {
  /* Initialize rgw_rest_obj. 
   * Reference: do_decode_rest_obj
   * Check how to copy headers content */ 
  rest_obj.init(obj->get_key());

  if (!multipart) {
    rest_obj.content_len = obj_size;
  } else {
    rest_obj.content_len = m_part_size;
  }

  /* For multipart attrs are sent as part of InitMultipartCR itself */
  if (multipart) {
    return 0;
  }

  /*
   * XXX: verify if its right way to copy attrs into rest obj
   */
  init_headers(attrs, rest_obj.attrs);

  const auto aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter != attrs.end()) {
    bufferlist& bl = aiter->second;
    auto bliter = bl.cbegin();
    try {
      rest_obj.acls.decode(bliter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode policy off attrs" << dendl;
      return -EIO;
    }
  } else {
    ldpp_dout(dpp, 0) << "WARNING: acl attrs not provided" << dendl;
  }
  return 0;
}

int RGWLCStreamRead::read(off_t ofs, off_t end, RGWGetDataCB *out_cb) {
  int ret = read_op->iterate(dpp, ofs, end, out_cb, null_yield);
  return ret;
}

int RGWLCCloudStreamPut::init() {
  /* init output connection */
  if (multipart.is_multipart) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", multipart.part_num);
    rgw_http_param_pair params[] = { { "uploadId", multipart.upload_id.c_str() },
                                     { "partNumber", buf },
                                     { nullptr, nullptr } };
    conn.put_obj_send_init(dest_obj, params, &out_req);
  } else {
    conn.put_obj_send_init(dest_obj, nullptr, &out_req);
  }

  return 0;
}

bool RGWLCCloudStreamPut::keep_attr(const string& h) {
  return (keep_headers.find(h) != keep_headers.end());
}

void RGWLCCloudStreamPut::init_send_attrs(const DoutPrefixProvider *dpp,
    const rgw_rest_obj& rest_obj,
    const rgw_lc_obj_properties& obj_properties,
    std::map<string, string>& attrs) {

  map<string, RGWTierACLMapping>& acl_mappings(obj_properties.target_acl_mappings);
  const std::string& target_storage_class = obj_properties.target_storage_class;

  attrs.clear();

  for (auto& hi : rest_obj.attrs) {
    if (keep_attr(hi.first)) {
      attrs.insert(hi);
    } else {
      std::string s1 = boost::algorithm::to_lower_copy(hi.first);
      const char* k = std::strstr(s1.c_str(), "x-amz");
      if (k) {
        attrs[k] = hi.second;
      }
    }
  }

  const auto acl = rest_obj.acls.get_acl();

  map<int, vector<string> > access_map;

  if (!acl_mappings.empty()) {
    for (auto& grant : acl.get_grant_map()) {
      auto& orig_grantee = grant.first;
      auto& perm = grant.second;

      string grantee;

      const auto& am = acl_mappings;

      const auto iter = am.find(orig_grantee);
      if (iter == am.end()) {
        ldpp_dout(dpp, 20) << "acl_mappings: Could not find " << orig_grantee << " .. ignoring" << dendl;
        continue;
      }

      grantee = iter->second.dest_id;

      string type;

      switch (iter->second.type) {
        case ACL_TYPE_CANON_USER:
          type = "id";
          break;
        case ACL_TYPE_EMAIL_USER:
          type = "emailAddress";
          break;
        case ACL_TYPE_GROUP:
          type = "uri";
          break;
        default:
          continue;
      }

      string tv = type + "=" + grantee;

      int flags = perm.get_permission().get_permissions();
      if ((flags & RGW_PERM_FULL_CONTROL) == RGW_PERM_FULL_CONTROL) {
        access_map[flags].push_back(tv);
        continue;
      }

      for (int i = 1; i <= RGW_PERM_WRITE_ACP; i <<= 1) {
        if (flags & i) {
          access_map[i].push_back(tv);
        }
      }
    }
  }

  for (const auto& aiter : access_map) {
    int grant_type = aiter.first;

    string header_str("x-amz-grant-");

    switch (grant_type) {
      case RGW_PERM_READ:
        header_str.append("read");
        break;
      case RGW_PERM_WRITE:
        header_str.append("write");
        break;
      case RGW_PERM_READ_ACP:
        header_str.append("read-acp");
        break;
      case RGW_PERM_WRITE_ACP:
        header_str.append("write-acp");
        break;
      case RGW_PERM_FULL_CONTROL:
        header_str.append("full-control");
        break;
    }

    string s;

    for (const auto& viter : aiter.second) {
      if (!s.empty()) {
        s.append(", ");
      }
      s.append(viter);
    }

    ldpp_dout(dpp, 20) << "acl_mappings: set acl: " << header_str << "=" << s << dendl;

    attrs[header_str] = s;
  }

  /* Copy target storage class */
  if (!target_storage_class.empty()) {
    attrs["x-amz-storage-class"] = target_storage_class;
  } else {
    attrs["x-amz-storage-class"] = "STANDARD";
  }

  /* New attribute to specify its transitioned from RGW */
  attrs["x-amz-meta-rgwx-source"] = "rgw";
  attrs["x-rgw-cloud"] = "true";
  attrs["x-rgw-cloud-keep-attrs"] = "true";

  char buf[32];
  snprintf(buf, sizeof(buf), "%llu", (long long)obj_properties.versioned_epoch);
  attrs["x-amz-meta-rgwx-versioned-epoch"] = buf;

  utime_t ut(obj_properties.mtime);
  snprintf(buf, sizeof(buf), "%lld.%09lld",
      (long long)ut.sec(),
      (long long)ut.nsec());

  attrs["x-amz-meta-rgwx-source-mtime"] = buf;
  attrs["x-amz-meta-rgwx-source-etag"] = obj_properties.etag;
  attrs["x-amz-meta-rgwx-source-key"] = rest_obj.key.name;
  if (!rest_obj.key.instance.empty()) {
    attrs["x-amz-meta-rgwx-source-version-id"] = rest_obj.key.instance;
  }
  for (const auto& a : attrs) {
    ldpp_dout(dpp, 30) << "init_send_attrs attr[" << a.first << "] = " << a.second <<dendl;
  }
}

void RGWLCCloudStreamPut::send_ready(const DoutPrefixProvider *dpp, const rgw_rest_obj& rest_obj) {
  auto r = static_cast<RGWRESTStreamS3PutObj *>(out_req);

  std::map<std::string, std::string> new_attrs;
  if (!multipart.is_multipart) {
    init_send_attrs(dpp, rest_obj, obj_properties, new_attrs);
  }

  r->set_send_length(rest_obj.content_len);

  RGWAccessControlPolicy policy;

  r->send_ready(dpp, conn.get_key(), new_attrs, policy);
}

void RGWLCCloudStreamPut::handle_headers(const map<string, string>& headers) {
  for (const auto& h : headers) {
    if (h.first == "ETAG") {
      etag = h.second;
    }
  }
}

bool RGWLCCloudStreamPut::get_etag(string *petag) {
  if (etag.empty()) {
    return false;
  }
  *petag = etag;
  return true;
}

void RGWLCCloudStreamPut::set_multipart(const string& upload_id, int part_num, uint64_t part_size) {
  multipart.is_multipart = true;
  multipart.upload_id = upload_id;
  multipart.part_num = part_num;
  multipart.part_size = part_size;
}

int RGWLCCloudStreamPut::send() {
  int ret = RGWHTTP::send(out_req);
  return ret;
}

RGWGetDataCB *RGWLCCloudStreamPut::get_cb() {
  return out_req->get_out_cb();
}

int RGWLCCloudStreamPut::complete_request() {
  int ret = conn.complete_request(out_req, etag, &obj_properties.mtime, null_yield);
  return ret;
}

/* Read local copy and write to Cloud endpoint */
static int cloud_tier_transfer_object(const DoutPrefixProvider* dpp,
                            RGWLCStreamRead* readf, RGWLCCloudStreamPut* writef) {
  std::string url;
  bufferlist bl;
  bool sent_attrs{false};
  int ret{0};
  off_t ofs;
  off_t end;

  ret = readf->init();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fail to initialize in_crf, ret = " << ret << dendl;
    return ret;
  }
  readf->get_range(ofs, end);
  rgw_rest_obj& rest_obj = readf->get_rest_obj();
  if (!sent_attrs) {
    ret = writef->init();
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: fail to initialize out_crf, ret = " << ret << dendl;
      return ret;
    }

    writef->send_ready(dpp, rest_obj);
    ret = writef->send();
    if (ret < 0) {
      return ret;
    }
    sent_attrs = true;
  }

  ret = readf->read(ofs, end, writef->get_cb());

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fail to read from in_crf, ret = " << ret << dendl;
    return ret;
  }

  ret = writef->complete_request();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fail to complete request, ret = " << ret << dendl;
    return ret;
  }

  return 0;
}

static int cloud_tier_plain_transfer(RGWLCCloudTierCtx& tier_ctx) {
  int ret;

  rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
                        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
                        tier_ctx.target_storage_class);
  std::string target_obj_name;

  rgw_bucket dest_bucket;
  dest_bucket.name = tier_ctx.target_bucket_name;

  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    tier_ctx.obj->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance(tier_ctx.obj->get_key());
  }

  rgw_obj dest_obj(dest_bucket, rgw_obj_key(target_obj_name));

  tier_ctx.obj->set_atomic();

  /* Prepare Read from source */
  /* TODO: Define readf, writef as stack variables. For some reason,
   * when used as stack variables (esp., readf), the transition seems to
   * be taking lot of time eventually erroring out at times.
   */
  std::shared_ptr<RGWLCStreamRead> readf;
  readf.reset(new RGWLCStreamRead(tier_ctx.cct, tier_ctx.dpp,
        tier_ctx.obj, tier_ctx.o.meta.mtime));

  std::shared_ptr<RGWLCCloudStreamPut> writef;
  writef.reset(new RGWLCCloudStreamPut(tier_ctx.dpp, obj_properties, tier_ctx.conn,
               dest_obj));

  /* actual Read & Write */
  ret = cloud_tier_transfer_object(tier_ctx.dpp, readf.get(), writef.get());

  return ret;
}

static int cloud_tier_send_multipart_part(RGWLCCloudTierCtx& tier_ctx,
                                const std::string& upload_id,
                                const rgw_lc_multipart_part_info& part_info,
                                std::string *petag) {
  int ret;

  rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
                        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
                        tier_ctx.target_storage_class);
  std::string target_obj_name;
  off_t end;

  rgw_bucket dest_bucket;
  dest_bucket.name = tier_ctx.target_bucket_name;

  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    tier_ctx.obj->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance(tier_ctx.obj->get_key());
  }

  rgw_obj dest_obj(dest_bucket, rgw_obj_key(target_obj_name));

  tier_ctx.obj->set_atomic();

  /* TODO: Define readf, writef as stack variables. For some reason,
   * when used as stack variables (esp., readf), the transition seems to
   * be taking lot of time eventually erroring out at times. */
  std::shared_ptr<RGWLCStreamRead> readf;
  readf.reset(new RGWLCStreamRead(tier_ctx.cct, tier_ctx.dpp,
        tier_ctx.obj, tier_ctx.o.meta.mtime));

  std::shared_ptr<RGWLCCloudStreamPut> writef;
  writef.reset(new RGWLCCloudStreamPut(tier_ctx.dpp, obj_properties, tier_ctx.conn,
               dest_obj));

  /* Prepare Read from source */
  end = part_info.ofs + part_info.size - 1;
  readf->set_multipart(part_info.size, part_info.ofs, end);

  /* Prepare write */
  writef->set_multipart(upload_id, part_info.part_num, part_info.size);

  /* actual Read & Write */
  ret = cloud_tier_transfer_object(tier_ctx.dpp, readf.get(), writef.get());
  if (ret < 0) {
    return ret;
  }

  if (!(writef->get_etag(petag))) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to get etag from PUT request" << dendl;
    return -EIO;
  }

  return 0;
}

static int cloud_tier_abort_multipart(const DoutPrefixProvider *dpp,
      RGWRESTConn& dest_conn, const rgw_obj& dest_obj,
      const std::string& upload_id) {
  int ret;
  bufferlist out_bl;
  bufferlist bl;
  rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };

  string resource = obj_to_aws_path(dest_obj);
  ret = dest_conn.send_resource(dpp, "DELETE", resource, params, nullptr,
      out_bl, &bl, nullptr, null_yield);


  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to abort multipart upload for dest object=" << dest_obj << " (ret=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}

static int cloud_tier_init_multipart(const DoutPrefixProvider *dpp,
      RGWRESTConn& dest_conn, const rgw_obj& dest_obj,
      uint64_t obj_size, std::map<std::string, std::string>& attrs,
      std::string& upload_id) {
  bufferlist out_bl;
  bufferlist bl;

  struct InitMultipartResult {
    std::string bucket;
    std::string key;
    std::string upload_id;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("UploadId", upload_id, obj);
    }
  } result;

  int ret;
  rgw_http_param_pair params[] = { { "uploads", nullptr }, {nullptr, nullptr} };

  string resource = obj_to_aws_path(dest_obj);

  ret = dest_conn.send_resource(dpp, "POST", resource, params, &attrs,
      out_bl, &bl, nullptr, null_yield);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
    return ret;
  }
  /*
   * If one of the following fails we cannot abort upload, as we cannot
   * extract the upload id. If one of these fail it's very likely that that's
   * the least of our problem.
   */
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
    return -EIO;
  }

  if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
    string str(out_bl.c_str(), out_bl.length());
    ldpp_dout(dpp, 5) << "ERROR: failed to parse xml initmultipart: " << str << dendl;
    return -EIO;
  }

  try {
    RGWXMLDecoder::decode_xml("InitiateMultipartUploadResult", result, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    string str(out_bl.c_str(), out_bl.length());
    ldpp_dout(dpp, 5) << "ERROR: unexpected xml: " << str << dendl;
    return -EIO;
  }

  ldpp_dout(dpp, 20) << "init multipart result: bucket=" << result.bucket << " key=" << result.key << " upload_id=" << result.upload_id << dendl;

  upload_id = result.upload_id;

  return 0;
}

static int cloud_tier_complete_multipart(const DoutPrefixProvider *dpp,
      RGWRESTConn& dest_conn, const rgw_obj& dest_obj,
      std::string& upload_id,
      const std::map<int, rgw_lc_multipart_part_info>& parts) {
  rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };

  stringstream ss;
  XMLFormatter formatter;
  int ret;

  bufferlist bl, out_bl;
  string resource = obj_to_aws_path(dest_obj);

  struct CompleteMultipartReq {
    std::map<int, rgw_lc_multipart_part_info> parts;

    explicit CompleteMultipartReq(const std::map<int, rgw_lc_multipart_part_info>& _parts) : parts(_parts) {}

    void dump_xml(Formatter *f) const {
      for (const auto& p : parts) {
        f->open_object_section("Part");
        encode_xml("PartNumber", p.first, f);
        encode_xml("ETag", p.second.etag, f);
        f->close_section();
      };
    }
  } req_enc(parts);

  struct CompleteMultipartResult {
    std::string location;
    std::string bucket;
    std::string key;
    std::string etag;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Location", bucket, obj);
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("ETag", etag, obj);
    }
  } result;

  encode_xml("CompleteMultipartUpload", req_enc, &formatter);

  formatter.flush(ss);
  bl.append(ss.str());

  ret = dest_conn.send_resource(dpp, "POST", resource, params, nullptr,
      out_bl, &bl, nullptr, null_yield);


  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to complete multipart upload for dest object=" << dest_obj << dendl;
    return ret;
  }
  /*
   * If one of the following fails we cannot abort upload, as we cannot
   * extract the upload id. If one of these fail it's very likely that that's
   * the least of our problem.
   */
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
    return -EIO;
  }

  if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
    string str(out_bl.c_str(), out_bl.length());
    ldpp_dout(dpp, 5) << "ERROR: failed to parse xml Completemultipart: " << str << dendl;
    return -EIO;
  }

  try {
    RGWXMLDecoder::decode_xml("CompleteMultipartUploadResult", result, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    string str(out_bl.c_str(), out_bl.length());
    ldpp_dout(dpp, 5) << "ERROR: unexpected xml: " << str << dendl;
    return -EIO;
  }

  ldpp_dout(dpp, 20) << "complete multipart result: location=" << result.location << " bucket=" << result.bucket << " key=" << result.key << " etag=" << result.etag << dendl;

  return ret;
}

static int cloud_tier_abort_multipart_upload(RGWLCCloudTierCtx& tier_ctx,
      const rgw_obj& dest_obj, const rgw_raw_obj& status_obj,
      const std::string& upload_id) {
  int ret;

  ret = cloud_tier_abort_multipart(tier_ctx.dpp, tier_ctx.conn, dest_obj, upload_id);

  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to abort multipart upload dest obj=" << dest_obj << " upload_id=" << upload_id << " ret=" << ret << dendl;
    /* ignore error, best effort */
  }
  /* remove status obj */
  ret = delete_upload_status(tier_ctx.dpp, tier_ctx.driver, &status_obj);
  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to remove sync status obj obj=" << status_obj << " ret=" << ret << dendl;
    // ignore error, best effort 
  }
  return 0;
}

static int cloud_tier_multipart_transfer(RGWLCCloudTierCtx& tier_ctx) {
  rgw_obj src_obj;
  rgw_obj dest_obj;

  uint64_t obj_size;
  std::string src_etag;
  rgw_rest_obj rest_obj;

  rgw_lc_multipart_upload_info status;

  std::map<std::string, std::string> new_attrs;

  rgw_raw_obj status_obj;

  RGWBucketInfo b;
  std::string target_obj_name;
  rgw_bucket target_bucket;

  int ret;

  rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
        tier_ctx.target_storage_class);

  uint32_t part_size{0};
  uint32_t num_parts{0};

  int cur_part{0};
  uint64_t cur_ofs{0};
  std::map<int, rgw_lc_multipart_part_info> parts;

  obj_size = tier_ctx.o.meta.size;

  target_bucket.name = tier_ctx.target_bucket_name;

  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    tier_ctx.obj->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance(tier_ctx.obj->get_key());
  }
  dest_obj.init(target_bucket, target_obj_name);

  rgw_pool pool = static_cast<rgw::sal::RadosStore*>(tier_ctx.driver)->svc()->zone->get_zone_params().log_pool;
  status_obj = rgw_raw_obj(pool, "lc_multipart_" + tier_ctx.obj->get_oid());

  ret = read_upload_status(tier_ctx.dpp, tier_ctx.driver, &status_obj, &status);

  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to read sync status of object " << src_obj << " ret=" << ret << dendl;
    return ret;
  }

  if (ret >= 0) {
    // check here that mtime and size did not change 
    if (status.mtime != obj_properties.mtime || status.obj_size != obj_size ||
        status.etag != obj_properties.etag) {
      cloud_tier_abort_multipart_upload(tier_ctx, dest_obj, status_obj, status.upload_id);
      ret = -ENOENT;
    }
  }

  if (ret == -ENOENT) { 
    RGWLCStreamRead readf(tier_ctx.cct, tier_ctx.dpp, tier_ctx.obj, tier_ctx.o.meta.mtime);

    readf.init();

    rest_obj = readf.get_rest_obj();

    RGWLCCloudStreamPut::init_send_attrs(tier_ctx.dpp, rest_obj, obj_properties, new_attrs);

    ret = cloud_tier_init_multipart(tier_ctx.dpp, tier_ctx.conn, dest_obj, obj_size, new_attrs, status.upload_id);
    if (ret < 0) {
      return ret;
    }

    status.obj_size = obj_size;
    status.mtime = obj_properties.mtime;
    status.etag = obj_properties.etag;

    ret = put_upload_status(tier_ctx.dpp, tier_ctx.driver, &status_obj, &status);

    if (ret < 0) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to driver multipart upload state, ret=" << ret << dendl;
      // continue with upload anyway 
    }

#define MULTIPART_MAX_PARTS 10000
#define MULTIPART_MAX_PARTS 10000
    uint64_t min_part_size = obj_size / MULTIPART_MAX_PARTS;
    uint64_t min_conf_size = tier_ctx.multipart_min_part_size;

    if (min_conf_size < MULTIPART_MIN_POSSIBLE_PART_SIZE) {
      min_conf_size = MULTIPART_MIN_POSSIBLE_PART_SIZE;
    }

    part_size = std::max(min_conf_size, min_part_size);
    num_parts = (obj_size + part_size - 1) / part_size;
    cur_part = 1;
    cur_ofs = 0;
  }

  for (; (uint32_t)cur_part <= num_parts; ++cur_part) {
    ldpp_dout(tier_ctx.dpp, 20) << "cur_part = "<< cur_part << ", info.ofs = " << cur_ofs << ", info.size = " << part_size << ", obj size = " << obj_size<< ", num_parts:" << num_parts << dendl;
    rgw_lc_multipart_part_info& cur_part_info = parts[cur_part];
    cur_part_info.part_num = cur_part;
    cur_part_info.ofs = cur_ofs;
    cur_part_info.size = std::min((uint64_t)part_size, obj_size - cur_ofs);

    cur_ofs += cur_part_info.size;

    ret = cloud_tier_send_multipart_part(tier_ctx,
            status.upload_id,
            cur_part_info,
            &cur_part_info.etag);

    if (ret < 0) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to send multipart part of obj=" << tier_ctx.obj << ", sync via multipart upload, upload_id=" << status.upload_id << " part number " << cur_part << " (error: " << cpp_strerror(-ret) << ")" << dendl;
      cloud_tier_abort_multipart_upload(tier_ctx, dest_obj, status_obj, status.upload_id);
      return ret;
    }

  }

  ret = cloud_tier_complete_multipart(tier_ctx.dpp, tier_ctx.conn, dest_obj, status.upload_id, parts);
  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to complete multipart upload of obj=" << tier_ctx.obj << " (error: " << cpp_strerror(-ret) << ")" << dendl;
    cloud_tier_abort_multipart_upload(tier_ctx, dest_obj, status_obj, status.upload_id);
    return ret;
  }

  /* remove status obj */
  ret = delete_upload_status(tier_ctx.dpp, tier_ctx.driver, &status_obj);
  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to abort multipart upload obj=" << tier_ctx.obj << " upload_id=" << status.upload_id << " part number " << cur_part << " (" << cpp_strerror(-ret) << ")" << dendl;
    // ignore error, best effort 
  }
  return 0;
}

/* Check if object has already been transitioned */
static int cloud_tier_check_object(RGWLCCloudTierCtx& tier_ctx, bool& already_tiered) {
  int ret;
  std::map<std::string, std::string> headers;

  /* Fetch Head object */
  ret = cloud_tier_get_object(tier_ctx, true, headers);

  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to fetch HEAD from cloud for obj=" << tier_ctx.obj << " , ret = " << ret << dendl;
    return ret;
  }

  already_tiered = is_already_tiered(tier_ctx.dpp, headers, tier_ctx.o.meta.mtime);

  if (already_tiered) {
    ldpp_dout(tier_ctx.dpp, 20) << "is_already_tiered true" << dendl;
  } else {
    ldpp_dout(tier_ctx.dpp, 20) << "is_already_tiered false..going with out_crf writing" << dendl;
  }

  return ret;
}

static int cloud_tier_create_bucket(RGWLCCloudTierCtx& tier_ctx) {
  bufferlist out_bl;
  int ret = 0;
  pair<string, string> key(tier_ctx.storage_class, tier_ctx.target_bucket_name);
  struct CreateBucketResult {
    std::string code;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Code", code, obj);
    }
  } result;

  ldpp_dout(tier_ctx.dpp, 30) << "Cloud_tier_ctx: creating bucket:" << tier_ctx.target_bucket_name << dendl;
  bufferlist bl;
  string resource = tier_ctx.target_bucket_name;

  ret = tier_ctx.conn.send_resource(tier_ctx.dpp, "PUT", resource, nullptr, nullptr,
                                    out_bl, &bl, nullptr, null_yield);

  if (ret < 0 ) {
    ldpp_dout(tier_ctx.dpp, 0) << "create target bucket : " << tier_ctx.target_bucket_name << " returned ret:" << ret << dendl;
  }
  if (out_bl.length() > 0) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize xml parser for parsing create_bucket response from server" << dendl;
      return -EIO;
    }

    if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
      string str(out_bl.c_str(), out_bl.length());
      ldpp_dout(tier_ctx.dpp, 5) << "ERROR: failed to parse xml createbucket: " << str << dendl;
      return -EIO;
    }

    try {
      RGWXMLDecoder::decode_xml("Error", result, &parser, true);
    } catch (RGWXMLDecoder::err& err) {
      string str(out_bl.c_str(), out_bl.length());
      ldpp_dout(tier_ctx.dpp, 5) << "ERROR: unexpected xml: " << str << dendl;
      return -EIO;
    }

    if (result.code != "BucketAlreadyOwnedByYou" && result.code != "BucketAlreadyExists") {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: Creating target bucket failed with error: " << result.code << dendl;
      return -EIO;
    }
  }

  return 0;
}

int rgw_cloud_tier_transfer_object(RGWLCCloudTierCtx& tier_ctx, std::set<std::string>& cloud_targets) {
  int ret = 0;

  // check if target_path is already created
  std::set<std::string>::iterator it;

  it = cloud_targets.find(tier_ctx.target_bucket_name);
  tier_ctx.target_bucket_created = (it != cloud_targets.end());

  /* If run first time attempt to create the target bucket */
  if (!tier_ctx.target_bucket_created) {
    ret = cloud_tier_create_bucket(tier_ctx);

    if (ret < 0) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to create target bucket on the cloud endpoint ret=" << ret << dendl;
      return ret;
    }
    tier_ctx.target_bucket_created = true;
    cloud_targets.insert(tier_ctx.target_bucket_name);
  }

  /* Since multiple zones may try to transition the same object to the cloud,
   * verify if the object is already transitioned. And since its just a best
   * effort, do not bail out in case of any errors.
   */
  bool already_tiered = false;
  ret = cloud_tier_check_object(tier_ctx, already_tiered);

  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to check object on the cloud endpoint ret=" << ret << dendl;
  }

  if (already_tiered) {
    ldpp_dout(tier_ctx.dpp, 20) << "Object (" << tier_ctx.o.key << ") is already tiered" << dendl;
    return 0;
  }

  uint64_t size = tier_ctx.o.meta.size;
  uint64_t multipart_sync_threshold = tier_ctx.multipart_sync_threshold;

  if (multipart_sync_threshold < MULTIPART_MIN_POSSIBLE_PART_SIZE) {
    multipart_sync_threshold = MULTIPART_MIN_POSSIBLE_PART_SIZE;
  }

  if (size < multipart_sync_threshold) {
    ret = cloud_tier_plain_transfer(tier_ctx);
  } else {
    tier_ctx.is_multipart_upload = true;
    ret = cloud_tier_multipart_transfer(tier_ctx);
  } 

  if (ret < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to transition object ret=" << ret << dendl;
  }

  return ret;
}
