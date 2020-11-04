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

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

static string get_key_oid(const rgw_obj_key& key)
{
  string oid = key.name;
  if (!key.instance.empty() &&
      !key.have_null_instance()) {
    oid += string(":") + key.instance;
  }
  return oid;
}

static string obj_to_aws_path(const rgw_obj& obj)
{
  string path = obj.bucket.name + "/" + get_key_oid(obj.key);
  return path;
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
  for (auto kv : attrs) {
    const char * name = kv.first.c_str();
    const auto aiter = rgw_to_http_attrs.find(name);

    if (aiter != std::end(rgw_to_http_attrs)) {
      headers[aiter->second] = rgw_bl_str(kv.second);
    } else if (strcmp(name, RGW_ATTR_SLO_UINDICATOR) == 0) {
      // this attr has an extra length prefix from encode() in prior versions
      headers["X-Object-Meta-Static-Large-Object"] = "True";
    } else if (strncmp(name, RGW_ATTR_META_PREFIX,
          sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
      name += sizeof(RGW_ATTR_META_PREFIX) - 1;
      string sname(name);
      string name_prefix = "X-Object-Meta-";
      char full_name_buf[name_prefix.size() + sname.size() + 1];
      snprintf(full_name_buf, sizeof(full_name_buf), "%.*s%.*s",
          static_cast<int>(name_prefix.length()),
          name_prefix.data(),
          static_cast<int>(sname.length()),
          sname.data());
      headers[full_name_buf] = rgw_bl_str(kv.second);
    } else if (strcmp(name,RGW_ATTR_CONTENT_TYPE) == 0) {
      /* Verify if its right way to copy this field */
      headers["CONTENT_TYPE"] = rgw_bl_str(kv.second);
    }
  }
}

class RGWLCStreamGetCRF : public RGWStreamReadHTTPResourceCRF
{
  RGWRESTConn::get_obj_params req_params;

  CephContext *cct;
  RGWHTTPManager *http_manager;
  rgw_lc_obj_properties obj_properties;
  std::shared_ptr<RGWRESTConn> conn;
  rgw::sal::RGWObject* dest_obj;
  string etag;
  RGWRESTStreamRWRequest *in_req;
  map<string, string> headers;

  public:
  RGWLCStreamGetCRF(CephContext *_cct,
      RGWCoroutinesEnv *_env,
      RGWCoroutine *_caller,
      RGWHTTPManager *_http_manager,
      const rgw_lc_obj_properties&  _obj_properties,
      std::shared_ptr<RGWRESTConn> _conn,
      rgw::sal::RGWObject* _dest_obj) :
    RGWStreamReadHTTPResourceCRF(_cct, _env, _caller, _http_manager, _dest_obj->get_key()),
                                 cct(_cct), http_manager(_http_manager), obj_properties(_obj_properties),
                                 conn(_conn), dest_obj(_dest_obj) {}

  int init()  {
    /* init input connection */
    req_params.get_op = false; /* Need only headers */
    req_params.prepend_metadata = true;
    req_params.rgwx_stat = true;
    req_params.sync_manifest = true;
    req_params.skip_decrypt = true;

    string etag;
    real_time set_mtime;

    int ret = conn->get_obj(dest_obj, req_params, true /* send */, &in_req);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): conn->get_obj() returned ret=" << ret << dendl;
      return ret;
    }

    /* fetch only headers */
    ret = conn->complete_request(in_req, nullptr, nullptr,
        nullptr, nullptr, &headers, null_yield);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 20) << "ERROR: " << __func__ << "(): conn->complete_request() returned ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }

  int is_already_tiered() {
    char buf[32];
    map<string, string> attrs = headers;

    for (auto a : attrs) {
      ldout(cct, 20) << "GetCrf attr[" << a.first << "] = " << a.second <<dendl;
    }
    utime_t ut(obj_properties.mtime);
    snprintf(buf, sizeof(buf), "%lld.%09lld",
        (long long)ut.sec(),
        (long long)ut.nsec());

    string s = attrs["X_AMZ_META_RGWX_SOURCE_MTIME"];

    if (s.empty())
      s = attrs["x_amz_meta_rgwx_source_mtime"];

    ldout(cct, 20) << "is_already_tiered attrs[X_AMZ_META_RGWX_SOURCE_MTIME] = " << s <<dendl;
    ldout(cct, 20) << "is_already_tiered mtime buf = " << buf <<dendl;

    if (!s.empty() && !strcmp(s.c_str(), buf)){
      return 1;
    }
    return 0;
  }
};

class RGWLCStreamReadCRF : public RGWStreamReadCRF
{
  CephContext *cct;
  map<string, bufferlist> attrs;
  uint64_t obj_size;
  rgw_obj& obj;
  const real_time &mtime;

  bool multipart;
  uint64_t m_part_size;
  off_t m_part_off;
  off_t m_part_end;

  public:
  RGWLCStreamReadCRF(CephContext *_cct, RGWRados* rados, RGWBucketInfo& bucket_info,
                     RGWObjectCtx& obj_ctx, rgw_obj& _obj, const real_time &_mtime) :
                     RGWStreamReadCRF(rados, bucket_info, obj_ctx, _obj), cct(_cct),
                     obj(_obj), mtime(_mtime) {}

  ~RGWLCStreamReadCRF() {};

  void set_multipart(uint64_t part_size, off_t part_off, off_t part_end) {
    multipart = true;
    m_part_size = part_size;
    m_part_off = part_off;
    m_part_end = part_end;
  }

  int init() override {
    optional_yield y = null_yield;
    real_time read_mtime;

    read_op.params.attrs = &attrs;
    read_op.params.lastmod = &read_mtime;
    read_op.params.obj_size = &obj_size;

    int ret = read_op.prepare(y);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: fail to prepare read_op, ret = " << ret << dendl;
      return ret;
    }

    if (read_mtime != mtime) {
      /* raced */
      return -ECANCELED;
    }

    ret = init_rest_obj();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: fail to initialize rest_obj, ret = " << ret << dendl;
      return ret;
    }

    if (!multipart) {
      set_range(0, obj_size - 1);
    } else {
      set_range(m_part_off, m_part_end);
    }
    return 0;
  }

  int init_rest_obj() override {
    /* Initialize rgw_rest_obj. 
     * Reference: do_decode_rest_obj
     * Check how to copy headers content */ 
    rest_obj.init(obj.key);

    if (!multipart) {
      rest_obj.content_len = obj_size;
    } else {
      rest_obj.content_len = m_part_size;
    }

    /* For mulitpart attrs are sent as part of InitMultipartCR itself */
    if (multipart) {
      return 0;
    }

    /*
     * XXX: verify if its right way to copy attrs into
     * rest obj
     */
    init_headers(attrs, rest_obj.attrs);

    rest_obj.acls.set_ctx(cct);
    auto aiter = attrs.find(RGW_ATTR_ACL);
    if (aiter != attrs.end()) {
      bufferlist& bl = aiter->second;
      auto bliter = bl.cbegin();
      try {
        rest_obj.acls.decode(bliter);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: failed to decode policy off attrs" << dendl;
        return -EIO;
      }
    } else {
      ldout(cct, 0) << "WARNING: acl attrs not provided" << dendl;
    }
    return 0;
  }

  int read(off_t ofs, off_t end, bufferlist &bl) {
    optional_yield y = null_yield;

    return read_op.read(ofs, end, bl, y);
  }
};


class RGWLCStreamPutCRF : public RGWStreamWriteHTTPResourceCRF
{
  CephContext *cct;
  RGWHTTPManager *http_manager;
  rgw_lc_obj_properties obj_properties;
  std::shared_ptr<RGWRESTConn> conn;
  rgw::sal::RGWObject* dest_obj;
  string etag;

  public:
  RGWLCStreamPutCRF(CephContext *_cct,
      RGWCoroutinesEnv *_env,
      RGWCoroutine *_caller,
      RGWHTTPManager *_http_manager,
      const rgw_lc_obj_properties&  _obj_properties,
      std::shared_ptr<RGWRESTConn> _conn,
      rgw::sal::RGWObject* _dest_obj) :
    RGWStreamWriteHTTPResourceCRF(_cct, _env, _caller, _http_manager),
    cct(_cct), http_manager(_http_manager), obj_properties(_obj_properties), conn(_conn), dest_obj(_dest_obj) {
    }


  int init() override {
    /* init output connection */
    RGWRESTStreamS3PutObj *out_req{nullptr};

    if (multipart.is_multipart) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%d", multipart.part_num);
      rgw_http_param_pair params[] = { { "uploadId", multipart.upload_id.c_str() },
        { "partNumber", buf },
        { nullptr, nullptr } };
      conn->put_obj_send_init(dest_obj, params, &out_req);
    } else {
      conn->put_obj_send_init(dest_obj, nullptr, &out_req);
    }

    set_req(out_req);

    return RGWStreamWriteHTTPResourceCRF::init();
  }

  static bool keep_attr(const string& h) {
    return (keep_headers.find(h) != keep_headers.end() ||
        boost::algorithm::starts_with(h, "X_AMZ_"));
  }

  static void init_send_attrs(CephContext *cct, const rgw_rest_obj& rest_obj,
                              const rgw_lc_obj_properties& obj_properties,
                              map<string, string> *attrs) {

    map<string, RGWTierACLMapping>& acl_mappings(obj_properties.target_acl_mappings);
    string target_storage_class = obj_properties.target_storage_class;

    auto& new_attrs = *attrs;

    new_attrs.clear();

    for (auto& hi : rest_obj.attrs) {
      if (keep_attr(hi.first)) {
        new_attrs.insert(hi);
      }
    }

    auto acl = rest_obj.acls.get_acl();

    map<int, vector<string> > access_map;

    if (!acl_mappings.empty()) {
      for (auto& grant : acl.get_grant_map()) {
        auto& orig_grantee = grant.first;
        auto& perm = grant.second;

        string grantee;

        const auto& am = acl_mappings;

        auto iter = am.find(orig_grantee);
        if (iter == am.end()) {
          ldout(cct, 20) << "acl_mappings: Could not find " << orig_grantee << " .. ignoring" << dendl;
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

    for (auto aiter : access_map) {
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

      for (auto viter : aiter.second) {
        if (!s.empty()) {
          s.append(", ");
        }
        s.append(viter);
      }

      ldout(cct, 20) << "acl_mappings: set acl: " << header_str << "=" << s << dendl;

      new_attrs[header_str] = s;
    }

    /* Copy target storage class */
    if (!target_storage_class.empty()) {
      new_attrs["x-amz-storage-class"] = target_storage_class;
    } else {
      new_attrs["x-amz-storage-class"] = "STANDARD";
    }

    /* New attribute to specify its transitioned from RGW */
    new_attrs["x-amz-meta-rgwx-source"] = "rgw";

    char buf[32];
    snprintf(buf, sizeof(buf), "%llu", (long long)obj_properties.versioned_epoch);
    new_attrs["x-amz-meta-rgwx-versioned-epoch"] = buf;

    utime_t ut(obj_properties.mtime);
    snprintf(buf, sizeof(buf), "%lld.%09lld",
        (long long)ut.sec(),
        (long long)ut.nsec());

    new_attrs["x-amz-meta-rgwx-source-mtime"] = buf;
    new_attrs["x-amz-meta-rgwx-source-etag"] = obj_properties.etag;
    new_attrs["x-amz-meta-rgwx-source-key"] = rest_obj.key.name;
    if (!rest_obj.key.instance.empty()) {
      new_attrs["x-amz-meta-rgwx-source-version-id"] = rest_obj.key.instance;
    }
  }

  void send_ready(const rgw_rest_obj& rest_obj) override {
    RGWRESTStreamS3PutObj *r = static_cast<RGWRESTStreamS3PutObj *>(req);

    map<string, string> new_attrs;
    if (!multipart.is_multipart) {
      init_send_attrs(cct, rest_obj, obj_properties, &new_attrs);
    }

    r->set_send_length(rest_obj.content_len);

    RGWAccessControlPolicy policy;

    r->send_ready(conn->get_key(), new_attrs, policy, false);
  }

  void handle_headers(const map<string, string>& headers) {
    for (auto h : headers) {
      if (h.first == "ETAG") {
        etag = h.second;
      }
    }
  }

  bool get_etag(string *petag) {
    if (etag.empty()) {
      return false;
    }
    *petag = etag;
    return true;
  }
};



class RGWLCStreamObjToCloudPlainCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;

  std::shared_ptr<RGWStreamReadCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

  std::shared_ptr<rgw::sal::RGWRadosBucket> dest_bucket;
  std::shared_ptr<rgw::sal::RGWRadosObject> dest_obj;

  public:
  RGWLCStreamObjToCloudPlainCR(RGWLCCloudTierCtx& _tier_ctx)
    : RGWCoroutine(_tier_ctx.cct), tier_ctx(_tier_ctx) {}

  int operate() override {
    rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime,
        tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch,
        tier_ctx.acl_mappings,
        tier_ctx.target_storage_class);

    rgw_bucket target_bucket;
    string target_obj_name;

    target_bucket.name = tier_ctx.target_bucket_name;
    target_obj_name = tier_ctx.obj.key.name; // cross check with aws module

    dest_bucket.reset(new rgw::sal::RGWRadosBucket(tier_ctx.store, target_bucket));

    dest_obj.reset(new rgw::sal::RGWRadosObject(tier_ctx.store, rgw_obj_key(target_obj_name),
                   (rgw::sal::RGWRadosBucket *)(dest_bucket.get())));
    rgw::sal::RGWObject *o = static_cast<rgw::sal::RGWObject *>(dest_obj.get());


    reenter(this) {
      /* Prepare Read from source */
      in_crf.reset(new RGWLCStreamReadCRF(tier_ctx.cct, tier_ctx.store->getRados(), tier_ctx.bucket_info,
                   tier_ctx.rctx, tier_ctx.obj, tier_ctx.o.meta.mtime));

      out_crf.reset(new RGWLCStreamPutCRF((CephContext *)(tier_ctx.cct), get_env(), this,
                    (RGWHTTPManager*)(tier_ctx.http_manager), obj_properties, tier_ctx.conn, o));

      /* actual Read & Write */
      yield call(new RGWStreamWriteCR(cct, (RGWHTTPManager*)(tier_ctx.http_manager), in_crf, out_crf));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      return set_cr_done(); 
    }

    return 0;
  }
};

class RGWLCStreamObjToCloudMultipartPartCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;

  string upload_id;

  rgw_lc_multipart_part_info part_info;

  string *petag;
  std::shared_ptr<RGWStreamReadCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

  std::shared_ptr<rgw::sal::RGWRadosBucket> dest_bucket;
  std::shared_ptr<rgw::sal::RGWRadosObject> dest_obj;

  public:
  RGWLCStreamObjToCloudMultipartPartCR(RGWLCCloudTierCtx& _tier_ctx, const string& _upload_id,
                                       const rgw_lc_multipart_part_info& _part_info,
                                       string *_petag) : RGWCoroutine(_tier_ctx.cct), tier_ctx(_tier_ctx),
                                       upload_id(_upload_id), part_info(_part_info), petag(_petag) {}

  int operate() override {
    rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
                                         tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
                                         tier_ctx.target_storage_class);
    rgw_bucket target_bucket;
    string target_obj_name;
    off_t end;

    target_bucket.name = tier_ctx.target_bucket_name;
    target_obj_name = tier_ctx.obj.key.name; // cross check with aws module

    dest_bucket.reset(new rgw::sal::RGWRadosBucket(tier_ctx.store, target_bucket));

    dest_obj.reset(new rgw::sal::RGWRadosObject(tier_ctx.store, rgw_obj_key(target_obj_name),
                   (rgw::sal::RGWRadosBucket *)(dest_bucket.get())));

    reenter(this) {
      /* Prepare Read from source */
      in_crf.reset(new RGWLCStreamReadCRF(tier_ctx.cct, tier_ctx.store->getRados(),
                   tier_ctx.bucket_info, tier_ctx.rctx, tier_ctx.obj, tier_ctx.o.meta.mtime));

      end = part_info.ofs + part_info.size - 1;
      std::static_pointer_cast<RGWLCStreamReadCRF>(in_crf)->set_multipart(part_info.size, part_info.ofs, end);

      /* Prepare write */
      out_crf.reset(new RGWLCStreamPutCRF((CephContext *)(tier_ctx.cct), get_env(), this,
                    (RGWHTTPManager*)(tier_ctx.http_manager), obj_properties, tier_ctx.conn,
                    static_cast<rgw::sal::RGWObject *>(dest_obj.get())));

      out_crf->set_multipart(upload_id, part_info.part_num, part_info.size);

      /* actual Read & Write */
      yield call(new RGWStreamWriteCR(cct, (RGWHTTPManager*)(tier_ctx.http_manager), in_crf, out_crf));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      if (!(static_cast<RGWLCStreamPutCRF *>(out_crf.get()))->get_etag(petag)) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to get etag from PUT request" << dendl;
        return set_cr_error(-EIO);
      }

      return set_cr_done(); 
    }

    return 0;
  }
};

class RGWLCAbortMultipartCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  string upload_id;

  public:
  RGWLCAbortMultipartCR(CephContext *_cct, RGWHTTPManager *_http_manager,
                        RGWRESTConn *_dest_conn, const rgw_obj& _dest_obj,
                        const string& _upload_id) : RGWCoroutine(_cct),
                        cct(_cct), http_manager(_http_manager),
                        dest_conn(_dest_conn), dest_obj(_dest_obj),
                        upload_id(_upload_id) {}

  int operate() override {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };
        bufferlist bl;
        call(new RGWDeleteRESTResourceCR(cct, dest_conn, http_manager,
                                         obj_to_aws_path(dest_obj), params));
      }

      if (retcode < 0) {
        ldout(cct, 0) << "ERROR: failed to abort multipart upload for dest object=" << dest_obj << " (retcode=" << retcode << ")" << dendl;
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWLCInitMultipartCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  uint64_t obj_size;
  map<string, string> attrs;

  bufferlist out_bl;

  string *upload_id;

  struct InitMultipartResult {
    string bucket;
    string key;
    string upload_id;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("UploadId", upload_id, obj);
    }
  } result;

  public:
  RGWLCInitMultipartCR(CephContext *_cct, RGWHTTPManager *_http_manager,
                       RGWRESTConn *_dest_conn, const rgw_obj& _dest_obj,
                       uint64_t _obj_size, const map<string, string>& _attrs,
                       string *_upload_id) : RGWCoroutine(_cct), cct(_cct),
                       http_manager(_http_manager), dest_conn(_dest_conn),
                       dest_obj(_dest_obj), obj_size(_obj_size),
                       attrs(_attrs), upload_id(_upload_id) {}

  int operate() override {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploads", nullptr }, {nullptr, nullptr} };
        bufferlist bl;
        call(new RGWPostRawRESTResourceCR <bufferlist> (cct, dest_conn, http_manager,
              obj_to_aws_path(dest_obj), params, &attrs, bl, &out_bl));
      }

      if (retcode < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
        return set_cr_error(retcode);
      }
      {
        /*
         * If one of the following fails we cannot abort upload, as we cannot
         * extract the upload id. If one of these fail it's very likely that that's
         * the least of our problem.
         */
        RGWXMLDecoder::XMLParser parser;
        if (!parser.init()) {
          ldout(cct, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
          return set_cr_error(-EIO);
        }

        if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        try {
          RGWXMLDecoder::decode_xml("InitiateMultipartUploadResult", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }
      }

      ldout(cct, 20) << "init multipart result: bucket=" << result.bucket << " key=" << result.key << " upload_id=" << result.upload_id << dendl;

      *upload_id = result.upload_id;

      return set_cr_done();
    }

    return 0;
  }
};

class RGWLCCompleteMultipartCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  bufferlist out_bl;

  string upload_id;

  struct CompleteMultipartReq {
    map<int, rgw_lc_multipart_part_info> parts;

    explicit CompleteMultipartReq(const map<int, rgw_lc_multipart_part_info>& _parts) : parts(_parts) {}

    void dump_xml(Formatter *f) const {
      for (auto p : parts) {
        f->open_object_section("Part");
        encode_xml("PartNumber", p.first, f);
        encode_xml("ETag", p.second.etag, f);
        f->close_section();
      };
    }
  } req_enc;

  struct CompleteMultipartResult {
    string location;
    string bucket;
    string key;
    string etag;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Location", bucket, obj);
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("ETag", etag, obj);
    }
  } result;

  public:
  RGWLCCompleteMultipartCR(CephContext *_cct, RGWHTTPManager *_http_manager,
                           RGWRESTConn *_dest_conn, const rgw_obj& _dest_obj,
                           string _upload_id, const map<int, rgw_lc_multipart_part_info>& _parts) :
                           RGWCoroutine(_cct), cct(_cct), http_manager(_http_manager),
                           dest_conn(_dest_conn), dest_obj(_dest_obj), upload_id(_upload_id),
                           req_enc(_parts) {}

  int operate() override {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };
        stringstream ss;
        XMLFormatter formatter;

        encode_xml("CompleteMultipartUpload", req_enc, &formatter);

        formatter.flush(ss);

        bufferlist bl;
        bl.append(ss.str());

        call(new RGWPostRawRESTResourceCR <bufferlist> (cct, dest_conn, http_manager,
              obj_to_aws_path(dest_obj), params, nullptr, bl, &out_bl));
      }

      if (retcode < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
        return set_cr_error(retcode);
      }
      {
        /*
         * If one of the following fails we cannot abort upload, as we cannot
         * extract the upload id. If one of these fail it's very likely that that's
         * the least of our problem.
         */
        RGWXMLDecoder::XMLParser parser;
        if (!parser.init()) {
          ldout(cct, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
          return set_cr_error(-EIO);
        }

        if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        try {
          RGWXMLDecoder::decode_xml("CompleteMultipartUploadResult", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }
      }

      ldout(cct, 20) << "complete multipart result: location=" << result.location << " bucket=" << result.bucket << " key=" << result.key << " etag=" << result.etag << dendl;

      return set_cr_done();
    }

    return 0;
  }
};


class RGWLCStreamAbortMultipartUploadCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;
  const rgw_obj dest_obj;
  const rgw_raw_obj status_obj;

  string upload_id;

  public:

  RGWLCStreamAbortMultipartUploadCR(RGWLCCloudTierCtx& _tier_ctx,
                                    const rgw_obj& _dest_obj, const rgw_raw_obj& _status_obj,
                                    const string& _upload_id) : RGWCoroutine(_tier_ctx.cct),
                                    tier_ctx(_tier_ctx), dest_obj(_dest_obj), status_obj(_status_obj),
                                    upload_id(_upload_id) {}

  int operate() override {
    reenter(this) {
      yield call(new RGWLCAbortMultipartCR(tier_ctx.cct, tier_ctx.http_manager, tier_ctx.conn.get(), dest_obj, upload_id));
      if (retcode < 0) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to abort multipart upload dest obj=" << dest_obj << " upload_id=" << upload_id << " retcode=" << retcode << dendl;
        /* ignore error, best effort */
      }
      yield call(new RGWRadosRemoveCR(tier_ctx.store, status_obj));
      if (retcode < 0) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to remove sync status obj obj=" << status_obj << " retcode=" << retcode << dendl;
        /* ignore error, best effort */
      }
      return set_cr_done();
    }

    return 0;
  }
};

class RGWLCStreamObjToCloudMultipartCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;
  RGWRESTConn *source_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  uint64_t obj_size;
  string src_etag;
  rgw_rest_obj rest_obj;

  rgw_lc_multipart_upload_info status;

  map<string, string> new_attrs;

  rgw_lc_multipart_part_info *pcur_part_info{nullptr};

  int ret_err{0};

  rgw_raw_obj status_obj;

  public:
  RGWLCStreamObjToCloudMultipartCR(RGWLCCloudTierCtx& _tier_ctx) : RGWCoroutine(_tier_ctx.cct),  tier_ctx(_tier_ctx) {}

  int operate() override {
    rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime,
        tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch,
        tier_ctx.acl_mappings,
        tier_ctx.target_storage_class);

    rgw_obj& obj = tier_ctx.obj;
    obj_size = tier_ctx.o.meta.size;

    rgw_bucket target_bucket;
    target_bucket.name = tier_ctx.target_bucket_name;
    string target_obj_name = obj.key.name; // cross check with aws module
    rgw_obj dest_obj(target_bucket, target_obj_name);
    std::shared_ptr<RGWStreamReadCRF> in_crf;
    rgw_rest_obj rest_obj;

    status_obj = rgw_raw_obj(tier_ctx.store->svc()->zone->get_zone_params().log_pool,
        "lc_multipart_" + obj.get_oid());

    reenter(this) {
      yield call(new RGWSimpleRadosReadCR<rgw_lc_multipart_upload_info>(tier_ctx.store->svc()->rados->get_async_processor(), tier_ctx.store->svc()->sysobj,
            status_obj, &status, false));

      if (retcode < 0 && retcode != -ENOENT) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to read sync status of object " << src_obj << " retcode=" << retcode << dendl;
        return retcode;
      }

      if (retcode >= 0) {
        /* check here that mtime and size did not change */
        if (status.mtime != obj_properties.mtime || status.obj_size != obj_size ||
            status.etag != obj_properties.etag) {
          yield call(new RGWLCStreamAbortMultipartUploadCR(tier_ctx, dest_obj, status_obj, status.upload_id));
          retcode = -ENOENT;
        }
      }

      if (retcode == -ENOENT) {
        in_crf.reset(new RGWLCStreamReadCRF(tier_ctx.cct, tier_ctx.store->getRados(), tier_ctx.bucket_info, tier_ctx.rctx, tier_ctx.obj, tier_ctx.o.meta.mtime));

        in_crf->init();

        rest_obj = in_crf->get_rest_obj();

        RGWLCStreamPutCRF::init_send_attrs(tier_ctx.cct, rest_obj, obj_properties, &new_attrs);

        yield call(new RGWLCInitMultipartCR(tier_ctx.cct, tier_ctx.http_manager, tier_ctx.conn.get(), dest_obj, obj_size, std::move(new_attrs), &status.upload_id));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        status.obj_size = obj_size;
        status.mtime = obj_properties.mtime;
        status.etag = obj_properties.etag;
#define MULTIPART_MAX_PARTS 10000
        uint64_t min_part_size = obj_size / MULTIPART_MAX_PARTS;
        uint64_t min_conf_size = tier_ctx.multipart_min_part_size;

        if (min_conf_size < MULTIPART_MIN_POSSIBLE_PART_SIZE) {
          min_conf_size = MULTIPART_MIN_POSSIBLE_PART_SIZE;
        }

        status.part_size = std::max(min_conf_size, min_part_size);
        status.num_parts = (obj_size + status.part_size - 1) / status.part_size;
        status.cur_part = 1;
      }

      for (; (uint32_t)status.cur_part <= status.num_parts; ++status.cur_part) {
        ldout(tier_ctx.cct, 20) << "status.cur_part = "<<status.cur_part <<", info.ofs = "<< status.cur_ofs <<", info.size = "<< status.part_size<< ", obj size = " << status.obj_size<<dendl;
        yield {
          rgw_lc_multipart_part_info& cur_part_info = status.parts[status.cur_part];
          cur_part_info.part_num = status.cur_part;
          cur_part_info.ofs = status.cur_ofs;
          cur_part_info.size = std::min((uint64_t)status.part_size, status.obj_size - status.cur_ofs);

          pcur_part_info = &cur_part_info;

          status.cur_ofs += status.part_size;

          call(new RGWLCStreamObjToCloudMultipartPartCR(tier_ctx,
                status.upload_id,
                cur_part_info,
                &cur_part_info.etag));
        }

        if (retcode < 0) {
          ldout(tier_ctx.cct, 0) << "ERROR: failed to sync obj=" << tier_ctx.obj << ", sync via multipart upload, upload_id=" << status.upload_id << " part number " << status.cur_part << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
          ret_err = retcode;
          yield call(new RGWLCStreamAbortMultipartUploadCR(tier_ctx, dest_obj, status_obj, status.upload_id));
          return set_cr_error(ret_err);
        }

        yield call(new RGWSimpleRadosWriteCR<rgw_lc_multipart_upload_info>(tier_ctx.store->svc()->rados->get_async_processor(), tier_ctx.store->svc()->sysobj, status_obj, status));
        if (retcode < 0) {
          ldout(tier_ctx.cct, 0) << "ERROR: failed to store multipart upload state, retcode=" << retcode << dendl;
          /* continue with upload anyway */
        }
        ldout(tier_ctx.cct, 0) << "sync of object=" << tier_ctx.obj << " via multipart upload, finished sending part #" << status.cur_part << " etag=" << pcur_part_info->etag << dendl;
      }

      yield call(new RGWLCCompleteMultipartCR(tier_ctx.cct, tier_ctx.http_manager, tier_ctx.conn.get(), dest_obj, status.upload_id, status.parts));
      if (retcode < 0) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to complete multipart upload of obj=" << tier_ctx.obj << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
        ret_err = retcode;
        yield call(new RGWLCStreamAbortMultipartUploadCR(tier_ctx, dest_obj, status_obj, status.upload_id));
        return set_cr_error(ret_err);
      }

      /* remove status obj */
      yield call(new RGWRadosRemoveCR(tier_ctx.store, status_obj));
      if (retcode < 0) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to abort multipart upload obj=" << tier_ctx.obj << " upload_id=" << status.upload_id << " part number " << status.cur_part << " (" << cpp_strerror(-retcode) << ")" << dendl;
        /* ignore error, best effort */
      }
      return set_cr_done();
    }
    return 0;
  }
};

int RGWLCCloudCheckCR::operate() {
  /* Check if object has already been transitioned */
  rgw_lc_obj_properties obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
                                       tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
                                       tier_ctx.target_storage_class);

  rgw_bucket target_bucket;
  string target_obj_name;

  target_bucket.name = tier_ctx.target_bucket_name;
  target_obj_name = tier_ctx.obj.key.name; // cross check with aws module

  std::shared_ptr<rgw::sal::RGWRadosBucket> dest_bucket;
  dest_bucket.reset(new rgw::sal::RGWRadosBucket(tier_ctx.store, target_bucket));

  std::shared_ptr<rgw::sal::RGWRadosObject> dest_obj;
  dest_obj.reset(new rgw::sal::RGWRadosObject(tier_ctx.store, rgw_obj_key(target_obj_name), (rgw::sal::RGWRadosBucket *)(dest_bucket.get())));


  std::shared_ptr<RGWLCStreamGetCRF> get_crf;
  get_crf.reset(new RGWLCStreamGetCRF((CephContext *)(tier_ctx.cct), get_env(), this,
        (RGWHTTPManager*)(tier_ctx.http_manager),
        obj_properties, tier_ctx.conn, static_cast<rgw::sal::RGWObject *>(dest_obj.get())));
  int ret = 0;

  reenter(this) {
    /* Having yield here doesn't seem to wait for init2() to fetch the headers
     * before calling is_already_tiered() below
     */
    ret = get_crf->init();
    if (ret < 0) {
      ldout(tier_ctx.cct, 0) << "ERROR: failed to fetch HEAD from cloud for obj=" << tier_ctx.obj << " , ret = " << ret << dendl;
      return set_cr_error(ret);
    }
    if ((static_cast<RGWLCStreamGetCRF *>(get_crf.get()))->is_already_tiered()) {
      *already_tiered = true;
      ldout(tier_ctx.cct, 20) << "is_already_tiered true" << dendl;
      return set_cr_done(); 
    }

    ldout(tier_ctx.cct, 20) << "is_already_tiered false..going with out_crf writing" << dendl;

    return set_cr_done();
  }
  return 0;
}

map <pair<string, string>, utime_t> target_buckets;

int RGWLCCloudTierCR::operate() {
  pair<string, string> key(tier_ctx.storage_class, tier_ctx.target_bucket_name);
  bool bucket_created = false;

  reenter(this) {

    if (target_buckets.find(key) != target_buckets.end()) {
      utime_t t = target_buckets[key];

      utime_t now = ceph_clock_now();

      if (now - t <  (2 * cct->_conf->rgw_lc_debug_interval)) { /* not expired */
        bucket_created = true;
      }
    }

    if (!bucket_created){
      yield {
        ldout(tier_ctx.cct,10) << "Cloud_tier_ctx: creating bucket:" << tier_ctx.target_bucket_name << dendl;
        bufferlist bl;
        call(new RGWPutRawRESTResourceCR <bufferlist> (tier_ctx.cct, tier_ctx.conn.get(),
             tier_ctx.http_manager,
             tier_ctx.target_bucket_name, nullptr, bl, &out_bl));
      }
      if (retcode < 0 ) {
        ldout(tier_ctx.cct, 0) << "ERROR: failed to create target bucket: " << tier_ctx.target_bucket_name << dendl;
        return set_cr_error(retcode);
      }
      if (out_bl.length() > 0) {
        RGWXMLDecoder::XMLParser parser;
        if (!parser.init()) {
          ldout(tier_ctx.cct, 0) << "ERROR: failed to initialize xml parser for parsing create_bucket response from server" << dendl;
          return set_cr_error(-EIO);
        }

        if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(tier_ctx.cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        try {
          RGWXMLDecoder::decode_xml("Error", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(tier_ctx.cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        if (result.code != "BucketAlreadyOwnedByYou") {
          ldout(tier_ctx.cct, 0) << "ERROR: Creating target bucket failed with error: " << result.code << dendl;
          return set_cr_error(-EIO);
        }
      }

      target_buckets[key] = ceph_clock_now();
    }

    yield {
      uint64_t size = tier_ctx.o.meta.size;
      uint64_t multipart_sync_threshold = tier_ctx.multipart_sync_threshold;

      if (multipart_sync_threshold < MULTIPART_MIN_POSSIBLE_PART_SIZE) {
        multipart_sync_threshold = MULTIPART_MIN_POSSIBLE_PART_SIZE;
      }

      if (size < multipart_sync_threshold) {
        call (new RGWLCStreamObjToCloudPlainCR(tier_ctx));
      } else {
        tier_ctx.is_multipart_upload = true;
        call(new RGWLCStreamObjToCloudMultipartCR(tier_ctx));

      } 
    }

    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    return set_cr_done();
  } //reenter

  return 0;
}

