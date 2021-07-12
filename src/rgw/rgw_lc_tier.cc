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

static int read_sync_status(const DoutPrefixProvider *dpp, rgw::sal::Store *store,
    const rgw_raw_obj *status_obj, rgw_lc_multipart_upload_info *status)
{
  int retcode = 0;
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(store);

  if (!rados) {
    ldpp_dout(dpp, 0) << "ERROR: Not a RadosStore. Cannot be transitioned to cloud." << dendl;
    return -1;
  }

  auto& pool = status_obj->pool;
  const auto oid = status_obj->oid;
  auto obj_ctx = rados->svc()->sysobj->init_obj_ctx();
  bufferlist bl;

  retcode = rgw_get_system_obj(obj_ctx, pool, oid, bl, nullptr, nullptr,
      null_yield, dpp);

  if (retcode < 0) {
    return retcode;
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

static int put_sync_status(const DoutPrefixProvider *dpp, rgw::sal::Store *store,
    const rgw_raw_obj *status_obj, rgw_lc_multipart_upload_info *status)
{
  int retcode = 0;
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(store);

  if (!rados) {
    ldpp_dout(dpp, 0) << "ERROR: Not a RadosStore. Cannot be transitioned to cloud." << dendl;
    return -1;
  }

  auto& pool = status_obj->pool;
  const auto oid = status_obj->oid;
  auto obj_ctx = rados->svc()->sysobj->init_obj_ctx();
  bufferlist bl;
  status->encode(bl);

  retcode = rgw_put_system_obj(dpp, obj_ctx, pool, oid, bl, true, nullptr,
      real_time{}, null_yield);

  return retcode;
}

static int delete_sync_status(const DoutPrefixProvider *dpp, rgw::sal::Store *store,
    const rgw_raw_obj *status_obj)
{
  int retcode = 0;
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(store);

  if (!rados) {
    ldpp_dout(dpp, 0) << "ERROR: Not a RadosStore. Cannot be transitioned to cloud." << dendl;
    return -1;
  }

  auto& pool = status_obj->pool;
  const auto oid = status_obj->oid;
  auto sysobj = rados->svc()->sysobj;

  retcode = rgw_delete_system_obj(dpp, sysobj, pool, oid, nullptr,
      null_yield);

  return retcode;
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

int RGWLCCloudStreamGet::init(const DoutPrefixProvider *dpp)  {
  /* init input connection */
  req_params.get_op = !head;
  req_params.prepend_metadata = true;
  req_params.rgwx_stat = true;
  req_params.sync_manifest = true;
  req_params.skip_decrypt = true;

  string etag;
  real_time set_mtime;

  int ret = conn->get_obj(dpp, dest_obj, req_params, true /* send */, &in_req);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): conn->get_obj() returned ret=" << ret << dendl;
    return ret;
  }

  /* fetch only headers */
  ret = conn->complete_request(in_req, nullptr, nullptr, nullptr, nullptr, &headers, null_yield);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 20) << "ERROR: " << __func__ << "(): conn->complete_request() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWLCCloudStreamGet::is_already_tiered() {
  char buf[32];
  map<string, string> attrs = headers;

  for (const auto& a : attrs) {
    ldpp_dout(dpp, 20) << "GetCrf attr[" << a.first << "] = " << a.second <<dendl;
  }
  utime_t ut(obj_properties.mtime);
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

rgw_rest_obj RGWLCStreamRead::get_rest_obj() {
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

  attrs = (*obj)->get_attrs();
  obj_size = (*obj)->get_obj_size();

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
  rest_obj.init((*obj)->get_key());

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
   * XXX: verify if its right way to copy attrs into rest obj
   */
  init_headers(attrs, rest_obj.attrs);

  rest_obj.acls.set_ctx(cct);
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
    conn->put_obj_send_init(dest_obj, params, &out_req);
  } else {
    conn->put_obj_send_init(dest_obj, nullptr, &out_req);
  }

  return 0;
}

bool RGWLCCloudStreamPut::keep_attr(const string& h) {
  return (keep_headers.find(h) != keep_headers.end() ||
      boost::algorithm::starts_with(h, "X_AMZ_"));
}

void RGWLCCloudStreamPut::init_send_attrs(const DoutPrefixProvider *dpp,
    const rgw_rest_obj& rest_obj,
    const rgw_lc_obj_properties& obj_properties,
    map<string, string> *attrs) {

  map<string, RGWTierACLMapping>& acl_mappings(obj_properties.target_acl_mappings);
  string target_storage_class = obj_properties.target_storage_class;

  attrs->clear();

  for (auto& hi : rest_obj.attrs) {
    if (keep_attr(hi.first)) {
      attrs->insert(hi);
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

    (*attrs)[header_str] = s;
  }

  /* Copy target storage class */
  if (!target_storage_class.empty()) {
    (*attrs)["x-amz-storage-class"] = target_storage_class;
  } else {
    (*attrs)["x-amz-storage-class"] = "STANDARD";
  }

  /* New attribute to specify its transitioned from RGW */
  (*attrs)["x-amz-meta-rgwx-source"] = "rgw";

  char buf[32];
  snprintf(buf, sizeof(buf), "%llu", (long long)obj_properties.versioned_epoch);
  (*attrs)["x-amz-meta-rgwx-versioned-epoch"] = buf;

  utime_t ut(obj_properties.mtime);
  snprintf(buf, sizeof(buf), "%lld.%09lld",
      (long long)ut.sec(),
      (long long)ut.nsec());

  (*attrs)["x-amz-meta-rgwx-source-mtime"] = buf;
  (*attrs)["x-amz-meta-rgwx-source-etag"] = obj_properties.etag;
  (*attrs)["x-amz-meta-rgwx-source-key"] = rest_obj.key.name;
  if (!rest_obj.key.instance.empty()) {
    (*attrs)["x-amz-meta-rgwx-source-version-id"] = rest_obj.key.instance;
  }
  for (const auto& a : (*attrs)) {
    ldpp_dout(dpp, 30) << "init_send_attrs attr[" << a.first << "] = " << a.second <<dendl;
  }
}

void RGWLCCloudStreamPut::send_ready(const DoutPrefixProvider *dpp, const rgw_rest_obj& rest_obj) {
  auto r = static_cast<RGWRESTStreamS3PutObj *>(out_req);

  map<string, string> new_attrs;
  if (!multipart.is_multipart) {
    init_send_attrs(dpp, rest_obj, obj_properties, &new_attrs);
  }

  r->set_send_length(rest_obj.content_len);

  RGWAccessControlPolicy policy;

  r->send_ready(dpp, conn->get_key(), new_attrs, policy);
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
  int ret = conn->complete_request(out_req, etag, &obj_properties.mtime, null_yield);
  return ret;
}

int RGWLCCloudStreamRW::process(const DoutPrefixProvider* dpp) {
  ret = readf->init();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fail to initialize in_crf, ret = " << ret << dendl;
    return ret;
  }
  readf->get_range(ofs, end);
  rest_obj = readf->get_rest_obj();
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

int RGWLCCloudStreamObjToPlain::process() {
  b.bucket.name = tier_ctx.target_bucket_name;
  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    (*tier_ctx.obj)->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance((*tier_ctx.obj)->get_key());
  }

  retcode = tier_ctx.store->get_bucket(nullptr, b, &dest_bucket);
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize dest_bucket - " << tier_ctx.target_bucket_name << " , retcode = " << retcode << dendl;
    return retcode;
  }

  dest_obj = dest_bucket->get_object(rgw_obj_key(target_obj_name));
  if (!dest_obj) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize dest_object path - " << target_obj_name << dendl;
    return -1;
  }

  o = dest_obj.get();

  (*tier_ctx.obj)->set_atomic(&tier_ctx.rctx);

  /* Prepare Read from source */
  readf.reset(new RGWLCStreamRead(tier_ctx.cct, tier_ctx.dpp,
        tier_ctx.rctx, tier_ctx.obj, tier_ctx.o.meta.mtime));

  writef.reset(new RGWLCCloudStreamPut(tier_ctx.dpp, 
        obj_properties, tier_ctx.conn, o));

  /* actual Read & Write */
  rwf.reset(new RGWLCCloudStreamRW(tier_ctx.dpp, readf, writef));
  retcode = rwf->process(tier_ctx.dpp);

  if (retcode < 0) {
    return retcode;
  }
  return retcode;
}

int RGWLCCloudStreamObjToMultipartPart::process() {
  b.bucket.name = tier_ctx.target_bucket_name;
  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    (*tier_ctx.obj)->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance((*tier_ctx.obj)->get_key());
  }

  retcode = tier_ctx.store->get_bucket(nullptr, b, &dest_bucket);
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize dest_bucket - " << tier_ctx.target_bucket_name << " , retcode = " << retcode << dendl;
    return retcode;
  }

  dest_obj = dest_bucket->get_object(rgw_obj_key(target_obj_name));
  if (!dest_obj) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize dest_object path - " << target_obj_name << dendl;
    return -1;
  }

  (*tier_ctx.obj)->set_atomic(&tier_ctx.rctx);

  /* Prepare Read from source */
  readf.reset(new RGWLCStreamRead(tier_ctx.cct, tier_ctx.dpp, 
        tier_ctx.rctx, tier_ctx.obj, tier_ctx.o.meta.mtime));

  end = part_info.ofs + part_info.size - 1;
  readf->set_multipart(part_info.size, part_info.ofs, end);

  /* Prepare write */
  writef.reset(new RGWLCCloudStreamPut(tier_ctx.dpp, 
        obj_properties, tier_ctx.conn,
        dest_obj.get()));

  writef->set_multipart(upload_id, part_info.part_num, part_info.size);

  /* actual Read & Write */
  rwf.reset(new RGWLCCloudStreamRW(tier_ctx.dpp, readf, writef));
  retcode = rwf->process(tier_ctx.dpp);
  if (retcode < 0) {
    return retcode;
  }

  if (!(static_cast<RGWLCCloudStreamPut *>(writef.get()))->get_etag(petag)) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to get etag from PUT request" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWLCCloudAbortMultipart::process() {
  rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };

  bufferlist bl;
  string resource = obj_to_aws_path(dest_obj);
  retcode = dest_conn->send_resource(dpp, "DELETE", resource, params, nullptr,
      out_bl, &bl, nullptr, null_yield);


  if (retcode < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to abort multipart upload for dest object=" << dest_obj << " (retcode=" << retcode << ")" << dendl;
    return retcode;
  }

  return 0;
}
int RGWLCCloudInitMultipart::process() {
  rgw_http_param_pair params[] = { { "uploads", nullptr }, {nullptr, nullptr} };

  bufferlist bl;
  string resource = obj_to_aws_path(dest_obj);

  retcode = dest_conn->send_resource(dpp, "POST", resource, params, &attrs,
      out_bl, &bl, nullptr, null_yield);

  if (retcode < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
    return retcode;
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

  *upload_id = result.upload_id;

  return 0;
}

int RGWLCCloudCompleteMultipart::process() {
  rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };

  stringstream ss;
  XMLFormatter formatter;

  encode_xml("CompleteMultipartUpload", req_enc, &formatter);

  formatter.flush(ss);

  bufferlist bl;
  bl.append(ss.str());
  string resource = obj_to_aws_path(dest_obj);

  retcode = dest_conn->send_resource(dpp, "POST", resource, params, nullptr,
      out_bl, &bl, nullptr, null_yield);


  if (retcode < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
    return retcode;
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

  return retcode;
}

int RGWLCCloudStreamAbortMultipartUpload::process() {
  abort_mp.reset(new RGWLCCloudAbortMultipart(tier_ctx.dpp, tier_ctx.conn.get(), dest_obj, upload_id));

  retcode = abort_mp->process();
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to abort multipart upload dest obj=" << dest_obj << " upload_id=" << upload_id << " retcode=" << retcode << dendl;
    /* ignore error, best effort */
  }
  /* remove status obj */
  retcode = delete_sync_status(tier_ctx.dpp, tier_ctx.store, &status_obj);
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to remove sync status obj obj=" << status_obj << " retcode=" << retcode << dendl;
    // ignore error, best effort 
  }
  return 0;
}

int RGWLCCloudStreamObjToMultipart::process() {
  obj_size = tier_ctx.o.meta.size;

  target_bucket.name = tier_ctx.target_bucket_name;

  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    (*tier_ctx.obj)->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance((*tier_ctx.obj)->get_key());
  }
  dest_obj.init(target_bucket, target_obj_name);

  status_obj = rgw_raw_obj(tier_ctx.store->get_zone()->get_params().log_pool,
      "lc_multipart_" + (*tier_ctx.obj)->get_oid());

  retcode = read_sync_status(tier_ctx.dpp, tier_ctx.store, &status_obj, &status);

  if (retcode < 0 && retcode != -ENOENT) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to read sync status of object " << src_obj << " retcode=" << retcode << dendl;
    return retcode;
  }

  if (retcode >= 0) {
    // check here that mtime and size did not change 
    if (status.mtime != obj_properties.mtime || status.obj_size != obj_size ||
        status.etag != obj_properties.etag) {
      std::unique_ptr<RGWLCCloudStreamAbortMultipartUpload> abort_mp;
      abort_mp.reset(new RGWLCCloudStreamAbortMultipartUpload(tier_ctx, dest_obj, status_obj, status.upload_id));
      abort_mp->process();
      retcode = -ENOENT;
    }
  }

  if (retcode == -ENOENT) { 
    readf.reset(new RGWLCStreamRead(tier_ctx.cct, tier_ctx.dpp, tier_ctx.rctx, tier_ctx.obj, tier_ctx.o.meta.mtime));

    readf->init();

    rest_obj = readf->get_rest_obj();

    RGWLCCloudStreamPut::init_send_attrs(tier_ctx.dpp, rest_obj, obj_properties, &new_attrs);

    std::unique_ptr<RGWLCCloudInitMultipart> init_mp;
    init_mp.reset(new RGWLCCloudInitMultipart(tier_ctx.dpp, tier_ctx.conn.get(), dest_obj, obj_size, std::move(new_attrs), &status.upload_id));
    retcode = init_mp->process();
    if (retcode < 0) {
      return retcode;
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
    status.cur_ofs = 0;
  }

  for (; (uint32_t)status.cur_part <= status.num_parts; ++status.cur_part) {
    ldpp_dout(tier_ctx.dpp, 20) << "status.cur_part = "<<status.cur_part <<", info.ofs = "<< status.cur_ofs <<", info.size = "<< status.part_size<< ", obj size = " << status.obj_size<< ", status.num_parts:" << status.num_parts << dendl;
    rgw_lc_multipart_part_info& cur_part_info = status.parts[status.cur_part];
    cur_part_info.part_num = status.cur_part;
    cur_part_info.ofs = status.cur_ofs;
    cur_part_info.size = std::min((uint64_t)status.part_size, status.obj_size - status.cur_ofs);

    status.cur_ofs += cur_part_info.size;

    std::unique_ptr<RGWLCCloudStreamObjToMultipartPart> cloud_mp_part;
    cloud_mp_part.reset(new RGWLCCloudStreamObjToMultipartPart(tier_ctx,
            status.upload_id,
            cur_part_info,
            &cur_part_info.etag));
    retcode = cloud_mp_part->process();

    if (retcode < 0) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to sync obj=" << tier_ctx.obj << ", sync via multipart upload, upload_id=" << status.upload_id << " part number " << status.cur_part << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
      std::unique_ptr<RGWLCCloudStreamAbortMultipartUpload> abort_mp;
      abort_mp.reset(new RGWLCCloudStreamAbortMultipartUpload(tier_ctx, dest_obj, status_obj, status.upload_id));
      retcode = abort_mp->process();
      return retcode;
    }

    retcode = put_sync_status(tier_ctx.dpp, tier_ctx.store, &status_obj, &status);

    if (retcode < 0) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to store multipart upload state, retcode=" << retcode << dendl;
      // continue with upload anyway 
    } 
    ldpp_dout(tier_ctx.dpp, 0) << "sync of object=" << tier_ctx.obj << " via multipart upload, finished sending part #" << status.cur_part << " etag=" << status.parts[status.cur_part].etag << dendl;
  }

  std::unique_ptr<RGWLCCloudCompleteMultipart> complete_mp;
  complete_mp.reset(new RGWLCCloudCompleteMultipart(tier_ctx.dpp, tier_ctx.conn.get(), dest_obj, status.upload_id, status.parts));
  retcode = complete_mp->process();
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to complete multipart upload of obj=" << tier_ctx.obj << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
    std::unique_ptr<RGWLCCloudStreamAbortMultipartUpload> abort_mp;
    abort_mp.reset(new RGWLCCloudStreamAbortMultipartUpload(tier_ctx, dest_obj, status_obj, status.upload_id));
    retcode = abort_mp->process();
    return retcode;
  }

  /* remove status obj */
  retcode = delete_sync_status(tier_ctx.dpp, tier_ctx.store, &status_obj);
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to abort multipart upload obj=" << tier_ctx.obj << " upload_id=" << status.upload_id << " part number " << status.cur_part << " (" << cpp_strerror(-retcode) << ")" << dendl;
    // ignore error, best effort 
  }
  return retcode;
}

int RGWLCCloudCheckObj::process() {
  /* Check if object has already been transitioned */
  b.bucket.name = tier_ctx.target_bucket_name;
  target_obj_name = tier_ctx.bucket_info.bucket.name + "/" +
    (*tier_ctx.obj)->get_name();
  if (!tier_ctx.o.is_current()) {
    target_obj_name += get_key_instance((*tier_ctx.obj)->get_key());
  }

  retcode = tier_ctx.store->get_bucket(nullptr, b, &dest_bucket);
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize dest_bucket - " << tier_ctx.target_bucket_name << " , reterr = " << retcode << dendl;
    return ret;
  }

  dest_obj = dest_bucket->get_object(rgw_obj_key(target_obj_name));
  if (!dest_obj) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to initialize dest_object path - " << target_obj_name << dendl;
    return -1;
  }

  getf.reset(new RGWLCCloudStreamGet(tier_ctx.dpp, true, obj_properties,
        tier_ctx.conn, dest_obj.get()));

  retcode = getf->init(tier_ctx.dpp);
  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to fetch HEAD from cloud for obj=" << tier_ctx.obj << " , retcode = " << retcode << dendl;
    return retcode;
  }
  if (getf.get()->is_already_tiered()) {
    *already_tiered = true;
    ldpp_dout(tier_ctx.dpp, 20) << "is_already_tiered true" << dendl;
    return retcode; 
  }

  ldpp_dout(tier_ctx.dpp, 20) << "is_already_tiered false..going with out_crf writing" << dendl;

  return retcode;
}

map <pair<string, string>, utime_t> target_buckets;

int RGWLCCloudTier::process() {
  pair<string, string> key(tier_ctx.storage_class, tier_ctx.target_bucket_name);
  bool bucket_created = false;

  /* Check and create target bucket */
  if (target_buckets.find(key) != target_buckets.end()) {
    utime_t t = target_buckets[key];

    utime_t now = ceph_clock_now();

    if (now - t <  (2 * tier_ctx.cct->_conf->rgw_lc_debug_interval)) { /* not expired */
      bucket_created = true;
    }
  }

  if (!bucket_created){
    ldpp_dout(tier_ctx.dpp, 30) << "Cloud_tier_ctx: creating bucket:" << tier_ctx.target_bucket_name << dendl;
    bufferlist bl;
    string resource = tier_ctx.target_bucket_name;

    retcode = tier_ctx.conn->send_resource(tier_ctx.dpp, "PUT", resource, nullptr, nullptr,
        out_bl, &bl, nullptr, null_yield);

    if (retcode < 0 ) {
      ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to create target bucket: " << tier_ctx.target_bucket_name << ", retcode:" << retcode << dendl;
      return retcode;
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

      if (result.code != "BucketAlreadyOwnedByYou") {
        ldpp_dout(tier_ctx.dpp, 0) << "ERROR: Creating target bucket failed with error: " << result.code << dendl;
        return -EIO;
      }
    }

    target_buckets[key] = ceph_clock_now();
  }

  /* Since multiple zones may try to transition the same object to the cloud,
   * verify if the object is already transitioned. And since its just a best
   * effort, do not bail out in case of any errors.
   */
  bool already_tiered = false;
  chk_cloud.reset(new RGWLCCloudCheckObj(tier_ctx, &already_tiered));

  retcode = chk_cloud->process();

  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed in RGWCloudCheckCR() retcode=" << retcode << dendl;
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
    cloud_tier_plain.reset(new RGWLCCloudStreamObjToPlain(tier_ctx));
    retcode = cloud_tier_plain->process();
  } else {
    tier_ctx.is_multipart_upload = true;
    cloud_tier_mp.reset(new RGWLCCloudStreamObjToMultipart(tier_ctx));
    retcode = cloud_tier_mp->process();
  } 

  if (retcode < 0) {
    ldpp_dout(tier_ctx.dpp, 0) << "ERROR: failed to transition object retcode=" << retcode << dendl;
  }

  return retcode;
}
