// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_data_access.h"
#include "rgw_acl_s3.h"
#include "rgw_aio_throttle.h"
#include "rgw_compression.h"
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_rgw

template<class H, size_t S>
class RGWEtag
{
  H hash;

public:
  RGWEtag() {
    if constexpr (std::is_same_v<H, MD5>) {
      // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
      hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    }
  }

  void update(const char *buf, size_t len) {
    hash.Update((const unsigned char *)buf, len);
  }

  void update(bufferlist& bl) {
    if (bl.length() > 0) {
      update(bl.c_str(), bl.length());
    }
  }

  void update(const std::string& s) {
    if (!s.empty()) {
      update(s.c_str(), s.size());
    }
  }
  void finish(std::string *etag) {
    char etag_buf[S];
    char etag_buf_str[S * 2 + 16];

    hash.Final((unsigned char *)etag_buf);
    buf_to_hex((const unsigned char *)etag_buf, S,
	       etag_buf_str);

    *etag = etag_buf_str;
  }
};

using RGWMD5Etag = RGWEtag<MD5, CEPH_CRYPTO_MD5_DIGESTSIZE>;

RGWDataAccess::RGWDataAccess(rgw::sal::Driver* _driver) : driver(_driver)
{
}

int RGWDataAccess::Bucket::finish_init()
{
  auto iter = attrs.find(RGW_ATTR_ACL);
  if (iter == attrs.end()) {
    return 0;
  }

  bufferlist::const_iterator bliter = iter->second.begin();
  try {
    policy.decode(bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

int RGWDataAccess::Bucket::init(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = sd->driver->load_bucket(dpp, rgw_bucket(tenant, name), &bucket, y);
  if (ret < 0) {
    return ret;
  }

  bucket_info = bucket->get_info();
  mtime = bucket->get_modification_time();
  attrs = bucket->get_attrs();

  return finish_init();
}

int RGWDataAccess::Bucket::init(const RGWBucketInfo& _bucket_info,
				const std::map<std::string, bufferlist>& _attrs)
{
  bucket_info = _bucket_info;
  attrs = _attrs;

  return finish_init();
}

int RGWDataAccess::Bucket::get_object(const rgw_obj_key& key,
				      ObjectRef *obj) {
  obj->reset(new Object(sd, shared_from_this(), key));
  return 0;
}

int RGWDataAccess::Object::put(bufferlist& data,
			       std::map<std::string, bufferlist>& attrs,
                               const DoutPrefixProvider *dpp,
                               optional_yield y)
{
  rgw::sal::Driver* driver = sd->driver;
  CephContext *cct = driver->ctx();

  std::string tag;
  append_rand_alpha(cct, tag, tag, 32);

  RGWBucketInfo& bucket_info = bucket->bucket_info;

  rgw::BlockingAioThrottle aio(driver->ctx()->_conf->rgw_put_obj_min_window_size);

  std::unique_ptr<rgw::sal::Bucket> b = driver->get_bucket(bucket_info);
  std::unique_ptr<rgw::sal::Object> obj = b->get_object(key);

  auto& owner = bucket->policy.get_owner();

  std::string req_id = driver->zone_unique_id(driver->get_new_req_id());

  std::unique_ptr<rgw::sal::Writer> processor;
  processor = driver->get_atomic_writer(dpp, y, obj.get(), owner.id,
				       nullptr, olh_epoch, req_id);

  int ret = processor->prepare(y);
  if (ret < 0)
    return ret;

  rgw::sal::DataProcessor *filter = processor.get();

  CompressorRef plugin;
  boost::optional<RGWPutObj_Compress> compressor;

  const auto& compression_type = driver->get_compression_type(bucket_info.placement_rule);
  if (compression_type != "none") {
    plugin = Compressor::create(driver->ctx(), compression_type);
    if (!plugin) {
      ldpp_dout(dpp, 1) << "Cannot load plugin for compression type "
        << compression_type << dendl;
    } else {
      compressor.emplace(driver->ctx(), plugin, filter);
      filter = &*compressor;
    }
  }

  off_t ofs = 0;
  auto obj_size = data.length();

  RGWMD5Etag etag_calc;

  do {
    size_t read_len = std::min(data.length(), (unsigned int)cct->_conf->rgw_max_chunk_size);

    bufferlist bl;

    data.splice(0, read_len, &bl);
    etag_calc.update(bl);

    ret = filter->process(std::move(bl), ofs);
    if (ret < 0)
      return ret;

    ofs += read_len;
  } while (data.length() > 0);

  ret = filter->process({}, ofs);
  if (ret < 0) {
    return ret;
  }
  bool has_etag_attr = false;
  auto iter = attrs.find(RGW_ATTR_ETAG);
  if (iter != attrs.end()) {
    bufferlist& bl = iter->second;
    etag = bl.to_str();
    has_etag_attr = true;
  }

  if (!aclbl) {
    RGWAccessControlPolicy policy;

    const auto& owner = bucket->policy.get_owner();
    policy.create_default(owner.id, owner.display_name); // default private policy

    policy.encode(aclbl.emplace());
  }

  if (etag.empty()) {
    etag_calc.finish(&etag);
  }

  if (!has_etag_attr) {
    bufferlist etagbl;
    etagbl.append(etag);
    attrs[RGW_ATTR_ETAG] = etagbl;
  }
  attrs[RGW_ATTR_ACL] = *aclbl;

  std::string *puser_data = nullptr;
  if (user_data) {
    puser_data = &(*user_data);
  }

  const req_context rctx{dpp, y, nullptr};
  return processor->complete(obj_size, etag,
			    &mtime, mtime,
			    attrs, delete_at,
                            nullptr, nullptr,
                            puser_data,
                            nullptr, nullptr,
                            rctx, rgw::sal::FLAG_LOG_OP);
}

void RGWDataAccess::Object::set_policy(const RGWAccessControlPolicy& policy)
{
  policy.encode(aclbl.emplace());
}

