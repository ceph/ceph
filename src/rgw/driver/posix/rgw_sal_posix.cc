// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_posix.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

static inline User* nextUser(User* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterUser*>(t)->get_next();
}

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<POSIXBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<POSIXObject*>(t)->get_next();
}

int POSIXDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  POSIXDriver::initialize(cct, dpp);

  return 0;
}

std::unique_ptr<User> POSIXDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<POSIXUser>(std::move(user), this);
}

int POSIXDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new FilterUser(std::move(nu));
  user->reset(u);
  return 0;
}

int POSIXDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_email(dpp, email, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new FilterUser(std::move(nu));
  user->reset(u);
  return 0;
}

int POSIXDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_swift(dpp, user_str, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new FilterUser(std::move(nu));
  user->reset(u);
  return 0;
}

std::unique_ptr<Object> POSIXDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<POSIXObject>(std::move(o), this);
}

int POSIXDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(dpp, nu, b, &nb, y);
  if (ret != 0)
    return ret;

  Bucket* fb = new FilterBucket(std::move(nb), u);
  bucket->reset(fb);
  return 0;
}

int POSIXDriver::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(nu, i, &nb);
  if (ret != 0)
    return ret;

  Bucket* fb = new FilterBucket(std::move(nb), u);
  bucket->reset(fb);
  return 0;
}

int POSIXDriver::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(dpp, nu, tenant, name, &nb, y);
  if (ret != 0)
    return ret;

  Bucket* fb = new FilterBucket(std::move(nb), u);
  bucket->reset(fb);
  return 0;
}

std::unique_ptr<Writer> POSIXDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  std::unique_ptr<Object> no = nextObject(_head_obj.get())->clone();

  std::unique_ptr<Writer> writer = next->get_append_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   unique_tag, position,
							   cur_accounted_size);

  return std::make_unique<FilterWriter>(std::move(writer), std::move(_head_obj));
}

std::unique_ptr<Writer> POSIXDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Object> no = nextObject(_head_obj.get())->clone();

  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<POSIXWriter>(std::move(writer), std::move(_head_obj), this);
}

void POSIXDriver::finalize(void)
{
  next->finalize();
}

void POSIXDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  return next->register_admin_apis(mgr);
}

int POSIXUser::list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &buckets, optional_yield y)
{
  BucketList bl;
  int ret;

  buckets.clear();
  ret = next->list_buckets(dpp, marker, end_marker, max, need_stats, bl, y);
  if (ret < 0)
    return ret;

  buckets.set_truncated(bl.is_truncated());
  for (auto& ent : bl.get_buckets()) {
    buckets.add(std::make_unique<POSIXBucket>(std::move(ent.second), this, driver));
  }

  return 0;
}

int POSIXUser::create_bucket(const DoutPrefixProvider* dpp,
			      const rgw_bucket& b,
			      const std::string& zonegroup_id,
			      rgw_placement_rule& placement_rule,
			      std::string& swift_ver_location,
			      const RGWQuotaInfo * pquota_info,
			      const RGWAccessControlPolicy& policy,
			      Attrs& attrs,
			      RGWBucketInfo& info,
			      obj_version& ep_objv,
			      bool exclusive,
			      bool obj_lock_enabled,
			      bool* existed,
			      req_info& req_info,
			      std::unique_ptr<Bucket>* bucket_out,
			      optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;

  ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule, swift_ver_location, pquota_info, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, existed, req_info, &nb, y);
  if (ret < 0)
    return ret;

  Bucket* fb = new POSIXBucket(std::move(nb), this, driver);
  bucket_out->reset(fb);
  return 0;
}

int POSIXUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->read_attrs(dpp, y);
}

int POSIXUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int POSIXUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->load_user(dpp, y);
}

int POSIXUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  return next->store_user(dpp, y, exclusive, old_info);
}

int POSIXUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->remove_user(dpp, y);
}

std::unique_ptr<Object> POSIXBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<POSIXObject>(std::move(o), this, driver);
}

int POSIXBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		       ListResults& results, optional_yield y)
{
  return next->list(dpp, params, max, results, y);
}

int POSIXBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int POSIXBucket::remove_bucket(const DoutPrefixProvider* dpp,
				bool delete_children,
				bool forward_to_master,
				req_info* req_info,
				optional_yield y)
{
  return next->remove_bucket(dpp, delete_children, forward_to_master, req_info, y);
}

int POSIXBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
			      bool get_stats)
{
  return next->load_bucket(dpp, y, get_stats);
}

std::unique_ptr<MultipartUpload> POSIXBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  std::unique_ptr<MultipartUpload> nmu =
    next->get_multipart_upload(oid, upload_id, owner, mtime);

  return std::make_unique<POSIXMultipartUpload>(std::move(nmu), this, driver);
}

int POSIXBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated)
{
  std::vector<std::unique_ptr<MultipartUpload>> nup;
  int ret;

  ret = next->list_multiparts(dpp, prefix, marker, delim, max_uploads, nup,
			      common_prefixes, is_truncated);
  if (ret < 0)
    return ret;

  for (auto& ent : nup) {
    uploads.emplace_back(std::make_unique<POSIXMultipartUpload>(std::move(ent), this, driver));
  }

  return 0;
}

int POSIXBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct)
{
  return next->abort_multiparts(dpp, cct);
}

int POSIXObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				bool prevent_versioning)
{
  return next->delete_object(dpp, y, prevent_versioning);
}

int POSIXObject::copy_object(User* user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  return next->copy_object(user, info, source_zone,
                           nextObject(dest_object),
                           nextBucket(dest_bucket),
                           nextBucket(src_bucket),
                           dest_placement, src_mtime, mtime,
                           mod_ptr, unmod_ptr, high_precision_time, if_match,
                           if_nomatch, attrs_mod, copy_if_newer, attrs,
                           category, olh_epoch, delete_at, version_id, tag,
                           etag, progress_cb, progress_data, dpp, y);
}

int POSIXObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y)
{
  return next->set_obj_attrs(dpp, setattrs, delattrs, y);
}

int POSIXObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  return next->get_obj_attrs(y, dpp, target_obj);
}

int POSIXObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp)
{
  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);
}

int POSIXObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  return next->delete_obj_attrs(dpp, attr_name, y);
}

std::unique_ptr<Object::ReadOp> POSIXObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<POSIXReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> POSIXObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<POSIXDeleteOp>(std::move(d), this);
}

int POSIXObject::POSIXReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  return next->prepare(y, dpp);
}

int POSIXObject::POSIXReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = next->read(ofs, end, bl, y, dpp);
  if (ret < 0)
    return ret;

  /* Copy params out of next */
  params = next->params;
  return ret;
}

int POSIXObject::POSIXReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int ret = next->iterate(dpp, ofs, end, cb, y);
  if (ret < 0)
    return ret;

  /* Copy params out of next */
  params = next->params;
  return ret;
}

int POSIXObject::POSIXReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  return next->get_attr(dpp, name, dest, y);
}

int POSIXObject::POSIXDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  return next->delete_obj(dpp, y);
}

int POSIXMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  return next->init(dpp, y, owner, dest_placement, attrs);
}

int POSIXMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated,
				      bool assume_unsorted)
{
  int ret;

  ret = next->list_parts(dpp, cct, num_parts, marker, next_marker, truncated,
			 assume_unsorted);
  if (ret < 0)
    return ret;

  parts.clear();

  for (auto& ent : next->get_parts()) {
    parts.emplace(ent.first, std::make_unique<FilterMultipartPart>(std::move(ent.second)));
  }

  return 0;
}

int POSIXMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct)
{
  return next->abort(dpp, cct);
}

int POSIXMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj)
{
  return next->complete(dpp, y, cct, part_etags, remove_objs, accounted_size,
			compressed, cs_info, ofs, tag, owner, olh_epoch,
			nextObject(target_obj));
}

std::unique_ptr<Writer> POSIXMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  std::unique_ptr<Object> no = nextObject(_head_obj.get())->clone();

  std::unique_ptr<Writer> writer;
  writer = next->get_writer(dpp, y, std::move(no), owner,
			    ptail_placement_rule, part_num, part_num_str);

  return std::make_unique<POSIXWriter>(std::move(writer), std::move(_head_obj), driver);
}

int POSIXWriter::prepare(optional_yield y)
{
  return next->prepare(y);
}

int POSIXWriter::process(bufferlist&& data, uint64_t offset)
{
  return next->process(std::move(data), offset);
}

int POSIXWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  return next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, y);
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newPOSIXDriver(rgw::sal::Driver* next)
{
  rgw::sal::POSIXDriver* driver = new rgw::sal::POSIXDriver(next);

  return driver;
}

}
