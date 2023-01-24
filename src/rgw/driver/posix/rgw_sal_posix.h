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

#pragma once

#include "rgw_sal_filter.h"
#include "common/dout.h"

namespace rgw { namespace sal {

class POSIXDriver : public FilterDriver {
private:

public:
  POSIXDriver(Driver* _next) : FilterDriver(_next)
  { }
  virtual ~POSIXDriver() { }

  virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const
				     std::string& key, optional_yield y,
				     std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp, const
				std::string& email, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const
				std::string& user_str, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  virtual int get_bucket(User* u, const RGWBucketInfo& i,
			 std::unique_ptr<Bucket>* bucket) override;
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const
			 rgw_bucket& b, std::unique_ptr<Bucket>* bucket,
			 optional_yield y) override;
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const
			 std::string& tenant, const std::string& name,
			 std::unique_ptr<Bucket>* bucket, optional_yield y) override;

  virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule
				  *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
  virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;

  virtual void finalize(void) override;
  virtual void register_admin_apis(RGWRESTMgr* mgr) override;
};

class POSIXUser : public FilterUser {
private:
  POSIXDriver* driver;

public:
  POSIXUser(std::unique_ptr<User> _next, POSIXDriver* _driver) :
    FilterUser(std::move(_next)),
    driver(_driver) {}
  virtual ~POSIXUser() = default;

  virtual int list_buckets(const DoutPrefixProvider* dpp,
			   const std::string& marker, const std::string& end_marker,
			   uint64_t max, bool need_stats, BucketList& buckets,
			   optional_yield y) override;
  virtual int create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo* pquota_info,
                            const RGWAccessControlPolicy& policy,
                            Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
                            bool exclusive,
                            bool obj_lock_enabled,
                            bool* existed,
                            req_info& req_info,
                            std::unique_ptr<Bucket>* bucket,
                            optional_yield y) override;
  virtual Attrs& get_attrs() override { return next->get_attrs(); }
  virtual void set_attrs(Attrs& _attrs) override { next->set_attrs(_attrs); }
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs&
				    new_attrs, optional_yield y) override;
  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override;
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;
};

class POSIXBucket : public FilterBucket {
private:
  POSIXDriver* driver;

public:
  POSIXBucket(std::unique_ptr<Bucket> _next, User* _user,
		    POSIXDriver* _driver) :
    FilterBucket(std::move(_next), _user),
    driver(_driver) {}
  virtual ~POSIXBucket() = default;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  virtual int list(const DoutPrefixProvider* dpp, ListParams&, int,
		   ListResults&, optional_yield y) override;
  virtual Attrs& get_attrs(void) override { return next->get_attrs(); }
  virtual int set_attrs(Attrs a) override { return next->set_attrs(a); }
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp,
				    Attrs& new_attrs, optional_yield y) override;
  virtual int remove_bucket(const DoutPrefixProvider* dpp, bool delete_children,
			    bool forward_to_master, req_info* req_info,
			    optional_yield y) override;
  virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
			  bool get_stats = false) override;

  virtual std::unique_ptr<Bucket> clone() override {
    std::unique_ptr<Bucket> nb = next->clone();
    return std::make_unique<POSIXBucket>(std::move(nb), get_owner(), driver);
  }

  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
  virtual int list_multiparts(const DoutPrefixProvider *dpp,
			      const std::string& prefix,
			      std::string& marker,
			      const std::string& delim,
			      const int& max_uploads,
			      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
			      std::map<std::string, bool> *common_prefixes,
			      bool *is_truncated) override;
  virtual int abort_multiparts(const DoutPrefixProvider* dpp,
			       CephContext* cct) override;

};

class POSIXObject : public FilterObject {
private:
  POSIXDriver* driver;

public:
  struct POSIXReadOp : FilterReadOp {
    POSIXObject* source;

    POSIXReadOp(std::unique_ptr<ReadOp> _next, POSIXObject* _source) :
      FilterReadOp(std::move(_next)),
      source(_source) {}
    virtual ~POSIXReadOp() = default;

    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
		     const DoutPrefixProvider* dpp) override;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			 bufferlist& dest, optional_yield y) override;
  };

  struct POSIXDeleteOp : FilterDeleteOp {
    POSIXObject* source;

    POSIXDeleteOp(std::unique_ptr<DeleteOp> _next, POSIXObject* _source) :
      FilterDeleteOp(std::move(_next)),
      source(_source) {}
    virtual ~POSIXDeleteOp() = default;

    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
  };

  POSIXObject(std::unique_ptr<Object> _next, POSIXDriver* _driver) :
    FilterObject(std::move(_next)),
    driver(_driver) {}
  POSIXObject(std::unique_ptr<Object> _next, Bucket* _bucket, POSIXDriver* _driver) :
    FilterObject(std::move(_next), _bucket),
    driver(_driver) {}
  POSIXObject(POSIXObject& _o) :
    FilterObject(_o),
    driver(_o.driver) {}
  virtual ~POSIXObject() = default;

  virtual int delete_object(const DoutPrefixProvider* dpp,
			    optional_yield y,
			    bool prevent_versioning = false) override;
  virtual int copy_object(User* user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
               boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
			    Attrs* delattrs, optional_yield y) override;
  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
			    rgw_obj* target_obj = NULL) override;
  virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
			       optional_yield y, const DoutPrefixProvider* dpp) override;
  virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
			       optional_yield y) override;

  virtual std::unique_ptr<ReadOp> get_read_op() override;
  virtual std::unique_ptr<DeleteOp> get_delete_op() override;

};

class POSIXMultipartUpload : public FilterMultipartUpload {
protected:
  POSIXDriver* driver;

public:
  POSIXMultipartUpload(std::unique_ptr<MultipartUpload> _next,
		       Bucket* _b,
		       POSIXDriver* _driver) :
    FilterMultipartUpload(std::move(_next), _b),
    driver(_driver) {}
  virtual ~POSIXMultipartUpload() = default;

  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated,
			 bool assume_unsorted = false) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct) override;
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj) override;

  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  std::unique_ptr<rgw::sal::Object> _head_obj,
			  const rgw_user& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;
};

class POSIXWriter : public FilterWriter {
private:
  POSIXDriver* driver;

public:
  POSIXWriter(std::unique_ptr<Writer> _next,
		    std::unique_ptr<Object> _head_obj,
		    POSIXDriver* _driver) :
    FilterWriter(std::move(_next),
		 std::move(_head_obj)),
    driver(_driver) {}
  virtual ~POSIXWriter() = default;

  virtual int prepare(optional_yield y);
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
		       std::map<std::string, bufferlist>& attrs,
		       ceph::real_time delete_at,
		       const char *if_match, const char *if_nomatch,
		       const std::string *user_data,
		       rgw_zone_set *zones_trace, bool *canceled,
		       optional_yield y) override;
};

} } // namespace rgw::sal
