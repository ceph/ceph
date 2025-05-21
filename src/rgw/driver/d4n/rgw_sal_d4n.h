// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal_filter.h"
#include "rgw_sal.h"
#include "rgw_role.h"
#include "common/dout.h" 
#include "rgw_aio_throttle.h"
#include "rgw_ssd_driver.h"
#include "rgw_redis_driver.h"

#include "driver/d4n/d4n_directory.h"
#include "driver/d4n/d4n_policy.h"

#include <boost/intrusive/list.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include <fmt/core.h>

namespace rgw::d4n {
  class PolicyDriver;
}

namespace rgw { namespace sal {

inline std::string get_cache_block_prefix(rgw::sal::Object* object, const std::string& version)
{
  return fmt::format("{}{}{}{}{}", url_encode(object->get_bucket()->get_bucket_id(), true), CACHE_DELIM, url_encode(version, true), CACHE_DELIM, url_encode(object->get_name(), true));
}

inline std::string get_key_in_cache(const std::string& prefix, const std::string& offset, const std::string& len)
{
  return fmt::format("{}{}{}{}{}", prefix, CACHE_DELIM, offset, CACHE_DELIM, len);
}

using boost::redis::connection;

class D4NFilterDriver : public FilterDriver {
  private:
    std::shared_ptr<connection> conn;
    std::unique_ptr<rgw::cache::CacheDriver> cacheDriver;
    std::unique_ptr<rgw::d4n::ObjectDirectory> objDir;
    std::unique_ptr<rgw::d4n::BlockDirectory> blockDir;
    std::unique_ptr<rgw::d4n::BucketDirectory> bucketDir;
    std::unique_ptr<rgw::d4n::PolicyDriver> policyDriver;
    boost::asio::io_context& io_context;

  public:
    D4NFilterDriver(Driver* _next, boost::asio::io_context& io_context);
    virtual ~D4NFilterDriver();

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    virtual std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override;
    int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                  std::unique_ptr<Bucket>* bucket, optional_yield y) override;
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
    rgw::cache::CacheDriver* get_cache_driver() { return cacheDriver.get(); }
    rgw::d4n::ObjectDirectory* get_obj_dir() { return objDir.get(); }
    rgw::d4n::BlockDirectory* get_block_dir() { return blockDir.get(); }
    rgw::d4n::BucketDirectory* get_bucket_dir() { return bucketDir.get(); }
    rgw::d4n::PolicyDriver* get_policy_driver() { return policyDriver.get(); }
    void shutdown() override;
};

class D4NFilterUser : public FilterUser {
  private:
    D4NFilterDriver* driver;

  public:
    D4NFilterUser(std::unique_ptr<User> _next, D4NFilterDriver* _driver) : 
      FilterUser(std::move(_next)),
      driver(_driver) {}
    virtual ~D4NFilterUser() = default;
};

class D4NFilterBucket : public FilterBucket {
  private:
    struct rgw_bucket_list_entries{
      rgw_obj_key key;
      uint16_t flags;
    };
    D4NFilterDriver* filter;

  public:
    D4NFilterBucket(std::unique_ptr<Bucket> _next, D4NFilterDriver* _filter) :
      FilterBucket(std::move(_next)),
      filter(_filter) {}
    virtual ~D4NFilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
    virtual int list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		   ListResults& results, optional_yield y) override;
    virtual int create(const DoutPrefixProvider* dpp,
                       const CreateParams& params,
                       optional_yield y) override;
    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
};

class D4NFilterObject : public FilterObject {
  private:
    D4NFilterDriver* driver;
    std::string version;
    std::string prefix;
    rgw_obj obj;
    rgw::sal::Object* dest_object{nullptr}; //for copy-object
    rgw::sal::Bucket* dest_bucket{nullptr}; //for copy-object
    bool multipart{false};
    bool delete_marker{false};
    bool exists_in_cache{false};
    bool load_from_store{false};

  public:
    struct D4NFilterReadOp : FilterReadOp {
      public:
	class D4NFilterGetCB: public RGWGetDataCB {
	  private:
	    D4NFilterDriver* filter;
	    D4NFilterObject* source;
	    RGWGetDataCB* client_cb;
	    int64_t ofs = 0, len = 0;
      int64_t adjusted_start_ofs{0};
	    bufferlist bl_rem;
	    bool last_part{false};
	    bool write_to_cache{true};
	    const DoutPrefixProvider* dpp;
	    optional_yield* y;
      int part_count{0};

	  public:
	    D4NFilterGetCB(D4NFilterDriver* _filter, D4NFilterObject* _source) : filter(_filter),
												        source(_source) {}

	    int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
	    void set_client_cb(RGWGetDataCB* client_cb, const DoutPrefixProvider* dpp, optional_yield* y) { 
              this->client_cb = client_cb; 
              this->dpp = dpp;
              this->y = y;
            }
	    void set_ofs(uint64_t ofs) { this->ofs = ofs; }
      void set_adjusted_start_ofs(uint64_t adjusted_start_ofs) { this->adjusted_start_ofs = adjusted_start_ofs; }
      void set_part_num(uint64_t part_num) { this->part_count = part_num; }
	    int flush_last_part();
	    void bypass_cache_write() { this->write_to_cache = false; }
	};

	D4NFilterObject* source;

	D4NFilterReadOp(std::unique_ptr<ReadOp> _next, D4NFilterObject* _source) : FilterReadOp(std::move(_next)),
										   source(_source) 
        {
          cb = std::make_unique<D4NFilterGetCB>(source->driver, source);
	}
	virtual ~D4NFilterReadOp() = default;

	virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
	virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			     RGWGetDataCB* cb, optional_yield y) override;
	virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			      bufferlist& dest, optional_yield y) override;

      private:
	RGWGetDataCB* client_cb;
	std::unique_ptr<D4NFilterGetCB> cb;
        std::unique_ptr<rgw::Aio> aio;
	uint64_t offset = 0; // next offset to write to client
        rgw::AioResultList completed; // completed read results, sorted by offset
	std::unordered_map<uint64_t, std::pair<uint64_t,uint64_t>> blocks_info;

	int flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y);
	void cancel();
	int drain(const DoutPrefixProvider* dpp, optional_yield y);
    };

    struct D4NFilterDeleteOp : FilterDeleteOp {
      D4NFilterObject* source;

      D4NFilterDeleteOp(std::unique_ptr<DeleteOp> _next, D4NFilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~D4NFilterDeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
    };

    D4NFilterObject(std::unique_ptr<Object> _next, D4NFilterDriver* _driver) : FilterObject(std::move(_next)),
									      driver(_driver) {}
    D4NFilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, D4NFilterDriver* _driver) : FilterObject(std::move(_next), _bucket),
											       driver(_driver) {}
    D4NFilterObject(D4NFilterObject& _o, D4NFilterDriver* _driver) : FilterObject(_o),
								    driver(_driver) {}
    virtual ~D4NFilterObject() = default;

    virtual int copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
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
                              optional_yield y) override;

    virtual const std::string &get_name() const override { return next->get_name(); }
    virtual int load_obj_state(const DoutPrefixProvider *dpp, optional_yield y,
                             bool follow_olh = true) override;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags) override;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                            rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp,
			       uint32_t flags = rgw::sal::FLAG_LOG_OP) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) override;
    virtual ceph::real_time get_mtime(void) const override { return next->get_mtime(); };

    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;

    void set_object_version(const std::string& version) { this->version = version; }
    const std::string get_object_version() { return this->version; }

    void set_prefix(const std::string& prefix) { this->prefix = prefix; }
    const std::string get_prefix() { return this->prefix; }
    int get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y);
    void set_attrs_from_obj_state(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool dirty = false);
    int calculate_version(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, rgw::sal::Attrs& attrs);
    int set_head_obj_dir_entry(const DoutPrefixProvider* dpp, std::vector<std::string>* exec_responses, optional_yield y, bool is_latest_version = true, bool dirty = false);
    int set_data_block_dir_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty = false);
    int delete_data_block_cache_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty = false);
    bool check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::sal::Attrs& attrs, rgw::d4n::CacheBlock& blk, optional_yield y);
    rgw::sal::Bucket* get_destination_bucket(const DoutPrefixProvider* dpp) { return dest_bucket;}
    rgw::sal::Object* get_destination_object(const DoutPrefixProvider* dpp) { return dest_object; }
    bool is_multipart() { return multipart; }
    int set_attr_crypt_parts(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs);
    int create_delete_marker(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_delete_marker() { return delete_marker; }
    bool exists(void) override { if (exists_in_cache) { return true;} return next->exists(); };
    bool load_obj_from_store() { return load_from_store; }
    void set_load_obj_from_store(bool load_from_store) { this->load_from_store = load_from_store; }
};

class D4NFilterWriter : public FilterWriter {
  private:
    D4NFilterDriver* driver; 
    D4NFilterObject* object;
    const DoutPrefixProvider* dpp;
    bool atomic;
    optional_yield y;
    bool d4n_writecache;
    std::string version;
    std::string prev_oid_in_cache;

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, optional_yield _y) : FilterWriter(std::move(_next), _obj),
							     driver(_driver),
							     dpp(_dpp), atomic(false), y(_y) { object = static_cast<D4NFilterObject*>(obj); }
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, bool _atomic, optional_yield _y) : FilterWriter(std::move(_next), _obj),
									   driver(_driver),
									   dpp(_dpp), atomic(_atomic), y(_y) { object = static_cast<D4NFilterObject*>(obj); }
    virtual ~D4NFilterWriter() = default;

    virtual int prepare(optional_yield y);
    virtual int process(bufferlist&& data, uint64_t offset) override;
    virtual int complete(size_t accounted_size, const std::string& etag,
			 ceph::real_time *mtime, ceph::real_time set_mtime,
			 std::map<std::string, bufferlist>& attrs,
			 const std::optional<rgw::cksum::Cksum>& cksum,
			 ceph::real_time delete_at,
			 const char *if_match, const char *if_nomatch,
			 const std::string *user_data,
			 rgw_zone_set *zones_trace, bool *canceled,
			 const req_context& rctx,
			 uint32_t flags) override;
   bool is_atomic() { return atomic; };
   const DoutPrefixProvider* get_dpp() { return this->dpp; } 
};

class D4NFilterMultipartUpload : public FilterMultipartUpload {
private:
  D4NFilterDriver* driver;
public:
  D4NFilterMultipartUpload(std::unique_ptr<MultipartUpload> _next, Bucket* _b, D4NFilterDriver* driver) :
    FilterMultipartUpload(std::move(_next), _b),
    driver(driver) {}
  virtual ~D4NFilterMultipartUpload() = default;

  virtual int complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
            prefix_map_t& processed_prefixes) override;
};

} } // namespace rgw::sal
