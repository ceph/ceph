// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
:* Copyright (C) 2025 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/*
 * With the introduction of a second rados bucket index type, we need
 * a way to allow the two types to co-exist. So the idea is to use a
 * virtual base class and a derived class to represet and hold the
 * logic of each type of bucket index.
 */


#pragma once


#include <ostream>
#include "rgw_bucket_layout.h"


class CephContext;

static const std::string dir_oid_prefix = ".dir.";

namespace rgw {

/*
 * Defined Bucket Index Namespaces
 */
#define RGW_OBJ_NS_MULTIPART "multipart"
#define RGW_OBJ_NS_SHADOW    "shadow"

namespace rados {

class unimplemented_bindexer_error : public std::runtime_error {

public:

  unimplemented_bindexer_error(const std::string& what_arg) :
    runtime_error(what_arg)
  { }

  unimplemented_bindexer_error(const char* what_arg) :
    runtime_error(what_arg)
  { }
};


#define UNIMPLEMENTED() throw_unimplemented(typeid(*this).name(), __func__)
#define UNIMPLEMENTED_MSG(msg) throw_unimplemented(typeid(*this).name(), __func__, msg)


class BIndexer {

protected:

  static RGWSI_Bucket_SObj* bucket_sobj;

  CephContext* cct;
  const DoutPrefixProvider* dpp;
  const RGWBucketInfo& bucket_info;
  bucket_index_layout_generation layout_gen;

  std::string get_bucket_oid_base() const {
    std::string bucket_oid_base = dir_oid_prefix;
    bucket_oid_base.append(bucket_info.bucket.bucket_id);
    return bucket_oid_base;
  }

  // throws the exception after documenting the unimplemented function
  // in the log, including a backtrace
  void throw_unimplemented(const std::string& type_name,
			   const char* func_name,
			   const char* msg = nullptr) const;

  static std::string calc_preprefix(const std::string& prefix);

public:

  static const BIShardIndex BI_ALL_SHARDS = -1;

  BIndexer(CephContext* _cct,
	   const DoutPrefixProvider* _dpp,
	   const RGWBucketInfo& _bucket_info) :
    cct(_cct),
    dpp(_dpp),
    bucket_info(_bucket_info),
    layout_gen(bucket_info.layout.current_index)
  { }

  BIndexer(CephContext* _cct,
	   const DoutPrefixProvider* _dpp,
	   const RGWBucketInfo& _bucket_info,
	   const bucket_index_layout_generation& _layout_gen) :
    cct(_cct),
    dpp(_dpp),
    bucket_info(_bucket_info),
    layout_gen(_layout_gen)
  { }

  virtual ~BIndexer()
  { }

  static void init(RGWSI_Bucket_SObj* _bucket_sobj);

  const RGWBucketInfo& get_bucket_info() const {
    return bucket_info;
  }

  const bucket_index_layout_generation& get_layout_gen() const {
    return layout_gen;
  }

  virtual BucketIndexType get_index_type() const = 0;
  virtual bool entries_ordered_by_shard() const = 0;
  virtual bool may_support_log_record() const = 0;

  virtual uint32_t get_actual_num_shards() const = 0;
  virtual uint32_t get_stored_num_shards() const = 0;

  virtual int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    std::unique_ptr<BIShardIdent>& shard_ident) const = 0;

  virtual int get_bucket_index_object(
    uint64_t gen_id,
    const BIShardIdent& shard_ident,
    std::string* bucket_obj) const
  {
    UNIMPLEMENTED();
    return -EINVAL;
  }

  // retrieve specific shard or all shards if shard_index is -1
  virtual int get_bucket_index_objects(
    BIShardIndex shard_index,
    std::map<BIShardIndex, std::string>* shard_oids) const
  {
    UNIMPLEMENTED();
    return -1;
  }

  /**
   * shard_oids maps: arbitrary int ids to shard oids

   * per_shard_data: parallel to shard_oids, using the same ids, but
   * provides data to be stored in the omap heads of the shards in the
   * index-specific area

   * binfo_map_data: key-value pairs for the bucket instance (bucket
   * info) objects; optional and nullptr means don't fill; may already
   * have data in it, so do not clear before adding key/value pairs
   */
  virtual void get_initial_bucket_index_objects(
    uint64_t gen,
    BIShardCount num_shards,
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data)
  {
    UNIMPLEMENTED();
  }

  virtual void get_bucket_instance_ids(
    int num_shards,
    BIShardIndex shard_index,
    std::map<int, std::string>* result) const
  {
    UNIMPLEMENTED();
  }

  virtual int get_shard_idents(std::vector<std::unique_ptr<BIShardIdent>>& result) const
  {
    UNIMPLEMENTED();
    return 0;
  }

  // create_unique

  static std::unique_ptr<BIndexer> create_unique(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info)
  {
    return std::unique_ptr<BIndexer>(create(cct, dpp,
					    info, info.layout.current_index));
  }

  // supply the layout generation in case we want the bindexer to,
  // say, use the target rather than the current layout generation
  static std::unique_ptr<BIndexer> create_unique(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info,
						 const bucket_index_layout_generation& idx_layout) {
    return std::unique_ptr<BIndexer>(create(cct, dpp, info, idx_layout));
  }

  // convenience versions

  static std::unique_ptr<BIndexer> create_unique(RGWRados* rados,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info);
  static std::unique_ptr<BIndexer> create_unique(rgw::sal::RadosStore* store,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info);
  static std::unique_ptr<BIndexer> create_unique(RGWRados* rados,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info,
						 const bucket_index_layout_generation& idx_layout);
  static std::unique_ptr<BIndexer> create_unique(rgw::sal::RadosStore* store,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info,
						 const bucket_index_layout_generation& idx_layout);

  // create_shared

  static std::shared_ptr<BIndexer> create_shared(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info) {
    return std::shared_ptr<BIndexer>(create(cct, dpp,
					    info, info.layout.current_index));
  }

  // supply the layout generation in case we want the bindexer to,
  // say, use the target rather than the current layout generation
  static std::shared_ptr<BIndexer> create_shared(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info,
						 const bucket_index_layout_generation& idx_layout) {
    return std::shared_ptr<BIndexer>(create(cct, dpp, info, idx_layout));
  }

  static BIndexer* create(CephContext* cct,
			  const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  const bucket_index_layout_generation& idx_gen);

  // convenience versions

  static BIndexer* create(RGWRados* rados,
			  const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  const bucket_index_layout_generation& idx_gen);

  static BIndexer* create(rgw::sal::RadosStore* store,
			  const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  const bucket_index_layout_generation& idx_gen);

  virtual std::string to_string() const = 0;

protected:

  class InteriorShardIterator {
  public:
    virtual ~InteriorShardIterator() { }
    virtual const BIndexer* get_bindexer() const = 0;
    virtual bool valid() const = 0;
    virtual int next() = 0;
    virtual std::unique_ptr<BIShardIdent> get_ident() const = 0;
    virtual std::string get_oid() const = 0;
  };

  class ErrorShardIterator : public InteriorShardIterator {
    const BIndexer* get_bindexer() const override { return nullptr; }
    bool valid() const override { return false; }
    int next() override { return -ENOENT; }
    std::unique_ptr<BIShardIdent> get_ident() const { return nullptr; }
    std::string get_oid() const override { return ""; }
  };

public:

  // acts as a wrapper around a pointer iterator that can employ
  // inheritance polymorphism
  class ShardIterator {
    std::unique_ptr<InteriorShardIterator> interior;

  public:
    // allows us to declare and later assign
    ShardIterator() : interior(std::make_unique<ErrorShardIterator>()) {}

    ShardIterator(InteriorShardIterator* i) : interior(i) {}

    ShardIterator(ShardIterator&& other) {
      std::swap(interior, other.interior);
    }

    ShardIterator& operator=(ShardIterator&& rhs) {
      std::swap(interior, rhs.interior);
      return *this;
    }

    const BIndexer* get_bindexer() const { return interior->get_bindexer(); }
    bool valid() { return interior->valid(); }
    int next() { return interior->next(); }
    std::unique_ptr<BIShardIdent> get_ident() const {
      return interior->get_ident();
    }
    std::string get_oid() const { return interior->get_oid(); }
  }; // class BIndexer::ShardIterator

  virtual ShardIterator create_shard_iterator() const = 0;
  virtual ShardIterator create_shard_iterator(const std::string& start_at) const = 0;
}; // class BIndexer


std::ostream& operator<<(std::ostream&, const BIndexer&);
std::ostream& operator<<(std::ostream&, const std::unique_ptr<BIndexer>&);


class HashedBIndexer : public BIndexer {

  friend RGWSI_BucketIndex_RADOS;

public:

  HashedBIndexer(CephContext* cct,
		 const DoutPrefixProvider* dpp,
		 const RGWBucketInfo& bucket_info) :
    BIndexer(cct, dpp, bucket_info)
  { }

  HashedBIndexer(CephContext* cct,
		 const DoutPrefixProvider* dpp,
		 const RGWBucketInfo& bucket_info,
		 const bucket_index_layout_generation& layout_gen) :
    BIndexer(cct, dpp, bucket_info, layout_gen)
  { }

  std::string to_string() const override;

  ShardIterator create_shard_iterator() const override;
  ShardIterator create_shard_iterator(const std::string& start_at) const override;

  BucketIndexType get_index_type() const override {
    return BucketIndexType::Hashed;
  }

  bool entries_ordered_by_shard() const override {
    return false;
  }

  bool may_support_log_record() const override {
    // might support based on various factors
    return true;
  }

  uint32_t get_actual_num_shards() const override {
    auto result = get_stored_num_shards();
    if (0 == result) {
      return 1;
    } else {
      return result;
    }
  }

  uint32_t get_stored_num_shards() const override {
    return num_shards(layout_gen);
  }

  static std::string index_shard_oid(
    const std::string& bucket_oid_base,
    uint32_t shard_id,
    uint64_t gen_id = 0);

  std::string index_shard_oid(
    uint32_t shard_id,
    uint64_t gen_id) const;

  std::string index_shard_oid(uint32_t shard_id) const;

  // retrieve specific shard or all shards if shard_index is -1
  int get_bucket_index_objects(
    BIShardIndex shard_index,
    std::map<int, std::string>* shard_oids) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    std::unique_ptr<BIShardIdent>& shard_ident) const override;

  int get_bucket_index_object(
    uint64_t gen_id,
    const BIShardIdent& shard_ident,
    std::string* bucket_obj) const override;

    void get_bucket_instance_ids(
    int num_shards,
    BIShardIndex shard_index,
    std::map<int, std::string>* result) const override;

  int get_shard_idents(std::vector<std::unique_ptr<BIShardIdent>>&) const override;


protected:

  static BIShardIndex get_shard_index(const std::string& key,
				 int num_shards)
  {
    uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
    uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
    return rgw_shards_mod(sid2, num_shards);
  }

  static BIShardIndex get_shard_index(const rgw_obj_key& obj_key,
				      int num_shards)
  {
    std::string sharding_key;
    if (obj_key.ns == RGW_OBJ_NS_MULTIPART) {
      RGWMPObj mp;
      mp.from_meta(obj_key.name);
      sharding_key = mp.get_key();
    } else {
      sharding_key = obj_key.name;
    }

    return get_shard_index(sharding_key, num_shards);
  }

  void get_initial_bucket_index_objects(
    uint64_t gen,
    BIShardCount num_shards,
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data) override;

public:

  class HashedShardIterator : public InteriorShardIterator{

    const HashedBIndexer& bindexer;
    BIShardIndex index;

  public:

    HashedShardIterator(const HashedBIndexer& b) :
      bindexer(b),
      index(0)
    { }

    HashedShardIterator(const HashedBIndexer& b, BIShardIndex i) :
      bindexer(b),
      index(i)
    { }

    ~HashedShardIterator()
    { }

    const BIndexer* get_bindexer() const {
      return &bindexer;
    }

    bool valid() const override;
    int next() override;
    std::unique_ptr<BIShardIdent> get_ident() const override;
    std::string get_oid() const override;
  }; // class HashedShardIterator

};  // class HashedBIndexer


class OrderedBIndexer : public BIndexer {

  friend RGWSI_BucketIndex_RADOS;

public:

  struct ShardLayout {
    const std::string split_point;
    const NestedIndex index_id;

    ShardLayout(const std::string& _split_point,
		const NestedIndex& _index_id) :
      split_point(_split_point),
      index_id(_index_id)
    { }
  };

  using InitialLayout = std::map<std::string, ShardLayout>;

protected:

  static constexpr std::string OMAP_KEY_PREFIX = std::string(RGW_ATTR_OBI1_KEY_PREFIX);
  static const InitialLayout default_initial_layout;

  std::optional<InitialLayout> initial_layout;

public:

  static constexpr int STARTING_INDEX_ID = 64000;

  OrderedBIndexer(CephContext* cct,
		  const DoutPrefixProvider* dpp,
		  const RGWBucketInfo& bucket_info) :
    BIndexer(cct, dpp, bucket_info)
  { }

  OrderedBIndexer(CephContext* cct,
		  const DoutPrefixProvider* dpp,
		  const RGWBucketInfo& bucket_info,
		  const bucket_index_layout_generation& layout_gen) :
    BIndexer(cct, dpp, bucket_info, layout_gen)
  { }

  std::string to_string() const override;

  ShardIterator create_shard_iterator() const override;
  ShardIterator create_shard_iterator(const std::string& start_at) const override;

  BucketIndexType get_index_type() const override {
    return BucketIndexType::Ordered;
  }

  bool entries_ordered_by_shard() const override {
    return true;
  }

  bool may_support_log_record() const override {
    // currently does not support
    return false;
  }

  uint32_t get_actual_num_shards() const override {
    return get_stored_num_shards();
  }

  uint32_t get_stored_num_shards() const override {
    return num_shards(layout_gen);
  }

  static std::string index_shard_oid(
    const std::string& bucket_oid_base,
    const NestedIndex& index_hierarchy,
    uint64_t gen_id = 0);

  std::string index_shard_oid(
    const NestedIndex& index_hierarchy,
    uint64_t gen_id) const;

  std::string index_shard_oid(
    const NestedIndex& index_hierarchy) const;

  const std::optional<InitialLayout>& get_initial_shard_data() const {
    return initial_layout;
  }

  static uint32_t get_default_initial_shard_count() {
    return default_initial_layout.size();
  }

  const InitialLayout& make_initial_layout();

  void set_initial_layout(const InitialLayout& layout);

  void get_initial_bucket_index_objects(
    uint64_t gen,
    BIShardCount num_shards,
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data) override;

  // retrieve specific shard or all shards if shard_index is -1
  int get_bucket_index_objects(
    BIShardIndex shard_index,
    std::map<int, std::string>* shard_oids) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    std::unique_ptr<BIShardIdent>& shard_ident) const;

  int get_bucket_index_object(
    uint64_t gen_id,
    const BIShardIdent& shard_ident,
    std::string* bucket_obj) const override;

  void get_bucket_instance_ids(
    int num_shards,
    BIShardIndex shard_index,
    std::map<int, std::string>* result) const override;

  int get_shard_idents(std::vector<std::unique_ptr<BIShardIdent>>&) const override;

  class OrderedShardIterator : public InteriorShardIterator{

    const OrderedBIndexer& bindexer;
    std::map<std::string, rgw_ordered_bi_omap_value> batch;
    std::map<std::string, rgw_ordered_bi_omap_value>::const_iterator batch_it;

    enum class Status {
      pre_read, // before any read
      batch_incomplete, // read a batch, there are more shards
      batch_with_last_shard, // read up to and including last shard
      complete, // completed
      error // error status
    } status;

  protected:

    void init(const std::string& start_from);

    int load_batch(
      const std::string& start_from, // should include prefix if necessary
      bool* has_more);

  public:

    OrderedShardIterator(const OrderedBIndexer& b);
    OrderedShardIterator(const OrderedBIndexer& b, const std::string& start_from);

    ~OrderedShardIterator()
    { }

    const BIndexer* get_bindexer() const {
      return &bindexer;
    }

    const std::string& get_split() const {
      if (valid() && batch_it != batch.cend()) {
	return batch_it->first;
      } else {
	throw std::runtime_error(
	  "OrderedBIndexer::OrderedShardIterator::get_split() "
	  "called with invaid data");
      }
    }

    const NestedIndex& get_nested_index() const {
      if (valid() && batch_it != batch.cend()) {
	return batch_it->second.shard_ident;
      } else {
	throw std::runtime_error(
	  "OrderedBIndexer::OrderedShardIterator::get_nested_index() "
	  "called with invaid data");
      }
    }

    bool valid() const override;
    int next() override;
    std::unique_ptr<BIShardIdent> get_ident() const override;
    std::string get_oid() const override;

    friend std::ostream& operator<<(std::ostream&, const OrderedShardIterator&);
  }; // class OrderedShardIterator

}; // class OrderedBIndexer

std::ostream& operator<<(std::ostream&, const OrderedBIndexer::InitialLayout&);


} // namespace rados
} // namespace rgw
