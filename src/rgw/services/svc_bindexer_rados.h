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
  const bucket_index_layout_generation& layout_gen;

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
	   const bucket_index_layout_generation _layout_gen) :
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

  virtual bool entries_ordered_by_shard() const = 0;

  virtual BucketIndexType get_index_type() const = 0;

  virtual uint32_t get_actual_num_shards() const = 0;
  virtual uint32_t get_stored_num_shards() const = 0;

  virtual std::unique_ptr<BIShardIdent> shard_of(
    const std::string& obj_key) const = 0;
  virtual std::unique_ptr<BIShardIdent> shard_of(
    const rgw_obj_key& obj_key) const = 0;


#if 1 // we want to deprecate this
  virtual int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    BIShardIndex* shard_index) const
  {
    UNIMPLEMENTED();
    return -1;
  }
#endif

  virtual int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    BIShardIdent* shard_ident) const
  {
    UNIMPLEMENTED();
    return -1;
  }

  virtual int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    std::unique_ptr<BIShardIdent>& shard_ident) const
  {
    UNIMPLEMENTED();
    return -1;
  }

#if 0
  virtual int get_bucket_index_object(
    const std::string& bucket_oid_base,
    uint64_t gen_id,
    const std::string& obj_key,
    std::string* bucket_index_obj) const
  {
    UNIMPLEMENTED();
    return -1;
  }
#endif

  virtual void get_bucket_index_object(
    uint64_t gen_id,
    BIShardIndex shard_idx,
    std::string* bucket_obj) const
  {
    UNIMPLEMENTED();
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
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data) const
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

#if 0
  static std::unique_ptr<BIndexer> create_unique(CephContext* cct, const LayoutVariant& layout) {
    return std::unique_ptr<BIndexer>(create(cct, layout));
  }
#endif

  static std::unique_ptr<BIndexer> create_unique(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info)
  {
    return std::unique_ptr<BIndexer>(create(cct, dpp,
					    info, info.get_current_layout_variant()));
  }

  static std::unique_ptr<BIndexer> create_unique(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info,
						 LayoutVariant& layout)
  {
    return std::unique_ptr<BIndexer>(create(cct, dpp, info, layout));
  }

#if 0
  static std::shared_ptr<BIndexer> create_shared(CephContext* cct, const LayoutVariant& layout) {
    return std::shared_ptr<BIndexer>(create(cct, layout));
  }
#endif

  static std::shared_ptr<BIndexer> create_shared(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info)
  {
    return std::shared_ptr<BIndexer>(create(cct, dpp,
					    info, info.get_current_layout_variant()));
  }

  static std::shared_ptr<BIndexer> create_shared(CephContext* cct,
						 const DoutPrefixProvider* dpp,
						 const RGWBucketInfo& info,
						 const LayoutVariant& layout)
  {
    return std::shared_ptr<BIndexer>(create(cct, dpp, info, layout));
  }

  protected:

  static BIndexer* create(CephContext* cct,
			  const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  const LayoutVariant& layout);

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


class HashedBIndexer : public BIndexer {

  friend RGWSI_BucketIndex_RADOS;

protected:

  bucket_index_hashed_layout layout;

public:

  HashedBIndexer(CephContext* cct,
		 const DoutPrefixProvider* dpp,
		 const RGWBucketInfo& bucket_info,
		 const bucket_index_hashed_layout& _layout) :
    BIndexer(cct, dpp, bucket_info),
    layout(_layout)
  { }

  ShardIterator create_shard_iterator() const override;
  ShardIterator create_shard_iterator(const std::string& start_at) const override;

  BucketIndexType get_index_type() const override {
    return BucketIndexType::Hashed;
  }

  bool entries_ordered_by_shard() const override {
    return false;
  }

  uint32_t get_actual_num_shards() const override {
    if (layout.num_shards == 0) {
      return 1;
    } else {
      return layout.num_shards;
    }
  }

  uint32_t get_stored_num_shards() const override {
    return layout.num_shards;
  }

  std::unique_ptr<BIShardIdent> shard_of(
    const std::string& obj_key) const override;
  std::unique_ptr<BIShardIdent> shard_of(
    const rgw_obj_key& obj_key) const override;

  // convenience method
  void shard_of(const std::string& obj_key, BIShardIndex* index) const;

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
    BIShardIndex* shard_index) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    BIShardIdent* shard_ident) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    std::unique_ptr<BIShardIdent>& shard_ident) const override;

  void get_bucket_index_object(
    uint64_t gen_id,
    BIShardIndex shard_id,
    std::string* bucket_obj) const override;

  void get_bucket_instance_ids(
    int num_shards,
    BIShardIndex shard_index,
    std::map<int, std::string>* result) const override;

protected:

  static int32_t get_shard_index(const std::string& key,
				 int num_shards)
  {
    uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
    uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
    return rgw_shards_mod(sid2, num_shards);
  }

  static int32_t get_shard_index(const rgw_obj_key& obj_key,
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
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data) const override;

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

protected:

  static constexpr int STARTING_INDEX_ID = 64000;
  static constexpr std::string OMAP_KEY_PREFIX = std::string(RGW_ATTR_OBI1_KEY_PREFIX);

  bucket_index_ordered_layout layout;

public:

  OrderedBIndexer(CephContext* cct,
		  const DoutPrefixProvider* dpp,
		  const RGWBucketInfo& bucket_info,
		  const bucket_index_ordered_layout& _layout) :
    BIndexer(cct, dpp, bucket_info),
    layout(_layout)
  { }

  ShardIterator create_shard_iterator() const override;
  ShardIterator create_shard_iterator(const std::string& start_at) const override;

  BucketIndexType get_index_type() const override {
    return BucketIndexType::Ordered;
  }

  bool entries_ordered_by_shard() const override {
    return true;
  }

  uint32_t get_actual_num_shards() const override {
    return layout.num_shards;
  }

  uint32_t get_stored_num_shards() const override {
    return layout.num_shards;
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

  std::unique_ptr<BIShardIdent> shard_of(const std::string& obj_key) const override;
  std::unique_ptr<BIShardIdent> shard_of(const rgw_obj_key& obj_key) const override;

  static const std::vector<std::pair<std::string, NestedIndex>>& get_initial_shard_data();

  static uint32_t get_initial_shard_count() {
    return get_initial_shard_data().size();
  }

  void get_initial_bucket_index_objects(
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data) const override;

  // retrieve specific shard or all shards if shard_index is -1
  int get_bucket_index_objects(
    BIShardIndex shard_index,
    std::map<int, std::string>* shard_oids) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    BIShardIndex* shard_index) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    BIShardIdent* shard_id) const override;

  int get_bucket_index_object(
    const std::string& obj_key,
    std::string* bucket_obj,
    std::unique_ptr<BIShardIdent>& shard_ident) const;

  void get_bucket_index_object(
    uint64_t gen_id,
    BIShardIndex shard_id,
    std::string* bucket_obj) const override;

  void get_bucket_instance_ids(
    int num_shards,
    BIShardIndex shard_index,
    std::map<int, std::string>* result) const override;


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

} // namespace rados
} // namespace rgw
