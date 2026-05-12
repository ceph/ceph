// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "common/ceph_context.h"
#include "common/dout.h"
#include "services/svc_bi_rados.h"
#include "services/svc_bucket_sobj.h"
#include "svc_bindexer_rados.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"

// OBI: remove once things stabilize
#if 1
#include "common/BackTrace.h"
#endif

#define dout_subsys ceph_subsys_rgw

namespace rgw::rados {

/******************** BIndexer ********************/

RGWSI_Bucket_SObj* BIndexer::bucket_sobj = nullptr; // until init() run

void BIndexer::init(RGWSI_Bucket_SObj* _bucket_sobj) {
  bucket_sobj = _bucket_sobj;
}


// OBI: temporary for  use while actively being worked on
void BIndexer::throw_unimplemented(const std::string& type_name,
				   const char* func_name,
				   const char* msg) const {
  std::string message;
  if (msg && strlen(msg) != 0) {
    message = std::string(" message:\"") + std::string(msg) + std::string("\"");
  }

  ldpp_dout(dpp, 0) << "ERROR: UNIMPLEMENTED BIndexer function " <<
    type_name << "::" << func_name << message <<
    "; backtrace: " << ClibBackTrace(0) << dendl;

  throw unimplemented_bindexer_error(type_name +
				     std::string("::") +
				     std::string(func_name) +
				     message);
}


BIndexer* BIndexer::create(CephContext* cct,
			   const DoutPrefixProvider* dpp,
			   const RGWBucketInfo& bucket_info,
			   const bucket_index_layout_generation& idx_gen)
{
  const auto* nl = std::get_if<bucket_index_hashed_layout>(&idx_gen.layout.specs);
  if (nl) {
    auto result = new HashedBIndexer(cct, dpp, bucket_info, idx_gen);
    return result;
  }

  const auto* ol = std::get_if<bucket_index_ordered_layout>(&idx_gen.layout.specs);
  if (ol) {
    auto result = new OrderedBIndexer(cct, dpp, bucket_info, idx_gen);
    return result;
  }

  return nullptr;
}

BIndexer* BIndexer::create(RGWRados* rados,
			   const DoutPrefixProvider* dpp,
			   const RGWBucketInfo& bucket_info,
			   const bucket_index_layout_generation& idx_gen) {
  return create(rados->ctx(), dpp, bucket_info, idx_gen);
}

BIndexer* BIndexer::create(rgw::sal::RadosStore* store,
			   const DoutPrefixProvider* dpp,
			   const RGWBucketInfo& bucket_info,
			   const bucket_index_layout_generation& idx_gen) {
  return create(store->getRados()->ctx(), dpp, bucket_info, idx_gen);
}

std::unique_ptr<BIndexer> BIndexer::create_unique(RGWRados* rados,
						  const DoutPrefixProvider* dpp,
						  const RGWBucketInfo& info) {
  return create_unique(rados->ctx(), dpp, info);
}

std::unique_ptr<BIndexer> BIndexer::create_unique(rgw::sal::RadosStore* store,
						  const DoutPrefixProvider* dpp,
						  const RGWBucketInfo& info) {
  return create_unique(store->getRados()->ctx(), dpp, info);
}

std::unique_ptr<BIndexer> BIndexer::create_unique(RGWRados* rados,
						  const DoutPrefixProvider* dpp,
						  const RGWBucketInfo& info,
						  const bucket_index_layout_generation& idx_layout) {
  return create_unique(rados->ctx(), dpp, info, idx_layout);
}

std::unique_ptr<BIndexer> BIndexer::create_unique(rgw::sal::RadosStore* store,
						  const DoutPrefixProvider* dpp,
						  const RGWBucketInfo& info,
						  const bucket_index_layout_generation& idx_layout) {
  return create_unique(store->getRados()->ctx(), dpp, info, idx_layout);
}

std::string BIndexer::calc_preprefix(const std::string& prefix) {
  if (prefix.empty()) {
    return prefix;
  }

  const char last = prefix[prefix.size() - 1];
  const std::string new_ending{ char(last - 1), '\xFF' };
  std::string result = prefix;
  result.replace(result.end() - 1, result.end(), new_ending);
  return result;
}


std::ostream& operator<<(std::ostream& o, const BIndexer& b) {
    return o << b.to_string();
  }

std::ostream& operator<<(std::ostream& o, const std::unique_ptr<BIndexer>& b) {
  if (b) {
    return o << b->to_string();
  } else {
    return o << "std::unique_ptr<BIndexer> has no managed object";
  }
}


/******************** HashedBIndexer ********************/

std::string HashedBIndexer::to_string() const {
  std::stringstream ss;
  ss << "HashedBIndexer: { bucket: " << bucket_info.bucket <<
    ", layout: { " << layout_gen << " } }";
  return ss.str();
}

BIndexer::ShardIterator HashedBIndexer::create_shard_iterator() const {
  return ShardIterator(new HashedShardIterator(*this));
}

BIndexer::ShardIterator HashedBIndexer::create_shard_iterator(
  const std::string& start_at) const
{
  BIShardIndex index = get_shard_index(start_at, get_actual_num_shards());
  return ShardIterator(new HashedShardIterator(*this, index));
}

std::string HashedBIndexer::index_shard_oid(
  const std::string& bucket_oid_base,
  uint32_t shard_id,
  uint64_t gen_id)
{
  std::stringstream ss;

  ss << bucket_oid_base;
  if (gen_id) {
    ss << '.' << gen_id;
  }
  ss << '.' << shard_id;

  return ss.str();
}

std::string HashedBIndexer::index_shard_oid(
  uint32_t shard_id,
  uint64_t gen_id) const
{
  return index_shard_oid(get_bucket_oid_base(), shard_id, gen_id);
}

std::string HashedBIndexer::index_shard_oid(uint32_t shard_id) const
{
  return index_shard_oid(get_bucket_oid_base(), shard_id, layout_gen.gen);
}

int HashedBIndexer::get_bucket_index_objects(
  BIShardIndex shard_index,
  std::map<BIShardIndex, std::string>* shard_oids) const
{
  const std::string bucket_oid_base = get_bucket_oid_base();
  const auto gen_id = layout_gen.gen;

  if (shard_index != BI_ALL_SHARDS) {
    (*shard_oids)[shard_index] = index_shard_oid(bucket_oid_base, shard_index, gen_id);
  } else {
    const BIShardIndex shard_count = (BIShardIndex) num_shards(layout_gen);
    for (BIShardIndex s = 0; s < shard_count; ++s) {
      (*shard_oids)[s] = index_shard_oid(bucket_oid_base, s, gen_id);
    }
  }

  return 0;
}

void HashedBIndexer::get_bucket_instance_ids(
  int num_shards,
  BIShardIndex shard_index,
  std::map<int, std::string>* result) const
{
  const rgw_bucket& bucket = bucket_info.bucket;
  std::string plain_id = bucket.name + ":" + bucket.bucket_id;

  if (!num_shards) {
    (*result)[0] = plain_id;
  } else {
    char buf[16];
    if (shard_index == BIndexer::BI_ALL_SHARDS) {
      for (int i = 0; i < num_shards; ++i) {
        snprintf(buf, sizeof(buf), ":%d", i);
        (*result)[i] = plain_id + buf;
      }
    } else {
      if (shard_index > num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), ":%d", shard_index);
      (*result)[shard_index] = plain_id + buf;
    }
  }
}

int HashedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  std::unique_ptr<BIShardIdent>& shard_id) const
{
  std::string bucket_oid_base = dir_oid_prefix;
  bucket_oid_base.append(bucket_info.bucket.bucket_id);
  const auto gen_id = layout_gen.gen;
  int32_t shard_idx = -1;

  if (! num_shards(layout_gen)) {
    // by default with no sharding, we use the bucket oid as itself
    if (bucket_obj) {
      *bucket_obj = bucket_oid_base;
    }

    shard_idx = -1;
  } else {
    shard_idx = get_shard_index(obj_key, num_shards(layout_gen));

    if (bucket_obj) {
      *bucket_obj = index_shard_oid(bucket_oid_base, shard_idx, gen_id);
    }
  }

  shard_id.reset(new HashedShardIdent(shard_idx));

  return 0;
}

int HashedBIndexer::get_bucket_index_object(
  uint64_t gen_id,
  const BIShardIdent& shard_ident,
  std::string* bucket_obj) const
{
  if (shard_ident.get_type() != BIShardIdent::Type::HashedIdent) {
    return -EINVAL;
  }

  const std::string oid_base = get_bucket_oid_base();
  const auto& hashed_ident = dynamic_cast<const HashedShardIdent&>(shard_ident);
  *bucket_obj = index_shard_oid(oid_base, hashed_ident.get_index(), gen_id);

  return 0;
}


void HashedBIndexer::get_initial_bucket_index_objects(
  uint64_t gen,
  BIShardCount num_shards,
  std::map<int, std::string>& shard_oids,
  std::map<int, bufferlist>& per_shard_data,
  std::map<std::string, bufferlist>* binfo_map_data)
{
  const std::string bucket_oid_base = get_bucket_oid_base();

  for (BIShardIndex i = 0; i < num_shards; ++i) {
    shard_oids.emplace(std::make_pair(int(i),
				      index_shard_oid(bucket_oid_base, i, gen)));
  }
}

int HashedBIndexer::get_shard_idents(std::vector<std::unique_ptr<BIShardIdent>>& result) const {
  for (uint32_t i = 0; i < get_actual_num_shards(); ++i) {
    result.push_back(std::make_unique<HashedShardIdent>(i));
  }
  return 0;
}

/******************** OrderedBIndexer ********************/


std::string OrderedBIndexer::to_string() const {
  std::stringstream ss;
  ss << "OrderedBIndexer: { bucket: " << bucket_info.bucket <<
    ", layout: { " << layout_gen <<
    " }, initial_layout:" << (bool(initial_layout) ? "exists" : "empty") <<
    " }";
  return ss.str();
}

BIndexer::ShardIterator OrderedBIndexer::create_shard_iterator() const {
  try {
    return ShardIterator(new OrderedShardIterator(*this));
  } catch (std::runtime_error& e) {
    return ShardIterator(new ErrorShardIterator());
  }
}

BIndexer::ShardIterator OrderedBIndexer::create_shard_iterator(
  const std::string& start_from) const
{
  try {
    return ShardIterator(new OrderedShardIterator(*this, start_from));
  } catch (std::runtime_error& e) {
    return ShardIterator(new ErrorShardIterator());
  }
}

std::string OrderedBIndexer::index_shard_oid(
  const std::string& bucket_oid_base,
  const NestedIndex& shard_ident,
  uint64_t gen_id)
{
  std::stringstream ss;
  ss << bucket_oid_base <<
    ".ordered" << // this is to make the oids parseable in the opposite direction
    '.' << gen_id;
  for (const auto& i : shard_ident) {
    ss << '.' << i;
  }
  return ss.str();
}

std::string OrderedBIndexer::index_shard_oid(
  const NestedIndex& shard_ident,
  uint64_t gen_id) const
{
  return index_shard_oid(get_bucket_oid_base(), shard_ident, gen_id);
}

std::string OrderedBIndexer::index_shard_oid(
  const NestedIndex& shard_ident) const
{
  return index_shard_oid(get_bucket_oid_base(), shard_ident, layout_gen.gen);
}

// The default ordered shard layout. The shard with an empty-string
// cut-off is the last shard
const OrderedBIndexer::InitialLayout OrderedBIndexer::default_initial_layout =
{
  // OBI: we should make this better so there's no room for error
  // (mismatching keys, out of order keys, out of order indexes)
  { ".", ShardLayout( ".", { 0 + STARTING_INDEX_ID } ) },
  { "A", ShardLayout( "A", { 1 + STARTING_INDEX_ID } ) },
  { "E", ShardLayout( "E", { 2 + STARTING_INDEX_ID } ) },
  { "U", ShardLayout( "U", { 3 + STARTING_INDEX_ID } ) },
  { "a", ShardLayout( "a", { 4 + STARTING_INDEX_ID } ) },
  { "e", ShardLayout( "e", { 5 + STARTING_INDEX_ID } ) },
  { "u", ShardLayout( "u", { 6 + STARTING_INDEX_ID } ) },
  { "À", ShardLayout( "À", { 7 + STARTING_INDEX_ID } ) },
  { "É", ShardLayout( "É", { 8 + STARTING_INDEX_ID } ) },
  { "Ü", ShardLayout( "Ü", { 9 + STARTING_INDEX_ID } ) },
  { "à", ShardLayout( "à", { 10 + STARTING_INDEX_ID } ) },
  { "é", ShardLayout( "é", { 11 + STARTING_INDEX_ID } ) },
  { "ü", ShardLayout( "ü", { 12 + STARTING_INDEX_ID } ) },
  { "",  ShardLayout( "",  { 13 + STARTING_INDEX_ID } ) }
};

// OBI: does this need to return anything?
const OrderedBIndexer::InitialLayout& OrderedBIndexer::make_initial_layout()
{
  initial_layout = default_initial_layout;
  bucket_index_ordered_layout newspec{ (uint32_t) default_initial_layout.size() };
  layout_gen.layout.specs = newspec;
  return *initial_layout;
}

void OrderedBIndexer::set_initial_layout(const OrderedBIndexer::InitialLayout& layout) {
  // OBI: try to protect from having too few shards; is this good?
  if (layout.size() <= default_initial_layout.size() / 2) {
    // use our default layout if there are too few shards in this layout
    std::ignore = make_initial_layout();
    return;
  }

  initial_layout = layout;
  bucket_index_ordered_layout newspec{ (uint32_t) layout.size() };
  layout_gen.layout.specs = newspec;
}

void OrderedBIndexer::get_initial_bucket_index_objects(
    uint64_t gen,
    BIShardCount num_shards,
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data)
{
  if (! initial_layout) {
    std::ignore = make_initial_layout();
  }

  const std::string bucket_oid_base = get_bucket_oid_base();

  const auto gen_id = layout_gen.gen;

  const auto& fixed_data = get_initial_shard_data();

  if (binfo_map_data) {
    for (const auto& p : *fixed_data) {
      rgw_ordered_bi_omap_value v;
      v.split = p.first;
      v.shard_ident = p.second.index_id;
      bufferlist bl;
      v.encode(bl);
      const std::string key = OMAP_KEY_PREFIX + v.split;
      (*binfo_map_data)[key] = bl;
    }
  }

  const NestedIndex empty_index;

  std::vector<rgw_ordered_bi_shard_data> per_shard;
  NestedIndex prev_ident = empty_index;
  for (const auto& p : *fixed_data) {
    rgw_ordered_bi_shard_data d;
    d.split = p.first;
    d.shard_ident = p.second.index_id;
    d.prev_shard_ident = prev_ident;
    prev_ident = p.second.index_id;
    per_shard.push_back(d);
  }

  NestedIndex next_ident = empty_index;
  for (auto d = per_shard.rbegin(); d != per_shard.rend(); ++d) {
    d->next_shard_ident = next_ident;
    next_ident = d->shard_ident;
  }

  int idx = 0;
  for (auto d : per_shard) {
    bufferlist bl;
    d.encode(bl);
    shard_oids.insert(std::pair{
	idx,
	index_shard_oid(bucket_oid_base, d.shard_ident, gen_id)
      });
    per_shard_data.insert(std::pair{ idx, bl });
    ++idx;
  }
}

int OrderedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  std::unique_ptr<BIShardIdent>& shard_ident) const
{
  const std::string oid_base = get_bucket_oid_base();
  const auto& gen_id = layout_gen.gen;

  NestedIndex index;

  if (initial_layout) {
    auto it = initial_layout->upper_bound(obj_key);
    if (it == initial_layout->end()) {
      it = initial_layout->find("");
      if (it == initial_layout->upper_bound(obj_key)) {
	ldpp_dout(dpp, 0) << "INTERNAL ERROR: " << __func__ <<
	  ": received initial_layout exists but does not have an entry keyed by "
	  "the empty string" << dendl;
	return -ENOENT;
      }
    }
    index = it->second.index_id;
  } else {
    std::map<std::string, bufferlist> results;

    const std::string prefixed_key = OMAP_KEY_PREFIX + obj_key;
    int r = bucket_sobj->read_bucket_info_map(bucket_info.bucket.get_key(),
					      prefixed_key, OMAP_KEY_PREFIX, 1, &OMAP_KEY_PREFIX,
					      &results, nullptr, null_yield, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": call to read_bucket_info_map returned " << r << dendl;
      return r;
    }

    if (results.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": no map results to determine shard" << dendl;
      return -ENOENT;
    }

    if (results.size() > 1) {
      ldpp_dout(dpp, 0) << "INTERNAL ERROR: " << __func__ <<
	": received more map results than requested" << dendl;
      throw std::logic_error("expected 1 result, but got " +
			     std::to_string(results.size()));
    }

    rgw_ordered_bi_omap_value value;
    auto it = results.cbegin()->second.cbegin();
    value.decode(it);

    index = value.shard_ident;
  }

  if (bucket_obj) {
    *bucket_obj = index_shard_oid(oid_base, index, gen_id);
  }

  shard_ident.reset(new OrderedShardIdent(std::move(index)));

  return 0;
}

int OrderedBIndexer::get_bucket_index_object(
  uint64_t gen_id,
  const BIShardIdent& shard_ident,
  std::string* bucket_obj) const
{
  if (shard_ident.get_type() != BIShardIdent::Type::OrderedIdent) {
    return -EINVAL;
  }

  const std::string oid_base = get_bucket_oid_base();
  const auto& ordered_ident = dynamic_cast<const OrderedShardIdent&>(shard_ident);
  *bucket_obj = index_shard_oid(oid_base, ordered_ident.get_indices(), gen_id);

  return 0;
}


// OBI: priority to work on
void OrderedBIndexer::get_bucket_instance_ids(
  int num_shards,
  BIShardIndex shard_index,
  std::map<int, std::string>* result) const
{
  UNIMPLEMENTED();
}

int OrderedBIndexer::get_bucket_index_objects(
  BIShardIndex shard_index,
  std::map<BIShardIndex, std::string>* shard_oids) const
{
  // we need to convert this over to BIShardIdent
  const std::string oid_base = get_bucket_oid_base();
  const uint64_t gen_id = layout_gen.gen;

  if (shard_index != BI_ALL_SHARDS) {
    UNIMPLEMENTED();
    // (*shard_oids)[shard_index] = index_shard_oid(oid_base, shard_index, gen_id);
  } else {
    std::string marker = calc_preprefix(OMAP_KEY_PREFIX);
    bool more = true;
    BIShardIndex counter = 0;
    std::string saved_final_oid;
    constexpr uint64_t batch_size = 1000;

    while (more) {
      std::map<std::string, bufferlist> results;
      // note: the following does not update the marker
      int r = bucket_sobj->read_bucket_info_map(bucket_info.bucket.get_key(),
						marker, OMAP_KEY_PREFIX, batch_size, &OMAP_KEY_PREFIX,
						&results, &more, null_yield, dpp);
      if (r < 0) {
	ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	  ": call to read_bucket_info_map returned " << r << dendl;
	return r;
      }

      for (const auto& i : results) {
	rgw_ordered_bi_omap_value value;
	auto it = i.second.cbegin();
	value.decode(it);
	const std::string oid = index_shard_oid(oid_base, value.shard_ident, gen_id);
	if (i.first == OMAP_KEY_PREFIX) {
	  saved_final_oid = oid;
	} else {
	  (*shard_oids)[counter++] = oid;
	}
      }

      if (more) {
	marker = results.crbegin()->first;
      }
    } // while

    if (! saved_final_oid.empty()) {
      (*shard_oids)[counter++] = saved_final_oid;
    }
  }
  return 0;
}

int OrderedBIndexer::get_shard_idents(std::vector<std::unique_ptr<BIShardIdent>>& result) const {
  if (initial_layout) {
    for (const auto& i : *initial_layout) {
      result.push_back(std::make_unique<OrderedShardIdent>(i.second.index_id));
    }
  } else {
    std::string marker = "";
    bool more = true;
    while (more) {
      std::map<std::string, bufferlist> results;
      int r = bucket_sobj->read_bucket_info_map(bucket_info.bucket.get_key(),
						marker, OMAP_KEY_PREFIX, 1, nullptr,
						&results, &more, null_yield, dpp);
      if (r < 0) {
	ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	  ": call to read_bucket_info_map returned " << r << dendl;
	return r;
      }

      for (const auto& i : results) {
	rgw_ordered_bi_omap_value value;
	auto it = i.second.cbegin();
	value.decode(it);
	result.push_back(std::make_unique<OrderedShardIdent>(std::move(value.shard_ident)));
      }
    } // while
  } // if

  return 0;
}


bool HashedBIndexer::HashedShardIterator::valid() const {
  return index < (BIShardIndex) bindexer.get_actual_num_shards();
}

int HashedBIndexer::HashedShardIterator::next() {
  ++index;
  return 0;
}

std::unique_ptr<BIShardIdent> HashedBIndexer::HashedShardIterator::get_ident() const {
  return std::make_unique<HashedShardIdent>(index);
}

std::string HashedBIndexer::HashedShardIterator::get_oid() const {
  return bindexer.index_shard_oid(index);
}

OrderedBIndexer::OrderedShardIterator::OrderedShardIterator(
  const OrderedBIndexer& b) :
  bindexer(b),
  status(Status::pre_read)
{
  init("");
}

OrderedBIndexer::OrderedShardIterator::OrderedShardIterator(
  const OrderedBIndexer& b, const std::string& start_from) :
  bindexer(b),
  status(Status::pre_read)
{
  init(start_from);
}

void OrderedBIndexer::OrderedShardIterator::init(const std::string& start_from) {
  bool has_more;

  int r = load_batch(OMAP_KEY_PREFIX + start_from, &has_more);
  if (r < 0) {
    status = Status::error;
  } else {
    status = has_more ? Status::batch_incomplete : Status::batch_with_last_shard;
  }
}

int OrderedBIndexer::OrderedShardIterator::next() {
  int r = 0;
  bool has_more;

  switch (status) {
  case Status::pre_read:
    ldpp_dout(bindexer.dpp, 0) <<
      "ERROR: OrderedBIndexer::OrderedShardIterator next() called "
      "before any read" << dendl;
    return -EINVAL;

  case Status::batch_incomplete:
    ++batch_it;

    // if at end of this batch, load more starting after split of last
    // shard in batch
    if (batch_it == batch.cend()) {
      r = load_batch(batch.crbegin()->first, &has_more);
      if (r < 0) {
	status = Status::error;
	ldpp_dout(bindexer.dpp, 0) <<
	  "ERROR: OrderedBIndexer::OrderedShardIterator::load_batch() returned r=" <<
	  r << dendl;
	return r;
      }

      if (! has_more) {
	status = Status::batch_with_last_shard;
      }
    }

    return 0;

  case Status::batch_with_last_shard:
    ++batch_it;
    if (batch_it == batch.cend()) {
      status = Status::complete;
    }

    return 0;

  case Status::complete:
    ldpp_dout(bindexer.dpp, 0) <<
      "ERROR: OrderedBIndexer::OrderedShardIterator next() called "
      "in complete status" << dendl;
    return -EINVAL;

  case Status::error:
    return -EINVAL;

  default:
    status = Status::error;
    ldpp_dout(bindexer.dpp, 0) <<
      "ERROR: OrderedBIndexer::OrderedShardIterator in unknown status" << dendl;
    return -EINVAL;
  };

  // all paths have a return above, so no return necessary here
}

bool OrderedBIndexer::OrderedShardIterator::valid() const {
  return status == Status::batch_incomplete ||
    status == Status::batch_with_last_shard;
}

std::unique_ptr<BIShardIdent> OrderedBIndexer::OrderedShardIterator::get_ident() const {
  if (valid()) {
    return std::make_unique<OrderedShardIdent>(get_nested_index());
  } else {
    return std::make_unique<OrderedShardIdent>();
  }
}

std::string OrderedBIndexer::OrderedShardIterator::get_oid() const {
  if (valid()) {
    return bindexer.index_shard_oid(get_nested_index());
  } else {
    return "";
  }
}

/*
 * we can expect one of three types of values for start_from:
 *   * the cut point from a previous shard entry (starts with
 *     OMAP_KEY_PREFIX)
 *   * a marker from an incomplete previous cycle (will already have
 *     OMAP_KEY_PREFIX)
 *   * a special value just before OMAP_KEY_PREFIX specifically to
 *     retrieve the final shard at OMAP_KEY_PREFIX
 *
 */
int OrderedBIndexer::OrderedShardIterator::load_batch(
  const std::string& start_from, // will already have prefix if needed
  bool* has_more_p)
{
  constexpr uint16_t default_batch_size = 20;

  batch.clear();

  // optimize in case we're seeking out the last shard, let's only
  // retrieve one
  uint16_t batch_size = start_from < OMAP_KEY_PREFIX ? 1 : default_batch_size;

  int r;
  std::map<std::string, bufferlist> intermediate; // intermediate results

  r = bindexer.bucket_sobj->read_bucket_info_map(bindexer.bucket_info.bucket.get_key(),
						 start_from,
						 OMAP_KEY_PREFIX,
						 batch_size,
						 &OMAP_KEY_PREFIX,
						 &intermediate,
						 nullptr, // has_more
						 null_yield,
						 bindexer.dpp);
  if (r < 0) {
    ldpp_dout(bindexer.dpp, 0) << "ERROR: " << __func__ <<
      ": call to read_bucket_info_map returned " << r << dendl;
    return r;
  }

  // our has_more logic is different than that of
  // read_bucket_info_map; has_more is true until the batch contains
  // the last shard
  *has_more_p = true;

  // decode and transfer retrieved items; if we hit the last shard, we
  // stop
  for (const auto& i : intermediate) {
    rgw_ordered_bi_omap_value value;
    auto it = i.second.cbegin();
    value.decode(it);
    batch[i.first] = value;
    if (i.first == OMAP_KEY_PREFIX) {
      *has_more_p = false;
      break;
    }
  }

  // if we did not retrieve anything, we ran out of main line shards
  // and must retrieve the last shard
  if (batch.empty()) {
    // set up a "marker" that will guarantee we get the last shard,
    // which is keyed by just OMAP_KEY_PREFIX; this is because
    // read_bucket_info gets entries starting *after* the marker
    std::string preprefix = calc_preprefix(OMAP_KEY_PREFIX);

    r = bindexer.bucket_sobj->read_bucket_info_map(bindexer.bucket_info.bucket.get_key(),
						   preprefix,
						   OMAP_KEY_PREFIX,
						   1,
						   nullptr, // default key
						   &intermediate,
						   nullptr, // has_more
						   null_yield,
						   bindexer.dpp);
    if (r < 0) {
      ldpp_dout(bindexer.dpp, 0) << "ERROR: " << __func__ <<
	" call to read_bucket_info_map returned " << r << dendl;
    }

    // sanity check 1
    if (intermediate.size() != 1) {
      ldpp_dout(bindexer.dpp, 0) << "ERROR: " << __func__ <<
	" when loading last shard expected one entry but got " <<
	intermediate.size() << dendl;
      return -ENOENT;
    }

    // sanity check 2
    auto i = intermediate.cbegin();
    if (i->first != OMAP_KEY_PREFIX) {
      ldpp_dout(bindexer.dpp, 0) << "ERROR: " << __func__ <<
	" when loading last shard expected the key to be \"" << OMAP_KEY_PREFIX <<
	"\" but it was instead \"" << i->first << "\"" << dendl;
      return -ENOENT;
    }

    // decode and transfer the last shard's data to batch
    auto it = i->second.cbegin();
    rgw_ordered_bi_omap_value value;
    value.decode(it);
    batch[i->first] = value;
    *has_more_p = false;
  }

  batch_it = batch.cbegin();

  return 0;
}

std::ostream& operator<<(std::ostream& out,
			 const OrderedBIndexer::OrderedShardIterator& i)
{
  out << "OrderedShardIterator:{ status=" << static_cast<int>(i.status) <<
    ", batch.size=" << i.batch.size() <<
    ", (batch_it == end)=" << (i.batch_it == i.batch.cend() ? "true" : "false");
  if (i.batch_it != i.batch.cend()) {
    out << ", split=" << i.get_split() <<
      ", nested_index=" << i.get_nested_index();
  }
  out << " }";

  return out;
}

std::ostream& operator<<(std::ostream& out, const OrderedBIndexer::InitialLayout& layout)
{
  out << "InitialLayout:{ ";
  for (const auto& i : layout) {
    out << i.first << ":{ " << i.second.split_point <<
      ", " << i.second.index_id << " }, ";
  }
  out << " }";

  return out;
}


} // namespace rgw::rados
