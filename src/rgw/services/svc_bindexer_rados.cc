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
			   const LayoutVariant& layout) {
  const auto* nl = std::get_if<bucket_index_hashed_layout>(&layout);
  if (nl) {
    return new HashedBIndexer(cct, dpp, bucket_info, *nl);
  }

  const auto* ol = std::get_if<bucket_index_ordered_layout>(&layout);
  if (ol) {
    return new OrderedBIndexer(cct, dpp, bucket_info, *ol);
  }

  return nullptr;
}


/******************** HashedBIndexer ********************/

BIndexer::ShardIterator HashedBIndexer::create_shard_iterator() const {
  return ShardIterator(new HashedShardIterator(*this));
}

BIndexer::ShardIterator HashedBIndexer::create_shard_iterator(
  const std::string& start_at) const
{
  BIShardIndex index;
  shard_of(start_at, &index);
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

std::unique_ptr<BIShardIdent> HashedBIndexer::shard_of(const std::string& obj_key) const {
  return std::make_unique<HashedShardIdent>(
    get_shard_index(obj_key, get_actual_num_shards()));
}

std::unique_ptr<BIShardIdent> HashedBIndexer::shard_of(const rgw_obj_key& obj_key) const {
  return std::make_unique<HashedShardIdent>(
    get_shard_index(obj_key, get_actual_num_shards()));
}

void HashedBIndexer::shard_of(const std::string& obj_key, BIShardIndex* index) const {
  get_shard_index(obj_key, get_actual_num_shards());
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
  BIShardIndex* shard_index) const
{
  std::string bucket_oid_base = dir_oid_prefix;
  bucket_oid_base.append(bucket_info.bucket.bucket_id);
  const auto gen_id = layout_gen.gen;

  if (! layout.num_shards) {
    // by default with no sharding, we use the bucket oid as itself
    *bucket_obj = bucket_oid_base;
    if (shard_index) {
      *shard_index = -1;
    }
  } else {
    uint32_t sid = get_shard_index(obj_key, layout.num_shards);
    if (bucket_obj) {
      *bucket_obj = index_shard_oid(bucket_oid_base, sid, gen_id);
    }
    if (shard_index) {
      *shard_index = int(sid);
    }
  }

  return 0;
}


int HashedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  BIShardIdent* shard_id) const
{
  BIShardIndex index;

  int ret = get_bucket_index_object(obj_key, bucket_obj, &index);
  if (ret < 0) {
    return ret;
  }

  *shard_id = HashedShardIdent(index);
  return 0;
}


int HashedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  std::unique_ptr<BIShardIdent>& shard_id) const
{
  BIShardIndex index;

  int ret = get_bucket_index_object(obj_key, bucket_obj, &index);
  if (ret < 0) {
    return ret;
  }

  shard_id = std::make_unique<HashedShardIdent>(index);
  return 0;
}


void HashedBIndexer::get_bucket_index_object(
  uint64_t gen_id,
  BIShardIndex shard_id,
  std::string* bucket_obj) const
{
  const std::string oid_base = get_bucket_oid_base();
  *bucket_obj = index_shard_oid(oid_base, shard_id, gen_id);
}

void HashedBIndexer::get_initial_bucket_index_objects(
  std::map<int, std::string>& shard_oids,
  std::map<int, bufferlist>& per_shard_data,
  std::map<std::string, bufferlist>* binfo_map_data) const
{
  const std::string bucket_oid_base = get_bucket_oid_base();
  const BIShardIndex shard_count = num_shards(layout_gen);

  const auto gen_id = layout_gen.gen;

  for (BIShardIndex i = 0; i < shard_count; ++i) {
    shard_oids.emplace(
      std::make_pair(int(i), index_shard_oid(bucket_oid_base, i, gen_id)));
  }
}


/******************** OrderedBIndexer ********************/


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

std::unique_ptr<BIShardIdent> OrderedBIndexer::shard_of(const std::string& obj_key) const {
  UNIMPLEMENTED();
  return std::make_unique<OrderedShardIdent>(64000);
}

std::unique_ptr<BIShardIdent> OrderedBIndexer::shard_of(const rgw_obj_key& obj_key) const {
  UNIMPLEMENTED();
  return std::make_unique<OrderedShardIdent>(64000);
}

const std::vector<std::pair<std::string, NestedIndex>>&
OrderedBIndexer::get_initial_shard_data()
{
  // initially we'll create 3 shards, the first designed to capture
  // names that begin with "/", the seconds with upper-case letters,
  // and the third for for the rest, which includes lower-case letters
  // and non-ASCII. Please note that all entries in a shard are LESS
  // THAN the cut-off. The shard with an empty-string cut-off is the
  // last shard
  static const std::vector<std::pair<std::string, NestedIndex>> fixed_data =
  {
#if 0
      { "_", { 0 + STARTING_INDEX_ID } },
      { "~", { 1 + STARTING_INDEX_ID, 7 } },
      { "", { 2 + STARTING_INDEX_ID, 13, 17 } }
#else
      { "C", { -5 + STARTING_INDEX_ID } },
      { "E", { 0 + STARTING_INDEX_ID, 2 } },
      { "J", { 0 + STARTING_INDEX_ID, 99 } },
      { "_", { 1 + STARTING_INDEX_ID, 8 } },
      { "e", { 1 + STARTING_INDEX_ID, 8, 1, 2, 4 } },
      { "i", { 1 + STARTING_INDEX_ID, 99 } },
      { "q", { 2 + STARTING_INDEX_ID } },
      { "~", { 2 + STARTING_INDEX_ID, 7 } },
      { "Æ", { 7 + STARTING_INDEX_ID, 12 } },
      { "Ü", { 50 + STARTING_INDEX_ID } },
      { "", { 51 + STARTING_INDEX_ID, 13, 17 } }
#endif
    };
  return fixed_data;
}

void OrderedBIndexer::get_initial_bucket_index_objects(
    std::map<int, std::string>& shard_oids,
    std::map<int, bufferlist>& per_shard_data,
    std::map<std::string, bufferlist>* binfo_map_data) const
{
  const std::string bucket_oid_base = get_bucket_oid_base();

  const auto gen_id = layout_gen.gen;

  const auto& fixed_data = get_initial_shard_data();

  if (binfo_map_data) {
    for (const auto& p : fixed_data) {
      rgw_ordered_bi_omap_value v;
      v.split = p.first;
      v.shard_ident = p.second;
      bufferlist bl;
      v.encode(bl);
      const std::string key = OMAP_KEY_PREFIX + v.split;
      (*binfo_map_data)[key] = bl;
    }
  }

  const NestedIndex empty_index;

  std::vector<rgw_ordered_bi_shard_data> per_shard;
  NestedIndex prev_ident = empty_index;
  for (const auto& p : fixed_data) {
    rgw_ordered_bi_shard_data d;
    d.split = p.first;
    d.shard_ident = p.second;
    d.prev_shard_ident = prev_ident;
    prev_ident = p.second;
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

#warning "ERIC work on this NEXT"
int OrderedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  BIShardIndex* shard_index) const
{
#warning "TRACE 127"
  UNIMPLEMENTED();
  return -1;
}

int OrderedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  BIShardIdent* shard_id) const
{
  const std::string oid_base = get_bucket_oid_base();
  const auto gen_id = layout_gen.gen;

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

  if (bucket_obj) {
    *bucket_obj = index_shard_oid(oid_base, value.shard_ident, gen_id);
  }

  if (shard_id) {
    *shard_id = OrderedShardIdent(std::move(value.shard_ident));
  }

  return 0;
}

int OrderedBIndexer::get_bucket_index_object(
  const std::string& obj_key,
  std::string* bucket_obj,
  std::unique_ptr<BIShardIdent>& shard_id) const
{
  const std::string oid_base = get_bucket_oid_base();
  const auto gen_id = layout_gen.gen;
  static const std::string default_key = "";

  std::map<std::string, bufferlist> results;

  const std::string prefixed_key = OMAP_KEY_PREFIX + obj_key;
  int r =
    bucket_sobj->read_bucket_info_map(bucket_info.bucket.get_key(),
				      prefixed_key, OMAP_KEY_PREFIX, 1, &default_key,
				      &results, nullptr,
				      null_yield, dpp);
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

  if (bucket_obj) {
    *bucket_obj = index_shard_oid(oid_base, value.shard_ident, gen_id);
  }

#warning "is this implemented correctly?"
  shard_id.reset(new OrderedShardIdent(value.shard_ident));

  return 0;
}

void OrderedBIndexer::get_bucket_index_object(
  uint64_t gen_id,
  BIShardIndex shard_id,
  std::string* bucket_obj) const
{
  UNIMPLEMENTED();
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
    std::string marker = "";
    bool more = true;
    BIShardIndex counter = 0;
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
	const std::string oid = index_shard_oid(oid_base, value.shard_ident, gen_id);
	(*shard_oids)[counter++] = oid;
      }
    } // while
  }
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
    std::string preprefix = OMAP_KEY_PREFIX;
    preprefix.replace(preprefix.end() - 1, preprefix.end(), "-");

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

} // namespace rgw::rados
