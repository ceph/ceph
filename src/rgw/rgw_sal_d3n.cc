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

#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_sal_d3n.h"
#include "rgw_aio_throttle.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

class RadosStore;

int D3NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterDriver::initialize(cct, dpp);
  d3n_cache->init(cct);

  return 0;
}

std::unique_ptr<Object> D3NFilterDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> obj = next->get_object(k);
  return std::make_unique<D3NFilterObject>(std::move(obj), this);
}

std::unique_ptr<Bucket> D3NFilterDriver::get_bucket(const RGWBucketInfo& i)
{
  std::unique_ptr<Bucket> bp = next->get_bucket(i);
  return std::make_unique<D3NFilterBucket>(std::move(bp), this);
}

std::unique_ptr<Object> D3NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> obj = next->get_object(k);
  return std::make_unique<D3NFilterObject>(std::move(obj), this, this->filter);
}

std::unique_ptr<Object::ReadOp> D3NFilterObject::get_read_op()
{
  std::unique_ptr<Object::ReadOp> rop = next->get_read_op();
  return std::make_unique<D3NFilterReadOp>(std::move(rop), this, filter);
}

int D3NFilterObject::D3NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  next->params.mod_ptr = params.mod_ptr;
  next->params.unmod_ptr = params.unmod_ptr;
  next->params.high_precision_time = params.high_precision_time;
  next->params.mod_zone_id = params.mod_zone_id;
  next->params.mod_pg_ver = params.mod_pg_ver;
  next->params.if_match = params.if_match;
  next->params.if_nomatch = params.if_nomatch;
  next->params.lastmod = params.lastmod;

  auto ret = next->prepare(y, dpp);
  if (ret < 0) {
    return ret;
  }

  if (! this->source->have_instance()) {
    RGWObjState* state = nullptr;
    if (this->source->get_obj_state(dpp, &state, y) == 0) {
      auto it = state->attrset.find(RGW_ATTR_ID_TAG);
      if (it != state->attrset.end()) {
        bufferlist bl = it->second;
        this->source->set_object_version(bl.c_str());
        ldpp_dout(dpp, 20) << __func__ << "id tag version is: " << this->source->get_object_version() << dendl;
      } else {
        ldpp_dout(dpp, 20) << __func__ << "Failed to find id tag" << dendl;
      }
    }
  }

  return 0;
}

void D3NFilterObject::D3NFilterReadOp::cancel() {
  aio->drain();
}

int D3NFilterObject::D3NFilterReadOp::drain(const DoutPrefixProvider* dpp) {
  auto c = aio->drain();
  int r = flush(dpp, std::move(c));
  if (r < 0) {
    cancel();
    return r;
  }
  return 0;
}

int D3NFilterObject::D3NFilterReadOp::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results) {
  int r = rgw::check_for_errors(results);
  if (r < 0) {
    return r;
  }
  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  ldpp_dout(dpp, 20) << "D3NFilterObject::In flush:: " << dendl;

  while (!completed.empty() && completed.front().id == offset) {
    auto bl = std::move(completed.front().data);

    ldpp_dout(dpp, 20) << "D3NFilterObject::flush:: calling handle_data for offset: " << offset << " bufferlist length: " << bl.length() << dendl;

    bl_list.push_back(bl);
    offset += bl.length();
    int r = client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      return r;
    }
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }
  return 0;
}

int D3NFilterObject::D3NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y)
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string version = source->get_object_version();
  std::string prefix;
  if (version.empty()) {
    prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();
  } else {
    prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_key().get_oid();
  }

  this->cb->set_prefix(prefix);
  ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << "prefix: " << prefix << dendl;
  ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << "oid: " << source->get_key().get_oid() << " ofs: " << ofs << " end: " << end << dendl;
  
  this->client_cb = cb;
  this->cb->set_client_cb(cb);
  aio = rgw::make_throttle(window_size, y);

  uint64_t obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/obj_max_req_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*obj_max_req_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t diff_ofs = ofs - adjusted_start_ofs; //difference between actual offset and adjusted offset for the first chunk/part
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%obj_max_req_size) == 0 ? len/obj_max_req_size : (len/obj_max_req_size) + 1; //calculate num parts based on adjusted offset
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << "obj_max_req_size " << obj_max_req_size << " num_parts " << num_parts << " adjusted_start_offset: " << adjusted_start_ofs << " len: " << len << dendl;
  ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << "diff_ofs " << diff_ofs << dendl;

  this->offset = ofs;
  do {
    uint64_t id = adjusted_start_ofs, read_ofs = 0; //read_ofs is the actual offset to start reading from the current part/ chunk
    if (start_part_num == (num_parts - 1)) {
      len_to_read = len;
      part_len = len;
      cost = len;
    } else {
      len_to_read = obj_max_req_size;
      cost = obj_max_req_size;
      part_len = obj_max_req_size;
    }
    if (start_part_num == 0) {
      len_to_read -= diff_ofs;
      id += diff_ofs;
      read_ofs = diff_ofs;
    }
    std::string oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);
    rgw_raw_obj r_obj;
    r_obj.oid = oid_in_cache;
    ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << " read_ofs: " << read_ofs << " part len: " << part_len << dendl;
    if (filter->get_d3n_cache()->get(oid_in_cache, part_len)) {
      // Read From Cache
      auto completed = aio->get(r_obj, rgw::d3n::cache_read_op(dpp, y, read_ofs, len_to_read, filter->get_d3n_cache()->cache_location), cost, id);
      ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
      auto r = flush(dpp, std::move(completed));
      if (r < 0) {
        ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
        drain(dpp);
        return r;
      }
    } else {
      //for ranged requests, for last part, the whole part might exist in the cache
      oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(obj_max_req_size);
      r_obj.oid = oid_in_cache;
      ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << " read_ofs: " << read_ofs << " part len: " << part_len << dendl;
      if ((part_len != obj_max_req_size) && filter->get_d3n_cache()->get(oid_in_cache, obj_max_req_size)) {
        // Read From Cache
        auto completed = aio->get(r_obj, rgw::d3n::cache_read_op(dpp, y, read_ofs, len_to_read, filter->get_d3n_cache()->cache_location), cost, id);
        ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
        auto r = flush(dpp, std::move(completed));
        if (r < 0) {
          ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
          drain(dpp);
          return r;
        }
      } else {
        ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
        auto r = drain(dpp);
        if (r < 0) {
          ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, r= " << r << dendl;
          return r;
        }
        break;
      }
    }
    if (start_part_num == (num_parts - 1)) {
      ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Info: draining datafor oid: " << oid_in_cache << dendl;
      return drain(dpp);
    } else {
      adjusted_start_ofs += obj_max_req_size;
    }
    start_part_num += 1;
    len -= obj_max_req_size;
  } while(start_part_num < num_parts);

  ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << dendl;
  Attrs obj_attrs;
  if (source->has_attrs()) {
    obj_attrs = source->get_attrs();
  }
  if (source->is_compressed() || obj_attrs.find(RGW_ATTR_CRYPT_MODE) != obj_attrs.end()) {
    ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Skipping writing to cache" << dendl;
    this->cb->bypass_cache_write();
  }
  if (start_part_num == 0) {
    this->cb->set_ofs(ofs);
  } else {
    this->cb->set_ofs(adjusted_start_ofs);
    ofs = adjusted_start_ofs;
  }
  this->cb->set_ofs(ofs);
  auto r = next->iterate(dpp, ofs, end, this->cb.get(), y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "D3NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, r= " << r << dendl;
    return r;
  }
  return this->cb->flush_last_part();
}

int D3NFilterObject::D3NFilterReadOp::D3NFilterGetCB::flush_last_part()
{
  last_part = true;
  return handle_data(bl_rem, 0, bl_rem.length());
}

int D3NFilterObject::D3NFilterReadOp::D3NFilterGetCB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  auto rgw_get_obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;
  if (!last_part && bl.length() <= rgw_get_obj_max_req_size) {
    auto r = client_cb->handle_data(bl, bl_ofs, bl_len);
    if (r < 0) {
      return r;
    }
  }

  //Accumulating data from backend store into rgw_get_obj_max_req_size sized chunks and then writing to cache
  if (write_to_cache) {
    const std::lock_guard l(d3n_get_data.d3n_lock);
    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = this->prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      filter->get_d3n_cache()->put(bl, bl.length(), oid);
    } else if (bl.length() == rgw_get_obj_max_req_size && bl_rem.length() == 0) { // if bl is the same size as rgw_get_obj_max_req_size, write it to cache
        std::string oid = this->prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
        ofs += bl_len;
        filter->get_d3n_cache()->put(bl, bl.length(), oid);
    } else { //copy data from incoming bl to bl_rem till it is rgw_get_obj_max_req_size, and then write it to cache
      uint64_t rem_space = rgw_get_obj_max_req_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;
      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);
      if (bl_rem.length() == g_conf()->rgw_get_obj_max_req_size) {
        std::string oid = this->prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_rem.length());
        ofs += bl_rem.length();
        filter->get_d3n_cache()->put(bl_rem, bl_rem.length(), oid);
        bl_rem.clear();
        bl_rem = std::move(bl);
      }
  }
}
  return 0;
}

} }// namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD3NFilter(rgw::sal::Driver* next)
{
  rgw::sal::D3NFilterDriver* filter = new rgw::sal::D3NFilterDriver(next);

  return filter;
}

}