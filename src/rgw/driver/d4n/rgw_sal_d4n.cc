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

#include "rgw_sal_d4n.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;
  
  return dynamic_cast<FilterObject*>(t)->get_next();
}

int D4NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterDriver::initialize(cct, dpp);

  cacheDriver->initialize(cct, dpp);

  objDir->init(cct);
  blockDir->init(cct);

  policyDriver->init();
  policyDriver->cachePolicy->init(cct);
  
  return 0;
}

std::unique_ptr<User> D4NFilterDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<D4NFilterUser>(std::move(user), this);
}

std::unique_ptr<Object> D4NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this, filter);
}

int D4NFilterBucket::create(const DoutPrefixProvider* dpp,
                            const CreateParams& params,
                            optional_yield y)
{
  return next->create(dpp, params, y);
}

int D4NFilterObject::copy_object(User* user,
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
  /* Build cache block copy */
  rgw::d4n::CacheBlock* copyCacheBlock = new rgw::d4n::CacheBlock(); // How will this copy work in lfuda? -Sam

  copyCacheBlock->hostsList.push_back(driver->get_cache_block()->hostsList[0]); 
  copyCacheBlock->size = driver->get_cache_block()->size;
  copyCacheBlock->size = driver->get_cache_block()->globalWeight; // Do we want to reset the global weight? -Sam
  copyCacheBlock->cacheObj.bucketName = dest_bucket->get_name();
  copyCacheBlock->cacheObj.objName = dest_object->get_key().get_oid();
  
  int copy_valueReturn = driver->get_block_dir()->set_value(copyCacheBlock);

  if (copy_valueReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory copy operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory copy operation succeeded." << dendl;
  }

  delete copyCacheBlock;

  /* Append additional metadata to attributes */
  rgw::sal::Attrs baseAttrs = this->get_attrs();
  buffer::list bl;

  bl.append(to_iso_8601(*mtime));
  baseAttrs.insert({"mtime", bl});
  bl.clear();
  
  if (version_id != NULL) { 
    bl.append(*version_id);
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  }
 
  if (!etag->empty()) {
    bl.append(*etag);
    baseAttrs.insert({"etag", bl});
    bl.clear();
  }

  if (attrs_mod == rgw::sal::ATTRSMOD_REPLACE) { /* Replace */
    rgw::sal::Attrs::iterator iter;

    for (const auto& pair : attrs) {
      iter = baseAttrs.find(pair.first);
    
      if (iter != baseAttrs.end()) {
        iter->second = pair.second;
      } else {
        baseAttrs.insert({pair.first, pair.second});
      }
    }
  } else if (attrs_mod == rgw::sal::ATTRSMOD_MERGE) { /* Merge */
    baseAttrs.insert(attrs.begin(), attrs.end()); 
  }

  /*
  int copy_attrsReturn = driver->get_cache_driver()->copy_attrs(this->get_key().get_oid(), dest_object->get_key().get_oid(), &baseAttrs);

  if (copy_attrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache copy attributes operation failed." << dendl;
  } else {
    int copy_dataReturn = driver->get_cache_driver()->copy_data(this->get_key().get_oid(), dest_object->get_key().get_oid());

    if (copy_dataReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache copy data operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache copy object operation succeeded." << dendl;
    }
  }*/

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

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) 
{
  if (setattrs != NULL) {
    /* Ensure setattrs and delattrs do not overlap */
    if (delattrs != NULL) {
      for (const auto& attr : *delattrs) {
        if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }
    }

    int update_attrsReturn = driver->get_cache_driver()->set_attrs(dpp, this->get_key().get_oid(), *setattrs);

    if (update_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object attributes operation succeeded." << dendl;
    }
  }

  if (delattrs != NULL) {
    Attrs::iterator attr;
    Attrs currentattrs = this->get_attrs();

    /* Ensure all delAttrs exist */
    for (const auto& attr : *delattrs) {
      if (std::find(currentattrs.begin(), currentattrs.end(), attr) == currentattrs.end()) {
	delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
      }
    }

    int del_attrsReturn = driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), *delattrs);

    if (del_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attributes operation succeeded." << dendl;
    }
  }

  return next->set_obj_attrs(dpp, setattrs, delattrs, y);  
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  rgw::sal::Attrs attrs;
  int get_attrsReturn = driver->get_cache_driver()->get_attrs(dpp, this->get_key().get_oid(), attrs);

  if (get_attrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation failed." << dendl;

    return next->get_obj_attrs(y, dpp, target_obj);
  } else {
    int set_attrsReturn = this->set_attrs(attrs);
    
    if (set_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation failed." << dendl;

      return next->get_obj_attrs(y, dpp, target_obj);
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation succeeded." << dendl;
  
      return 0;
    }
  }
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  int update_attrsReturn = driver->get_cache_driver()->update_attrs(dpp, this->get_key().get_oid(), update);

  if (update_attrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache modify object attribute operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache modify object attribute operation succeeded." << dendl;
  }

  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);  
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  buffer::list bl;
  Attrs delattr;
  delattr.insert({attr_name, bl});
  Attrs currentattrs = this->get_attrs();
  rgw::sal::Attrs::iterator attr = delattr.begin();

  /* Ensure delAttr exists */
  if (std::find_if(currentattrs.begin(), currentattrs.end(),
        [&](const auto& pair) { return pair.first == attr->first; }) != currentattrs.end()) {
    int delAttrReturn = driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), delattr);

    if (delAttrReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attribute operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attribute operation succeeded." << dendl;
    }
  } else 
    return next->delete_obj_attrs(dpp, attr_name, y);  

  return 0;
}

std::unique_ptr<Object> D4NFilterDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this);
}

std::unique_ptr<Writer> D4NFilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<D4NFilterWriter>(std::move(writer), this, obj, dpp, true);
}

std::unique_ptr<Object::ReadOp> D4NFilterObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<D4NFilterReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> D4NFilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<D4NFilterDeleteOp>(std::move(d), this);
}

int D4NFilterObject::D4NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  rgw::sal::Attrs attrs;
  int getObjReturn = source->driver->get_cache_driver()->get_attrs(dpp, 
		                                                         source->get_key().get_oid(), 
					   				 attrs);

  int ret = next->prepare(y, dpp);
  
  if (getObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get object operation failed." << dendl;
  } else {
    /* Set metadata locally */
    RGWObjState* astate;
    RGWQuotaInfo quota_info;
    std::unique_ptr<rgw::sal::User> user = source->driver->get_user(source->get_bucket()->get_owner());
    source->get_obj_state(dpp, &astate, y);

    for (auto it = attrs.begin(); it != attrs.end(); ++it) {
      if (it->second.length() > 0) { // or return? -Sam
	if (it->first == "mtime") {
	  parse_time(it->second.c_str(), &astate->mtime);
	  attrs.erase(it->first);
	} else if (it->first == "object_size") {
	  source->set_obj_size(std::stoull(it->second.c_str()));
	  attrs.erase(it->first);
	} else if (it->first == "accounted_size") {
	  astate->accounted_size = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "epoch") {
	  astate->epoch = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "version_id") {
	  source->set_instance(it->second.c_str());
	attrs.erase(it->first);
      } else if (it->first == "source_zone_short_id") {
	astate->zone_short_id = static_cast<uint32_t>(std::stoul(it->second.c_str()));
	attrs.erase(it->first);
      } else if (it->first == "user_quota.max_size") {
        quota_info.max_size = std::stoull(it->second.c_str());
	attrs.erase(it->first);
      } else if (it->first == "user_quota.max_objects") {
        quota_info.max_objects = std::stoull(it->second.c_str());
	attrs.erase(it->first);
      } else if (it->first == "max_buckets") {
        user->set_max_buckets(std::stoull(it->second.c_str()));
	attrs.erase(it->first);
      }
    }
    user->set_info(quota_info);
    source->set_obj_state(*astate);
   
    /* Set attributes locally */
    int set_attrsReturn = source->set_attrs(attrs);

    if (set_attrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object operation succeeded." << dendl;
    }   
  }
  }

  return ret;
}

void D4NFilterObject::D4NFilterReadOp::cancel() {
  aio->drain();
}

int D4NFilterObject::D4NFilterReadOp::drain(const DoutPrefixProvider* dpp) {
  auto c = aio->wait();
  while (!c.empty()) {
    int r = flush(dpp, std::move(c));
    if (r < 0) {
      cancel();
      return r;
    }
    c = aio->wait();
  }
  return flush(dpp, std::move(c));
}

int D4NFilterObject::D4NFilterReadOp::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  ldpp_dout(dpp, 20) << "D4NFilterObject::In flush:: " << dendl;

  while (!completed.empty() && completed.front().id == offset) {
    auto bl = std::move(completed.front().data);

    ldpp_dout(dpp, 20) << "D4NFilterObject::flush:: calling handle_data for offset: " << offset << " bufferlist length: " << bl.length() << dendl;

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

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string oid = source->get_key().get_oid();

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "oid: " << oid << " ofs: " << ofs << " end: " << end << dendl;

  this->client_cb = cb;
  this->cb->set_client_cb(cb); // what's this for? -Sam

  uint64_t obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/obj_max_req_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*obj_max_req_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t diff_ofs = ofs - adjusted_start_ofs; //difference between actual offset and adjusted offset
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%obj_max_req_size) == 0 ? len/obj_max_req_size : (len/obj_max_req_size) + 1; //calculate num parts based on adjusted offset
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  if (y) {
    aio = rgw::make_throttle(window_size, y);

    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "obj_max_req_size " << obj_max_req_size << 
      " num_parts " << num_parts << " adjusted_start_offset: " << adjusted_start_ofs << " len: " << len << dendl;

    this->offset = ofs;

    do {
      uint64_t id = adjusted_start_ofs;
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
      }

      uint64_t read_ofs = diff_ofs; //read_ofs is the actual offset to start reading from the current part/ chunk
      std::string oid_in_cache = source->get_bucket()->get_marker() + "_" + oid + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);
      rgw_raw_obj r_obj;
      r_obj.oid = oid_in_cache;
      ceph::bufferlist bl;

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << 
	oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
	" read_ofs: " << read_ofs << " part len: " << part_len << dendl;

      if (source->driver->get_cache_driver()->get(dpp, oid_in_cache, ofs, part_len, bl, source->get_attrs()) == 0) { 
        // Read From Cache
        auto completed = aio->get(r_obj, rgw::Aio::cache_read_op(dpp, y, source->driver->get_cache_driver(), 
			   read_ofs, len_to_read, oid_in_cache), cost, id); 

        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;

        auto r = flush(dpp, std::move(completed));

        if (r < 0) {
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
          return r;
        }
      } else {
        //for ranged requests, for last part, the whole part might exist in the cache
        oid_in_cache = source->get_bucket()->get_marker() + "_" + oid + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(obj_max_req_size);
        r_obj.oid = oid_in_cache;

        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << 
	  oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
	  " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

        if (source->driver->get_cache_driver()->get(dpp, oid_in_cache, ofs, obj_max_req_size, bl, source->get_attrs()) == 0) {
          // Read From Cache
	  auto completed = aio->get(r_obj, rgw::Aio::cache_read_op(dpp, y, source->driver->get_cache_driver(), 
			     read_ofs, len_to_read, oid_in_cache), cost, id); 

          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;

          auto r = flush(dpp, std::move(completed));

          if (r < 0) {
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
            return r;
          }
        } else {
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;

          auto r = drain(dpp);

          if (r < 0) {
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, r= " << r << dendl;
            return r;
          }

          break;
        }
      }

      if (start_part_num == (num_parts - 1)) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
        return drain(dpp);
      } else {
        adjusted_start_ofs += obj_max_req_size;
      }

      start_part_num += 1;
      len -= obj_max_req_size;
    } while (start_part_num < num_parts);
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << dendl;

  Attrs obj_attrs;
  if (source->has_attrs()) {
    obj_attrs = source->get_attrs();
  }

  if (source->is_compressed() || obj_attrs.find(RGW_ATTR_CRYPT_MODE) != obj_attrs.end() || !y) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Skipping writing to cache" << dendl;
    this->cb->bypass_cache_write();
  }

  if (start_part_num == 0) {
    this->cb->set_ofs(ofs);
  } else {
    this->cb->set_ofs(adjusted_start_ofs);
    ofs = adjusted_start_ofs; // redundant? -Sam
  }

  this->cb->set_ofs(ofs);
  auto r = next->iterate(dpp, ofs, end, this->cb.get(), y);
  
  if (r < 0) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, r= " << r << dendl;
    return r;
  }
  
  return this->cb->flush_last_part(dpp);

  /*
  / Execute cache replacement policy /
  int policyRet = source->driver->get_policy_driver()->cachePolicy->get_block(dpp, source->driver->get_cache_block(), 
		    source->driver->get_cache_driver());
  
  if (policyRet < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache replacement operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache replacement operation succeeded." << dendl;
  }

  int ret = -1;
  bufferlist bl;
  uint64_t len = end - ofs + 1;
  std::string oid(source->get_name());
  
  / Local cache check /
  if (source->driver->get_cache_driver()->key_exists(dpp, oid)) { // Entire object for now -Sam
    ret = source->driver->get_cache_driver()->get(dpp, source->get_key().get_oid(), ofs, len, bl, source->get_attrs());
    cb->handle_data(bl, ofs, len);
  } else {
    / Block directory check /
    int getDirReturn = source->driver->get_block_dir()->get_value(source->driver->get_cache_block()); 

    if (getDirReturn >= -1) {
      if (getDirReturn == -1) {
        ldpp_dout(dpp, 20) << "D4N Filter: Block directory get operation failed." << dendl;
      } else {
        ldpp_dout(dpp, 20) << "D4N Filter: Block directory get operation succeeded." << dendl;
      }

      // remote cache get

      / Cache block locally /
      ret = source->driver->get_cache_driver()->put(dpp, source->get_key().get_oid(), bl, len, source->get_attrs()); // May be put_async -Sam

      if (!ret) {
	int updateValueReturn = source->driver->get_block_dir()->update_field(source->driver->get_cache_block(), "hostsList", ""/local cache ip from config/);

	if (updateValueReturn < 0) {
	  ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation failed." << dendl;
	} else {
	  ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation succeeded." << dendl;
	}
	
	cb->handle_data(bl, ofs, len);
      }
    } else {
      / Write tier retrieval /
      ldpp_dout(dpp, 20) << "D4N Filter: Block directory get operation failed." << dendl;
      getDirReturn = source->driver->get_obj_dir()->get_value(&(source->driver->get_cache_block()->cacheObj));

      if (getDirReturn >= -1) {
	if (getDirReturn == -1) {
	  ldpp_dout(dpp, 20) << "D4N Filter: Object directory get operation failed." << dendl;
	} else {
	  ldpp_dout(dpp, 20) << "D4N Filter: Object directory get operation succeeded." << dendl;
	}
	
	// retrieve from write back cache, which will be stored as a cache driver instance in the filter

	/ Cache block locally /
	ret = source->driver->get_cache_driver()->put(dpp, source->get_key().get_oid(), bl, len, source->get_attrs()); // May be put_async -Sam

	if (!ret) {
	  int updateValueReturn = source->driver->get_block_dir()->update_field(source->driver->get_cache_block(), "hostsList", ""/local cache ip from config/);

	  if (updateValueReturn < 0) {
	    ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation failed." << dendl;
	  } else {
	    ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation succeeded." << dendl;
	  }
	  
	  cb->handle_data(bl, ofs, len);
	}
      } else {
	/ Backend store retrieval /
	ldpp_dout(dpp, 20) << "D4N Filter: Object directory get operation failed." << dendl;
	ret = next->iterate(dpp, ofs, end, cb, y);

	if (!ret) {
	  / Cache block locally /
	  ret = source->driver->get_cache_driver()->put(dpp, source->get_key().get_oid(), bl, len, source->get_attrs()); // May be put_async -Sam

	  / Store block in directory /
	  rgw::d4n::BlockDirectory* tempBlockDir = source->driver->get_block_dir(); // remove later -Sam

	  source->driver->get_cache_block()->hostsList.push_back(tempBlockDir->get_addr().host + ":" + std::to_string(tempBlockDir->get_addr().port)); // local cache address -Sam 
	  source->driver->get_cache_block()->size = source->get_obj_size();
	  source->driver->get_cache_block()->cacheObj.bucketName = source->get_bucket()->get_name();
	  source->driver->get_cache_block()->cacheObj.objName = source->get_key().get_oid();

	  int setDirReturn = tempBlockDir->set_value(source->driver->get_cache_block());

	  if (setDirReturn < 0) {
	    ldpp_dout(dpp, 20) << "D4N Filter: Block directory set operation failed." << dendl;
	  } else {
	    ldpp_dout(dpp, 20) << "D4N Filter: Block directory set operation succeeded." << dendl;
	  }
	}
      }
    }
  }

  if (ret < 0) 
    ldpp_dout(dpp, 20) << "D4N Filter: Cache iterate operation failed." << dendl;

  return next->iterate(dpp, ofs, end, cb, y); */
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::flush_last_part(const DoutPrefixProvider* dpp)
{
  save_dpp = dpp;
  last_part = true;
  return handle_data(bl_rem, 0, bl_rem.length());
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
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
    Attrs attrs;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = this->oid + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);

      filter->get_cache_driver()->put(save_dpp, oid, bl, bl.length(), attrs); // need attrs for just chunk? -Sam
    } else if (bl.length() == rgw_get_obj_max_req_size && bl_rem.length() == 0) { // if bl is the same size as rgw_get_obj_max_req_size, write it to cache
      std::string oid = this->oid + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      ofs += bl_len;

      filter->get_cache_driver()->put(save_dpp, oid, bl, bl.length(), attrs); // need attrs for just chunk? -Sam
    } else { //copy data from incoming bl to bl_rem till it is rgw_get_obj_max_req_size, and then write it to cache
      uint64_t rem_space = rgw_get_obj_max_req_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;

      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);

      if (bl_rem.length() == g_conf()->rgw_get_obj_max_req_size) {
        std::string oid = this->oid + "_" + std::to_string(ofs) + "_" + std::to_string(bl_rem.length());
        ofs += bl_rem.length();

        filter->get_cache_driver()->put(save_dpp, oid, bl_rem, bl_rem.length(), attrs); // need attrs for just chunk? -Sam

        bl_rem.clear();
        bl_rem = std::move(bl);
      }
    }
  }

  return 0;
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                                   optional_yield y, uint32_t flags)
{
  int delDirReturn = source->driver->get_block_dir()->del_value(source->driver->get_cache_block());

  if (delDirReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory delete operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Block directory delete operation succeeded." << dendl;
  }

  Attrs::iterator attrs;
  Attrs currentattrs = source->get_attrs();
  std::vector<std::string> currentFields;
  
  /* Extract fields from current attrs */
  for (attrs = currentattrs.begin(); attrs != currentattrs.end(); ++attrs) {
    currentFields.push_back(attrs->first);
  }

  int delObjReturn = source->driver->get_cache_driver()->delete_data(dpp, source->get_key().get_oid());

  if (delObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object operation failed." << dendl;
  } else {
    Attrs delattrs = source->get_attrs();
    delObjReturn = source->driver->get_cache_driver()->delete_attrs(dpp, source->get_key().get_oid(), delattrs);
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete operation succeeded." << dendl;
  }

  return next->delete_obj(dpp, y, flags);
}

int D4NFilterWriter::prepare(optional_yield y) 
{
  int del_dataReturn = driver->get_cache_driver()->delete_data(save_dpp, obj->get_key().get_oid());

  if (del_dataReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache delete data operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache delete data operation succeeded." << dendl;
  }

  return next->prepare(y);
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
  int append_dataReturn = driver->get_cache_driver()->append_data(save_dpp, obj->get_key().get_oid(), data);

  if (append_dataReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache append data operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache append data operation succeeded." << dendl;
  }

  return next->process(std::move(data), offset);
}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  rgw::d4n::BlockDirectory* tempBlockDir = driver->get_block_dir();

  driver->get_cache_block()->hostsList.push_back(tempBlockDir->get_addr().host + ":" + std::to_string(tempBlockDir->get_addr().port)); 
  driver->get_cache_block()->size = accounted_size;
  driver->get_cache_block()->cacheObj.bucketName = obj->get_bucket()->get_name();
  driver->get_cache_block()->cacheObj.objName = obj->get_key().get_oid();

  int setDirReturn = tempBlockDir->set_value(driver->get_cache_block());

  if (setDirReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Block directory set operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Block directory set operation succeeded." << dendl;
  }
   
  /* Retrieve complete set of attrs */
  int ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, rctx, flags);
  obj->get_obj_attrs(rctx.y, save_dpp, NULL);

  /* Append additional metadata to attributes */ 
  rgw::sal::Attrs baseAttrs = obj->get_attrs();
  rgw::sal::Attrs attrs_temp = baseAttrs;
  buffer::list bl;
  RGWObjState* astate;
  obj->get_obj_state(save_dpp, &astate, rctx.y);

  bl.append(to_iso_8601(obj->get_mtime()));
  baseAttrs.insert({"mtime", bl});
  bl.clear();

  bl.append(std::to_string(obj->get_obj_size()));
  baseAttrs.insert({"object_size", bl});
  bl.clear();

  bl.append(std::to_string(accounted_size));
  baseAttrs.insert({"accounted_size", bl});
  bl.clear();
 
  bl.append(std::to_string(astate->epoch));
  baseAttrs.insert({"epoch", bl});
  bl.clear();

  if (obj->have_instance()) {
    bl.append(obj->get_instance());
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  } else {
    bl.append(""); /* Empty value */
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  }

  auto iter = attrs_temp.find(RGW_ATTR_SOURCE_ZONE);
  if (iter != attrs_temp.end()) {
    bl.append(std::to_string(astate->zone_short_id));
    baseAttrs.insert({"source_zone_short_id", bl});
    bl.clear();
  } else {
    bl.append("0"); /* Initialized to zero */
    baseAttrs.insert({"source_zone_short_id", bl});
    bl.clear();
  }

  baseAttrs.insert(attrs.begin(), attrs.end());

  int set_attrsReturn = driver->get_cache_driver()->set_attrs(save_dpp, obj->get_key().get_oid(), baseAttrs);

  if (set_attrsReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache set attributes operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache set attributes operation succeeded." << dendl;
  }
  
  return ret;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next)
{
  rgw::sal::D4NFilterDriver* driver = new rgw::sal::D4NFilterDriver(next);

  return driver;
}

}
