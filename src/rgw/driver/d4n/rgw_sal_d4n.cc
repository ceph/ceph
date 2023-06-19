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

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  /* Execute cache replacement policy */
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
  
  /* Local cache check */
  if (source->driver->get_cache_driver()->key_exists(dpp, oid)) { // Entire object for now -Sam
    ret = source->driver->get_cache_driver()->get(dpp, source->get_key().get_oid(), ofs, len, bl, source->get_attrs());
    cb->handle_data(bl, ofs, len);
  } else {
    /* Block directory check */
    int getDirReturn = source->driver->get_block_dir()->get_value(source->driver->get_cache_block()); 

    if (getDirReturn >= -1) {
      if (getDirReturn == -1) {
        ldpp_dout(dpp, 20) << "D4N Filter: Block directory get operation failed." << dendl;
      } else {
        ldpp_dout(dpp, 20) << "D4N Filter: Block directory get operation succeeded." << dendl;
      }

      // remote cache get

      /* Cache block locally */
      ret = source->driver->get_cache_driver()->put(dpp, source->get_key().get_oid(), bl, len, source->get_attrs()); // May be put_async -Sam

      if (!ret) {
	int updateValueReturn = source->driver->get_block_dir()->update_field(source->driver->get_cache_block(), "hostsList", ""/*local cache ip from config*/);

	if (updateValueReturn < 0) {
	  ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation failed." << dendl;
	} else {
	  ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation succeeded." << dendl;
	}
	
	cb->handle_data(bl, ofs, len);
      }
    } else {
      /* Write tier retrieval */
      ldpp_dout(dpp, 20) << "D4N Filter: Block directory get operation failed." << dendl;
      getDirReturn = source->driver->get_obj_dir()->get_value(&(source->driver->get_cache_block()->cacheObj));

      if (getDirReturn >= -1) {
	if (getDirReturn == -1) {
	  ldpp_dout(dpp, 20) << "D4N Filter: Object directory get operation failed." << dendl;
	} else {
	  ldpp_dout(dpp, 20) << "D4N Filter: Object directory get operation succeeded." << dendl;
	}
	
	// retrieve from write back cache, which will be stored as a cache driver instance in the filter

	/* Cache block locally */
	ret = source->driver->get_cache_driver()->put(dpp, source->get_key().get_oid(), bl, len, source->get_attrs()); // May be put_async -Sam

	if (!ret) {
	  int updateValueReturn = source->driver->get_block_dir()->update_field(source->driver->get_cache_block(), "hostsList", ""/*local cache ip from config*/);

	  if (updateValueReturn < 0) {
	    ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation failed." << dendl;
	  } else {
	    ldpp_dout(dpp, 20) << "D4N Filter: Block directory update value operation succeeded." << dendl;
	  }
	  
	  cb->handle_data(bl, ofs, len);
	}
      } else {
	/* Backend store retrieval */
	ldpp_dout(dpp, 20) << "D4N Filter: Object directory get operation failed." << dendl;
	ret = next->iterate(dpp, ofs, end, cb, y);

	if (!ret) {
	  /* Cache block locally */
	  ret = source->driver->get_cache_driver()->put(dpp, source->get_key().get_oid(), bl, len, source->get_attrs()); // May be put_async -Sam

	  /* Store block in directory */
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

  return next->iterate(dpp, ofs, end, cb, y); 
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
