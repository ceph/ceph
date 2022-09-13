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

int D4NFilterStore::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterStore::initialize(cct, dpp);
  blk_dir->init(cct);
  d4n_cache->init(cct);
  
  return 0;
}

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) 
{
  // Currently untested -Sam
  if (setattrs != NULL) {
    int updateAttrsReturn = filter->get_d4n_cache()->updateAttrs(this->get_name(), setattrs);

    if (updateAttrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache update attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache update attributes operation succeeded." << dendl;
    }
  }
  
  if (delattrs != NULL) {
    std::vector<std::string> fields;
    Attrs::iterator attrs;

    for (attrs = delattrs->begin(); attrs != delattrs->end(); ++attrs) {
      fields.push_back(attrs->first);
    }

    int delAttrsReturn = filter->get_d4n_cache()->delAttrs(this->get_name(), fields);

    if (delAttrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete attributes operation succeeded." << dendl;
    }
  }

  return next->set_obj_attrs(dpp, setattrs, delattrs, y);  
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  int getObjReturn = filter->get_d4n_cache()->getObject(this); // Currently untested -Sam

  if (getObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get attrs operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get attrs operation succeeded." << dendl;
  }

  return next->get_obj_attrs(y, dpp, target_obj);
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  int updateAttrsReturn = filter->get_d4n_cache()->updateAttrs(this->get_name(), &update); // Currently untested -Sam

  if (updateAttrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache update attribute operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache update attribute operation succeeded." << dendl;
  }

  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);  
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) 
{
  std::vector<std::string> fields;
  fields.push_back((std::string)attr_name);
  int delAttrReturn = filter->get_d4n_cache()->delAttrs(this->get_name(), fields); // Currently untested -Sam

  if (delAttrReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete attribute operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete attribute operation succeeded." << dendl;
  }
  
  return next->delete_obj_attrs(dpp, attr_name, y);  
}

std::unique_ptr<Object> D4NFilterStore::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);
  return std::make_unique<D4NFilterObject>(std::move(o), this);
}

std::unique_ptr<Writer> D4NFilterStore::get_atomic_writer(const DoutPrefixProvider *dpp,
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

  return std::make_unique<D4NFilterWriter>(std::move(writer), this, std::move(_head_obj), dpp);
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
  int getDirReturn = source->filter->get_block_dir()->getValue(source->filter->get_cache_block());

  if (getDirReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory get operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory get operation succeeded." << dendl;
  }

  int getObjReturn = source->filter->get_d4n_cache()->getObject(source);

  if (getObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get operation succeeded." << dendl;
  }
  
  return next->prepare(y, dpp);
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  int delDirReturn = source->filter->get_block_dir()->delValue(source->filter->get_cache_block());

  if (delDirReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory delete operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory delete operation succeeded." << dendl;
  }

  int delObjReturn = source->filter->get_d4n_cache()->delObject(source);

  if (delObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete operation succeeded." << dendl;
  }

  return next->delete_obj(dpp, y);
}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  cache_block* temp_cache_block = filter->get_cache_block();
  RGWBlockDirectory* temp_block_dir = filter->get_block_dir();

  temp_cache_block->hosts_list.push_back(temp_block_dir->get_host() + ":" + std::to_string(temp_block_dir->get_port())); 
  temp_cache_block->size_in_bytes = accounted_size;
  temp_cache_block->c_obj.bucket_name = head_obj->get_bucket()->get_name();
  temp_cache_block->c_obj.obj_name = head_obj->get_name();

  int setDirReturn = temp_block_dir->setValue(temp_cache_block);

  if (setDirReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Directory set operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Directory set operation succeeded." << dendl;
  }

  int returnVal = next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, y);
  head_obj->get_obj_attrs(y, save_dpp);
  
  int setObjReturn = filter->get_d4n_cache()->setObject(head_obj->get_attrs(), &attrs, head_obj->get_name());

  if (setObjReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache set operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache set operation succeeded." << dendl;
  }
  
  return returnVal;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Store* newD4NFilter(rgw::sal::Store* next)
{
  rgw::sal::D4NFilterStore* store = new rgw::sal::D4NFilterStore(next);

  return store;
}

}
