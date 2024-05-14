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

D4NFilterDriver::D4NFilterDriver(Driver* _next, boost::asio::io_context& io_context) : FilterDriver(_next),
                                                                                       io_context(io_context) 
{
  conn = std::make_shared<connection>(boost::asio::make_strand(io_context));

  rgw::cache::Partition partition_info;
  partition_info.location = g_conf()->rgw_d4n_l1_datacache_persistent_path;
  partition_info.name = "d4n";
  partition_info.type = "read-cache";
  partition_info.size = g_conf()->rgw_d4n_l1_datacache_size;

  cacheDriver = new rgw::cache::SSDDriver(partition_info);
  objDir = new rgw::d4n::ObjectDirectory(conn);
  blockDir = new rgw::d4n::BlockDirectory(conn);
  policyDriver = new rgw::d4n::PolicyDriver(conn, cacheDriver, "lfuda");
}

D4NFilterDriver::~D4NFilterDriver()
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });

  delete cacheDriver;
  delete objDir; 
  delete blockDir; 
  delete policyDriver;
}

int D4NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  namespace net = boost::asio;
  using boost::redis::config;

  std::string address = cct->_conf->rgw_d4n_address;
  config cfg;
  cfg.addr.host = address.substr(0, address.find(":"));
  cfg.addr.port = address.substr(address.find(":") + 1, address.length());
  cfg.clientname = "D4N.Filter";

  if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
    ldpp_dout(dpp, 10) << "D4NFilterDriver::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
    return -EDESTADDRREQ;
  }

  conn->async_run(cfg, {}, net::consign(net::detached, conn));

  FilterDriver::initialize(cct, dpp);

  cacheDriver->initialize(dpp);
  objDir->init(cct);
  blockDir->init(cct);
  policyDriver->get_cache_policy()->init(cct, dpp, io_context, next);

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

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags)
{
  //can we assume that get_obj_attrs() has been invoked before calling set_obj_attrs()
  rgw::sal::Attrs attrs;
  std::string head_oid_in_cache;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, y)) {
    if (setattrs != nullptr) {
      /* Ensure setattrs and delattrs do not overlap */
      if (delattrs != nullptr) {
        for (const auto& attr : *delattrs) {
          if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
            delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
          }
        }
      }
      //if set_obj_attrs() can be called to update existing attrs, then update_attrs() need to be called
      if (auto ret = driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, *setattrs, y); ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver set_attrs method failed with ret: " << ret << dendl;
        return ret;
      }
    } //if setattrs != nullptr

    if (delattrs != nullptr) {
      Attrs::iterator attr;
      Attrs currentattrs = this->get_attrs();

      /* Ensure all delAttrs exist */
      for (const auto& attr : *delattrs) {
        if (std::find(currentattrs.begin(), currentattrs.end(), attr) == currentattrs.end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }

      if (auto ret = driver->get_cache_driver()->delete_attrs(dpp, head_oid_in_cache, *delattrs, y); ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed with ret: " << ret << dendl;
        return ret;
      }
    } //if delattrs != nullptr
  } else {
    auto ret = next->set_obj_attrs(dpp, setattrs, delattrs, y, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): set_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

bool D4NFilterObject::get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool found_in_cache;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  found_in_cache = check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, y);

  if (found_in_cache) {
    /* Set metadata locally */

    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): obj is: " << this->get_obj().key.name << dendl;
    std::string instance;
    for (auto& attr : attrs) {
      if (attr.second.length() > 0) {
        if (attr.first == "user.rgw.mtime") {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting mtime." << dendl;
          auto mtime = ceph::real_clock::from_double(std::stod(attr.second.c_str()));
          this->set_mtime(mtime);
        } else if (attr.first == "user.rgw.object_size") {
          auto size = std::stoull(attr.second.to_str());
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting object_size to: " << size << dendl;
          this->set_obj_size(size);
        } else if (attr.first == "user.rgw.accounted_size") {
          auto accounted_size = std::stoull(attr.second.to_str());
          this->set_accounted_size(accounted_size);
        } else if (attr.first == "user.rgw.epoch") {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting epoch." << dendl;
          auto epoch = std::stoull(attr.second.c_str());
          this->set_epoch(epoch);
        } else if (attr.first == "user.rgw.version_id") {
          instance = attr.second.to_str();
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting version_id to: " << instance << dendl;
        } else if (attr.first == "user.rgw.source_zone") {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting source zone id." << dendl;
          auto short_zone_id = static_cast<uint32_t>(std::stoul(attr.second.c_str()));
          this->set_short_zone_id(short_zone_id);
        } else {
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Unexpected attribute; not locally set, attr name: " << attr.first << dendl;
        }
      }//end-if
    }//end-for
    this->set_instance(instance); //set this only after setting object state else it won't take effect
    attrs.erase("user.rgw.mtime");
    attrs.erase("user.rgw.object_size");
    attrs.erase("user.rgw.accounted_size");
    attrs.erase("user.rgw.epoch");
    /* Set attributes locally */
    auto ret = this->set_attrs(attrs);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): D4NFilterObject set_attrs method failed." << dendl;
    }
  } // if found_in_cache = true

  return found_in_cache;
}

void D4NFilterObject::set_obj_state_attrs(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs)
{
  bufferlist bl_val;
  bl_val.append(std::to_string(this->get_size()));
  attrs["user.rgw.object_size"] = std::move(bl_val);

  bl_val.append(std::to_string(this->get_epoch()));
  attrs["user.rgw.epoch"] = std::move(bl_val);

  bl_val.append(std::to_string(ceph::real_clock::to_double(this->get_mtime())));
  attrs["user.rgw.mtime"] = std::move(bl_val);

  if(this->have_instance()) {
    bl_val.append(this->get_instance());
    attrs["user.rgw.version_id"] = std::move(bl_val);
  }

  bl_val.append(std::to_string(this->get_short_zone_id()));
  attrs["user.rgw.source_zone"] = std::move(bl_val);

  bl_val.append(std::to_string(this->get_accounted_size()));
  attrs["user.rgw.accounted_size"] = std::move(bl_val); // will this get updated?

  return;
}

int D4NFilterObject::calculate_version(const DoutPrefixProvider* dpp, optional_yield y, std::string& version)
{
  //versioned objects have instance set to versionId, and get_oid() returns oid containing instance, hence using id tag as version for non versioned objects only
  if (! this->have_instance() && version.empty()) {
    bufferlist bl;
     if (this->get_attr(RGW_ATTR_ID_TAG, bl)) {
      version = bl.c_str();
      ldpp_dout(dpp, 20) << __func__ << " id tag version is: " << version << dendl;
    } else {
      ldpp_dout(dpp, 20) << __func__ << " Failed to find id tag" << dendl;
      return -ENOENT;
    }
  }
  bufferlist bl;
  if (this->have_instance()) {
    version = this->get_instance();
  }

  this->set_object_version(version);

  return 0;
}

int D4NFilterObject::set_head_obj_dir_entry(const DoutPrefixProvider* dpp, optional_yield y, bool is_latest_version, bool dirty)
{
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object name: " << this->get_name() << " bucket name: " << this->get_bucket()->get_name() << dendl;
  // entry that contains latest version for versioned and non-versioned objects
  int ret = 0;
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  if (is_latest_version) {
    rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
      .objName = this->get_name(),
      .bucketName = this->get_bucket()->get_name(),
      .dirty = dirty,
      .hostsList = { driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address },
      };

    rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
      .cacheObj = object,
      .blockID = 0,
      .version = this->get_object_version(),
      .dirty = dirty,
      .size = 0,
      .hostsList = { driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address },
      };
    ret = blockDir->set(dpp, &block, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
    }
  }

  // In case of a distributed cache - an entry corresponding to each instance will be needed to locate the head block
  // this will also be needed for deleting an object from a version enabled bucket.
  // and in that case instead of having a separate entry for an object, this entry could be used during object listing.
  if (this->have_instance()) {
    rgw::d4n::CacheObj version_object = rgw::d4n::CacheObj{
    .objName = this->get_oid(),
    .bucketName = this->get_bucket()->get_name(),
    .dirty = dirty,
    .hostsList = { driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address },
    };

    rgw::d4n::CacheBlock version_block = rgw::d4n::CacheBlock{
      .cacheObj = version_object,
      .blockID = 0,
      .version = this->get_object_version(),
      .dirty = dirty,
      .size = 0,
      .hostsList = { driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address },
      };
    auto ret = blockDir->set(dpp, &version_block, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
    }
  }

  return ret;
}

bool D4NFilterObject::check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::sal::Attrs& attrs, optional_yield y)
{
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
        .objName = this->get_oid(), //version-enabled buckets will not have version for latest version, so this will work even when versio is not provided in input
        .bucketName = this->get_bucket()->get_name(),
        };

  rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
          .cacheObj = object,
          .blockID = 0,
          .size = 0
          };

  bool found_in_cache = true;
  //if the block corresponding to head object does not exist in directory, implies it is not cached
  if (blockDir->exist_key(dpp, &block, y) && (blockDir->get(dpp, &block, y) == 0)) {
    std::string version;
    version = block.version;
    this->set_object_version(version);

    //for distributed cache-the blockHostsList can be used to determine if the head block resides on the localhost, then get the block from localhost, whether or not the block is dirty
    //can be determined using the block entry.

    //uniform name for versioned and non-versioned objects, since input for versioned objects might not contain version
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Is block dirty: " << block.dirty << dendl;
    if (block.dirty) {
      head_oid_in_cache = "D_" + get_bucket()->get_name() + "_" + version + "_" + get_name();
    } else {
      head_oid_in_cache = get_bucket()->get_name() + "_" + version + "_" + get_name();
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from cache for head obj id: " << head_oid_in_cache << dendl;
    auto ret = this->driver->get_cache_driver()->get_attrs(dpp, head_oid_in_cache, attrs, y);
    if (ret < 0) {
      found_in_cache = false;
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver get_attrs method failed." << dendl;
    }
  } else { //if blockDir->exist_key
    found_in_cache = false;
  }
  return found_in_cache;
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  bool is_latest_version = true;
  if (this->have_instance()) {
    is_latest_version = false;
  }
  if (!get_obj_attrs_from_cache(dpp, y)) {
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from backend store." << dendl;
    auto ret = next->get_obj_attrs(y, dpp, target_obj);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to fetching attrs from backend store with ret: " << ret << dendl;
      return ret;
    }
  
    this->load_obj_state(dpp, y);
    this->obj = this->get_obj();
    if (!this->obj.key.instance.empty()) {
      this->set_instance(this->obj.key.instance);
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): this->obj oid is: " << this->obj.key.name << "instance is: " << this->obj.key.instance << dendl;
    attrs = this->get_attrs();
    this->set_obj_state_attrs(dpp, y, attrs);

    ret = calculate_version(dpp, y, version);
    if (ret < 0 || version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    head_oid_in_cache = this->get_bucket()->get_name() + "_" + version + "_" + this->get_name();
    if (this->driver->get_policy_driver()->get_cache_policy()->exist_key(head_oid_in_cache) > 0) {
      ret = this->driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, attrs, y);
    } else {
      ret = this->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
      if (ret == 0) {
        bufferlist bl;
        ret = this->driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
      } else {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to evict data with err: " << ret << dendl;
      }
    }
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->get_object_version() << dendl;
      this->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, 0, version, false, y);
      ret = set_head_obj_dir_entry(dpp, y, is_latest_version);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): failed to cache head object in cache backend with error: " << ret << dendl;
    }
  }

  return 0;
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, y)) {
    if (auto ret = driver->get_cache_driver()->update_attrs(dpp, head_oid_in_cache, update, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver update_attrs method failed with ret: " << ret << dendl;
      return ret;
    }
  } else {
    auto ret = next->modify_obj_attrs(attr_name, attr_val, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): modify_obj_attrs of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  buffer::list bl;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  Attrs delattr;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, y)) {
    delattr.insert({attr_name, bl});
    Attrs currentattrs = this->get_attrs();
    rgw::sal::Attrs::iterator attr = delattr.begin();

    /* Ensure delAttr exists */
    if (std::find_if(currentattrs.begin(), currentattrs.end(),
        [&](const auto& pair) { return pair.first == attr->first; }) != currentattrs.end()) {

      if (auto ret = driver->get_cache_driver()->delete_attrs(dpp, head_oid_in_cache, delattr, y); ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed with ret: " << ret << dendl;
        return ret;
      }
    }
  } else {
    if (auto ret = next->delete_obj_attrs(dpp, attr_name, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): delete_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }

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
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<D4NFilterWriter>(std::move(writer), this, obj, dpp, true, y);
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
  //set a flag to show that incoming instance has no version specified
  bool is_latest_version = true;
  if (source->have_instance()) {
    is_latest_version = false;
  }
  if (!source->get_obj_attrs_from_cache(dpp, y)) {
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): fetching head object from backend store" << dendl;
    next->params = params;
    auto ret = next->prepare(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): next->prepare method failed with error: " << ret << dendl;
      return ret;
    }
    if (params.part_num) {
      params.parts_count = next->params.parts_count;
      if (params.parts_count > 1) {
        ldpp_dout(dpp, 20) << __func__ << "params.part_count: " << params.parts_count << dendl;
        return 0; // d4n wont handle multipart read requests with part number for now
      }
    }
    this->source->load_obj_state(dpp, y);
    attrs = source->get_attrs();
    source->set_obj_state_attrs(dpp, y, attrs);

    ret = source->calculate_version(dpp, y, version);
    if (ret < 0 || version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    bufferlist bl;
    head_oid_in_cache = source->get_bucket()->get_name() + "_" + version + "_" + source->get_name();
    ret = source->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
    if (ret == 0) {
      ret = source->driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
      if (ret == 0) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->source->get_object_version() << dendl;
        source->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, bl.length(), version, false, y);
        ret = source->set_head_obj_dir_entry(dpp, y, is_latest_version);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
        }
        //write object to directory.
        rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
          .objName = source->get_oid(),
          .bucketName = source->get_bucket()->get_name(),
          .creationTime = std::to_string(ceph::real_clock::to_double(source->get_mtime())),
          .dirty = false,
          .hostsList = { source->driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address }
        };
        ret = source->driver->get_obj_dir()->set(&object, y);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): ObjectDirectory set method failed with err: " << ret << dendl;
          return ret;
        }
      } else {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): put for head object failed with error: " << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): failed to cache head object, eviction returned error: " << ret << dendl;
    }
  }
  
  return 0;
}

void D4NFilterObject::D4NFilterReadOp::cancel() {
  aio->drain();
}

int D4NFilterObject::D4NFilterReadOp::drain(const DoutPrefixProvider* dpp, optional_yield y) {
  auto c = aio->drain();
  int r = flush(dpp, std::move(c), y);
  if (r < 0) {
    cancel();
    return r;
  }
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y) {
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
    int r = client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      return r;
    }
    auto it = blocks_info.find(offset);
    if (it != blocks_info.end()) {
      std::string version = source->get_object_version();
      std::string prefix = source->get_prefix();
      std::pair<uint64_t, uint64_t> ofs_len_pair = it->second;
      uint64_t ofs = ofs_len_pair.first;
      uint64_t len = ofs_len_pair.second;
      bool dirty = false;

      rgw::d4n::CacheBlock block;
      block.cacheObj.objName = source->get_key().get_oid();
      block.cacheObj.bucketName = source->get_bucket()->get_name();
      block.blockID = ofs;
      block.size = len;

      std::string oid_in_cache = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(len);

      if (source->driver->get_block_dir()->get(dpp, &block, y) == 0){
        if (block.dirty == true){ 
          dirty = true;
        }
      }

      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " calling update for offset: " << offset << " adjusted offset: " << ofs  << " length: " << len << " oid_in_cache: " << oid_in_cache << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << version << " " << source->get_object_version() << dendl;
      source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, dirty, y);
      blocks_info.erase(it);
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " offset not found: " << offset << dendl;
    }
  
    offset += bl.length();
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::returning from flush:: " << dendl;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string version = source->get_object_version();
  std::string prefix;

  prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_name();

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "prefix: " << prefix << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "oid: " << source->get_key().get_oid() << " ofs: " << ofs << " end: " << end << dendl;

  this->client_cb = cb;
  this->cb->set_client_cb(cb, dpp, &y);
  source->set_prefix(prefix);

  /* This algorithm stores chunks for ranged requests also in the cache, which might be smaller than obj_max_req_size
     One simplification could be to overwrite the smaller chunks with a bigger chunk of obj_max_req_size, and to serve requests for smaller
     chunks using the larger chunk, but all corner cases need to be considered like the last chunk which might be smaller than obj_max_req_size
     and also ranged requests where a smaller chunk is overwritten by a larger chunk size != obj_max_req_size */

  uint64_t obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/obj_max_req_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*obj_max_req_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t diff_ofs = ofs - adjusted_start_ofs; //difference between actual offset and adjusted offset
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%obj_max_req_size) == 0 ? len/obj_max_req_size : (len/obj_max_req_size) + 1; //calculate num parts based on adjusted offset
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  aio = rgw::make_throttle(window_size, y);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "obj_max_req_size " << obj_max_req_size << 
  " num_parts " << num_parts << " adjusted_start_offset: " << adjusted_start_ofs << " len: " << len << dendl;

  this->offset = ofs;

  rgw::d4n::CacheBlock block;
  block.cacheObj.objName = source->get_key().get_oid();
  block.cacheObj.bucketName = source->get_bucket()->get_name();

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

    block.blockID = adjusted_start_ofs;
    block.size = part_len; 

    ceph::bufferlist bl;
    std::string oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);

    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
    " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

    int ret = -1;
    if (source->driver->get_block_dir()->exist_key(dpp, &block, y) > 0 && (ret = source->driver->get_block_dir()->get(dpp, &block, y)) == 0) { 
      auto it = find(block.hostsList.begin(), block.hostsList.end(), source->driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address);
      if (it != block.hostsList.end()) { /* Local copy */
	ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block found in directory. " << oid_in_cache << dendl;
        std::string key = oid_in_cache;
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: block is dirty = " << block.dirty << dendl;
        if (block.dirty == true) { 
          key = "D_" + oid_in_cache; //we keep track of dirty data in the cache for the metadata failure case
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: key=" << key << " data is Dirty." << dendl;
        }
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__  << "(): " << __LINE__ << ": READ FROM CACHE: block dirty =" << block.dirty << dendl;
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): " << __LINE__ << ": READ FROM CACHE: key=" << key << dendl;
	if (block.version == version) {
	  if (source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache) > 0) { 
	    // Read From Cache
	    auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), key, read_ofs, len_to_read, cost, id); 

	    this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, part_len)));

	    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
	    auto r = flush(dpp, std::move(completed), y);

	    if (r < 0) {
	      drain(dpp, y);
	      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
	      return r;
	    }
	  // if (source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache) > 0) 
	  } else {
	    oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(obj_max_req_size);
	    //for ranged requests, for last part, the whole part might exist in the cache
	     ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << 
	     " length to read is: " << len_to_read << " part num: " << start_part_num << " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

	    if ((part_len != obj_max_req_size) && source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache) > 0) {
        if (block.dirty == true){ 
          key = "D_" + oid_in_cache; //we keep track of dirty data in the cache for the metadata failure case
        }
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__  << "(): " << __LINE__ << ": READ FROM CACHE: block dirty =" << block.dirty << dendl;
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): " << __LINE__ << ": READ FROM CACHE: key=" << key << dendl;
	      // Read From Cache
	      auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), key, read_ofs, len_to_read, cost, id);  

	      this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, obj_max_req_size)));

	      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
	      auto r = flush(dpp, std::move(completed), y);

	      if (r < 0) {
          drain(dpp, y);
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
          return r;
	      }
	    }
	  }
	// if (block.version == version)
	} else {
	  // TODO: If data has already been returned for any older versioned block, then return ‘retry’ error, else

	  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;

	  auto r = drain(dpp, y);

	  if (r < 0) {
	    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, r= " << r << dendl;
	    return r;
	  }
	}
      } else if (block.hostsList.size()) { /* Remote copy */
	ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block found in remote cache. " << oid_in_cache << dendl;
	// TODO: Retrieve remotely
      }
    // if (source->driver->get_block_dir()->exist_key(&block, y) > 0 && int ret = source->driver->get_block_dir()->get(&block, y) == 0)  
    } else { /* Fetch from backend */
      if (ret < 0)
	ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << block.cacheObj.objName << " blockID: " << block.blockID << " block size: " << block.size << dendl;

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;

      auto r = drain(dpp, y);

      if (r < 0) {
	ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, r= " << r << dendl;
	return r;
      }

      break;
    }

    if (start_part_num == (num_parts - 1)) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
      return drain(dpp, y);
    } else {
      adjusted_start_ofs += obj_max_req_size;
    }

    start_part_num += 1;
    len -= obj_max_req_size;
  } while (start_part_num < num_parts);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << dendl;

  Attrs obj_attrs;
  if (source->has_attrs()) {
    obj_attrs = source->get_attrs();
  }

  if (source->is_compressed() || obj_attrs.find(RGW_ATTR_CRYPT_MODE) != obj_attrs.end() || !y) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Skipping writing to cache" << dendl;
    this->cb->bypass_cache_write();
  }

  if (start_part_num != 0) {
    ofs = adjusted_start_ofs;
  }

  this->cb->set_ofs(ofs);
  auto r = next->iterate(dpp, ofs, end, this->cb.get(), y);
  
  if (r < 0) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, r= " << r << dendl;
    return r;
  }

  return this->cb->flush_last_part();
}

int D4NFilterObject::D4NFilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  rgw::sal::Attrs& attrs = source->get_attrs();
  if (attrs.empty()) {
    rgw_obj obj = source->get_obj();
    auto ret = source->get_obj_attrs(y, dpp, &obj);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Error: failed to fetch attrs, ret= " << ret << dendl;
      return ret;
    }
    //get_obj_attrs() calls set_attrs() internally, hence get_attrs() can be invoked to get the latest attrs.
    attrs = source->get_attrs();
  }
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Attribute value NOT found for attr name= " << name << dendl;
    return next->get_attr(dpp, name, dest, y);
  }

  dest = it->second;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::flush_last_part()
{
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
    Attrs attrs; // empty attrs for cache sets
    std::string version = source->get_object_version();
    std::string prefix = source->get_prefix();

    rgw::d4n::CacheBlock block, existing_block;
    rgw::d4n::BlockDirectory* blockDir = source->driver->get_block_dir();
    block.hostsList.push_back(blockDir->cct->_conf->rgw_d4n_l1_datacache_address); 
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_name();
    std::stringstream s;
    block.cacheObj.creationTime = std::to_string(ceph::real_clock::to_time_t(source->get_mtime()));
    block.cacheObj.dirty = false;
    bool dirty = block.dirty = false; //Reading from the backend, data is clean
    block.version = version;

    //populating fields needed for building directory index
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;

    ldpp_dout(dpp, 20) << __func__ << ": version stored in update method is: " << version << dendl;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        block.blockID = ofs;
        block.size = bl.length();

        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
  	    std::string objEtag = "";
 	    filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, dirty, *y);

	    /* Store block in directory */
            if (!blockDir->exist_key(dpp, &block, *y)) {
              if (blockDir->set(dpp, &block, *y) < 0) //should we revert previous steps if this step fails?
		ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;
	      existing_block.dirty = block.dirty;
              if (blockDir->get(dpp, &existing_block, *y) < 0) {
                ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
              } else {
                if (existing_block.version != block.version) {
                  if (blockDir->del(dpp, &existing_block, *y) < 0) //delete existing block
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set(dpp, &block, *y) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
                } else {
                if (blockDir->update_field(dpp, &block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, *y) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed for hostsList." << dendl;
                }
              }
            }
            //write object to directory.
            rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
                .objName = source->get_oid(),
                .bucketName = source->get_bucket()->get_name(),
                .dirty = dirty,
                .hostsList = { source->driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address }
            };
            ret = source->driver->get_obj_dir()->set(&object, *y);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::::" << __func__ << "(): ObjectDirectory set method failed with err: " << ret << dendl;
              return ret;
            }
          } else {
	    ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
          }
        }
      }
    } else if (bl.length() == rgw_get_obj_max_req_size && bl_rem.length() == 0) { // if bl is the same size as rgw_get_obj_max_req_size, write it to cache
      std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      block.blockID = ofs;
      block.size = bl.length();
      ofs += bl_len;
      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, dirty, *y);

            /* Store block in directory */
            if (!blockDir->exist_key(dpp, &block, *y)) {
              if (blockDir->set(dpp, &block, *y) < 0)
		ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;
	      existing_block.dirty = block.dirty;
              if (blockDir->get(dpp, &existing_block, *y) < 0) {
                ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
              }
              if (existing_block.version != block.version) {
                if (blockDir->del(dpp, &existing_block, *y) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set(dpp, &block, *y) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
              } else {
                if (blockDir->update_field(dpp, &block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, *y) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed for blockHosts." << dendl;
              }
            }
          } else {
            ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
          }
        }
      }
    } else { //copy data from incoming bl to bl_rem till it is rgw_get_obj_max_req_size, and then write it to cache
      uint64_t rem_space = rgw_get_obj_max_req_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;

      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);

      if (bl_rem.length() == rgw_get_obj_max_req_size) {
        std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_rem.length());
          if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
          block.blockID = ofs;
          block.size = bl_rem.length();
          ofs += bl_rem.length();
          auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
          if (ret == 0) {
            ret = filter->get_cache_driver()->put(dpp, oid, bl_rem, bl_rem.length(), attrs, *y);
            if (ret == 0) {
              filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl_rem.length(), version, dirty, *y);

              /* Store block in directory */
              if (!blockDir->exist_key(dpp, &block, *y)) {
                if (blockDir->set(dpp, &block, *y) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
	      } else {
		existing_block.blockID = block.blockID;
		existing_block.size = block.size;
	  	existing_block.dirty = block.dirty;
		if (blockDir->get(dpp, &existing_block, *y) < 0) {
		  ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
		} else {
		  if (existing_block.version != block.version) {
		    if (blockDir->del(dpp, &existing_block, *y) < 0)
		      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
		    if (blockDir->set(dpp, &block, *y) < 0)
		      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
		  } else {
		    if (blockDir->update_field(dpp, &block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, *y) < 0)
		      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed." << dendl;
		  }
		}
	      }
            } else {
              ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
            }
          } else {
            ldpp_dout(dpp, 20) << "D4N Filter: " << __func__ << " An error occured during eviction: " << " error: " << ret << dendl;
          }
        }

        bl_rem.clear();
        bl_rem = std::move(bl);
      }//bl_rem.length()
    }
  }

  /* Clean-up:
  1. do we need to clean up older versions of the cache backend, when we update version in block directory?
  2. do we need to clean up keys belonging to older versions (the last blocks), in case the size of newer version is different
  3. do we need to revert the cache ops, in case the directory ops fail
  */

  return 0;
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                                   optional_yield y, uint32_t flags)
{
  /*
  1. Check if object exists in Object Directory
  2. get dirty flag and version to construct the oid in cache correctly 
  3. loop and delete all blocks in the cache
  4. delete the head block
  5. Update the in memory data structure for all blocks, and update the block directory also
  6. Update the in memory data structure for head block, and update the block directory also
  7. If the blocks reside in other caches, send remote request for the same
  8. Need to figure out a way to get all versions to be deleted in case of versioned objects when a version is not specified.
  9. If the object is not in object directory call next->delete_obj
  */
  rgw::d4n::CacheObj obj = rgw::d4n::CacheObj{ // TODO: Add logic to ObjectDirectory del method to also delete all blocks belonging to that object
			     .objName = source->get_key().get_oid(),
			     .bucketName = source->get_bucket()->get_name()
			   };

  if (source->driver->get_obj_dir()->del(&obj, y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): ObjectDirectory del method failed." << dendl;

  Attrs::iterator attrs;
  Attrs currentattrs = source->get_attrs();
  std::vector<std::string> currentFields;
  
  /* Extract fields from current attrs */
  for (attrs = currentattrs.begin(); attrs != currentattrs.end(); ++attrs) {
    currentFields.push_back(attrs->first);
  }

  if (source->driver->get_cache_driver()->del(dpp, source->get_key().get_oid(), y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver del method failed." << dendl;

  return next->delete_obj(dpp, y, flags);
}


int D4NFilterWriter::prepare(optional_yield y) 
{
  startTime = time(NULL);
  if (driver->get_cache_driver()->delete_data(dpp, obj->get_key().get_oid(), y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): CacheDriver delete_data method failed." << dendl;
  d4n_writecache = g_conf()->d4n_writecache_enabled;
  if (d4n_writecache == false){
    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): calling next iterate" << dendl;
    return next->prepare(y);
  }
  if (!object->have_instance()) {
    if (object->get_bucket()->versioned() && !object->get_bucket()->versioning_enabled()) { //if versioning is suspended
      this->version = "null";
      object->set_instance(this->version);
    } else {
      enum { OBJ_INSTANCE_LEN = 32 };
      char buf[OBJ_INSTANCE_LEN + 1];
      gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf, OBJ_INSTANCE_LEN);
      this->version = buf; // using gen_rand_alphanumeric_no_underscore for the time being
      ldpp_dout(dpp, 20) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): generating version: " << version << dendl;
    }
  } else {
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): " << "version is: " << object->get_instance() << dendl;
  }
  return 0;
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
    bufferlist bl = data;
    off_t bl_len = bl.length();
    off_t ofs = offset;
    bool dirty = true;
    rgw::d4n::CacheBlock block, existing_block;

    std::string version;
    std::string prefix;
    if (object->have_instance()) {
      version = obj->get_instance();
    } else {
      version = this->version;
    }
    prefix = obj->get_bucket()->get_name() + "_" + version + "_" + obj->get_name();
    rgw::d4n::BlockDirectory* blockDir = driver->get_block_dir();

    block.hostsList.push_back(blockDir->cct->_conf->rgw_d4n_l1_datacache_address); 
    block.cacheObj.bucketName = obj->get_bucket()->get_name();
    block.cacheObj.objName = obj->get_key().get_oid();
    block.cacheObj.dirty = dirty;
    block.cacheObj.hostsList.push_back(blockDir->cct->_conf->rgw_d4n_l1_datacache_address);
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;


    int ret = 0;

    if (d4n_writecache == false) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): calling next process" << dendl;
      ret = next->process(std::move(data), offset);
    } else {
      std::string oid = prefix + "_" + std::to_string(ofs);
      std::string key = "D_" + oid + "_" + std::to_string(bl_len);
      std::string oid_in_cache = oid + "_" + std::to_string(bl_len);
      block.size = bl.length();
      block.blockID = ofs;
      block.dirty = true;
      block.hostsList.push_back(blockDir->cct->_conf->rgw_d4n_l1_datacache_address);
      block.version = version;
      dirty = true;
      ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, y);
      if (ret == 0) {     
	if (bl.length() > 0) {          
          ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): key is: " << key << dendl;
          ret = driver->get_cache_driver()->put(dpp, key, bl, bl.length(), obj->get_attrs(), y);
          if (ret == 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): oid_in_cache is: " << oid_in_cache << dendl;
 	    driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, bl.length(), version, dirty, y);
            if (!blockDir->exist_key(dpp, &block, y)) {
              if (blockDir->set(dpp, &block, y) < 0) //should we revert previous steps if this step fails?
  	        ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;
              if (blockDir->get(dpp, &existing_block, y) < 0) {
                ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
              } else {
                if (existing_block.version != block.version) {
                  if (blockDir->del(dpp, &existing_block, y) < 0) //delete existing block
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set(dpp, &block, y) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory set method failed." << dendl;
                } else {
                  if (blockDir->update_field(dpp, &block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, y) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory update_field method failed for hostsList." << dendl;
                }
              }
	    }
          } else {
            ldpp_dout(dpp, 1) << "D4NFilterObject::D4NFilterWriteOp::process" << __func__ << "(): ERROR: writting data to the cache failed!" << dendl;
	    return ret;
	  }
	}
      }
    } 
    return 0;
}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  bool dirty = false;
  std::vector<std::string> hostsList = {};
  auto creationTime = startTime;
  std::string objEtag = etag;
  bool write_to_backend_store = false;
  int ret;
  
  if (d4n_writecache == true) {
    dirty = true;
    std::string version;
    if (object->have_instance()) {
      version = obj->get_instance();
    } else {
      version = this->version;
    } 
    std::string key = obj->get_bucket()->get_name() + "_" + version + "_" + obj->get_name();

    ceph::real_time m_time;
    if (mtime) {
      m_time = *mtime;
    } else {
      m_time = real_clock::now();
    }
    object->set_mtime(m_time);
    object->set_accounted_size(accounted_size);
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << " size is: " << object->get_size() << dendl;
    object->set_obj_state_attrs(dpp, y, attrs);
    bufferlist bl;
    std::string head_oid_in_cache = "D_" + key; //same as key, as there is no len or offset attached to head oid in cache
    ret = driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
    attrs.erase("user.rgw.mtime");
    attrs.erase("user.rgw.object_size");
    attrs.erase("user.rgw.accounted_size");
    attrs.erase("user.rgw.epoch");
    object->set_object_version(version);
    if (ret == 0) {
      object->set_attrs(attrs);
      ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << " version stored in update method is: " << version << dendl;
      driver->get_policy_driver()->get_cache_policy()->update(dpp, key, 0, bl.length(), version, dirty, y);
      ret = object->set_head_obj_dir_entry(dpp, y, true, true);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
        return ret;
      }
      driver->get_policy_driver()->get_cache_policy()->updateObj(dpp, key, version, dirty, accounted_size, creationTime, std::get<rgw_user>(obj->get_bucket()->get_owner()), objEtag, obj->get_bucket()->get_name(), obj->get_key(), y);

      //write object to directory.
      hostsList = { driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address };
      rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
          .objName = obj->get_oid(),
          .bucketName = obj->get_bucket()->get_name(),
          .creationTime = std::to_string(creationTime), 
          .dirty = dirty,
          .hostsList = hostsList
      };
      ret = driver->get_obj_dir()->set(&object, y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): ObjectDirectory set method failed with err: " << ret << dendl;
        return ret;
      }
    } else { //if get_cache_driver()->put()
      write_to_backend_store = true;
      ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << " put failed for head_oid_in_cache wih error: " << ret << dendl;
      ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << " calling complete of backend store: " << dendl;
    }
  } else { // if d4n_writecache = true
    write_to_backend_store = true;
  }

  if (write_to_backend_store) {
    ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs, cksum,
                            delete_at, if_match, if_nomatch, user_data, zones_trace,
                            canceled, rctx, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): Writing to backend store failed with err: " << ret << dendl;
    }
  }

  return 0;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, void* io_context)
{
  rgw::sal::D4NFilterDriver* driver = new rgw::sal::D4NFilterDriver(next, *static_cast<boost::asio::io_context*>(io_context));

  return driver;
}

}
