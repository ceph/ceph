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
  policyDriver->get_cache_policy()->init(cct, dpp, io_context);

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
  if (setattrs != NULL) {
    /* Ensure setattrs and delattrs do not overlap */
    if (delattrs != NULL) {
      for (const auto& attr : *delattrs) {
        if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }
    }

    if (driver->get_cache_driver()->set_attrs(dpp, this->get_key().get_oid(), *setattrs, y) < 0)
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver set_attrs method failed." << dendl;
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

    if (driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), *delattrs, y) < 0) 
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed." << dendl;
  }

  return next->set_obj_attrs(dpp, setattrs, delattrs, y, flags);
}

bool D4NFilterObject::get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y)
{
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
        .objName = this->get_name(),
        .bucketName = this->get_bucket()->get_name(),
        };

  rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
          .cacheObj = object,
          .blockID = 0,
          .version = version,
          .size = 0
          };

  bool found_in_cache = true;
  //if the block corresponding to head object does not exist in directory, implies it is not cached
  if (blockDir->exist_key(&block, y) && (blockDir->get(&block, y) == 0)) {
    rgw::sal::Attrs attrs;
    std::string version = block.version;
    this->set_object_version(version);
    //uniform name for versioned and non-versioned objects, since input for versioned objects might not contain version
    std::string head_oid_in_cache = get_bucket()->get_name() + "_" + version + "_" + get_name();
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from cache." << dendl;
    auto ret = this->driver->get_cache_driver()->get_attrs(dpp, head_oid_in_cache, attrs, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver get_attrs method failed." << dendl;
      found_in_cache = false;
    } else {
      /* Set metadata locally */
      RGWQuotaInfo quota_info;

      std::string instance;
      for (auto& attr : attrs) {
        if (attr.second.length() > 0) {
          if (attr.first == "user.rgw.mtime") {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting mtime." << dendl;
            auto mtime = ceph::real_clock::from_double(std::stod(attr.second.c_str()));
            this->set_mtime(mtime);
          } else if (attr.first == "user.rgw.object_size") {
            auto size = std::stoull(attr.second.c_str());
            this->set_obj_size(size);
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting object_size to: " << size << dendl;
          } else if (attr.first == "user.rgw.accounted_size") {
            auto accounted_size = std::stoull(attr.second.c_str());
            this->set_accounted_size(accounted_size);
          } else if (attr.first == "user.rgw.epoch") {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting epoch." << dendl;
            auto epoch = std::stoull(attr.second.c_str());
            this->set_epoch(epoch);
          } else if (attr.first == "user.rgw.version_id") {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting version_id." << dendl;
            instance = attr.second.to_str();
          } else if (attr.first == "user.rgw.source_zone") {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting source zone id." << dendl;
            auto zone_short_id = static_cast<uint32_t>(std::stoul(attr.second.c_str()));
            this->set_short_zone_id(zone_short_id);
          } else {
            ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Unexpected attribute; not locally set, attr name: " << attr.first << dendl;
          }
        }//end-if
      }//end-for
      //this->set_obj_state(astate);
      this->set_instance(instance); //set this only after setting object state else it won't take effect
      attrs.erase("user.rgw.mtime");
      attrs.erase("user.rgw.object_size");
      attrs.erase("user.rgw.accounted_size");
      attrs.erase("user.rgw.epoch");
      /* Set attributes locally */
      ret = this->set_attrs(attrs);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): D4NFilterObject set_attrs method failed." << dendl;
      }
    }
  } else {
    found_in_cache = false;
  }

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

int D4NFilterObject::set_head_obj_dir_entry(const DoutPrefixProvider* dpp, optional_yield y)
{
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
        .objName = this->get_name(),
        .bucketName = this->get_bucket()->get_name(),
        };

  rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
          .cacheObj = object,
          .blockID = 0,
          .version = this->get_object_version(),
          .size = 0
          };

  auto ret = blockDir->set(&block, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
  }

  return ret;
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
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
    attrs = this->get_attrs();
    this->set_obj_state_attrs(dpp, y, attrs);

    ret = calculate_version(dpp, y, version);
    if (ret < 0 || version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    head_oid_in_cache = this->get_bucket()->get_name() + "_" + version + "_" + this->get_name();
    ret = this->driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, attrs, y);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->get_object_version() << dendl;
      this->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, 0, version, y);
      ret = set_head_obj_dir_entry(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): failed to cache head object in block dir with error: " << ret << dendl;
    }
  }

  return 0;
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp,  uint32_t flags)
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;

  if (driver->get_cache_driver()->update_attrs(dpp, this->get_key().get_oid(), update, y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver update_attrs method failed." << dendl;

  return next->modify_obj_attrs(attr_name, attr_val, y, dpp, flags);
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

    if (driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), delattr, y) < 0) 
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed." << dendl;
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
        source->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, bl.length(), version, y);
        ret = source->set_head_obj_dir_entry(dpp, y);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
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
  auto c = aio->wait();
  while (!c.empty()) {
    int r = flush(dpp, std::move(c), y);
    if (r < 0) {
      cancel();
      return r;
    }
    c = aio->wait();
  }
  return flush(dpp, std::move(c), y);
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
      std::string oid_in_cache = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(len);
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " calling update for offset: " << offset << " adjusted offset: " << ofs  << " length: " << len << " oid_in_cache: " << oid_in_cache << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << version << " " << source->get_object_version() << dendl;
      source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, y);
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
  /* After prepare() method, for versioned objects, get_oid() returns an oid with versionId added,
   * even for versioned objects, where version id is not provided as input
   */
  if (source->have_instance()) {
    prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();
  } else {
    prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_key().get_oid();
  }

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

    ceph::bufferlist bl;
    std::string oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);

    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
    " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

    if (source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache) > 0) { 
      // Read From Cache
      auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), oid_in_cache, read_ofs, len_to_read, cost, id); 

      this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, part_len)));

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
      auto r = flush(dpp, std::move(completed), y);

      if (r < 0) {
        drain(dpp, y);
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
        return r;
      }
    } else {
      oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(obj_max_req_size);
      //for ranged requests, for last part, the whole part might exist in the cache
       ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
      " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

      if ((part_len != obj_max_req_size) && source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache) > 0) {
        // Read From Cache
        auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), oid_in_cache, read_ofs, len_to_read, cost, id);  

        this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, obj_max_req_size)));

        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
        auto r = flush(dpp, std::move(completed), y);

        if (r < 0) {
          drain(dpp, y);
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
          return r;
        }

      } else {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;

        auto r = drain(dpp, y);

        if (r < 0) {
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, r= " << r << dendl;
          return r;
        }

        break;
      }
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
    rgw::d4n::CacheBlock block, existing_block;
    rgw::d4n::BlockDirectory* blockDir = source->driver->get_block_dir();
    block.hostsList.push_back(blockDir->cct->_conf->rgw_d4n_l1_datacache_address); 
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_name();
    std::stringstream s;
    utime_t ut(source->get_mtime());
    ut.gmtime(s);
    block.cacheObj.creationTime = s.str(); 
    block.cacheObj.dirty = false;

    //populating fields needed for building directory index
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;
    Attrs attrs; // empty attrs for cache sets
    std::string version = source->get_object_version();
    std::string prefix = source->get_prefix();

    ldpp_dout(dpp, 20) << __func__ << ": version stored in update method is: " << version << dendl;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len);
      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        block.blockID = ofs;
        block.size = bl.length();
        block.version = version;
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, *y);

	    /* Store block in directory */
            if (!blockDir->exist_key(&block, *y)) {
              if (blockDir->set(&block, *y) < 0) //should we revert previous steps if this step fails?
		ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;
              if (blockDir->get(&existing_block, *y) < 0) {
                ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
              } else {
                if (existing_block.version != block.version) {
                  if (blockDir->del(&existing_block, *y) < 0) //delete existing block
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set(&block, *y) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
                } else {
                if (blockDir->update_field(&block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, *y) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed for hostsList." << dendl;
                }
              }
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
      block.version = version;
      ofs += bl_len;

      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, *y);

            /* Store block in directory */
            if (!blockDir->exist_key(&block, *y)) {
              if (blockDir->set(&block, *y) < 0)
		ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;
              if (blockDir->get(&existing_block, *y) < 0) {
                ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
              }
              if (existing_block.version != block.version) {
                if (blockDir->del(&existing_block, *y) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set(&block, *y) < 0)
                    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
              } else {
                if (blockDir->update_field(&block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, *y) < 0)
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
          block.version = version;
          ofs += bl_rem.length();

          auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
          if (ret == 0) {
            ret = filter->get_cache_driver()->put(dpp, oid, bl_rem, bl_rem.length(), attrs, *y);
            if (ret == 0) {
              filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl_rem.length(), version, *y);

              /* Store block in directory */
              if (!blockDir->exist_key(&block, *y)) {
                if (blockDir->set(&block, *y) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
	      } else {
		existing_block.blockID = block.blockID;
		existing_block.size = block.size;
		if (blockDir->get(&existing_block, *y) < 0) {
		  ldpp_dout(dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
		} else {
		  if (existing_block.version != block.version) {
		    if (blockDir->del(&existing_block, *y) < 0)
		      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory del method failed." << dendl;
		    if (blockDir->set(&block, *y) < 0)
		      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
		  } else {
		  if (blockDir->update_field(&block, "blockHosts", blockDir->cct->_conf->rgw_d4n_l1_datacache_address, *y) < 0)
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
  if (driver->get_cache_driver()->delete_data(save_dpp, obj->get_key().get_oid(), y) < 0) 
    ldpp_dout(save_dpp, 10) << "D4NFilterWriter::" << __func__ << "(): CacheDriver delete_data method failed." << dendl;

  return next->prepare(y);
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
  /*
  int append_dataReturn = driver->get_cache_driver()->append_data(save_dpp, obj->get_key().get_oid(), 
  								    data, y);

  if (append_dataReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache append data operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache append data operation succeeded." << dendl;
  }*/

  return next->process(std::move(data), offset);
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
  rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
				 .objName = obj->get_key().get_oid(), 
				 .bucketName = obj->get_bucket()->get_name(),
				 .creationTime = to_iso_8601(*mtime), 
				 .dirty = false,
				 .hostsList = { /*driver->get_block_dir()->cct->_conf->rgw_d4n_l1_datacache_address*/ } //TODO: Object is not currently being cached 
                               };

  if (driver->get_obj_dir()->set(&object, y) < 0) 
    ldpp_dout(save_dpp, 10) << "D4NFilterWriter::" << __func__ << "(): ObjectDirectory set method failed." << dendl;
   
  /* Retrieve complete set of attrs */
  int ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs, cksum,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, rctx, flags);
  obj->get_obj_attrs(rctx.y, save_dpp, NULL);

  /* Append additional metadata to attributes */ 
  rgw::sal::Attrs baseAttrs = obj->get_attrs();
  rgw::sal::Attrs attrs_temp = baseAttrs;
  buffer::list bl;
  obj->load_obj_state(save_dpp, rctx.y);

  bl.append(to_iso_8601(obj->get_mtime()));
  baseAttrs.insert({"mtime", bl});
  bl.clear();

  bl.append(std::to_string(obj->get_size()));
  baseAttrs.insert({"object_size", bl});
  bl.clear();

  bl.append(std::to_string(accounted_size));
  baseAttrs.insert({"accounted_size", bl});
  bl.clear();
 
  bl.append(std::to_string(obj->get_epoch()));
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
    bl.append(std::to_string(obj->get_short_zone_id()));
    baseAttrs.insert({"source_zone_short_id", bl});
    bl.clear();
  } else {
    bl.append("0"); /* Initialized to zero */
    baseAttrs.insert({"source_zone_short_id", bl});
    bl.clear();
  }

  baseAttrs.insert(attrs.begin(), attrs.end());
  
  //bufferlist bl_empty;
  //int putReturn = driver->get_cache_driver()->
  //	  put(save_dpp, obj->get_key().get_oid(), bl_empty, accounted_size, baseAttrs, y); /* Data already written during process call */
  /*
  if (putReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache put operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache put operation succeeded." << dendl;
  }
  */
  return ret;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, void* io_context)
{
  rgw::sal::D4NFilterDriver* driver = new rgw::sal::D4NFilterDriver(next, *static_cast<boost::asio::io_context*>(io_context));

  return driver;
}

}
