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

#include "rgw_perf_counters.h"
#include "rgw_sal_d4n.h"

namespace rgw { namespace sal {

static constexpr uint8_t OBJ_INSTANCE_LEN = 32;

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
  rgw::cache::Partition partition_info;
  partition_info.location = g_conf()->rgw_d4n_l1_datacache_persistent_path;
  partition_info.name = "d4n";
  partition_info.type = "read-cache";
  partition_info.size = g_conf()->rgw_d4n_l1_datacache_size;
  cacheDriver = std::make_unique<rgw::cache::SSDDriver>(partition_info);
}

D4NFilterDriver::~D4NFilterDriver() = default;

int D4NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  namespace net = boost::asio;
  using boost::redis::config;

  conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
  objDir = std::make_unique<rgw::d4n::ObjectDirectory>(conn);
  blockDir = std::make_unique<rgw::d4n::BlockDirectory>(conn);
  bucketDir = std::make_unique<rgw::d4n::BucketDirectory>(conn);
  policyDriver = std::make_unique<rgw::d4n::PolicyDriver>(conn,
							  cacheDriver.get(),
							  "lfuda");

  std::string address = cct->_conf->rgw_d4n_address;
  config cfg;
  cfg.addr.host = address.substr(0, address.find(":"));
  cfg.addr.port = address.substr(address.find(":") + 1, address.length());
  cfg.clientname = "D4N.Filter";

  if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
    ldpp_dout(dpp, 0) << "D4NFilterDriver::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
    return -EDESTADDRREQ;
  }

  conn->async_run(cfg, {}, net::consign(net::detached, conn));

  FilterDriver::initialize(cct, dpp);

  cacheDriver->initialize(dpp);
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

std::unique_ptr<Bucket> D4NFilterDriver::get_bucket(const RGWBucketInfo& i)
{
  return std::make_unique<D4NFilterBucket>(next->get_bucket(i), this);
}

int D4NFilterDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
				 std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  const int ret = next->load_bucket(dpp, b, &nb, y);
  *bucket = std::make_unique<D4NFilterBucket>(std::move(nb), this);
  return ret;
}

int D4NFilterBucket::create(const DoutPrefixProvider* dpp,
                            const CreateParams& params,
                            optional_yield y)
{
  return next->create(dpp, params, y);
}

int D4NFilterBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
                          ListResults& results, optional_yield y)
{
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.marker.name: " << params.marker.name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.marker.instance: " << params.marker.instance << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.end_marker.key: " << params.end_marker.name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " max: " << max << dendl;

  if (max == 0) {
    return 0;
  }

  //Get objects from cache
  auto bucketDir = this->filter->get_bucket_dir();
  auto objDir = this->filter->get_obj_dir();
  std::vector<rgw_obj_key> objects;
  std::vector<rgw_bucket_list_entries> entries;

  ListResults cache_results;
  ListResults store_results;
  if (g_conf()->d4n_writecache_enabled) {
    cache_results.is_truncated = false;
    if (!params.prefix.empty()) {
      std::string pattern = params.prefix + "*";
      /* zscan does not always take into account COUNT as smaller sizes of ordered set are stored as one sequential blob of memory
        so the entire set might be returned regarless of value of COUNT.
        also valid values of cursor are only zero during start of iteration and cursor returned by previous zscan call
        Refer: https://valkey.io/commands/scan/ */
      uint64_t cursor = 0, next_cursor = 0;
      int num_objs = 0;
      bool instance_marker_processed = false;
      std::string last_version;
      do {
        std::vector<std::string> temp_objects;
        auto ret = bucketDir->zscan(dpp, this->get_bucket_id(), cursor, pattern, (max + 1), temp_objects, next_cursor, y);
        if (ret < 0 && ret != -ENOENT) {
          ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zscan failed with ret: " << ret << dendl;
          return ret;
        }
        //filter elements before marker (exclude marker from output)
        std::string last_element_processed;
        for (auto it = temp_objects.begin(); it != temp_objects.end(); it++) {
          last_element_processed = *it;
          if (!params.marker.name.empty() && *it <= params.marker.name) {
            if (*it != params.marker.name || !params.list_versions || params.marker.instance.empty()) {
              continue;
            }
          } else {
            auto pos = it->find(params.delim, params.prefix.length());
            if (!params.delim.empty() && pos != std::string::npos) {
              std::string delim_str = it->erase((pos + 1), (it->length() - 1));
              ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " delim_str: " << delim_str << dendl;
              if (cache_results.common_prefixes.find(delim_str) == cache_results.common_prefixes.end()) {
                cache_results.common_prefixes.emplace(std::make_pair(delim_str, true));
                store_results.common_prefixes.emplace(std::make_pair(delim_str, true));
                num_objs++; //all objects under a common prefix are counted as one
              }
            } else {
              std::vector<std::string> temp_versions;
              //if params.list_versions is given, get the versions of the object
              if (params.list_versions) {
                std::string objName = *it;
                // special handling for name starting with '_'
                if (objName[0] == '_') {
                  objName = "_" + *it;
                }
                rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
                  .objName = objName,
                  .bucketName = this->get_bucket_id(),
                };
                std::string start;
                if (params.marker.instance.empty() || instance_marker_processed || params.marker.name.empty()) {
                  start = "0";
                } else {
                  if (!instance_marker_processed) {
                    std::string member = params.marker.instance;
                    std::string index;
                    auto ret = objDir->zrank(dpp, &dir_obj, member, index, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zrank failed with: " << ret << dendl;
                      return ret;
                    }
                    ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " start: " << index << dendl;
                    start = index;
                    instance_marker_processed = true;
                  }
                }
                auto ret = objDir->zrevrange(dpp, &dir_obj, start, "-1", temp_versions, y);
                if (ret < 0 && ret != -ENOENT) {
                  ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zrevrange failed with ret: " << ret << dendl;
                  return ret;
                }
              } //params.list_version
              if (!temp_versions.empty()) {
              for (auto version_it = temp_versions.begin(); version_it != temp_versions.end(); version_it++) {
                std::string version = *(version_it);
                if (std::next(version_it) == temp_versions.end()) {
                  last_version = *version_it;
                }
                rgw_bucket_list_entries entry;
                entry.flags = rgw_bucket_dir_entry::FLAG_VER;
                if (version_it == temp_versions.begin()) {
                  entry.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
                } else {
                  entry.flags |= rgw_bucket_dir_entry::FLAG_VER_MARKER;
                }
                rgw_obj_key key{std::move(*it), std::move(*version_it)};
                entry.key = std::move(key);
                entries.emplace_back(entry);
                num_objs++;
                if (num_objs == max) {
                  if (std::next(version_it) != temp_versions.end()) {
                    cache_results.is_truncated = true;
                    cache_results.next_marker.instance = version;
                    cache_results.next_marker.name = last_element_processed;
                  }
                  break; //break from the 'for' loop that processes temp_versions
                }
              }
            } else {
                rgw_bucket_list_entries entry;
                entry.key.name = std::move(*it);
                entries.emplace_back(entry);
                num_objs++;
              }
            }
          }
          if (num_objs == max) {
            if (std::next(it) != temp_objects.end()) {
              if (cache_results.next_marker.empty()) {
                cache_results.is_truncated = true;
                cache_results.next_marker.name = last_element_processed;
                if (cache_results.next_marker.instance.empty() && params.list_versions) {
                  cache_results.next_marker.instance = last_version;
                }
              }
              break; //break from the 'for' loop that processes temp_objects
            }
          }
        }
        /* break if next_cursor is 0 which means end of ordered set or if no entries are found in ordered set
           or if num_objs after filtering is equal to max */
        if ((next_cursor == 0) || (ret == -ENOENT) || (num_objs == max)) {
          if ((num_objs == max) && (next_cursor != 0)) {
            if (cache_results.next_marker.empty()) {
              cache_results.is_truncated = true;
              cache_results.next_marker.name = entries[(max - 1)].key.name;
            }
          }
          break;
        }
        cursor = next_cursor;
      } while(next_cursor != 0);
    } else { //no prefix is specified
      std::string start;
      if (params.marker.name.empty()) {
        start = "-";
      } else {
        if (!params.marker.instance.empty()) {
          start = "[" + params.marker.name;
        } else {
          start = "(" + params.marker.name;
        }
      }
      int num_objs = 0;
      bool instance_marker_processed = false;
      std::string last_version;
      do {
        std::vector<std::string> temp_objects;
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " zrange start is: " << start << dendl;
        auto ret = bucketDir->zrange(dpp, this->get_bucket_id(), start, "+", 0, (max + 1), temp_objects, y);
        if (ret < 0 && ret != -ENOENT) {
          ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zrange failed with ret: " << ret << dendl;
          return ret;
        }
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.delim: " << params.delim << dendl;
        std::string last_element_processed;
        for (auto it = temp_objects.begin(); it != temp_objects.end(); it++) {
          last_element_processed = *it;
          auto pos = it->find(params.delim);
          if (!params.delim.empty() && pos != std::string::npos) {
            std::string delim_str = it->erase((pos + 1), (it->length() - 1));
            ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " delim_str: " << delim_str << dendl;
            if (cache_results.common_prefixes.find(delim_str) == cache_results.common_prefixes.end()) {
              cache_results.common_prefixes.emplace(std::make_pair(delim_str, true));
              store_results.common_prefixes.emplace(std::make_pair(delim_str, true));
              num_objs++; //all objects under a common prefix are counted as one
              if (num_objs == max) {
                uint64_t cursor = 0, next_cursor = 0;
                //get all the keys matching with delim_str so as to find the last element which will be the next marker
                do {
                  std::vector<std::string> delim_objects;
                  std::string delim_pattern = delim_str + "*";
                  auto ret = bucketDir->zscan(dpp, this->get_bucket_id(), cursor, delim_pattern, 10, delim_objects, next_cursor, y);
                  if (ret < 0 && ret != -ENOENT) {
                    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " zscan failed with ret: " << ret << dendl;
                    return ret;
                  }
                  if ((next_cursor == 0) || (ret == -ENOENT)) {
                    if (!delim_objects.empty()) {
                      cache_results.next_marker.name = delim_objects.back();
                      std::string start = "(" + cache_results.next_marker.name;
                      std::vector<std::string> one_object;
                      auto ret = bucketDir->zrange(dpp, this->get_bucket_id(), start, "+", 0, 1, one_object, y);
                      if (ret < 0 && ret != -ENOENT) {
                        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " zrange failed with ret: " << ret << dendl;
                        return ret;
                      }
                      if (one_object.size() >= 1) {
                        cache_results.is_truncated = true;
                      } else {
                        cache_results.is_truncated = false;
                      }
                    }
                    break;
                  }
                  cursor = next_cursor;
                  next_cursor = 0;
                } while(next_cursor != 0);
              } //end-if num_objs == max
            } //end-if cache_results.common_prefixes
          } else {
            std::vector<std::string> temp_versions;
            //if params.list_versions is given, get the versions of the object
            if (params.list_versions) {
              std::string objName = *it;
              // special handling for name starting with '_'
              if (objName[0] == '_') {
                objName = "_" + *it;
              }
              rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
                .objName = objName,
                .bucketName = this->get_bucket_id(),
              };
              std::string start;
              if (params.marker.instance.empty() || instance_marker_processed || params.marker.name.empty()) {
                start = "0";
              } else {
                if (!instance_marker_processed) {
                  std::string member = params.marker.instance;
                  std::string index;
                  auto ret = objDir->zrank(dpp, &dir_obj, member, index, y);
                  if (ret < 0) {
                    ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zrank failed with: " << ret << dendl;
                    return ret;
                  }
                  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " start: " << index << dendl;
                  start = index;
                  instance_marker_processed = true;
                }
              }
              auto ret = objDir->zrevrange(dpp, &dir_obj, start, "-1", temp_versions, y);
              if (ret < 0 && ret != -ENOENT) {
                ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zrevrange failed with ret: " << ret << dendl;
                return ret;
              }
            } //params.list_version
            if (!temp_versions.empty()) {
              for (auto version_it = temp_versions.begin(); version_it != temp_versions.end(); version_it++) {
                std::string version = *(version_it);
                if (std::next(version_it) == temp_versions.end()) {
                  last_version = *version_it;
                }
                rgw_bucket_list_entries entry;
                entry.flags = rgw_bucket_dir_entry::FLAG_VER;
                if (version_it == temp_versions.begin()) {
                  entry.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
                } else {
                  entry.flags |= rgw_bucket_dir_entry::FLAG_VER_MARKER;
                }
                rgw_obj_key key{std::move(*it), std::move(*version_it)};
                entry.key = std::move(key);
                entries.emplace_back(entry);
                num_objs++;
                if (num_objs == max) {
                  if (std::next(version_it) != temp_versions.end()) {
                    cache_results.is_truncated = true;
                    cache_results.next_marker.instance = version;
                    cache_results.next_marker.name = *it;
                  }
                  break; //break from the 'for' loop that processes temp_versions
                }
              }
            } else {
              rgw_bucket_list_entries entry;
              entry.key.name = std::move(*it);
              entries.emplace_back(entry);
              num_objs++;
            }
          }
          if (num_objs == max) {
            if (std::next(it) != temp_objects.end()) {
              //could have been set due to delimiter processing
              if (cache_results.next_marker.name.empty()) {
                cache_results.is_truncated = true;
                cache_results.next_marker.name = last_element_processed;
                if (cache_results.next_marker.instance.empty() && params.list_versions) {
                  cache_results.next_marker.instance = last_version;
                }
              }
            }
            break; //break from the 'for' loop that processes temp_objects
          }
        } //for - processes temp_objects
        //break from while loop if max+1 elements have been found or there are no more elements in the ordered set
        if ((num_objs == max) || (ret == -ENOENT) || temp_objects.empty()) {
          ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " Breaking out! " << dendl;
          ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " num_objs " << num_objs << dendl;
          ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " ret " << ret << dendl;
          break;
        }
        //adjust start to begin from the last element of objects, and exclude that element
        start = "(" + last_element_processed;
      } while(num_objs <= max);
    } //end - else

    rgw::d4n::BlockDirectory* blockDir = this->filter->get_block_dir();
    auto remainder_size = entries.size();
    size_t j = 0, start_j = 0;
    while (remainder_size > 0) {
      std::vector<rgw::d4n::CacheBlock> blocks(100);
      start_j = j;
      size_t batch_size = std::min(static_cast<size_t>(100), remainder_size);
      for (size_t i = 0; i < batch_size; i++) {
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " objects[j]: " << entries[j].key.name << dendl;
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " remainder_size: " << remainder_size << dendl;
        if (entries[j].key.instance == "null") {
          blocks[i].cacheObj.objName = "_:null_" + entries[j].key.name;
        } else {
          blocks[i].cacheObj.objName = entries[j].key.get_oid();
        }
        blocks[i].cacheObj.bucketName = this->get_bucket_id();
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " blocks[i].cacheObj.objName: " << blocks[i].cacheObj.objName << dendl;
        j++;
      }
      auto ret = blockDir->get(dpp, blocks, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " blockDir->get() returned error: " << ret << dendl;
        return ret;
      }

      for (auto block : blocks) {
        if (block.cacheObj.objName.empty()) {
          start_j++;
          continue;
        }
        rgw_bucket_dir_entry entry;
        entry.key.name = entries[start_j].key.name;
        // special handling for name starting with '_'
        if (entry.key.name[0] == '_') {
          entry.key.name = "_" + entries[start_j].key.name;
        }
        entry.key.instance = entries[start_j].key.instance;
        entry.flags = entries[start_j].flags;
        if (block.deleteMarker) {
          entry.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
        }
        entry.meta.storage_class = "CACHE";
        entry.meta.size = block.cacheObj.size;
        entry.meta.accounted_size = block.cacheObj.size;
        try {
          double time = std::stod(block.cacheObj.creationTime);
          entry.meta.mtime = ceph::real_clock::from_double(time);
        } catch (const std::exception& e) {
          ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " Invalid value of time: " << block.cacheObj.creationTime << dendl;
        }
        entry.meta.etag = block.cacheObj.etag;
        entry.meta.owner = block.cacheObj.user_id;
        entry.meta.owner_display_name = block.cacheObj.display_name;
        cache_results.objs.emplace_back(entry);
        start_j++;
      }

      if (remainder_size <= 100) {
        remainder_size = 0;
      } else {
        remainder_size = remainder_size - 100;
      }
    }
  } //d4n_write_cache_enabled = true

  //Get objects from backend store
  auto ret = next->list(dpp, params, max, store_results, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " next->list returned: " << ret << dendl;
    return ret;
  }

  //return store results if cache results is empty
  if (cache_results.objs.empty() && !store_results.objs.empty()) {
    results = std::move(store_results);
    return 0;
  }

  //return cache results if store results is empty
  if (store_results.objs.empty() && !cache_results.objs.empty()) {
    results = std::move(cache_results);
    return 0;
  }

  //if both lists are non-empty then merge them, as they are already sorted
  if (!cache_results.objs.empty() && !store_results.objs.empty()) {
    std::vector<rgw_bucket_dir_entry> objs;
    objs.reserve(max);

    size_t i = 0, j = 0;
    int elementsAdded = 0;

    // Compare elements from both vectors and merge in sorted order
    while (elementsAdded < max && i < cache_results.objs.size() && j < store_results.objs.size()) {
      std::string key_name_in_cache = cache_results.objs[i].key.name;
      std::string key_name_in_store = store_results.objs[j].key.name;
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " key_name_in_cache: " << key_name_in_cache << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " key_name_in_store: " << key_name_in_store << dendl;
      std::string key_instance_in_cache = cache_results.objs[i].key.instance;
      std::string key_instance_in_store = store_results.objs[j].key.instance;
      if (key_name_in_cache == key_name_in_store) {
        objs.push_back(cache_results.objs[i]);
        i++;
        //if list versions is not given or if instance values are different then skip the store result
        if (!params.list_versions ||  key_instance_in_cache == key_instance_in_store) {
          j++;
        }
      }
      else if (key_name_in_cache < key_name_in_store) {
        objs.push_back(cache_results.objs[i]);
        i++;
      } else {
        objs.push_back(store_results.objs[j]);
        j++;
      }
      elementsAdded++;
    }

    while (elementsAdded < max && i < cache_results.objs.size()) {
      objs.push_back(cache_results.objs[i]);
      i++;
      elementsAdded++;
    }

    while (elementsAdded < max && j < store_results.objs.size()) {
      objs.push_back(store_results.objs[j]);
      j++;
      elementsAdded++;
    }

    //there are elements in cache_results
    if (i < (cache_results.objs.size() - 1)) {
      results.is_truncated = true;
      results.next_marker.name = cache_results.objs[(i - 1)].key.to_string();
    }

    //there are elements in store_results
    if (j < (store_results.objs.size() - 1)) {
      results.is_truncated = true;
      results.next_marker.name = store_results.objs[(j - 1)].key.to_string();
    }
    results.objs = std::move(objs);
  }

  if (!store_results.common_prefixes.empty()) {
    results.common_prefixes = std::move(store_results.common_prefixes);
    //results next_marker is not set at this point which means result.objs is empty
    if (results.next_marker.empty()) {
      results.is_truncated = store_results.is_truncated | cache_results.is_truncated;
      if (store_results.is_truncated && cache_results.is_truncated) {
        if (cache_results.next_marker <= store_results.next_marker) {
          results.next_marker = std::move(cache_results.next_marker);
        } else {
          results.next_marker = std::move(store_results.next_marker);
        }
      } else if (store_results.is_truncated && !cache_results.is_truncated) {
        results.next_marker = std::move(store_results.next_marker);
      } else if (!store_results.is_truncated && cache_results.is_truncated) {
        results.next_marker = std::move(cache_results.next_marker);
      }
    }
  }

  return 0;
}

std::unique_ptr<MultipartUpload> D4NFilterBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  std::unique_ptr<MultipartUpload> nmu =
    next->get_multipart_upload(oid, upload_id, owner, mtime);

  return std::make_unique<D4NFilterMultipartUpload>(std::move(nmu), this, this->filter);
}

int D4NFilterObject::copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
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
  bool write_to_cache = g_conf()->d4n_writecache_enabled;
  bool dirty{false};
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(this->get_read_op());
  read_op->params.mod_ptr = mod_ptr;
  read_op->params.unmod_ptr = unmod_ptr;
  read_op->params.high_precision_time = high_precision_time;
  read_op->params.if_match = if_match;
  read_op->params.if_nomatch = if_nomatch;
  if (auto ret = read_op->prepare(y, dpp); ret < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): prepare method failed with ret: " << ret << dendl;
    if (ret == -ERR_NOT_MODIFIED) {
      ret = ERR_PRECONDITION_FAILED;
    }
    return ret;
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): is_multipart: " << is_multipart() << dendl;
  //for multipart objects or for read only cache, write to backend store
  if (is_multipart() || !write_to_cache) {
    write_to_cache = false;
    auto ret = next->copy_object(owner, remote_user, info, source_zone,
                           nextObject(dest_object),
                           nextBucket(dest_bucket),
                           nextBucket(src_bucket),
                           dest_placement, src_mtime, mtime,
                           mod_ptr, unmod_ptr, high_precision_time, if_match,
                           if_nomatch, attrs_mod, copy_if_newer, attrs,
                           category, olh_epoch, delete_at, version_id, tag,
                           etag, progress_cb, progress_data, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): next->copy_object failed with ret: " << ret << dendl;
      return ret;
    }
  }

  this->dest_object = dest_object;
  this->dest_bucket = dest_bucket;
  D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(dest_object);

  rgw::sal::Attrs baseAttrs;
  //ATTRSMOD_NONE - the attributes of the source object will be copied without modifications, attrs parameter is ignored
  if (attrs_mod == rgw::sal::ATTRSMOD_NONE) {
    baseAttrs = this->get_attrs();
    baseAttrs.erase(RGW_CACHE_ATTR_VERSION_ID); //delete source version id
    if (version_id) {
      bufferlist bl_val;
      bl_val.append(*version_id);
      baseAttrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val); //populate destination version id
    }
  }

  //ATTRSMOD_MERGE - any conflicting meta keys on the source object's attributes are overwritten by values contained in attrs parameter.
  if (attrs_mod == rgw::sal::ATTRSMOD_MERGE) { /* Merge */
    rgw::sal::Attrs::iterator iter;

    for (const auto& pair : attrs) {
      iter = baseAttrs.find(pair.first);

      if (iter != baseAttrs.end()) {
        iter->second = pair.second;
      } else {
        baseAttrs.insert({pair.first, pair.second});
      }
    }
  } else if (attrs_mod == rgw::sal::ATTRSMOD_REPLACE) { /* Replace */
    //ATTRSMOD_REPLACE - new object will have the attributes provided by attrs parameter, source object attributes are not copied;
    baseAttrs.insert(attrs.begin(), attrs.end());
  }


  time_t creationTime = -1;
  std::string dest_version;
  if (write_to_cache) {
    dirty = true;
    if (!dest_object->have_instance()) {
      if (dest_object->get_bucket()->versioned() && !dest_object->get_bucket()->versioning_enabled()) { //if versioning is suspended
        dest_version = "null";
      } else {
        char buf[OBJ_INSTANCE_LEN + 1];
        gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf, OBJ_INSTANCE_LEN);
        dest_version = buf; //version for non-versioned objects, using gen_rand_alphanumeric_no_underscore for the time being
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): generating version: " << version << dendl;
      }
    } else {
      dest_version = dest_object->get_instance();
    }
    d4n_dest_object->set_object_version(dest_version);
    if (auto ret = read_op->iterate(dpp, 0, (this->get_size() - 1), nullptr, y); ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): iterate method failed with ret: " << ret << dendl;
      return ret;
    }

    ceph::real_time dest_mtime;
    if (mtime) {
      if (real_clock::is_zero(*mtime)) {
        *mtime = real_clock::now();
      }
      dest_mtime = *mtime;
    } else {
      dest_mtime = real_clock::now();
    }
    creationTime = ceph::real_clock::to_double(dest_mtime);
    dest_object->set_mtime(dest_mtime);
    dest_object->set_obj_size(this->get_size());
    dest_object->set_accounted_size(this->get_accounted_size());
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " size is: " << dest_object->get_size() << dendl;
    d4n_dest_object->set_attrs_from_obj_state(dpp, y, baseAttrs, dirty);
  } else {
    auto o_attrs = baseAttrs; 
    dest_object->load_obj_state(dpp, y);
    baseAttrs = dest_object->get_attrs();
    d4n_dest_object->set_attrs_from_obj_state(dpp, y, baseAttrs, dirty);
    d4n_dest_object->calculate_version(dpp, y, dest_version, o_attrs);
    if (dest_version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }
  }
  bufferlist bl_val;
  bl_val.append(std::to_string(this->is_multipart()));
  baseAttrs[RGW_CACHE_ATTR_MULTIPART] = std::move(bl_val);
  bl_val.append(*etag);
  baseAttrs[RGW_ATTR_ETAG] = std::move(bl_val);
  baseAttrs[RGW_ATTR_ACL] = std::move(attrs[RGW_ATTR_ACL]);

  bufferlist bl_data;
  dest_version = d4n_dest_object->get_object_version();

  //same as key, as there is no len or offset attached to head oid in cache
  std::string key = get_cache_block_prefix(dest_object, dest_version);
  auto ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, baseAttrs.size(), y);
  if (ret == 0) {
    ret = driver->get_cache_driver()->put(dpp, key, bl_data, 0, baseAttrs, y);
    baseAttrs.erase(RGW_CACHE_ATTR_MTIME);
    baseAttrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    baseAttrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    baseAttrs.erase(RGW_CACHE_ATTR_EPOCH);
    baseAttrs.erase(RGW_CACHE_ATTR_MULTIPART);
    baseAttrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    baseAttrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    baseAttrs.erase(RGW_CACHE_ATTR_DIRTY);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << dest_version << dendl;
      bufferlist bl;
      driver->get_policy_driver()->get_cache_policy()->update(dpp, key, 0, bl.length(), dest_version, dirty, rgw::d4n::RefCount::NOOP, y);
      d4n_dest_object->set_object_version(dest_version);
      ret = d4n_dest_object->set_head_obj_dir_entry(dpp, nullptr, y, true, dirty);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
        return ret;
      }
      if (dirty) {
        driver->get_policy_driver()->get_cache_policy()->update_dirty_object(dpp, key, dest_version, false, this->get_size(), creationTime, std::get<rgw_user>(dest_object->get_bucket()->get_owner()), *etag, dest_object->get_bucket()->get_name(), dest_object->get_bucket()->get_bucket_id(), dest_object->get_key(), rgw::d4n::RefCount::NOOP, y);
      }
    }
  }

  return 0;
}

int D4NFilterObject::load_obj_state(const DoutPrefixProvider *dpp, optional_yield y,
                             bool follow_olh)
{
  if (load_from_store) {
    return next->load_obj_state(dpp, y, follow_olh);
  }
  bool has_instance = false;
  if (!this->get_instance().empty()) {
    has_instance = true;
  }
  int ret = get_obj_attrs_from_cache(dpp, y);
  if (ret) {
    /* clearing instance if not present in object before
       calling get_obj_attrs_from_cache as it incorrectly
       causes delete obj to be invoked for an instance
       even though a simple delete request has been issued
       (after load_obj_state is invoked) */
    if (!has_instance) {
      this->clear_instance();
    }
    return 0;
  }
  return next->load_obj_state(dpp, y, follow_olh);
}

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags)
{
  rgw::sal::Attrs attrs;
  std::string head_oid_in_cache;
  rgw::d4n::CacheBlock block;
  bool found_in_cache = false;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    found_in_cache = true;
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
      if (this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::INCR, y)) {
        auto ret = driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, *setattrs, y);
        this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::DECR, y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver set_attrs method failed with ret: " << ret << dendl;
          return ret;
        }
      } else {
        found_in_cache = false;
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
      if (this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::INCR, y)) {
        auto ret = driver->get_cache_driver()->delete_attrs(dpp, head_oid_in_cache, *delattrs, y);
        this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::DECR, y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed with ret: " << ret << dendl;
          return ret;
        }
      } else {
        found_in_cache = false;
      }
    } //if delattrs != nullptr
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }
  }

  if (!found_in_cache) {
    auto ret = next->set_obj_attrs(dpp, setattrs, delattrs, y, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): set_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

int D4NFilterObject::get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y)
{
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  rgw::d4n::CacheBlock block;
  bool found_in_cache = check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y);

  if (block.deleteMarker) {
    return -ENOENT;
  } else if (found_in_cache) {
    /* Set metadata locally */

    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): obj is: " << this->get_obj().key.name << dendl;
    std::string instance;
    for (auto& attr : attrs) {
      if (attr.second.length() > 0) {
        if (attr.first == RGW_CACHE_ATTR_MTIME) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting mtime." << dendl;
          auto mtime = ceph::real_clock::from_double(std::stod(attr.second.to_str()));
          this->set_mtime(mtime);
        } else if (attr.first == RGW_CACHE_ATTR_OBJECT_SIZE) {
          auto size = std::stoull(attr.second.to_str());
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting object_size to: " << size << dendl;
          this->set_obj_size(size);
        } else if (attr.first == RGW_CACHE_ATTR_ACCOUNTED_SIZE) {
          auto accounted_size = std::stoull(attr.second.to_str());
          this->set_accounted_size(accounted_size);
        } else if (attr.first == RGW_CACHE_ATTR_EPOCH) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting epoch." << dendl;
          auto epoch = std::stoull(attr.second.to_str());
          this->set_epoch(epoch);
        } else if (attr.first == RGW_CACHE_ATTR_VERSION_ID) {
          instance = attr.second.to_str();
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting version_id to: " << instance << dendl;
        } else if (attr.first == RGW_CACHE_ATTR_SOURC_ZONE) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting source zone id." << dendl;
          auto short_zone_id = static_cast<uint32_t>(std::stoul(attr.second.to_str()));
          this->set_short_zone_id(short_zone_id);
        } else if (attr.first == RGW_CACHE_ATTR_MULTIPART) {
          std::string multipart = attr.second.to_str();
          this->multipart = (multipart == "1") ? true : false;
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): is_multipart: " << this->multipart << " multipart: " << multipart << dendl;
        } else {
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Unexpected attribute; not locally set, attr name: " << attr.first << dendl;
        }
      }//end-if
    }//end-for
    if (!instance.empty()) {
      this->set_instance(instance); //set this only after setting object state else it won't take effect
    }
    attrs.erase(RGW_CACHE_ATTR_MTIME);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    attrs.erase(RGW_CACHE_ATTR_EPOCH);
    attrs.erase(RGW_CACHE_ATTR_MULTIPART);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    /* Set attributes locally */
    auto ret = this->set_attrs(attrs);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): D4NFilterObject set_attrs method failed." << dendl;
    }
  } // if found_in_cache = true

  return found_in_cache;
}

int D4NFilterObject::set_attr_crypt_parts(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs)
{
  if (attrs.count(RGW_ATTR_CRYPT_MODE)) {
    std::vector<size_t> parts_len;
    uint64_t obj_size = this->get_size();
    uint64_t obj_max_chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;
    uint64_t num_parts = (obj_size%obj_max_chunk_size) == 0 ? obj_size/obj_max_chunk_size : (obj_size/obj_max_chunk_size) + 1;
    size_t remainder_size = obj_size;
    for (uint64_t part = 0; part < num_parts; part++) {
      size_t part_len;
      if (part == (num_parts - 1)) { //last part
        part_len = remainder_size;
      } else {
        part_len = obj_max_chunk_size;
      }
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): part_num: " << part << " part_len: " << part_len << dendl;
      parts_len.emplace_back(part_len);
      remainder_size -= part_len;
    }

    bufferlist parts_bl;
    ceph::encode(parts_len, parts_bl);
    attrs[RGW_ATTR_CRYPT_PARTS] = std::move(parts_bl);
  }
  return 0;
}

void D4NFilterObject::set_attrs_from_obj_state(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool dirty)
{
  bufferlist bl_val;
  bl_val.append(std::to_string(this->get_size()));
  attrs[RGW_CACHE_ATTR_OBJECT_SIZE] = std::move(bl_val);

  bl_val.append(std::to_string(this->get_epoch()));
  attrs[RGW_CACHE_ATTR_EPOCH] = std::move(bl_val);

  bl_val.append(std::to_string(ceph::real_clock::to_double(this->get_mtime())));
  attrs[RGW_CACHE_ATTR_MTIME] = std::move(bl_val);

  if(this->have_instance()) {
    bl_val.append(this->get_instance());
    attrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val);
  }

  bl_val.append(std::to_string(this->get_short_zone_id()));
  attrs[RGW_CACHE_ATTR_SOURC_ZONE] = std::move(bl_val);

  bl_val.append(std::to_string(this->get_accounted_size()));
  attrs[RGW_CACHE_ATTR_ACCOUNTED_SIZE] = std::move(bl_val); // will this get updated?

  bl_val.append(this->get_key().ns);
  attrs[RGW_CACHE_ATTR_OBJECT_NS] = std::move(bl_val);

  bl_val.append(this->get_bucket()->get_name());
  attrs[RGW_CACHE_ATTR_BUCKET_NAME] = std::move(bl_val);
  
  if (dirty) {
    bl_val.append("1"); // only set xattr if dirty
    attrs[RGW_CACHE_ATTR_DIRTY] = std::move(bl_val);
  }

  return;
}

int D4NFilterObject::calculate_version(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, rgw::sal::Attrs& attrs)
{
  //versioned objects have instance set to versionId, and get_oid() returns oid containing instance, hence using id tag as version for non versioned objects only
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object name: " << this->get_name() << " instance: " << this->have_instance() << dendl;
  if (! this->have_instance() && version.empty()) {
    bufferlist bl = attrs[RGW_ATTR_ID_TAG];
    if (bl.length()) {
      version = bl.c_str();
      if (!version.empty()) {
	ldpp_dout(dpp, 20) << __func__ << " id tag version is: " << version << dendl;
      }
    }
  }
  if (this->have_instance()) {
    version = this->get_instance();
  }

  this->set_object_version(version);

  return 0;
}

/* This method creates a delete marker for dirty objects:
1. creates a head block entry in cache driver - so that data can be restored from this when rgw goes down
2. calls set_head_obj_dir_entry to set block entries for a delete marker */
int D4NFilterObject::create_delete_marker(const DoutPrefixProvider* dpp, optional_yield y)
{
  this->delete_marker = true;
  if (this->get_bucket()->versioned() && !this->get_bucket()->versioning_enabled()) { //if versioning is suspended
    this->version = "null";
    this->set_instance("null");
  } else {
    char buf[OBJ_INSTANCE_LEN + 1];
    gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf, OBJ_INSTANCE_LEN);
    this->version = buf; // using gen_rand_alphanumeric_no_underscore for the time being
    this->set_instance(version);
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): generating delete marker: " << version << dendl;
  }

  auto m_time = real_clock::now();

  this->set_mtime(m_time);
  this->set_accounted_size(0); //setting 0 as this is a delete marker
  this->set_obj_size(0); // setting 0 as this is a delete marker
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " size is: " << this->get_size() << dendl;
  rgw::sal::Attrs attrs;
  this->set_attrs_from_obj_state(dpp, y, attrs, true);
  bufferlist bl_val;
  bl_val.append(std::to_string(this->delete_marker));
  attrs[RGW_CACHE_ATTR_DELETE_MARKER] = std::move(bl_val);
  std::string key = get_cache_block_prefix(this, this->version);

  bufferlist bl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): key is: " << key << dendl;
  auto ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
  if (ret == 0) {
    ret = driver->get_cache_driver()->put(dpp, key, bl, 0, attrs, y);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): version stored in update method is: " << version << dendl;
      driver->get_policy_driver()->get_cache_policy()->update(dpp, key, 0, bl.length(), version, true, rgw::d4n::RefCount::NOOP, y);
      std::vector<std::string> exec_responses;
      ret = this->set_head_obj_dir_entry(dpp, &exec_responses , y, true, true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
        return ret;
      }
      auto creationTime = ceph::real_clock::to_double(this->get_mtime());
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): key=" << key << dendl;
      std::string objEtag = "";
      driver->get_policy_driver()->get_cache_policy()->update_dirty_object(dpp, key, version, true, this->get_accounted_size(), creationTime, std::get<rgw_user>(this->get_bucket()->get_owner()), objEtag, this->get_bucket()->get_name(), this->get_bucket()->get_bucket_id(), this->get_key(), rgw::d4n::RefCount::NOOP, y);
    } else { //if get_cache_driver()->put()
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): put failed for key, ret=" << ret << " key: " << key << dendl;
      return ret;
    }
  } else {
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): eviction failed for key, ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

/*This method maintains adds the following entries:
1. A hash entry that maintains the latest version for dirty objects (versioned and non-versioned) and non-versioned clean objects.
2. A "null" hash entry that maintains the same version as the latest hash entry - this is used when get/delete requests are received
for "null" versions, when bucket is non-versioned.
3. The "null" hash entry is overwritten when we have a "null" instance when bucket versioning is suspended.
4. A versioned hash entry for every version for a version enabled bucket - this helps in get/delete requests with version-id specified
5. Redis ordered set to maintain the order of dirty objects added for a version enabled bucket. Even when the bucket is non-versioned, this set maintains a "null" entry
6. Another ordered set to maintain a lexicographically sorted order of objects for a bucket - used for bucket listing */
int D4NFilterObject::set_head_obj_dir_entry(const DoutPrefixProvider* dpp, std::vector<std::string>* exec_responses, optional_yield y, bool is_latest_version, bool dirty)
{
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object name: " << this->get_name() << " bucket name: " << this->get_bucket()->get_name() << dendl;
  rgw::d4n::CacheBlock block; 
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  auto attrs = this->get_attrs();
  bufferlist bl_etag, bl_acl;
  auto etag_it = attrs.find(RGW_ATTR_ETAG);
  if (etag_it != attrs.end()) {
    bl_etag = etag_it->second;
  }
  auto etag = bl_etag.to_str();
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): etag: " << etag << dendl;

  auto acl_it = attrs.find(RGW_ATTR_ACL);
  RGWAccessControlPolicy policy;
  std::string user_id, display_name;
  if (acl_it != attrs.end()) {
    auto bliter = acl_it->second.cbegin();
    try {
      policy.decode(bliter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): ERROR: could not decode policy, caught buffer::error" << dendl;
      return -EIO;
    }
    ACLOwner owner = policy.get_owner();
    rgw_user user = std::get<rgw_user>(owner.id);
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): INFO: user_d: " << user.to_str() << dendl;
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): INFO: display_name: " << owner.display_name << dendl;
    user_id = user.to_str();
    display_name = owner.display_name;
  }

  if (is_latest_version) {
    std::string objName = this->get_name();
    // special handling for name starting with '_'
    if (objName[0] == '_') {
      objName = "_" + this->get_name();
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): objName after special Handling: " << objName << dendl;
    rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
      .objName = objName,
      .bucketName = this->get_bucket()->get_bucket_id(),
      .creationTime = std::to_string(ceph::real_clock::to_double(this->get_mtime())),
      .dirty = dirty,
      .hostsList = { dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address },
      .etag = etag,
      .size = this->get_accounted_size(),
      .user_id = user_id,
      .display_name = display_name,
      };

    block.cacheObj = object;
    block.blockID = 0;
    block.version = this->get_object_version();
    block.size = 0;
    block.deleteMarker = this->delete_marker;

    /* adding an entry to maintain latest version, to serve simple get requests (without any version)
       but not for a clean object that belongs to a versioned bucket, as we will get the latest version from backend store
       to simplify delete object (maintaining correct order of versions) */

    //dirty objects
    if (dirty) {
      auto ret = blockDir->set(dpp, &block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
        return ret;
      }
      /* bucket is non versioned, set a null instance
         even when the bucket is non versioned, a get with "null" version-id returns the latest version, similarly
         delete-obj with "null" as version-id deletes the latest version */
      if (!(this->get_bucket()->versioned())) {
        block.cacheObj.objName = "_:null_" + this->get_name();
        ret = blockDir->set(dpp, &block, y);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
          return ret;
        }
      }
      std::string object_version;
      //add an entry to ordered set for both versioned and non versioned bucket
      if (!this->get_bucket()->versioned() || !this->get_bucket()->versioning_enabled()) {
        object_version = "null";
      } else {
        object_version = this->get_object_version();
      }
      auto mtime = this->get_mtime();
      auto score = ceph::real_clock::to_double(mtime);
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Score of object name: "<< this->get_name() << " version: " << object_version << " is: "  << score << ret << dendl;
      rgw::d4n::ObjectDirectory* objDir = this->driver->get_obj_dir();
      ret = objDir->zadd(dpp, &object, score, object_version, y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to add version to ordered set with error: " << ret << dendl;
        blockDir->discard(dpp, y);
        return ret;
      }
      //add an entry to ordered set containing objects for bucket listing, set score to 0 always to lexicographically order the objects
      rgw::d4n::BucketDirectory* bucketDir = this->driver->get_bucket_dir();
      ret = bucketDir->zadd(dpp, this->get_bucket()->get_bucket_id(), 0, this->get_name(), y, true);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to add object to ordered set with error: " << ret << dendl;
        return ret;
      }
    } else { //for clean/non-dirty objects
      rgw::d4n::CacheBlock latest = block;
      auto ret = blockDir->get(dpp, &latest, y);
      if (ret == -ENOENT) {
        if (!(this->get_bucket()->versioned())) {
          //we can explore pipelining to send the two 'HSET' commands together
          ret = blockDir->set(dpp, &block, y);
          if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
            return ret;
          }
          //bucket is non versioned, set a null instance
          block.cacheObj.objName = "_:null_" + this->get_name();
          ret = blockDir->set(dpp, &block, y);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
            return ret;
          }
        }
      } else if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed for head object with ret: " << ret << dendl;
      } else { //head block is found
        /* for clean objects belonging to versioned buckets we will fetch the latest entry from backend store, hence removing latest head entry
           once a bucket transitions to a versioned state */
        if (this->get_bucket()->versioned()) {
          ret = blockDir->del(dpp, &block, y);
          //Ignore a racing delete that could have deleted the latest block
          if (ret < 0 && ret != -ENOENT) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory del method failed for head object with ret: " << ret << dendl;
          }
        }
        /* even if the head block is found, overwrite existing values with new version in case of non-versioned bucket, clean objects
           and versioned and non-versioned buckets dirty objects */
        if (!(this->get_bucket()->versioned())) {
          ret = blockDir->set(dpp, &block, y);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
            return ret;
          }
          //bucket is non versioned, set a null instance
          block.cacheObj.objName = "_:null_" + this->get_name();
          ret = blockDir->set(dpp, &block, y);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
            return ret;
          }
        }//end-if !(this->get_bucket()->versioned())
      } //end-if ret = 0
    } //end-else
  }//end-if latest-version

  /* An entry corresponding to each instance will be needed to locate the head block
     this will also be needed for deleting an object from a version enabled bucket. */
  if (this->get_bucket()->versioned()) {
    std::string objName = this->get_oid();
    /* for null version, creating a "null" block specifically to differentiate between the latest entry and the null entry
       since oid does not take "null" into account */
    if (this->get_instance() == "null" || !this->get_bucket()->versioning_enabled()) {
      objName = "_:null_" + this->get_name();
    }
    rgw::d4n::CacheObj version_object = rgw::d4n::CacheObj{
    .objName = objName,
    .bucketName = this->get_bucket()->get_bucket_id(),
    .creationTime = std::to_string(ceph::real_clock::to_double(this->get_mtime())),
    .dirty = dirty,
    .etag = etag,
    .size = this->get_accounted_size(),
    .user_id = user_id,
    .display_name = display_name,
    };

    version_object.hostsList.insert({ dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address });

    rgw::d4n::CacheBlock version_block = rgw::d4n::CacheBlock{
      .cacheObj = version_object,
      .blockID = 0,
      .version = this->get_object_version(),
      .deleteMarker = this->delete_marker,
      .size = 0,
    };

    auto ret = blockDir->set(dpp, &version_block, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for versioned head object with ret: " << ret << dendl;
      return ret;
    }
  }//end-if get_bucket_versioned()

  return 0;
}

int D4NFilterObject::set_data_block_dir_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty)
{
  rgw::d4n::BlockDirectory* blockDir = driver->get_block_dir();

  //update data block entries in directory
  off_t lst = this->get_size();
  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Object size =" << lst << dendl;
  off_t fst = 0;
  do {
    rgw::d4n::CacheBlock block, existing_block;
    if (fst >= lst){
      break;
    }
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;
    block.cacheObj.bucketName = this->get_bucket()->get_bucket_id();
    block.cacheObj.objName = this->get_key().get_oid();
    block.cacheObj.dirty = dirty;
    block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;

    block.size = cur_len;
    block.blockID = fst;
    block.version = version;

    /* Store block in directory */
    existing_block.blockID = block.blockID;
    existing_block.size = block.size;

    int ret;
    if ((ret = blockDir->get(dpp, &existing_block, y)) == 0 || ret == -ENOENT) {
      if (ret == 0) { //new versioned block will have new version, hostsList etc, how about globalWeight?
        block = existing_block;
        block.version = version;
        block.cacheObj.dirty = dirty;
      }

      block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

      if ((ret = blockDir->set(dpp, &block, y)) < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory set() method failed, ret=" << ret << dendl;
        return ret;
      }
    } else {
      ldpp_dout(dpp, 0) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << ", ret=" << ret << dendl;
      return ret;
    }
    fst += cur_len;
  } while(fst < lst);

  return 0;
}

int D4NFilterObject::delete_data_block_cache_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty)
{
  //delete cache entries
  off_t lst = this->get_size();
  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Object size =" << lst << dendl;
  off_t fst = 0;
  do {
    if (fst >= lst){
      break;
    }
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;

    std::string key =  get_key_in_cache(get_cache_block_prefix(this, version), std::to_string(fst), std::to_string(cur_len));
    int ret;
    if ((ret = driver->get_cache_driver()->delete_data(dpp, key, y)) == 0) {
	    if (!(ret = driver->get_policy_driver()->get_cache_policy()->erase(dpp, key, y))) {
	      ldpp_dout(dpp, 0) << "Failed to delete policy entry for: " << key << ", ret=" << ret << dendl;
	      return ret;
	    }
	  } else {
      ldpp_dout(dpp, 0) << "Failed to delete cache entry for: " << key << ", ret=" << ret << dendl;
	    return ret;
    }
    fst += cur_len;
  } while(fst < lst);

  return 0;
}

bool D4NFilterObject::check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::sal::Attrs& attrs, rgw::d4n::CacheBlock& blk, optional_yield y)
{
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  std::string objName = this->get_oid();
  //object oid does not contain "null" in case the instance is "null", so explicitly populating that
  if (this->have_instance() && this->get_instance() == "null") {
    objName = "_:null_" + this->get_name();
  }
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): objName: " << objName << dendl;
  rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
        .objName = objName, //version-enabled buckets will not have version for latest version, so this will work even when version is not provided in input
        .bucketName = this->get_bucket()->get_bucket_id(),
        };

  rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
          .cacheObj = object,
          .blockID = 0,
          .size = 0
          };

  bool found_in_cache = true;
  int ret;
  //if the block corresponding to head object does not exist in directory, implies it is not cached
  if ((ret = blockDir->get(dpp, &block, y)) == 0) {
    blk = block;

    std::string version;
    version = block.version;
    this->set_object_version(version);

    /* for distributed cache-the blockHostsList can be used to determine if the head block resides on the localhost, then get the block from localhost, whether or not the block is dirty
       can be determined using the block entry. */

    std::string key = get_cache_block_prefix(this, version);
    if (this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, key, rgw::d4n::RefCount::INCR, y)) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Is block dirty: " << block.cacheObj.dirty << dendl;
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version: " << block.version << dendl;
      head_oid_in_cache = get_cache_block_prefix(this, version);
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from cache for head obj id: " << head_oid_in_cache << dendl;
      auto ret = this->driver->get_cache_driver()->get_attrs(dpp, head_oid_in_cache, attrs, y);
      if (ret < 0) {
        found_in_cache = false;
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver get_attrs method failed." << dendl;
      }
      std::string key = head_oid_in_cache;
      this->driver->get_policy_driver()->get_cache_policy()->update(dpp, key, 0, 0, version, block.cacheObj.dirty, rgw::d4n::RefCount::DECR, y);
      this->exists_in_cache = true;
    } else {
      found_in_cache = false;
    }
  } else if (ret == -ENOENT) { //if blockDir->get
    found_in_cache = false;
  } else {
    found_in_cache = false;
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed, ret=" << ret << dendl;
  }

  if (block.deleteMarker) {
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
  
  int ret;
  if ((ret = get_obj_attrs_from_cache(dpp, y)) == -ENOENT) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): " << " object " << this->get_name() << " does not exist." << dendl;
    return -ENOENT;
  } else if (!ret) {
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_misses);
    }
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from backend store." << dendl;
    auto ret = next->get_obj_attrs(y, dpp, target_obj);
    if (ret < 0 || !target_obj) {
      if (!target_obj) {
        ret = -ENOENT;
      }
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to fetching attrs from backend store with ret: " << ret << dendl;
      return ret;
    }
  
    this->load_obj_state(dpp, y);
    this->obj = *target_obj;
    if (!this->obj.key.instance.empty()) {
      this->set_instance(this->obj.key.instance);
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): this->obj oid is: " << this->obj.key.name << "instance is: " << this->obj.key.instance << dendl;
    attrs = this->get_attrs();
    this->set_attrs_from_obj_state(dpp, y, attrs);

    calculate_version(dpp, y, version, attrs);
    if (version.empty()) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }
    std::string objName = this->get_name();
    head_oid_in_cache = get_cache_block_prefix(this, version);
    if (this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::INCR, y)) {
      ret = this->driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, attrs, y);
    } else {
      ret = this->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
      if (ret == 0) {
        bufferlist bl;
        ret = this->driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
      } else {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to evict data, ret=" << ret << dendl;
      }
    }
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->get_object_version() << dendl;
      this->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, 0, version, false, rgw::d4n::RefCount::DECR, y);
      ret = set_head_obj_dir_entry(dpp, nullptr, y, is_latest_version);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
      }
    } else {
      this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::DECR, y);
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): failed to cache head object in cache backend, ret=" << ret << dendl;
    }
  } else {
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
  }

  return 0;
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp,  uint32_t flags)
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  rgw::d4n::CacheBlock block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    if (auto ret = driver->get_cache_driver()->update_attrs(dpp, head_oid_in_cache, update, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver update_attrs method failed with ret: " << ret << dendl;
      return ret;
    }
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }

    auto ret = next->modify_obj_attrs(attr_name, attr_val, y, dpp, flags);
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
  rgw::d4n::CacheBlock block;
  bool found_in_cache = false;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    found_in_cache = true;
    delattr.insert({attr_name, bl});
    Attrs currentattrs = this->get_attrs();
    rgw::sal::Attrs::iterator attr = delattr.begin();

    /* Ensure delAttr exists */
    if (std::find_if(currentattrs.begin(), currentattrs.end(),
        [&](const auto& pair) { return pair.first == attr->first; }) != currentattrs.end()) {
      if (this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::INCR, y)) {
        auto ret = driver->get_cache_driver()->delete_attrs(dpp, head_oid_in_cache, delattr, y);
        this->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, head_oid_in_cache, rgw::d4n::RefCount::DECR, y);
        if ( ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed with ret: " << ret << dendl;
          return ret;
        }
      } else {
        found_in_cache = false;
      }
    }
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }
  }
  if (!found_in_cache) {
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

void D4NFilterDriver::shutdown()
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });

  cacheDriver.reset();
  objDir.reset();
  blockDir.reset();
  bucketDir.reset();
  policyDriver.reset();

  next->shutdown();
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

  int ret;
  if ((ret = source->get_obj_attrs_from_cache(dpp, y)) == -ENOENT) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::" << __func__ << "(): object " << source->get_name() << " does not exist." << dendl;
    return -ENOENT;
  } else if (!ret) {
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_misses);
    }
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): fetching head object from backend store" << dendl;
    next->params = params;
    auto ret = next->prepare(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): next->prepare method failed, ret=" << ret << dendl;
      return ret;
    }

    params.parts_count = next->params.parts_count;
    this->source->load_obj_state(dpp, y);
    attrs = source->get_attrs();
    source->set_attrs_from_obj_state(dpp, y, attrs);
    source->calculate_version(dpp, y, version, attrs);
    if (version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    this->source->set_attr_crypt_parts(dpp, y, attrs);

    bufferlist bl;
    head_oid_in_cache = get_cache_block_prefix(source, version);
    ret = source->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
    if (ret == 0) {
      ret = source->driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
      if (ret == 0) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->source->get_object_version() << dendl;
        source->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, bl.length(), version, false, rgw::d4n::RefCount::NOOP, y);
        ret = source->set_head_obj_dir_entry(dpp, nullptr, y, is_latest_version);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
        }
      } else {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): put for head object failed, ret=" << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): failed to cache head object during eviction, ret=" << ret << dendl;
    }
  } else {
    /* 
      The following if statement handles the following:
      1. When part_num is given: if it is anything other than 1 and if source is not multipart, then return error
      2. When part_num is 0 and source is multipart
      In both the cases the head is fetched from the backend store.
    */
    if (params.part_num || (!params.part_num && source->is_multipart())) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): source->is_multipart()= " << source->is_multipart() << dendl;
      if (params.part_num) { 
	ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): *(params.part_num)= " << *(params.part_num) << dendl;
      }
      if (!source->is_multipart()) {
        if (params.part_num && *(params.part_num) != 1) {
          return -ERR_INVALID_PART;
        }
      } else {
        next->params = params;
        auto ret = next->prepare(y, dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): next->prepare failed, ret=" << ret << dendl;
          return ret;
        }
        params.parts_count = next->params.parts_count;
        return 0;
      }
    }
    bufferlist etag_bl;
    if (get_attr(dpp, RGW_ATTR_ETAG, etag_bl, y) < 0) {
      return -EINVAL;
    }

    if (params.mod_ptr || params.unmod_ptr) {
      if (params.mod_ptr && !params.if_nomatch) {
	ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.mod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
	if (!(*params.mod_ptr < source->get_mtime())) {
	  return -ERR_NOT_MODIFIED;
	}
      }

      if (params.unmod_ptr && !params.if_match) {
	ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.unmod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
	if (*params.unmod_ptr < source->get_mtime()) {
	  return -ERR_PRECONDITION_FAILED;
	}
      }
    }

    if (params.if_match) {
      std::string if_match_str = rgw_string_unquote(params.if_match);
      ldpp_dout(dpp, 10) << "If-Match: " << if_match_str << " ETAG: " << etag_bl.c_str() << dendl;

      if (if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
	return -ERR_PRECONDITION_FAILED;
      }
    }
    if (params.if_nomatch) {
      std::string if_nomatch_str = rgw_string_unquote(params.if_nomatch);
      ldpp_dout(dpp, 10) << "If-No-Match: " << if_nomatch_str << " ETAG: " << etag_bl.c_str() << dendl;
      if (if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
	return -ERR_NOT_MODIFIED;
      }
    }

    if (params.lastmod) {
      *params.lastmod = source->get_mtime();
    }

    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
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
  std::string version = source->get_object_version();
  std::string prefix = source->get_prefix();
  for (auto it : blocks_info) {
    std::pair<uint64_t, uint64_t> ofs_len_pair = it.second;
    uint64_t ofs = ofs_len_pair.first;
    uint64_t len = ofs_len_pair.second;
    std::string oid_in_cache = prefix + CACHE_DELIM + std::to_string(ofs) + CACHE_DELIM + std::to_string(len);
    source->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, oid_in_cache, rgw::d4n::RefCount::DECR, y);
  }
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
    if (client_cb) {
      int r = client_cb->handle_data(bl, 0, bl.length());
      if (r < 0) {
        return r;
      }
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
      block.cacheObj.bucketName = source->get_bucket()->get_bucket_id();
      block.blockID = ofs;
      block.size = len;

      std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(ofs), std::to_string(len));

      if (source->driver->get_block_dir()->get(dpp, &block, y) == 0){
        if (block.cacheObj.dirty){ 
          dirty = true;
        }
      }

      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " calling update for offset: " << offset << " adjusted offset: " << ofs  << " length: " << len << " oid_in_cache: " << oid_in_cache << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << version << " " << source->get_object_version() << dendl;
      source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, dirty, rgw::d4n::RefCount::DECR, y);
      blocks_info.erase(it);
      if (source->dest_object && source->dest_bucket) {
        D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
        std::string dest_version = d4n_dest_object->get_object_version();
        rgw::d4n::CacheBlock dest_block;
        dest_block.cacheObj.objName = source->dest_object->get_oid();
        dest_block.cacheObj.bucketName = source->dest_bucket->get_bucket_id();
        dest_block.cacheObj.dirty = true; //writing to cache
        dest_block.blockID = ofs;
        dest_block.size = len;
        dest_block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);
        dest_block.version = dest_version;
        dest_block.cacheObj.dirty = true;
        std::string key =  get_key_in_cache(get_cache_block_prefix(source->dest_object, dest_version), std::to_string(ofs), std::to_string(len));
        auto ret = source->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, dest_block.size, y);
        if (ret == 0) {
          rgw::sal::Attrs attrs;
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " destination object version in update method is: " << dest_version << dendl;
          // destination key is the same as key
          ret = source->driver->get_cache_driver()->put(dpp, key, bl, bl.length(), attrs, y);
          if (ret == 0) {
            source->driver->get_policy_driver()->get_cache_policy()->update(dpp, key, ofs, bl.length(), dest_version, true, rgw::d4n::RefCount::NOOP, y);
          }
          if (ret = source->driver->get_block_dir()->set(dpp, &dest_block, y); ret < 0){
            ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " BlockDirectory set failed with ret: " << ret << dendl;
          }
        } else {
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " eviction returned ret: " << ret << dendl;
        }
      }
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " offset not found: " << offset << dendl;
    }
  
    offset += bl.length();
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::returning from flush:: " << dendl;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string version = source->get_object_version();
  std::string prefix = get_cache_block_prefix(source, version);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "prefix: " << prefix << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "oid: " << source->get_key().get_oid() << " ofs: " << ofs << " end: " << end << dendl;

  this->client_cb = cb;
  this->cb->set_client_cb(cb, dpp, &y);
  source->set_prefix(prefix);

  /* This algorithm stores chunks for ranged requests also in the cache, which might be smaller than max_chunk_size
     One simplification could be to overwrite the smaller chunks with a bigger chunk of max_chunk_size, and to serve requests for smaller
     chunks using the larger chunk, but all corner cases need to be considered like the last chunk which might be smaller than max_chunk_size
     and also ranged requests where a smaller chunk is overwritten by a larger chunk size != max_chunk_size */

  uint64_t max_chunk_size = g_conf()->rgw_max_chunk_size;
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/max_chunk_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*max_chunk_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t diff_ofs = ofs - adjusted_start_ofs; //difference between actual offset and adjusted offset
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%max_chunk_size) == 0 ? len/max_chunk_size : (len/max_chunk_size) + 1; //calculate num parts based on adjusted offset
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << " adjusted_start_offset: " << adjusted_start_ofs << " len: " << len << dendl;

  if ((params.part_num && !source->is_multipart()) || !params.part_num) {
    aio = rgw::make_throttle(window_size, y);

    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "max_chunk_size " << max_chunk_size << " num_parts " << num_parts << dendl;

    this->offset = ofs;

    rgw::d4n::CacheBlock block;
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_bucket_id();

    do {
      uint64_t id = adjusted_start_ofs, read_ofs = 0; //read_ofs is the actual offset to start reading from the current part/ chunk
      if (start_part_num == (num_parts - 1)) {
        len_to_read = len;
        part_len = len;
        cost = len;
      } else {
        len_to_read = max_chunk_size;
        cost = max_chunk_size;
        part_len = max_chunk_size;
      }
      if (start_part_num == 0) {
        len_to_read -= diff_ofs;
        id += diff_ofs;
        read_ofs = diff_ofs;
      }

      block.blockID = adjusted_start_ofs;
      block.size = part_len;

      ceph::bufferlist bl;
      std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(part_len));

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
      " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

      int ret;
      if ((ret = source->driver->get_block_dir()->get(dpp, &block, y)) == 0) {
        auto it = block.cacheObj.hostsList.find(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

        if (it != block.cacheObj.hostsList.end()) { /* Local copy */
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block found in directory. " << oid_in_cache << dendl;
          // we keep track of dirty data in the cache for the metadata failure case
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: block is dirty = " << block.cacheObj.dirty << dendl;
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): " << __LINE__ << ": READ FROM CACHE: oid_in_cache=" << oid_in_cache << dendl;

          if (block.version == version) {
            if (source->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, oid_in_cache, rgw::d4n::RefCount::INCR, y) > 0) {
              // Read From Cache
              auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), oid_in_cache, read_ofs, len_to_read, cost, id);
              this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, part_len)));
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
              auto r = flush(dpp, std::move(completed), y);
              if (r < 0) {
                drain(dpp, y);
                ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, ret=" << r << dendl;
                return r;
              }
            } else { // end - if update_refcount_if_key_exists
              int r = -1;
              if ((r = source->driver->get_block_dir()->remove_host(dpp, &block, dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, y)) < 0)
                ldpp_dout(dpp, 10) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to remove incorrect host from block with oid=" << oid_in_cache <<", ret=" << r << dendl;

              if ((block.cacheObj.hostsList.size() - 1) > 0 && r == 0) { /* Remote copy */
                ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in remote cache." << dendl;
                // TODO: Retrieve remotely
                // Policy decision: should we cache remote blocks locally?
              } else {
                ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
                auto r = drain(dpp, y);
                if (r < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
                  return r;
                }
                break;
              }
            } //end - else
          } else { // if (block.version == version)
            // TODO: If data has already been returned for any older versioned block, then return ‘retry’ error, else
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
            auto r = drain(dpp, y);
            if (r < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
              return r;
            }
            break;
          }
        } else if (block.cacheObj.hostsList.size()) { /* Remote copy */
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block found in remote cache. " << oid_in_cache << dendl;
          // TODO: Retrieve remotely
          // Policy decision: should we cache remote blocks locally?
        }
      } else if (ret == -ENOENT) { // end - if ((ret = source->driver->get_block_dir()->get
        block.blockID = adjusted_start_ofs;
        uint64_t obj_size = source->get_size(), chunk_size = 0;
        if (obj_size < max_chunk_size) {
          chunk_size = obj_size;
        } else {
          chunk_size = max_chunk_size;
        }
        block.size = chunk_size;

        if ((ret = source->driver->get_block_dir()->get(dpp, &block, y)) == 0) {
          auto it = block.cacheObj.hostsList.find(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

          if (it != block.cacheObj.hostsList.end()) { /* Local copy */
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in local cache." << dendl;

            if (block.version == version) {
              oid_in_cache = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(chunk_size));

              //for range requests, for last part, the whole part might exist in the cache
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache <<
                " length to read is: " << len_to_read << " part num: " << start_part_num << " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

              if ((part_len != chunk_size) && source->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, oid_in_cache, rgw::d4n::RefCount::INCR, y) > 0) {
                // Read From Cache
                ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): " << __LINE__ << ": READ FROM CACHE: oid_in_cache=" << oid_in_cache << dendl;

                auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), oid_in_cache, read_ofs, len_to_read, cost, id);
                this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, chunk_size)));
                ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
                auto r = flush(dpp, std::move(completed), y);
                if (r < 0) {
                  drain(dpp, y);
                  ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, ret=" << r << dendl;
                  return r;
                }
              } else { // end - if ((part_len != chunk_size) && update_refcount_if_key_exists
                int r = -1;
                if ((r = source->driver->get_block_dir()->remove_host(dpp, &block, dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, y)) < 0)
                  ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to remove incorrect host from block with oid=" << oid_in_cache << ", ret=" << r << dendl;

                if ((block.cacheObj.hostsList.size() - 1) > 0 && r == 0) { /* Remote copy */
                  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in remote cache." << dendl;
                  // TODO: Retrieve remotely
                  // Policy decision: should we cache remote blocks locally?
                } else {
                  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;

                  auto r = drain(dpp, y);
                  if (r < 0) {
                    ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
                    return r;
                  }
                  break;
                }
              }
            } else if (block.cacheObj.hostsList.size()) { /* Remote copy */
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in remote cache." << dendl;
              // TODO: Retrieve remotely
              // Policy decision: should we cache remote blocks locally?
            }
          } else { // end - if (block.version == version)
            // TODO: If data has already been returned for any older versioned block, then return ‘retry’ error, else
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
            auto r = drain(dpp, y);
            if (r < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
              return r;
            }
            break;
          }
        } else if (ret == -ENOENT) { // end - if source->driver->get_block_dir()->get(dpp, &block, y))
          block.blockID = adjusted_start_ofs;
          uint64_t last_part_size = source->get_size() - adjusted_start_ofs;
          block.size = last_part_size;
          if ((ret = source->driver->get_block_dir()->get(dpp, &block, y)) == 0) {
            auto it = block.cacheObj.hostsList.find(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);
            if (it != block.cacheObj.hostsList.end()) { /* Local copy */
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in local cache." << dendl;
              if (block.version == version) {
                oid_in_cache = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(last_part_size));
                ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache <<
                  " length to read is: " << len_to_read << " part num: " << start_part_num << " read_ofs: " << read_ofs << " part len: " << part_len << dendl;
                if ((part_len != last_part_size) && source->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, oid_in_cache, rgw::d4n::RefCount::INCR, y) > 0) {
                  // Read From Cache
                  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__  << "(): " << __LINE__ << ": READ FROM CACHE: block dirty =" << block.cacheObj.dirty << dendl;
                  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): " << __LINE__ << ": READ FROM CACHE: oid_in_cache=" << oid_in_cache << dendl;
                  auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), oid_in_cache, read_ofs, len_to_read, cost, id);
                  this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, last_part_size)));
                  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
                  auto r = flush(dpp, std::move(completed), y);
                  if (r < 0) {
                    drain(dpp, y);
                    ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, ret=" << r << dendl;
                    return r;
                  }
                } else { // if get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists
                  int r = -1;
                  if ((r = source->driver->get_block_dir()->remove_host(dpp, &block, dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, y)) < 0)
                    ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to remove incorrect host from block with oid=" << oid_in_cache << ", ret=" << r << dendl;
                  if ((block.cacheObj.hostsList.size() - 1) > 0 && r == 0) { /* Remote copy */
                    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in remote cache." << dendl;
                    // TODO: Retrieve remotely
                    // Policy decision: should we cache remote blocks locally?
                  } else {
                    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
                    auto r = drain(dpp, y);
                    if (r < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
                      return r;
                    }
                    break;
                  }
                }
              } else {// end - if (block.version == version)
                //TODO: return retry error
                ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
                auto r = drain(dpp, y);
                if (r < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
                  return r;
                }
                break;
              }
            } else if (block.cacheObj.hostsList.size()) {
              //TODO: get remote copy
            }
          } else if (ret == -ENOENT) {
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
            auto r = drain(dpp, y);
            if (r < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
              return r;
            }
            break;
          }
        }
      } else { // else if (ret == -ENOENT)
        if (ret < 0)
          ldpp_dout(dpp, 0) << "Failed to fetch existing block for: " << block.cacheObj.objName << " blockID: " << block.blockID << " block size: " << block.size << ", ret=" << ret << dendl;
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
        auto r = drain(dpp, y);
        if (r < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
          return r;
        }
        break;
      } //end - else

      if (start_part_num == (num_parts - 1)) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
        return drain(dpp, y);
      } else {
        adjusted_start_ofs += max_chunk_size;
      }

      start_part_num += 1;
      len -= max_chunk_size;
    } while (start_part_num < num_parts);
  }
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << dendl;

  Attrs obj_attrs;
  if (source->has_attrs()) {
    obj_attrs = source->get_attrs();
  }

  this->cb->set_ofs(diff_ofs);
  this->cb->set_adjusted_start_ofs(adjusted_start_ofs);
  this->cb->set_part_num(start_part_num);
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): adjusted_start_ofs: " << adjusted_start_ofs << " end: " << end << dendl;
  auto r = next->iterate(dpp, adjusted_start_ofs, end, this->cb.get(), y);
  //calculate the number of blocks read from backend store, and increment the perfcounter using that
  if(perfcounter) {
    uint64_t len_to_read_from_store = ((end - adjusted_start_ofs) + 1);
    uint64_t num_blocks = (len_to_read_from_store%max_chunk_size) == 0 ? len_to_read_from_store/max_chunk_size : (len_to_read_from_store/max_chunk_size) + 1;
    perfcounter->inc(l_rgw_d4n_cache_misses, num_blocks);
  }
  
  if (r < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, ret=" << r << dendl;
    return r;
  }
  /* Copy params out of next */
  params = next->params;
  return this->cb->flush_last_part();
}

int D4NFilterObject::D4NFilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  rgw::sal::Attrs& attrs = source->get_attrs();
  if (attrs.empty()) {
    rgw_obj obj = source->get_obj();
    auto ret = source->get_obj_attrs(y, dpp, &obj);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Error: failed to fetch attrs, ret=" << ret << dendl;
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
  auto rgw_max_chunk_size = g_conf()->rgw_max_chunk_size;
  ldpp_dout(dpp, 20) << __func__ << ": bl_ofs is: " << bl_ofs << " bl_len is: " << bl_len << " ofs is: " << ofs << " part_count: " << part_count << dendl;
  if (!last_part && bl.length() <= rgw_max_chunk_size) {
    if (client_cb) {
      int r = 0;
      //ranged request
      if (bl_ofs != ofs && part_count == 0) {
        if (ofs < bl_len) { // this can happen in case of multipart where each chunk returned is not always of size rgw_max_chunk_size
          off_t bl_part_len = bl_len - ofs;
          ldpp_dout(dpp, 20) << __func__ << ": bl_part_len is: " << bl_part_len << dendl;
          bufferlist bl_part;
          bl.begin(ofs).copy(bl_part_len, bl_part);
          ldpp_dout(dpp, 20) << __func__ << ": bl_part.length() is: " << bl_part.length() << dendl;
          r = client_cb->handle_data(bl_part, 0, bl_part_len);
          part_count += 1;
        } else {
          ofs = ofs - bl_len; //re-adjust the offset
          ldpp_dout(dpp, 20) << __func__ << ": New value ofs is: " << ofs << dendl;
        }
      } else {
        r = client_cb->handle_data(bl, bl_ofs, bl_len);
        part_count += 1;
      }

      if (r < 0) {
        ldpp_dout(dpp, 20) << __func__ << ": error returned is: " << r << dendl;
        return r;
      }
    }
  }

  //Accumulating data from backend store into rgw_max_chunk_size sized chunks and then writing to cache
  if (write_to_cache) {
    Attrs attrs; // empty attrs for cache sets
    std::string version = source->get_object_version();
    std::string prefix = source->get_prefix();
    std::string dest_prefix;

    rgw::d4n::CacheBlock block, existing_block, dest_block;
    rgw::d4n::BlockDirectory* blockDir = source->driver->get_block_dir();
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_bucket_id();
    std::stringstream s;
    block.cacheObj.creationTime = std::to_string(ceph::real_clock::to_double(source->get_mtime()));
    bool dirty = block.cacheObj.dirty = false; //Reading from the backend, data is clean
    block.version = version;

    if (source->dest_object && source->dest_bucket) {
      D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
      std::string dest_version = d4n_dest_object->get_object_version();
      dest_prefix = get_cache_block_prefix(source->dest_object, dest_version);
      dest_block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);
      dest_block.cacheObj.objName = source->dest_object->get_key().get_oid();
      dest_block.cacheObj.bucketName = source->dest_object->get_bucket()->get_bucket_id();
      //dest_block.cacheObj.creationTime = std::to_string(ceph::real_clock::to_time_t(source->get_mtime()));
      dest_block.cacheObj.dirty = false;
      dest_block.version = dest_version;
    }

    //populating fields needed for building directory index
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;

    ldpp_dout(dpp, 20) << __func__ << ": version stored in update method is: " << version << dendl;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        block.blockID = adjusted_start_ofs;
        block.size = bl.length();

        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            std::string objEtag = "";
            filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, adjusted_start_ofs, bl.length(), version, dirty, rgw::d4n::RefCount::NOOP, *y);

            /* Store block in directory */
            existing_block.blockID = block.blockID;
            existing_block.size = block.size;

            if ((ret = blockDir->get(dpp, &existing_block, *y)) == 0 || ret == -ENOENT) {
              if (ret == 0) { //new versioned block will have new version, hostsList etc, how about globalWeight?
                block = existing_block;
                block.version = version;
              }

              block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

              if ((ret = blockDir->set(dpp, &block, *y)) < 0)
                ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set() method failed, ret=" << ret << dendl;
            } else { //end -if blockDir->get
              ldpp_dout(dpp, 20) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << ", ret=" << ret << dendl;
            }
          } else {
            ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed, ret=" << ret << dendl;
          }
        } //end-if ret == 0
      } //end-if exist_key
      if (source->dest_object && source->dest_bucket) {
        D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
        std::string dest_version = d4n_dest_object->get_object_version();
        std::string dest_oid = get_key_in_cache(dest_prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
        dest_block.blockID = adjusted_start_ofs;
        dest_block.size = bl.length();
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, dest_block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, dest_oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            filter->get_policy_driver()->get_cache_policy()->update(dpp, dest_oid, adjusted_start_ofs, bl.length(), dest_version, dirty, rgw::d4n::RefCount::NOOP, *y);
            if (ret = blockDir->set(dpp, &dest_block, *y); ret < 0) {
              ldpp_dout(dpp, 20) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB:: " << __func__ << " BlockDirectory set failed with ret: " << ret << dendl;
            }
          }
        }
      }
    } else if (bl.length() == rgw_max_chunk_size && bl_rem.length() == 0) { // if bl is the same size as rgw_max_chunk_size, write it to cache
      std::string oid = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
      block.blockID = adjusted_start_ofs;
      block.size = bl.length();
      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, adjusted_start_ofs, bl.length(), version, dirty, rgw::d4n::RefCount::NOOP, *y);

            /* Store block in directory */
            existing_block.blockID = block.blockID;
            existing_block.size = block.size;

            if ((ret = blockDir->get(dpp, &existing_block, *y)) == 0 || ret == -ENOENT) {
              if (ret == 0) { //new versioned block will have new version, hostsList etc, how about globalWeight?
                block = existing_block;
                block.version = version;
              }

            block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

            if ((ret = blockDir->set(dpp, &block, *y)) < 0)
              ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set() method failed, ret=" << ret << dendl;
            } else {
              ldpp_dout(dpp, 20) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << ", ret=" << ret << dendl;
            }
          } else {
            ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed, ret=" << ret << dendl;
          }
        }
      }
      if (source->dest_object && source->dest_bucket) {
        D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
        std::string dest_version = d4n_dest_object->get_object_version();
        std::string dest_oid = get_key_in_cache(dest_prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
        dest_block.blockID = adjusted_start_ofs;
        dest_block.size = bl.length();
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, dest_block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, dest_oid, bl, bl.length(), attrs, *y);
          if (ret == 0) {
            filter->get_policy_driver()->get_cache_policy()->update(dpp, dest_oid, adjusted_start_ofs, bl.length(), dest_version, dirty, rgw::d4n::RefCount::NOOP, *y);
            if (ret = blockDir->set(dpp, &dest_block, *y); ret < 0) {
              ldpp_dout(dpp, 20) << "D4N Filter: " << __func__ << " BlockDirectory set failed with ret: " << ret << dendl;
            }
          }
        }
      }
      adjusted_start_ofs += bl_len;
    } else { //copy data from incoming bl to bl_rem till it is rgw_max_chunk_size, and then write it to cache
      uint64_t rem_space = rgw_max_chunk_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;

      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);

      if (bl_rem.length() == rgw_max_chunk_size) {
        std::string oid = prefix + CACHE_DELIM + std::to_string(adjusted_start_ofs) + CACHE_DELIM + std::to_string(bl_rem.length());
          if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
          block.blockID = adjusted_start_ofs;
          block.size = bl_rem.length();
          
          auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
          if (ret == 0) {
            ret = filter->get_cache_driver()->put(dpp, oid, bl_rem, bl_rem.length(), attrs, *y);
            if (ret == 0) {
              filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, adjusted_start_ofs, bl_rem.length(), version, dirty, rgw::d4n::RefCount::NOOP, *y);

              /* Store block in directory */
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;

              if ((ret = blockDir->get(dpp, &existing_block, *y)) == 0 || ret == -ENOENT) {
                if (ret == 0) { //new versioned block will have new version, hostsList etc, how about globalWeight?
                  block = existing_block;
                  block.version = version;
                }

                block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

                if ((ret = blockDir->set(dpp, &block, *y)) < 0)
                  ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set() method failed, ret=" << ret << dendl;
              } else {
                ldpp_dout(dpp, 0) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << ", ret=" << ret << dendl;
              }
            } else {
              ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed, ret=" << ret << dendl;
            }
          } else {
            ldpp_dout(dpp, 0) << "D4N Filter: " << __func__ << " An error occured during eviction, ret=" << ret << dendl;
          }
        }

        if (source->dest_object && source->dest_bucket) {
          D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
          std::string dest_version = d4n_dest_object->get_object_version();
          std::string dest_oid = dest_prefix + CACHE_DELIM + std::to_string(adjusted_start_ofs) + CACHE_DELIM + std::to_string(bl_rem.length());
          dest_block.blockID = adjusted_start_ofs;
          dest_block.size = bl_rem.length();
          auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, dest_block.size, *y);
          if (ret == 0) {
            ret = filter->get_cache_driver()->put(dpp, dest_oid, bl_rem, bl_rem.length(), attrs, *y);
            if (ret == 0) {
              filter->get_policy_driver()->get_cache_policy()->update(dpp, dest_oid, adjusted_start_ofs, bl_rem.length(), dest_version, dirty, rgw::d4n::RefCount::NOOP, *y);
              if (ret = blockDir->set(dpp, &dest_block, *y); ret < 0) {
                ldpp_dout(dpp, 20) << "D4N Filter: " << __func__ << " BlockDirectory set failed with ret: " << ret << dendl;
              }
            }
          }
        }
        adjusted_start_ofs += bl_rem.length();
        bl_rem.clear();
        bl_rem = std::move(bl);
      }//bl_rem.length()
    }
  }

  /* Clean-up:
  1. do we need to clean up keys belonging to older versions (the last blocks), in case the size of newer version is different
  2. do we need to revert the cache ops, in case the directory ops fail
  */

  return 0;
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                                   optional_yield y, uint32_t flags)
{
  // TODO: Send delete request to cache nodes with remote copies

  rgw::sal::Attrs attrs;
  std::string head_oid_in_cache;
  rgw::d4n::CacheBlock block;
  int ret = -1;

  /* check_head_exists_in_cache_get_oid also returns false if the head object is in the cache, but is a delete marker.
     As a result, the below check guarantees the head object is not in the cache. */
  if (!source->check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y) && !block.deleteMarker) {
    /* for a dirty object, if the first call is a simple delete after versioning is enabled, the call will go to the backend store and create a delete marker there
       since no object with source->get_name() will be found in the cache (and this is correct) */
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): head object not found; calling next->delete_obj" << dendl;
    next->params = params;
    ret = next->delete_obj(dpp, y, flags);
    result = next->result;
    return ret;
  } else {
    bool objDirty = block.cacheObj.dirty;
    auto blockDir = source->driver->get_block_dir();
    auto objDir = source->driver->get_obj_dir();
    auto bucketDir = source->driver->get_bucket_dir();
    std::string version = source->get_object_version();
    std::string objName = source->get_name();
    // special handling for name starting with '_'
    if (objName[0] == '_') {
      objName = "_" + source->get_name();
    }

    if (objDirty) { // head object dirty flag represents object dirty flag
      //for versioned buckets, for a simple delete we need to create a delete marker (and not invalidate/delete any object)
      if (!source->get_bucket()->versioned() || (block.cacheObj.objName != source->get_name())) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): calling invalidate_dirty_object for: " << head_oid_in_cache << dendl;
        if (!source->driver->get_policy_driver()->get_cache_policy()->invalidate_dirty_object(dpp, head_oid_in_cache)) {
          objDirty = false;
        }
      }
    }

    // Versioned buckets - this will delete the head object indexed by version-id (even null) and latest en
    if (source->get_bucket()->versioned()) {
        /* 1. clean objects - no latest head entry as latest entry to be retrieved from backend now
           hence delete only versioned head object */
        if (!objDirty) {
          if (source->have_instance()) {
            if ((ret = blockDir->del(dpp, &block, y)) < 0) {
              ldpp_dout(dpp, 0) << "Failed to delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl; 
              return ret;
            }
          }
          /* if versioning is suspended, we might have a latest head entry created from when bucket was non-versioned
             don't return error as that could already be deleted by set_head_obj_dir_entry */
          if (!source->get_bucket()->versioning_enabled()) {
            block.cacheObj.objName = objName;
            if ((ret = blockDir->del(dpp, &block, y)) < 0) {
              ldpp_dout(dpp, 0) << "Failed to delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
            }
          }
        } else if (objDirty) { //2. dirty objects - 1. add delete marker for simple request 2. delete version if given and correctly promote latest version if needed
          bool transaction_success = false;
          //add watch on latest entry, as it can be modified by a put or another del
          rgw::d4n::CacheBlock latest_block = block;
          latest_block.cacheObj.objName = objName;
          int retry = 3;
          while(retry) {
            retry--;
            //get latest entry
            ret = blockDir->get(dpp, &latest_block, y);
            if (ret < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to get latest entry in block directory for: " << latest_block.cacheObj.objName << ", ret=" << ret << dendl;
              return ret;
            }
            //simple delete request with no version id - create a delete marker
            if (block.cacheObj.objName == objName) {
              /* we are checking for latest_block and not block because latest_block has the most updated value of latest hash entry
                 if existing latest entry is already a delete marker, do not create a new one and simply return */
              if (!latest_block.deleteMarker) {
                ret = source->create_delete_marker(dpp, y);
                if (ret < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to create a delete marker for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
                  //ERR_INTERNAL_ERROR is returned when exec_responses are empty which means the watched key has been modified, hence retry
                  if (ret == -ERR_INTERNAL_ERROR) {
                    continue;
                  } else {
                    return ret;
                  }
                }
                if (ret >= 0) {
                  result.delete_marker = true;
                  result.version_id = source->get_instance();
                  transaction_success = true;
                  return 0;
                }
              }
              transaction_success = true;
              return 0;
            } else { //not a simple request, delete version requested
              //get latest entry ret is 0
              if (ret == 0) {
                rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
                  .objName = objName,
                  .bucketName = source->get_bucket()->get_bucket_id(),
                };
                //check if version to be deleted is the same as latest version
                if (latest_block.version == block.version) {
                  std::vector<std::string> members;
                  //get the second latest version
                  ret = objDir->zrevrange(dpp, &dir_obj, std::to_string(0), std::to_string(1), members, y);
                  if (ret < 0) {
                    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to get the second latest version for: " << dir_obj.objName << ", ret=" << ret << dendl;
                    return ret;
                  }
                  //if there is a second latest version
                  if (members.size() == 2) {
                    rgw::d4n::CacheBlock version_block = latest_block;
                    version_block.cacheObj.objName = "_:" + members[1] + "_" + source->get_name();
                    //get versioned entry
                    ret = blockDir->get(dpp, &version_block, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to get the versioned entry for: " << version_block.cacheObj.objName << ", ret=" << ret << dendl;
                      return 0;
                    }
                    //set versioned entry as the latest entry
                    version_block.cacheObj.objName = latest_block.cacheObj.objName;
                    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): INFO: promoting latest version entry to version: " << version_block.version << ", ret=" << ret << dendl;
                    ret = blockDir->set(dpp, &version_block, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to set new latest entry for: " << version_block.cacheObj.objName << ", ret=" << ret << dendl;
                      return 0;
                    }
                  } else { // there are no more versions left
                    //delete latest block entry
                    ret = blockDir->del(dpp, &latest_block, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to delete latest entry in block directory, when it is the same as version requested, for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
                      return ret;
                    }
                    //delete entry from ordered set of objects
                    ret = bucketDir->zrem(dpp, source->get_bucket()->get_bucket_id(), source->get_name(), y, true);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue zrem request in bucket directory for: " << source->get_name() << ", ret=" << ret << dendl;
                      return ret;
                    }
                  }
                } //end-if latest_block.version == block.version
                //delete versioned entry (handles delete markers also)
                if ((ret = blockDir->del(dpp, &block, y)) < 0 && ret != -ENOENT) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
                  return ret;
                }
                //delete entry from ordered set of versions
                std::string version = source->get_instance();
                ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Version to be deleted is: " << version << dendl;
                ret = objDir->zrem(dpp, &dir_obj, version, y, true);
                if (ret < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue zrem request in object directory for: " << source->get_name() << ", ret=" << ret << dendl;
                  return ret;
                }
                if (ret < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to execute exec in block directory: " << "ret= " << ret << dendl;
                  return ret;
                }
                result.delete_marker = block.deleteMarker;
                result.version_id = version;
                //success, hence break from loop
                transaction_success = true;
                break;
              }
            } //end-else (simple request)
          } //end-while retry
          if (!transaction_success) {
            ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Redis transaction failed after retrying! " << dendl;
            return -ERR_INTERNAL_ERROR;
          }
        } //end-if objDirty
    } //end-if versioned buckets

    /* Non-versioned buckets - we will delete the latest entry and the "null" entry
       dirty objects - delete "null" entry from ordered set also */
    if (!source->get_bucket()->versioned()) {
      //explore redis pipelining to send the two 'DEL' commands together in a single request
      ret = blockDir->del(dpp, &block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue delete head object op in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
        return ret;
      }
      //if we get request for latest head entry, delete the null block and vice versa
      if (block.cacheObj.objName == objName) {
        block.cacheObj.objName = "_:null_" + source->get_name();
      } else {
        block.cacheObj.objName = source->get_name();
      }
      ret = blockDir->del(dpp, &block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
        return ret;
      }
      //dirty objects - delete from ordered set of versions and objects
      if (objDirty) {
        rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
          .objName = source->get_name(),
          .bucketName = source->get_bucket()->get_bucket_id(),
        };
        //delete entry from ordered set of object versions
        ret = objDir->zrem(dpp, &dir_obj, "null", y, true);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue zrem request in object directory for: " << source->get_name() << ", ret=" << ret << dendl;
          return ret;
        }
        //delete entry from ordered set of objects
        ret = bucketDir->zrem(dpp, source->get_bucket()->get_bucket_id(), source->get_name(), y, true);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue zrem request in bucket directory for: " << source->get_name() << ", ret=" << ret << dendl;
          blockDir->discard(dpp, y);
          return ret;
        }
      }
    } //end-if non-versioned buckets

    int size;
    if (objDirty) {
      std::string size_str;

      if (attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE) != attrs.end()) {
        size_str = attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE)->second.to_str();
      } else {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to retrieve size for for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
        return -EINVAL;
      }
      size = stoi(size_str);
    } else { //for clean objects
      size = this->source->get_size();
    }
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Size of object is: " << size << dendl;

    /* delete data blocks directory entries, when,
       1. object is clean, bucket is versioned and there is an instance in the request
       2. object is clean, bucket is non-versioned
       3. object is dirty - delete blocks in cache also except for delete markers */
    if ((!objDirty && source->get_bucket()->versioned() && source->have_instance()) ||
        (!objDirty && !source->get_bucket()->versioned()) ||
        (objDirty && !block.deleteMarker)) {
        off_t lst = size;
        off_t fst = 0;

      do { // loop through the data blocks
        std::string prefix = get_cache_block_prefix(source, version);
        if (fst >= lst) {
          break;
        }
        //data blocks have cacheObj.objName set to oid always
        block.cacheObj.objName = source->get_oid();
        off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
        off_t cur_len = cur_size - fst;
        block.blockID = static_cast<uint64_t>(fst);
        block.size = static_cast<uint64_t>(cur_len);

          if ((ret = blockDir->get(dpp, &block, y)) < 0) {
            if (ret == -ENOENT) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Directory entry for: " << source->get_oid() << " blockid: " << fst << " block size: " << cur_len << " does not exist; continuing" << dendl;
              fst += cur_len;
              if (fst >= lst) {
                break;
              }
              continue;
            } else {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to retrieve directory entry for: " << source->get_oid() << " blockid: " << fst << " block size: " << cur_len << ", ret=" << ret << dendl;
              return ret;
            }
          }

          if ((ret = blockDir->del(dpp, &block, y)) == -ENOENT) { 
            continue;
          } else if (ret < 0) {
            ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to delete directory entry for: " << source->get_name() << " blockid: " << fst << " block size: " << cur_len << ", ret=" << ret << dendl;
            return ret;
          }

        fst += cur_len;
      } while (fst < lst);
    }

    if (!objDirty) {
      next->params = params;
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object is not dirty; calling next->delete_obj" << dendl;
      ret = next->delete_obj(dpp, y, flags);
      result = next->result;
      return ret;
    }
    return 0;
  }
}

int D4NFilterWriter::prepare(optional_yield y) 
{
  d4n_writecache = g_conf()->d4n_writecache_enabled;

  if (!d4n_writecache) {
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): calling next->prepare" << dendl;
    return next->prepare(y);
  } else {
    //for non-versioned buckets or version suspended buckets, we need to delete the older dirty blocks of the object from the cache as dirty blocks do not get evicted
    if (!object->get_bucket()->versioned() || (object->get_bucket()->versioned() && !object->get_bucket()->versioning_enabled())) {
      if (object->get_bucket()->versioned() && !object->get_bucket()->versioning_enabled()) {
        object->set_instance("null");
      }
      rgw::d4n::CacheBlock block;
      rgw::sal::Attrs attrs;
      if (object->check_head_exists_in_cache_get_oid(dpp, prev_oid_in_cache, attrs, block, y)) {
        ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): found in cache, prev_oid_in_cache=" << prev_oid_in_cache << dendl;
      }
      object->clear_instance();
    }
  }

  std::string version;
  if (!object->have_instance()) {
    if (object->get_bucket()->versioned() && !object->get_bucket()->versioning_enabled()) { //if versioning is suspended
      object->set_instance("null");
    }
    char buf[OBJ_INSTANCE_LEN + 1];
    gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf, OBJ_INSTANCE_LEN);
    version = buf; // using gen_rand_alphanumeric_no_underscore for the time being
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): generating version: " << version << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): version is: " << object->get_instance() << dendl;
    version = object->get_instance();
  }
  object->set_object_version(version);
  this->version = version;

  return 0;
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
    bufferlist bl = data;
    off_t bl_len = bl.length();
    off_t ofs = offset;
    bool dirty = true;

    std::string version = object->get_object_version();
    std::string prefix = get_cache_block_prefix(obj, version);

    int ret = 0;

    if (!d4n_writecache) {
      ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): calling next process" << dendl;
      return next->process(std::move(data), offset);
    } else {
      rgw::sal::Attrs attrs;
      std::string oid = prefix + CACHE_DELIM + std::to_string(ofs);
      std::string oid_in_cache = oid + CACHE_DELIM + std::to_string(bl_len);
      dirty = true;
      ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, bl.length(), y);
      if (ret == 0) {     
        if (bl.length() > 0) {          
          ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): oid_in_cache is: " << oid_in_cache << dendl;
          ret = driver->get_cache_driver()->put(dpp, oid_in_cache, bl, bl.length(), attrs, y);
          if (ret == 0) {
            ret = driver->get_cache_driver()->set_attr(dpp, oid_in_cache, RGW_CACHE_ATTR_DIRTY, "1", y);
            if (ret == 0) {
              driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, bl.length(), version, dirty, rgw::d4n::RefCount::NOOP, y);
            }
          } else {
            ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): ERROR: writting data to the cache failed, ret=" << ret << dendl;
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
  std::unordered_set<std::string> hostsList = {};
  std::string objEtag = etag;
  auto size = object->get_size();
  std::string instance;
  if (object->have_instance()) {
    instance = object->get_instance();
  }
  int ret;
  
  /* for cache coherence, we are going to cache the head even in case when read-only cache is enabled, just that
     the head will not be marked dirty and the entire object will written to backend store also. In case write-back
     cache is enabled, the head will be cached as dirty. */
  if (d4n_writecache) {
    auto ret = object->get_obj_attrs(y, dpp);
    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        if (ret == -ENOENT) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        rgw::sal::Attrs attrs = object->get_attrs();
        bufferlist bl;
        auto iter = attrs.find(RGW_ATTR_ETAG);
        if (iter == attrs.end()) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        } else {
          bl = iter->second;
        }
        if (strncmp(if_match, bl.c_str(), bl.length()) != 0) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        if (ret != -ENOENT) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        rgw::sal::Attrs attrs = object->get_attrs();
        bufferlist bl;
        auto iter = attrs.find(RGW_ATTR_ETAG);
        if (iter == attrs.end()) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        } else {
          bl = iter->second;
        }
        if (strncmp(if_nomatch, bl.c_str(), bl.length()) == 0) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    //get_obj_attrs will override object version and size with older values, hence setting it here again
    object->set_object_version(this->version);
    object->set_instance(instance);
    object->set_obj_size(size);

    //update data block entries in directory
    ret = object->set_data_block_dir_entries(dpp, y, this->version, true);
    if (ret < 0) {
      return ret;
    }

    dirty = true;
    ceph::real_time m_time;
    if (mtime) {
      if (real_clock::is_zero(*mtime)) {
        *mtime = real_clock::now();
      }
      m_time = *mtime;
    } else {
      m_time = real_clock::now();
    }
    object->set_mtime(m_time);
    object->set_accounted_size(accounted_size);
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << " size is: " << object->get_size() << dendl;
    object->set_attr_crypt_parts(dpp, y, attrs);
    object->set_attrs(attrs);
    object->set_attrs_from_obj_state(dpp, y, attrs, dirty);
  } else {
    // we need to call next->complete here so that we are able to correctly get the object state needed for caching head
    ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs, cksum,
                            delete_at, if_match, if_nomatch, user_data, zones_trace,
                            canceled, rctx, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): writing to backend store failed, ret=" << ret << dendl;
      return ret;
    }
    /* we want to always load latest object state from store
       to avoid reading stale state in case of object overwrites. */
    object->set_load_obj_from_store(true);
    object->load_obj_state(dpp, y);
    attrs = object->get_attrs();
    object->set_attrs_from_obj_state(dpp, y, attrs, dirty);

    std::string version;
    object->calculate_version(dpp, y, version, attrs);
    if (version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }
  }

  std::string version = object->get_object_version();
  std::string key = get_cache_block_prefix(obj, version);

  bufferlist bl;
  //same as key, as there is no len or offset attached to head oid in cache
  ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): key is: " << key << dendl;
  ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
  if (ret == 0) {
    ret = driver->get_cache_driver()->put(dpp, key, bl, 0, attrs, y);
    attrs.erase(RGW_CACHE_ATTR_MTIME);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    attrs.erase(RGW_CACHE_ATTR_EPOCH);
    attrs.erase(RGW_CACHE_ATTR_MULTIPART);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    object->set_object_version(version);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): version stored in update method is: " << version << dendl;
      driver->get_policy_driver()->get_cache_policy()->update(dpp, key, 0, bl.length(), version, dirty, rgw::d4n::RefCount::NOOP, y);
      ret = object->set_head_obj_dir_entry(dpp, nullptr, y, true, dirty);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
        return ret;
      }
      if (dirty) {
        auto creationTime = ceph::real_clock::to_double(object->get_mtime());
        ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): key=" << key << dendl;
        ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): obj->get_key()=" << obj->get_key() << dendl;
        driver->get_policy_driver()->get_cache_policy()->update_dirty_object(dpp, key, version, false, accounted_size, creationTime, std::get<rgw_user>(obj->get_bucket()->get_owner()), objEtag, obj->get_bucket()->get_name(), obj->get_bucket()->get_bucket_id(), obj->get_key(), rgw::d4n::RefCount::NOOP, y);
        if (!prev_oid_in_cache.empty()) {
          driver->get_policy_driver()->get_cache_policy()->invalidate_dirty_object(dpp, prev_oid_in_cache);
        }
      }
    } else { //if get_cache_driver()->put()
      ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): put failed for head_oid_in_cache, ret=" << ret << dendl;
      return ret;
    }
  } else {
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): eviction failed for head_oid_in_cache, ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int D4NFilterMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
            prefix_map_t& processed_prefixes)
{
  //call next->complete to complete writing the object to the backend store
  auto ret = next->complete(dpp, y, cct, part_etags, remove_objs, accounted_size,
			compressed, cs_info, ofs, tag, owner, olh_epoch,
			nextObject(target_obj), processed_prefixes);
  if (ret < 0) {
    return ret;
  }

  //Cache only the head object for multipart objects
  D4NFilterObject* d4n_target_obj = dynamic_cast<D4NFilterObject*>(target_obj);
  /* we want to always load latest object state from store
     to avoid reading stale state in case of object overwrites. */
  d4n_target_obj->set_load_obj_from_store(true);
  d4n_target_obj->load_obj_state(dpp, y);
  rgw::sal::Attrs attrs = d4n_target_obj->get_attrs();
  d4n_target_obj->set_attrs_from_obj_state(dpp, y, attrs);
  bufferlist bl_val;
  bool is_multipart = true;
  bl_val.append(std::to_string(is_multipart));
  attrs[RGW_CACHE_ATTR_MULTIPART] = std::move(bl_val);

  std::string version;
  d4n_target_obj->calculate_version(dpp, y, version, attrs);
  if (version.empty()) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
  }

  bufferlist bl;
  std::string head_oid_in_cache = get_cache_block_prefix(d4n_target_obj, version);
  // we are evicting data if needed, since the head object will be a part of read cache, as the whole multipart object is written to the backend store
  ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
  if (ret == 0) {
    ret = driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterMultipartUpload::" << __func__ << " version stored in update method is: " << d4n_target_obj->get_object_version() << dendl;
      driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, bl.length(), version, false, rgw::d4n::RefCount::NOOP, y);
      ret = d4n_target_obj->set_head_obj_dir_entry(dpp, nullptr, y, true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterMultipartUpload::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterMultipartUpload::" << __func__ << "(): put for head object failed, ret=" << ret << dendl;
      return ret;
    }
  } else {
    ldpp_dout(dpp, 0) << "D4NFilterMultipartUpload::" << __func__ << "(): failed to cache head object during eviction, ret=" << ret << dendl;
    return ret;
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
