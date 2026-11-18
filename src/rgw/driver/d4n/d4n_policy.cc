/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include "d4n_policy.h"
#include "d4n_log.h"

#include <iomanip>  // for std::setprecision
#include <utility>
#include <string_view>
#include <source_location>

#include "common/split.h"
#include "common/dout_fmt.h"
#include "common/random_string.h"
#include "common/async/yield_context.h"
#include "common/async/blocked_completion.h"

#include "yield_completion.h"
#include "rgw_perf_counters.h"

#include "include/scope_guard.h"

namespace rgw::d4n {

int LFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y)
{
  int synced_age = age;

  auto result = this->driver->retry_transaction(dpp, y,
      [this, dpp, y, &synced_age](rgw::d4n::Transaction& txn) -> int {
        std::string raw;
        int ret = dir.get_kv(dpp, y, "lfuda", "age", raw, txn);
        if (ret < 0) {
          return ret;
        }

        int stored_age = raw.empty() ? 0 : std::stoi(raw);

        synced_age = std::max(age, stored_age);

        if (age > stored_age) {
          ret = dir.set_kv(dpp, y, "lfuda", "age",
                           std::to_string(age), txn);
          if (ret < 0) {
            return ret;
          }
        }

        return 0;
      });

  if (int ret_txn = handle_transaction_result(dpp, result, [](const auto&) {
        // Handle FoundationDB unknown_commit: lazy age synchronization tolerates uncertainty:
        return 0;
      }); ret_txn < 0)
    return ret_txn;

  age = synced_age;
  return 0;
}

int LFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y)
{
  float synced_weight_sum = weightSum;
  float synced_posted_sum = postedSum;
  const float entries_map_size = static_cast<float>(entries_map.size());

  auto result = this->driver->retry_transaction(dpp, y,
      [this, dpp, y, entries_map_size, &synced_weight_sum, &synced_posted_sum](rgw::d4n::Transaction& txn) -> int {
        synced_weight_sum = weightSum;
        synced_posted_sum = postedSum;

        if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {
          std::map<std::string, std::string> fetched;
          int ret = dir.get_kv_multi(
              dpp, y, "lfuda",
              {"minLocalWeights_sum", "minLocalWeights_size"},
              fetched, txn);
          if (ret < 0) {
            return ret;
          }

          float min_avg_weight =
              std::stof(fetched.at("minLocalWeights_sum")) /
              std::stof(fetched.at("minLocalWeights_size"));

          float local_avg_weight = entries_map_size > 0.0f
              ? static_cast<float>(weightSum) / entries_map_size
              : 0.0f;

          if (local_avg_weight < min_avg_weight) {
            ret = dir.set_kv_multi(dpp, y, "lfuda", {
                {"minLocalWeights_sum", std::to_string(weightSum)},
                {"minLocalWeights_size", std::to_string(entries_map_size)},
                {"minLocalWeights_address",
                 dpp->get_cct()->_conf->rgw_d4n_local_rgw_address}
            }, txn);

            if (ret < 0) {
              return ret;
            }

  	    synced_posted_sum = weightSum;
          } else {
            synced_posted_sum = std::stof(fetched.at("minLocalWeights_sum")); // Preserved from original.
          }
        }

        return dir.set_kv_multi(
            dpp, y,
            dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, {
                {"avgLocalWeight_sum", std::to_string(synced_weight_sum)},
                {"avgLocalWeight_size", std::to_string(entries_map_size)}
            },
            txn);
      });

  if (int ret_txn = handle_transaction_result(dpp, result, [](const auto&) {
        // Handle FoundationDB unknown_commit: lazy weight synchronization tolerates uncertainty:
        return 0;
      }); ret_txn < 0)
    return ret_txn;

  postedSum = synced_posted_sum;

  return 0;
}

asio::awaitable<void> LFUDAPolicy::directory_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  rthread_timer.emplace(co_await asio::this_coro::executor);
  co_await asio::this_coro::throw_if_cancelled(true);
  co_await asio::this_coro::reset_cancellation_state(
    asio::enable_terminal_cancellation());

  for (;;) try {
    /* Update age */
    if (int ret = age_sync(dpp, y); 0 > ret) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ret << dendl;
    }
    
    /* Update minimum local weight sum */
    if (int ret = local_weight_sync(dpp, y); 0 > ret) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ret << dendl;
    }

    int interval = dpp->get_cct()->_conf->rgw_lfuda_sync_frequency;
    rthread_timer->expires_after(std::chrono::seconds(interval));
    co_await rthread_timer->async_wait(asio::use_awaitable);
  } catch (sys::system_error& e) {
    if (e.code() == asio::error::operation_aborted) {
      break;
    } else {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
      continue;
    }
  }
}

LFUDAPolicy::~LFUDAPolicy()
{
  rthread_stop();
  quit = true;
  if (cond.has_value()) {
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
    cond->notify(l);
  }
  wake_cleaning_timers();
  if (cleaning_coroutine_pool) {
    cleaning_coroutine_pool->stop();
    cleaning_coroutine_pool.reset();
  }
  if (eviction_timer.has_value()) {
    eviction_timer->cancel();
  }
  eviction_done_future.wait();
  lw_quit = true;
  lw_cond.notify_all();
  if (lwthread.joinable()) { lwthread.join(); }
  for (auto& it : entries_map) {
    delete it.second;
  }
  for (auto& it : o_entries_map) {
    delete it.second.first;
  }
}

int LFUDAPolicy::init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::D4NFilterDriver* _driver) {
  cache_capacity = cacheDriver->get_current_partition_info(dpp).size;
  eviction_watermark_bytes = cache_capacity * EVICTION_WATERMARK;
  target_bytes = cache_capacity * TARGET_WATERMARK;

  response<int, int, int, int> resp;
  auto obj_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, const std::string& instance, optional_yield y, std::string& restore_val) {
    std::string dirty_obj_key = rgw::sal::get_cache_block_prefix(bucket_id, obj_key.name, version);
    //Since there could be multiple data blocks of an object, we check if o_entries_map has already been populated for an object
    if (!find_obj_entry(dirty_obj_key)) {
      rgw::d4n::CacheBlock block;
      if (instance == "null") {
        block.cacheObj.objName = rgw::sal::get_versioned_head_block_name("null", obj_key.name);
      } else {
        block.cacheObj.objName = obj_key.get_oid();
      }
      block.cacheObj.bucketName = bucket_id;
      block.blockID = 0;
      block.size = 0;
      // An invalid cached marker must remain restorable after its directory cleanup commits.
      const bool invalid_marker = deleteMarker && "1" == restore_val;
      if (const int ret = invalid_marker
              ? cacheDriver->get_attrs(dpp, key, block.cacheObj.attrs, y)
              : blockDir.get(dpp, y, &block, std::nullopt); ret < 0) {
        const auto whence = std::source_location::current();
        detail::log_message(dpp, whence, "failed to restore object attributes: {}", ret);

        return;
      }
      rgw::sal::Attrs attrs = std::move(block.cacheObj.attrs);
      std::string etag;
      if (auto i = attrs.find(RGW_ATTR_ETAG); i != attrs.end()) {
        etag = i->second.to_str();
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): etag: " << etag << dendl;
      }
      uint64_t size = 0;
      if (auto i = attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE); i != attrs.end()) {
        size = std::stoull(i->second.to_str());
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): size: " << size << dendl;
      }
      ceph::real_time creationTime;
      if (auto i = attrs.find(RGW_CACHE_ATTR_MTIME); i != attrs.end()) {
        auto ns = std::stoll(i->second.to_str());
        creationTime = ceph::real_time(std::chrono::nanoseconds(ns));
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): creationTime: " << creationTime << dendl;
      }
      rgw_user user;
      if (auto i = attrs.find(RGW_ATTR_ACL); i != attrs.end()) {
        RGWAccessControlPolicy policy;
        auto iter = i->second.cbegin();
        policy.decode(iter);
        user = std::get<rgw_user>(policy.get_owner().id);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): rgw_user: " << user.to_str() << dendl;
      }
      std::string bucket_name;
      if (auto i = attrs.find(RGW_CACHE_ATTR_BUCKET_NAME); i != attrs.end()) {
        bucket_name = i->second.to_str();
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): bucket_name: " << bucket_name << dendl;
      }
      State state{State::INIT};
      if (!restore_val.empty() && restore_val == "1") {
        // Data block marked invalid via RGW_CACHE_ATTR_INVALID xattr on SSD
        state = State::INVALID;
        ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): State restored to INVALID from xattr." << dendl;
      } else if (block.invalid) {
        // HEAD block tombstoned in BlockDirectory (invalid=true flag)
        // Crash recovery path: cleaning thread will call do_delete to clean up HEAD blocks
        state = State::INVALID;
        ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                          << "(): State restored to INVALID from BlockDirectory tombstone." << dendl;
      } else {
        state = State::INIT;
      }
      //create new LFUDAObjEntry and populate o_entries_map and per_obj_versions map
      std::lock_guard<std::mutex> l(lfuda_cleaning_lock);
      create_obj_entry(dpp, dirty_obj_key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, state);
    }
  };

  auto block_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, const rgw_user user, const std::string& bucketName, optional_yield y, std::string& restore_val) {
    update(dpp, key, offset, len, version, dirty, user, bucketName, RefCount::NOOP, y, nullptr, restore_val);
  };

  if (const int ret = cacheDriver->restore_blocks_objects(dpp, obj_callback, block_callback); 0 > ret) {
    detail::log_message(dpp, std::source_location::current(), "failed to restore cached entries: {}", ret);
    return ret;
  }
  {
    //Now loop through the version map and populate heap with the first entry as they are sorted by creation time
    const std::lock_guard l(lfuda_cleaning_lock);
    for (auto& [obj_name, version_map] : per_obj_versions) {
      if (version_map.empty()) {
        continue;
      }
      queue_oldest(version_map);
      ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                        << "(): pushed to heap obj_name=" << obj_name
                        << " creationTime=" << version_map.begin()->first << dendl;
    }
  }
  driver = _driver;
  if (dpp->get_cct()->_conf->d4n_writecache_enabled) {
    int num_cleaning_threads = dpp->get_cct()->_conf->rgw_d4n_cleaning_threads;
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ 
                       << "(): initializing cleaning thread pool with " 
                       << num_cleaning_threads << " threads" << dendl;
    cleaning_thread_pool = std::make_unique<boost::asio::thread_pool>(num_cleaning_threads);
    quit = false;
    cond.emplace(io_context.get_executor());
    if (!cleaning_coroutine_pool) {
      cleaning_coroutine_pool = std::make_unique<rgw::sal::CoroutinePool>(cleaning_thread_pool->get_executor(), num_cleaning_threads);
      cleaning_coroutine_pool->start(dpp);
    }
  }

  lwthread = std::thread(&LFUDAPolicy::localweight_writer, this, dpp);
  lw_quit = false;

  const int ret_txn = this->driver->execute_transaction(dpp, y,
      [this, dpp](rgw::d4n::Transaction& txn) -> int {
        int ret = dir.set_kv_multi(
            dpp, y, "lfuda",
            {
                {"minLocalWeights_sum", std::to_string(weightSum)},
                {"minLocalWeights_size", std::to_string(entries_map.size())},
                {"minLocalWeights_address",
                 dpp->get_cct()->_conf->rgw_d4n_local_rgw_address}
            },
            txn);
        if (ret < 0) {
          return ret;
        }

        return dir.set_kv_if_not_exists(
            dpp, y, "lfuda", "age", std::to_string(age), txn);
      });

  // Handle FoundationDB unknown_commit: return failure for administrator-led directory reconciliation:
  if (0 > ret_txn) {
    return ret_txn;
  }

  asio::co_spawn(io_context.get_executor(),
        directory_sync(dpp, y), asio::detached);

  if (cleaning_coroutine_pool) {
    int num_cleaning_threads = dpp->get_cct()->_conf->rgw_d4n_cleaning_threads;
    for (int i = 0; i < num_cleaning_threads; ++i) {
      cleaning_coroutine_pool->submit([this, dpp](boost::asio::yield_context yield) {
      optional_yield y(yield);
      cleaning(dpp, y);
      });
    }
  }
  eviction_timer.emplace(io_context);
  boost::asio::spawn(
        io_context,
        [this, dpp](boost::asio::yield_context yield) {
          optional_yield y{yield};
          background_eviction_worker(dpp, y);
        },
        [this, dpp](std::exception_ptr e) {
          if (e) {
            eviction_done_promise.set_exception(e);
          } else {
            eviction_done_promise.set_value();
          }
          ldpp_dout(dpp, 10) << "Background eviction co-routine stopped" << dendl;
      }
    );
  return 0;
}

int LFUDAPolicy::getMinAvgWeight(const DoutPrefixProvider* dpp, int *minAvgWeight, std::string *cache_address, optional_yield y) 
{
  std::map<std::string, std::string> fetched;
  int ret = dir.get_kv_multi(dpp, y, "lfuda",
                                {"minLocalWeights_sum", "minLocalWeights_size", "minLocalWeights_address"},
                                fetched, std::nullopt);
  if (ret < 0) return ret;
  *minAvgWeight = std::stof(fetched.at("minLocalWeights_sum"))
                        / std::stof(fetched.at("minLocalWeights_size"));

  *cache_address =  fetched.at("minLocalWeights_address");
  ldpp_dout(dpp, 20) << __func__ << "(): Cache address with minimum local weight is " << *cache_address << dendl;
  return 0;
}

/* Changes state to INVALID for dirty objects. An INVALID state indicates that a delete request has been
 issued on an object and it must be deleted rather than written to the backend. This lazy deletion occurs
 in the Cleaning method and prevents data races during concurrent requests. The method below returns "false"
 if the state has not been set to INVALID, and "true" if it has. The state is not set to INVALID when
 cleaning is in progress, a process which writes the object to the backend store.

 After setting state to INVALID, this method also marks all data blocks as invalid by setting the
 RGW_CACHE_ATTR_INVALID xattr. This is done outside the lock to avoid blocking other cache operations.
 If a block is not found (due to racing delete in do_delete), the error is ignored. */
bool LFUDAPolicy::invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) {
  LFUDAObjEntry* entry = nullptr;
  uint64_t obj_size = 0;
  bool is_delete_marker = false;

  {
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);

    if (o_entries_map.empty())
      return false;

    auto p = o_entries_map.find(key);
    if (p == o_entries_map.end()) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): key=" << key << " not found" << dendl;
      return false;
    }

    if (p->second.second == State::INIT) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Setting State::INVALID for key=" << key << dendl;
      p->second.second = State::INVALID;
      entry = p->second.first;
      obj_size = entry->size;
      is_delete_marker = entry->delete_marker;
    } else if (p->second.second == State::IN_PROGRESS) {
      // Do not wait - writeback in progress will handle cleanup
      return false;
    } else {
      return false;
    }
  }  // Release lock before marking blocks invalid

  // Mark HEAD block invalid (only for delete markers)
  if (is_delete_marker) {
    int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_INVALID, "1", y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Failed to set xattr on HEAD block, ret=" << ret << dendl;
      // Continue anyway - this is not fatal
    }
  }

  // Mark all data blocks invalid (outside lock to avoid blocking)
  if (!is_delete_marker && obj_size > 0) {
    uint64_t chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;
    const off_t lst = obj_size;
    off_t fst = 0;

    while (fst < lst) {
      off_t cur_size = std::min<off_t>(fst + chunk_size, lst);
      off_t cur_len = cur_size - fst;
      std::string oid_in_cache = rgw::sal::get_key_in_cache(key, std::to_string(fst), std::to_string(cur_len));

      int ret = cacheDriver->set_attr(dpp, oid_in_cache, RGW_CACHE_ATTR_INVALID, "1", y);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Failed to set xattr on data block"
                          << " oid=" << oid_in_cache << ", ret=" << ret << dendl;
        // Continue anyway - racing delete may have removed the block
      }

      fst += cur_len;
    }
  }

  return true;
}

int LFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, CacheBlock* victim, optional_yield y) {
  if (entries_heap.empty())
    return -ENOENT;

  /* Get victim cache block */
  auto entry = entries_heap.top();
  std::string key = entry->key;

  if (rgw::sal::parse_block_from_cache(key).has_value()) {
    *victim = rgw::sal::parse_block_from_cache(key).value(); 
  } else {
    return -ENOENT;
  }

  /* check dirty flag of entry to be evicted, if the flag is dirty, all entries on the local node are dirty
    check refcount also, if refcount > 0 then no entries are available for eviction */
  if (entry->dirty || entry->refcount > 0) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Top entry in min heap is dirty or with positive refcount, no entry is available for eviction!" << dendl;
    return -ENOENT;
  }

  return 0;
}

int LFUDAPolicy::exist_key(const std::string& key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

int LFUDAPolicy::perform_background_eviction(const DoutPrefixProvider* dpp, uint64_t bytes_to_free, optional_yield y)
{
  uint64_t total_freed = 0;
  int ret = 0;

  // Evict in batches to avoid holding locks too long
  while (total_freed < bytes_to_free && !quit) {
    uint64_t before = cacheDriver->get_free_space(dpp, y);
    uint64_t batch_size = std::min(bytes_to_free - total_freed, EVICTION_BATCH_SIZE);

    // call eviction
    ret = eviction(dpp, batch_size, y);
    if (ret < 0) {
      //may fail due to all objects being dirty
      ldpp_dout(dpp, 5) << "Background eviction failed: " << ret << dendl;
      break;
    }

    uint64_t after = cacheDriver->get_free_space(dpp, y);
    uint64_t actually_freed = 0;
    if (after > before) {
      actually_freed = after - before;
      total_freed += actually_freed;
    }

    ldpp_dout(dpp, 20) << "Batch freed " << actually_freed << " bytes (requested " << batch_size << ")" << dendl;
    uint64_t used_space = cache_capacity - after;
    if (used_space < eviction_watermark_bytes) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                         << " used_space=" << used_space
                         << " dropped below watermark=" << eviction_watermark_bytes
                         << ", clearing flag" << dendl;
      above_watermark = false;
      break;
    }
  }
  uint64_t final_used = cache_capacity - cacheDriver->get_free_space(dpp, y);
  if (final_used < eviction_watermark_bytes) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                         << " final_used=" << final_used
                         << " dropped below watermark=" << eviction_watermark_bytes
                         << ", clearing flag" << dendl;
  }

  ldpp_dout(dpp, 10) << "Background eviction completed: freed " 
                 << (total_freed / 1024 / 1024) << "MB" << dendl;
  return ret;
}

void LFUDAPolicy::background_eviction_worker(const DoutPrefixProvider* dpp, optional_yield y)
{
  ldpp_dout(dpp, 10) << "Background eviction co-routine started" << dendl;
  int consecutive_failures = 0;
  const int MAX_BACKOFF = 5;  // Max 2^5 = 32x EVICTION_CHECK_INTERVAL
  while (!quit) {
    auto wait_duration = EVICTION_CHECK_INTERVAL;
    if (consecutive_failures > 0) {
      int multiplier = 1 << std::min(consecutive_failures, MAX_BACKOFF);
      wait_duration = EVICTION_CHECK_INTERVAL * multiplier;
      ldpp_dout(dpp, 10) << "Backing off: waiting " << wait_duration.count() << dendl;
    }
    eviction_timer->expires_after(wait_duration);
    boost::system::error_code ec;
    eviction_timer->async_wait(y.get_yield_context()[ec]);

    if (ec == boost::asio::error::operation_aborted || quit.load()) {
      break;
    }

    // Check current cache usage
    uint64_t free_space = cacheDriver->get_free_space(dpp, y);
    uint64_t used_space = (free_space < cache_capacity) ? (cache_capacity - free_space) : 0;

    ldpp_dout(dpp, 20) << "LFUDAPolicy:: " << __func__ << " cache_capacity: " << cache_capacity << dendl;
    ldpp_dout(dpp, 20) << "LFUDAPolicy:: " << __func__ << " free_space: " << free_space << dendl;
    ldpp_dout(dpp, 10) << "LFUDAPolicy:: " << __func__ << " used_space: " << used_space << dendl;

    // Only evict if above watermark
    if (used_space < eviction_watermark_bytes) {
      consecutive_failures = 0;
      continue;
    }

    above_watermark = true;

    // Calculate bytes to free (evict to TARGET_WATERMARK)
    uint64_t bytes_to_free = (used_space > target_bytes) ? (used_space - target_bytes) : 0;
    if (bytes_to_free == 0) {
      consecutive_failures = 0;
      continue;
    }

    double usage_pct = (static_cast<double>(used_space) / cache_capacity) * 100;
    ldpp_dout(dpp, 5) << "Cache at " << usage_pct 
                   << "% - evicting " << (bytes_to_free / 1024 / 1024) 
                   << "MB to reach " << TARGET_WATERMARK << dendl;

    // Perform eviction
    auto ret = perform_background_eviction(dpp, bytes_to_free, y);
    if (ret < 0) {
      consecutive_failures++;
    } else {
      consecutive_failures = 0;
    }

    //if still above watermark, trigger cleaning by waking every cleaning coroutine
    if (above_watermark) {
      wake_cleaning_timers();
    }
  }
}

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  int ret = -1;
  std::vector<LFUDAEntry> to_delete;

  // Under lfuda_lock, select victims until enough space is freed, delete them from the heap and map so that they do not get 
  // selected again by another thread when the lock is released
  {
    std::unique_lock<std::mutex> lfuda_l(lfuda_lock);

    uint64_t freed = 0;
    while (freed < size) {
	  CacheBlock victim;
	  ret = get_victim_block(dpp, &victim, y);
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
        return -ENOSPC;
      }

	  std::string victim_key = entries_heap.top()->key;
	  auto it = entries_map.find(victim_key);
	  if (it == entries_map.end()) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Could not locate victim block entry in entries_map." << dendl;
		return -ENOENT;
	  }

      uint64_t victim_size = victim.size;
      to_delete.push_back(*it->second);
      _erase(dpp, victim_key, y);
      freed += victim_size;
    }
  } // lfuda_lock released

  // Outside both locks, do expensive I/O
  for (auto& entry : to_delete) {
    rgw::d4n::CacheBlock block = rgw::sal::parse_block_from_cache(entry.key).value();

    int avgWeight;
    std::string remoteCacheAddress;
    const rgw::d4n::CacheBlock lookup_key {
      .cacheObj = {.objName = block.cacheObj.objName, .bucketName = block.cacheObj.bucketName},
      .blockID = block.blockID,
      .version = block.version,
      .size = block.size
    };
    const auto local_weight = entry.localWeight;
    const std::string local_addr = dpp->get_cct()->_conf->rgw_d4n_local_rgw_address;
    bool remote_copy_written = false;
    ret = getMinAvgWeight(dpp, &avgWeight, &remoteCacheAddress, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve min average weight." << dendl;
      return ret;
    } else if (remoteCacheAddress == dpp->get_cct()->_conf->rgw_d4n_local_rgw_address) {
      remoteCacheAddress.clear(); // evict normally without remote put 
    }

    const auto& object_name = lookup_key.cacheObj.objName;
    bufferlist out_bl;
	rgw::sal::Attrs obj_attrs;
	int ret = cacheDriver->get(dpp, entry.key, block.blockID, block.size, out_bl, obj_attrs, y);
    if (ret < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << ": Failed to retrieve victim data block from cache." << dendl;
	  return ret;
	} 

    std::string instance_id = "";
	if (obj_attrs.contains(RGW_CACHE_ATTR_VERSION_ID)) {
	  instance_id = obj_attrs.at(RGW_CACHE_ATTR_VERSION_ID).to_str();
	}

	bufferlist bl = obj_attrs[RGW_CACHE_ATTR_INVALID];
	if (!bl.length()) {
	  // we use nullptr for transaction since we don't want to do all operations in one transaction
	  if ((ret = blockDir.get(dpp, y, &block, std::nullopt)) < 0) {
		ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Unable to retrieve victim block's hostsList." << dendl;
		return ret;
	  }

	  ldpp_dout(dpp, 20) << __func__ << "(): " << __LINE__ << ": Victim host list size is " << block.cacheObj.hostsList.size() << dendl;

	  /* The following part takes care of updating the weight (globalWeight) of the block if this is the last copy in a remote setup
	   * and is pushed out to a remote cache where space is available. */
	  if (block.cacheObj.hostsList.size() == 1 && *(block.cacheObj.hostsList.begin()) == dpp->get_cct()->_conf->rgw_d4n_local_rgw_address) { // Last copy
		if (block.globalWeight) {
		  entry.localWeight += block.globalWeight;
		  block.globalWeight = 0;
		}

		if (!remoteCacheAddress.empty()) {
		  if (entry.localWeight > avgWeight) {
			rgw::d4n::RemoteCachePutOp::RemoteCachePutOpData op;
			op.bucket_name = entry.bucketName;
			op.object_name = object_name;
			op.offset = block.blockID;
			op.len = block.size;
			op.version = block.version;
			op.dirty = false;
			op.bucket_owner = entry.user;
			op.remote_addr = remoteCacheAddress;
			op.obj_size = block.cacheObj.size;
			op.instance_id = instance_id;
			// old_version left default (empty) - not applicable for eviction
			std::unique_ptr<rgw::d4n::RemoteCachePutOp> remote_put = std::make_unique<rgw::d4n::RemoteCachePutOp>(driver, op, true);
			if ((ret = remote_put->send_and_complete_request(dpp, y, &out_bl)) < 0){
			  ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << ": Sending to remote has failed: " << remoteCacheAddress << dendl;
			  return ret;
			}
			ldpp_dout(dpp, 20) << __func__ << "(): " << __LINE__ << ": Sending to remote is done." << dendl;
                        remote_copy_written = true;
		  }
		}
	  }
    }

    // Re-read ownership on every attempt; preserve replicas and metadata added during remote I/O.
    const auto update_ownership = [&](const std::optional<rgw::d4n::Transaction> txn) -> int {
      auto found = blockDir.find(dpp, y, lookup_key, txn);
      if (!found) {
        return found.error();
      }
      if (!*found) {
        return 0;
      }

      auto& current = **found;
      const bool owned_locally = 0 != current.cacheObj.hostsList.erase(local_addr);
      if (remote_copy_written) {
        current.cacheObj.hostsList.insert(remoteCacheAddress);
      }
      if (!owned_locally && !remote_copy_written) {
        return 0;
      }
      if (current.cacheObj.hostsList.empty()) {
        return blockDir.del(dpp, y, &current, txn);
      }
      if (owned_locally) {
        current.globalWeight += local_weight;
      }
      return blockDir.set(dpp, y, &current, txn);
    };

    const int update_ret = [&] {
      // Redis reads execute immediately; avoid holding a second pool checkout around them.
      if (!driver->uses_fdb()) {
        return update_ownership(std::nullopt);
      }
      // Handle FoundationDB unknown_commit: retain local data until directory completion is confirmed.
      return driver->execute_transaction(dpp, y, [&](rgw::d4n::Transaction& txn) {
        return update_ownership(txn);
      });
    }();
    if (0 > update_ret) {
      return update_ret;
    }

    if ((ret = cacheDriver->delete_data(dpp, entry.key, y)) < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << ": Failed to delete victim block from cache." << dendl;
      return ret;
    }

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << block.cacheObj.objName << " has been evicted." << dendl;

    if (perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_evictions);
    }
  }

  return 0;
}

bool LFUDAPolicy::update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y)
{
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updating refcount for entry: " << key << dendl;
  const std::lock_guard l(lfuda_lock);
  auto entry = find_entry(key);
  uint64_t refcount = 0;
  if (entry == nullptr) {
    return false;
  }
  refcount = entry->refcount;
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): old refcount is: " << refcount << dendl;
  if (op == RefCount::INCR) {
    refcount += 1;
  } else if (op == RefCount::DECR) {
    if (refcount > 0) {
      refcount -= 1;
    }
  }
  (*entry->handle)->refcount = refcount;
  entries_heap.update(entry->handle);
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << (*entry->handle)->refcount << dendl;

  return true;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, const rgw_user user, const std::string& bucketName, uint8_t op, optional_yield y, rgw::d4n::CacheBlock* block, std::string& restore_val)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updating entry: " << key << dendl;
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  bool updateLocalWeight = true, updateBucketName = true, should_notify = false;
  {
    const std::lock_guard l(lfuda_lock);
    int localWeight = age;
    auto entry = find_entry(key);
    uint64_t refcount = 0;
    if (!restore_val.empty()) {
      updateLocalWeight = false;
      updateBucketName = false;
      localWeight = std::stoi(restore_val);
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): restored localWeight is: " << localWeight << dendl;
    }

    /* check the dirty flag in the existing entry for the key and the incoming dirty flag. If the
      incoming dirty flag is false, that means update() is invoked as part of cleaning process,
      so we must not update its localWeight. */
    if (entry) {
      refcount = entry->refcount;
      if (entry->dirty && dirty.has_value()) {
        bool is_dirty = dirty.value();
        if (!is_dirty) {
          localWeight = entry->localWeight;
          updateLocalWeight = false;
        }
      }
      if (updateLocalWeight) {
        localWeight = entry->localWeight + age;
      }
      if (op == RefCount::INCR) {
        refcount += 1;
      }
      if (op == RefCount::DECR) {
        if (refcount > 0) {
          refcount -= 1;
        }
      }
    }
    //pick the existing value of dirty, if no value has been passed in
    bool is_dirty = false;
    if (dirty.has_value()) {
      is_dirty = dirty.value();
    } else if (entry) {
      is_dirty = entry->dirty;
    }
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << refcount << dendl;

    if (entry) {
      entry->key = key;
      entry->offset = offset;
      entry->len = len;
      entry->version = version;
      entry->dirty = is_dirty;
      entry->refcount = refcount;
      entry->localWeight = localWeight;
      entries_heap.update(entry->handle, entry);
      updateBucketName = false;
    } else {
      LFUDAEntry* e = new LFUDAEntry(key, offset, len, version, is_dirty, refcount, user, localWeight, bucketName);
      handle_type handle = entries_heap.push(e);
      e->set_handle(handle);
      entries_map.emplace(key, e);
    }

    if (updateLocalWeight) {
      updated_blocks.emplace(key, localWeight);
      if (updated_blocks.size() >= LOCALWEIGHT_BATCH_SIZE) {
        should_notify = true;
      }
    }

    weightSum += ((localWeight < 0) ? 0 : localWeight);
  } //lock will be released here
  /*adding bucket name as attribute as it is needed during eviction
  * for pushing the block to a remote rgw. needs to be added when the
  * entry is created initially only
  */
  if(updateBucketName) {
    if (auto ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_BUCKET_NAME, bucketName, y); ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed, ret=" << ret << dendl;
    }
  }
  if (should_notify) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): notify_one: "<< dendl;
    lw_cond.notify_one();
  }
  if (block) { // This parameter is only populated during remote get requests
    block->globalWeight += age; 
  }
}

std::string LFUDAPolicy::object_key(const LFUDAObjEntry& entry)
{
  return rgw::sal::get_cache_block_prefix(entry.bucket_id, entry.obj_key.name, {});
}

void LFUDAPolicy::queue_oldest(const ObjectVersions& versions)
{
  auto *entry = versions.begin()->second;
  entry->set_handle(object_heap.push(entry));
}

LFUDAPolicy::LFUDAObjEntry* LFUDAPolicy::create_obj_entry(const DoutPrefixProvider* dpp, const std::string& dirty_obj_key, const std::string& version,
                                              bool deleteMarker, uint64_t size, ceph::real_time creationTime,
                                              const rgw_user& user, const std::string& etag,
                                              const std::string& bucket_name, const std::string& bucket_id,
                                              const rgw_obj_key& obj_key, State state)
{
  if (o_entries_map.contains(dirty_obj_key)) {
    return nullptr;
  }
  LFUDAObjEntry* e = new LFUDAObjEntry{dirty_obj_key, version, deleteMarker, size, creationTime,
                                        user, etag, bucket_name, bucket_id, obj_key};
  o_entries_map.emplace(dirty_obj_key, std::make_pair(e, state));

  auto& versions = per_obj_versions[object_key(*e)];
  e->version_position = versions.emplace(e->creationTime, e);

  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                     << "(): created entry key=" << dirty_obj_key
                     << " obj_name=" << e->obj_key.name
                     << " creationTime=" << creationTime << dendl;
  return e;
}

void LFUDAPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, ceph::real_time creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id, const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  State state{State::INIT};
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Before acquiring lock, adding entry: " << key << dendl;

  std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
  LFUDAObjEntry* e = create_obj_entry(dpp, key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, state);
  if (nullptr == e) {
    return;
  }

  // next_retry_time is initialized to creationTime in constructor
  // cleaning() will add interval to determine when entry is ready

  auto& versions = per_obj_versions[object_key(*e)];
  if (1 == versions.size()) {
    queue_oldest(versions);
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                           << "(): added obj=" << e->obj_key.name
                           << " key=" << e->key
                           << " creationTime=" << e->creationTime
                           << dendl;
  } else {
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                           << "(): queued version for existing obj="
                           << e->obj_key.name
                           << " key=" << e->key
                           << " creationTime=" << e->creationTime
                           << " versions_count=" << versions.size()
                           << dendl;
  }
  cond->notify(l);
}

bool LFUDAPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  weightSum -= ((p->second->localWeight < 0) ? 0 : p->second->localWeight);

  entries_heap.erase(p->second->handle);
  delete p->second;
  p->second = nullptr;
  entries_map.erase(p);
  
  return true;
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  return _erase(dpp, key, y);
}

bool LFUDAPolicy::erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  std::unique_lock lock(lfuda_cleaning_lock);
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ 
                       << "(): erasing key=" << key << dendl;
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  LFUDAObjEntry* e = p->second.first;
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ 
                       << "(): obj_name=" << e->obj_key.name << dendl;
  if (auto v_it = per_obj_versions.find(object_key(*e)); v_it != per_obj_versions.end()) {
    v_it->second.erase(e->version_position);

    const auto versions_remain = !v_it->second.empty();

    if (!versions_remain) {
      per_obj_versions.erase(v_it);
    }

    if (versions_remain) {
      // Keep the next version tracked while queued or in progress. Retirement and
      // promotion share the lock so a new arrival cannot schedule it a second time.
      queue_oldest(v_it->second);
      cond->notify(lock);
    }
  }
  delete p->second.first;
  p->second.first = nullptr;
  o_entries_map.erase(p);
  state_cond.notify_one();
  return true;
}

/* This method deletes INVALID cache entries during cleaning.
   Invalid entries are dirty entries that have been marked invalid due to a delete request from the client.
   It defers the deletion time of the object, in case it is still being read (using its refcount) */
int LFUDAPolicy::do_delete(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, int interval, optional_yield y)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                      << "(): State is INVALID; deleting key=" << e->key << dendl;

  int ret;

  // Defer deletion (active GET lease or non-zero refcount) by re-queuing the entry in
  // per_obj_versions with a later creationTime, and signal cleaning() via -EALREADY.
  // The entry stays in per_obj_versions and is re-promoted when it becomes the oldest version,
  // allowing other versions to be cleaned in the meantime.
  auto defer_deletion = [&]() -> int {
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
    if (auto v_it = per_obj_versions.find(object_key(*e)); v_it != per_obj_versions.end()) {
      v_it->second.erase(e->version_position);
      e->creationTime = ceph::real_clock::now() + std::chrono::seconds(interval / 2);
      e->next_retry_time = e->creationTime;
      e->version_position = v_it->second.emplace(e->creationTime, e);
    }
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                       << "() deferring deletion due to active lease/refcount"
                       << " retry_count=" << e->retry_count
                       << " updated creationTime=" << e->creationTime
                       << " - entry remains in per_obj_versions for later promotion" << dendl;
    l.unlock();
    return -EALREADY;
  };

  // Delete the object's HEAD block directory entries. Regular objects keep their HEAD block only
  // in the directory, so these are cleaned up together with the data
  // block directory entries, before any local cache entry is removed.
  //  - main/null HEAD: deleted only if tombstoned and still referring to this version
  //  - version HEAD: deleted when it still refers to this version. A concurrent PUT invalidates the
  //    version via invalidate_dirty_object() WITHOUT tombstoning its version-specific HEAD block,
  //    so this must not be gated on the invalid flag - the specific version has been invalidated
  //    (either by the overwrite or by delete_obj) and its HEAD block must go.
  auto delete_head_blocks = [&](rgw::d4n::Transaction& txn) -> int {
    rgw::d4n::CacheBlock head_block {
      .cacheObj = {
        .objName = e->obj_key.get_oid(),
        .bucketName = e->bucket_id,
      },
      .blockID = 0,
      .size = 0,
    };

    int head_ret = blockDir.get(dpp, y, &head_block, txn);
    if (head_ret == 0 && head_block.invalid && e->version == head_block.version) {
      ret = blockDir.del(dpp, y, &head_block, txn);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "Failed to delete HEAD block for: "
                          << e->obj_key.get_oid() << ", ret=" << ret << dendl;
        return ret;
      }
    } else if (head_ret < 0 && head_ret != -ENOENT) {
      return head_ret;
    }

    if (e->obj_key.have_null_instance()) {
      rgw::d4n::CacheBlock null_head {
        .cacheObj = {
          .objName = rgw::sal::get_versioned_head_block_name("null", e->obj_key.name),
          .bucketName = e->bucket_id,
        },
        .blockID = 0,
        .size = 0,
      };

      int null_ret = blockDir.get(dpp, y, &null_head, txn);
      if (null_ret == 0 && null_head.invalid && e->version == null_head.version) {
        ret = blockDir.del(dpp, y, &null_head, txn);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "Failed to delete null HEAD block for: "
                            << e->obj_key.name << ", ret=" << ret << dendl;
          return ret;
        }
      } else if (null_ret < 0 && null_ret != -ENOENT) {
        return null_ret;
      }
    }

    rgw::d4n::CacheBlock ver_head {
      .cacheObj = {
        .objName = rgw::sal::get_versioned_head_block_name(e->version, e->obj_key.name),
        .bucketName = e->bucket_id,
      },
      .blockID = 0,
      .size = 0,
    };

    int ver_ret = blockDir.get(dpp, y, &ver_head, txn);
    if (ver_ret == 0 && e->version == ver_head.version) {
      ret = blockDir.del(dpp, y, &ver_head, txn);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "Failed to delete version-specific HEAD block for: "
                          << e->obj_key.name << ", ret=" << ret << dendl;
        return ret;
      }
    } else if (ver_ret < 0 && ver_ret != -ENOENT) {
      return ver_ret;
    }

    return 0;
  };


  if (e->delete_marker) {
    // Delete markers have a HEAD block (in both the directory and the local cache) and no data
    // blocks. Remove the directory entries first, then the local cache and policy entries, so a
    // crash mid-cleanup never leaves an orphaned directory entry.
    // Persist cleanup intent before a commit that may succeed without confirmation.
    if (const int ret = cacheDriver->set_attr(dpp, e->key, RGW_CACHE_ATTR_INVALID, "1", y);
        0 > ret && -ENOENT != ret) {
      const auto whence = std::source_location::current();
      detail::log_message(dpp, whence, "failed to preserve marker cleanup state: {}", ret);

      return ret;
    }

    const int ret_txn = this->driver->execute_transaction(dpp, y,
        [this, dpp, y, &delete_head_blocks](rgw::d4n::Transaction& txn) -> int {
          return delete_head_blocks(txn);
        });

    // Handle FoundationDB unknown_commit: retain the cached marker for cleanup recovery.
    if (0 > ret_txn) {
      return ret_txn;
    }

    ret = cacheDriver->delete_data(dpp, e->key, y);
    if (ret == 0 || ret == -ENOENT) {
      if (!(ret = erase(dpp, e->key, y))) {
        ldpp_dout(dpp, 0) << "Failed to delete policy entry for: "
                          << e->key << ", ret=" << ret << dendl;
        return -EINVAL;
      }
    } else {
      ldpp_dout(dpp, 0) << "Failed to delete head block for: "
                        << e->key << ", ret=" << ret << dendl;
      return ret;
    }

    return 0;
  }

  // ---- Regular (non delete-marker) object ----
  // Regular objects have data blocks in the local cache but no HEAD block in the cache.
  const off_t lst = e->size;
  const uint64_t chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;

  // Do not delete anything while a reader could still be using the object.
  //   (a) an active GET lease (any RGW) - defer
  //   (b) any block held locally with a non-zero refcount - defer
  // any_active() also opportunistically cleans up expired leases.
  if (lease) {
    std::string lease_prefix = rgw::sal::get_lease_resource_prefix(e->bucket_id, e->obj_key.name, e->version, "GET");
    auto lease_result = lease->any_active(dpp, lease_prefix);

    // The lease lookup failed:
    if (!lease_result) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << " lease check failed with error=" << lease_result.error()
                        << " for prefix=" << lease_prefix << " - deferring deletion" << dendl;
      return defer_deletion();  // conservative: on error, assume a lease exists
    }

    // A GET lease is active:
    if (*lease_result) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << " active GET lease exists for prefix=" << lease_prefix
                         << " - deferring deletion" << dendl;
      return defer_deletion();
    }
  }
  {
    std::unique_lock<std::mutex> ll(lfuda_lock);
    for (off_t fst = 0; fst < lst; ) {
      off_t cur_size = std::min<off_t>(fst + chunk_size, lst);
      off_t cur_len = cur_size - fst;
      std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
      auto it = entries_map.find(oid_in_cache);
      if (it != entries_map.end() && it->second->refcount > 0) {
        ll.unlock();
        return defer_deletion();
      }
      fst += cur_len;
    }
  }

  // Step 1: delete ALL directory entries (data blocks + HEAD blocks) in a single transaction,
  // while the local cache entries are still present. The local cache is the anchor used to
  // rebuild the in-memory policy on restart (see restore_blocks_objects()); deleting the
  // directory first guarantees that a crash mid-cleanup leaves cache entries that can be
  // rediscovered to finish (or repeat) the cleanup, rather than orphaned directory entries that
  // nothing knows to clean up.
  {
    std::string local_addr = dpp->get_cct()->_conf->rgw_d4n_local_rgw_address;

    const int ret_txn = this->driver->execute_transaction(dpp, y,
        [this, dpp, y, e, lst, chunk_size, &local_addr,
         &delete_head_blocks](rgw::d4n::Transaction& txn) -> int {
          for (off_t fst = 0; fst < lst; ) {
            off_t cur_size = std::min<off_t>(fst + chunk_size, lst);
            off_t cur_len = cur_size - fst;

            rgw::d4n::CacheBlock blk {
              .cacheObj = {
                .objName = e->obj_key.name,
                .bucketName = e->bucket_id,
              },
              .blockID = static_cast<uint64_t>(fst),
              .version = e->version,
              .size = static_cast<uint64_t>(cur_len),
            };

            int get_ret = blockDir.get(dpp, y, &blk, txn);
            if (get_ret == 0) {
	      if (blk.cacheObj.hostsList.contains(local_addr)) {
                if (blk.cacheObj.hostsList.size() <= 1) {
                  // Last copy: delete the entire block directory entry.
                  int del_ret = blockDir.del(dpp, y, &blk, txn);
                  if (del_ret < 0) {
                    ldpp_dout(dpp, 0) << "Failed to delete block directory entry for blockID="
                                    << fst << ", ret=" << del_ret << dendl;
                    return del_ret;
                  }
                } else if (blk.cacheObj.hostsList.contains(local_addr)) {
                  // Multiple copies: remove this host from the entry.
                  int remove_ret = blockDir.remove_host(
                      dpp, y, &blk, local_addr, txn);
                  if (remove_ret < 0) {
                    ldpp_dout(dpp, 0) << "Failed to remove host from block directory entry for blockID="
                                    << fst << ", ret=" << remove_ret << dendl;
                    return remove_ret;
                  }
                }
	      }
            } else if (get_ret != -ENOENT) {
              return get_ret;
            }

            fst += cur_len;
          }

          // Remove HEAD entries in the same transaction as the data blocks.
          return delete_head_blocks(txn);
        });

    // Handle FoundationDB unknown_commit: stop cleanup and let the caller reconcile:
    /* Deleting directory entries has failed. We should return 
    otherwise we will endup with orphan directory entries */
    if (0 > ret_txn) {
      return ret_txn;
    }

  }

  // Step 2: delete the local cache data blocks and their policy entries.
  for (off_t fst = 0; fst < lst; ) {
    off_t cur_size = std::min<off_t>(fst + chunk_size, lst);
    off_t cur_len = cur_size - fst;
    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));

    std::unique_lock<std::mutex> ll(lfuda_lock);
    auto it = entries_map.find(oid_in_cache);
    if (it != entries_map.end() && it->second->refcount > 0) {
      ll.unlock();
      return defer_deletion();
    }
    ll.unlock();

    ret = cacheDriver->delete_data(dpp, oid_in_cache, y);
    if (ret == 0 || ret == -ENOENT) {
      if (!(ret = erase(dpp, oid_in_cache, y))) {
        ldpp_dout(dpp, 0) << "Failed to delete policy entry for: " << oid_in_cache << ", ret=" << ret << dendl;
        return -EINVAL;
      }
    } else {
      ldpp_dout(dpp, 0) << "Failed to delete data block " << oid_in_cache << ", ret=" << ret << dendl;
      return ret;
    }
    fst += cur_len;
  }

  return 0;
}

/*
 * mark_data_blocks_dir_clean() - Clear the dirty flag on data-block directory entries
 *
 * Updates ONLY the SHARED block directory (FDB/Redis) dirty field to "false" for
 * every data block of the object. This is the SHARED-state half of marking an
 * object clean.
 *
 * Ordering rationale: this must run BEFORE any LOCAL cache state is touched (see
 * mark_local_blocks_clean()). If the LOCAL cache were marked clean first and RGW
 * crashed before the directory update, on restart the object would look clean
 * locally (rebuilt from the disk cache xattr), never be re-queued for cleaning,
 * yet remain dirty in the shared directory - an unrecoverable inconsistency. With
 * this order, a crash leaves the directory clean and the local cache dirty, which
 * is recoverable: the object is re-queued, the pre-flight check finds the directory
 * already clean, and mark_local_blocks_clean() reconciles the local state.
 */
int LFUDAPolicy::mark_data_blocks_dir_clean(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y,
                                            std::optional<Transaction> txn)
{
  int ret = 0;
  uint64_t chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;

  off_t lst = e->size;
  off_t fst = 0;
  while (fst < lst) {
    off_t cur_size = std::min<off_t>(fst + chunk_size, lst);
    off_t cur_len = cur_size - fst;

    rgw::d4n::CacheBlock block {
      .cacheObj = {
        .objName = e->obj_key.name,
        .bucketName = e->bucket_id,
      },
      .blockID = static_cast<uint64_t>(fst),
      .version = e->version,
      .size = static_cast<uint64_t>(cur_len),
    };
    std::string dirty = "false";
    ret = blockDir.update_field(dpp, y, &block, "dirty", dirty, txn);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty flag in block directory"
                        << " for blockID=" << fst << ", ret=" << ret << dendl;
      return ret;
    }

    fst += cur_len;
  }

  ldpp_dout(dpp, 10) << __func__ << "(): completed marking data-block directory entries clean for key=" << e->key << dendl;
  return 0;
}

/*
 * mark_local_blocks_clean() - Mark local cache blocks as clean
 *
 * Updates ONLY LOCAL state (cache xattr + in-memory metadata) to reflect that the
 * object has been written to the backend and the SHARED directory has already been
 * updated (see mark_data_blocks_dir_clean() and the HEAD-block directory updates in
 * do_writeback()).
 *
 * Ordering rationale: this must be the LAST step of a writeback. A crash before it
 * leaves the object still-dirty locally, so it is re-queued for cleaning on restart
 * and reconciled against the already-clean directory. See mark_data_blocks_dir_clean().
 *
 * Updates:
 * 1. Local cache xattr (RGW_CACHE_ATTR_DIRTY) - set to "0"
 * 2. In-memory metadata (entries_map) - via update() call
 *
 * Called from:
 * 1. Normal cleaning path - as the final step, after directory updates commit
 * 2. Pre-flight clean detection - when another RGW already cleaned the directory
 */
int LFUDAPolicy::mark_local_blocks_clean(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y)
{
  int ret = 0;
  uint64_t chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;

  // Mark all data blocks clean in LOCAL cache
  off_t lst = e->size;
  off_t fst = 0;
  while (fst < lst) {
    off_t cur_size = std::min<off_t>(fst + chunk_size, lst);
    off_t cur_len = cur_size - fst;

    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
    ldpp_dout(dpp, 20) << __func__ << "(): marking clean oid_in_cache=" << oid_in_cache << dendl;

    // Update LOCAL cache xattr
    ret = cacheDriver->set_attr(dpp, oid_in_cache, RGW_CACHE_ATTR_DIRTY, "0", y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty xattr in cache"
                        << " for oid=" << oid_in_cache << ", ret=" << ret << dendl;
      // Continue to try updating other blocks even if one fails
    }

    // Update LOCAL in-memory data structure for each block
    this->update(dpp, oid_in_cache, 0, 0, e->version, false, e->user, e->bucket_name, 0, y, nullptr);

    fst += cur_len;
  }

  // Mark HEAD block clean in LOCAL cache (only exists for delete markers)
  if (e->delete_marker) {
    ldpp_dout(dpp, 20) << __func__ << "(): marking clean HEAD block for key=" << e->key << dendl;

    // Update cache xattr
    ret = cacheDriver->set_attr(dpp, e->key, RGW_CACHE_ATTR_DIRTY, "0", y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty xattr for HEAD block"
                        << " key=" << e->key << ", ret=" << ret << dendl;
    }
    // Update in-memory metadata for head
    this->update(dpp, e->key, 0, 0, e->version, false, e->user, e->bucket_name, 0, y, nullptr);
  }

  ldpp_dout(dpp, 10) << __func__ << "(): completed marking local blocks clean for key=" << e->key << dendl;
  return 0;  // Return success even if some individual updates failed
}

std::expected<bool, int>
LFUDAPolicy::stage_writeback_directory(const DoutPrefixProvider *dpp, LFUDAObjEntry& entry,
    rgw::sal::Object& object, Transaction& txn, optional_yield y)
{
  const auto fail = [dpp](const int error, const std::string_view operation,
                          const std::source_location whence = std::source_location::current()) {
    detail::log_message(dpp, whence, "{} failed, ret={}", operation, error);
    return std::unexpected(error);
  };

  if (const int ret = mark_data_blocks_dir_clean(dpp, &entry, y, txn); 0 > ret) {
    return fail(ret, "mark data-block directory entries clean");
  }

  const auto mark_version_head_clean = [this, dpp, y, &entry, &object, &txn] {
    CacheBlock head {
      .cacheObj = {
        .objName = rgw::sal::get_versioned_head_block_name(entry.version, object.get_name()),
        .bucketName = object.get_bucket()->get_bucket_id()
      },
      .blockID = 0,
      .size = 0
    };
    const int ret = blockDir.get(dpp, y, &head, txn);
    if (-ENOENT == ret) {
      return 0;
    }
    if (0 > ret) {
      return ret;
    }
    if (entry.version != head.version) {
      return 0;
    }

    head.cacheObj.dirty = false;
    return blockDir.set(dpp, y, &head, txn);
  };

  const auto remove_version = [this, dpp, y, &entry, &object, &txn, &fail](
      const bool delete_after_commit) -> std::expected<bool, int> {
    if (const int ret = objDir.remove_version_by_creation_time(dpp, y, entry.bucket_id,
            object.get_name(), entry.creationTime, txn); 0 > ret && -ENOENT != ret) {
      return fail(ret, "remove object from ordered set");
    }
    return delete_after_commit;
  };

  CacheBlock block {};
  block.cacheObj.bucketName = entry.bucket_id;
  block.cacheObj.objName = object.get_name();
  if (!object.have_instance()) {
    if (const int ret = blockDir.get(dpp, y, &block, txn); 0 > ret) {
      return fail(ret, "get latest block directory entry");
    }

    if (block.invalid) {
      block.cacheObj.dirty = false;
      if (const int ret = blockDir.set(dpp, y, &block, txn); 0 > ret) {
        return fail(ret, "mark latest block clean");
      }

      CacheBlock null_block {
        .cacheObj = {
          .objName = rgw::sal::get_versioned_head_block_name("null", object.get_name()),
          .bucketName = entry.bucket_id
        },
        .blockID = 0,
        .size = 0
      };
      const int ret = blockDir.get(dpp, y, &null_block, txn);
      if (0 > ret && -ENOENT != ret) {
        return fail(ret, "get null-version block directory entry");
      }
      if (0 == ret && entry.version == null_block.version) {
        null_block.cacheObj.dirty = false;
        if (const int ret = blockDir.set(dpp, y, &null_block, txn); 0 > ret) {
          return fail(ret, "mark null-version block clean");
        }
      }

      if (const int ret = mark_version_head_clean(); 0 > ret) {
        return fail(ret, "mark version-specific HEAD clean");
      }
      return remove_version(true);
    }

    if (entry.version == block.version) {
      CacheBlock null_block {
        .cacheObj = {
          .objName = rgw::sal::get_versioned_head_block_name("null", object.get_name()),
          .bucketName = entry.bucket_id
        },
        .blockID = 0,
        .size = 0
      };
      const int ret = blockDir.get(dpp, y, &null_block, txn);
      if (0 > ret && -ENOENT != ret) {
        return fail(ret, "get null-version block directory entry");
      }
      if (0 == ret && entry.version == null_block.version) {
        block.cacheObj.dirty = false;
        null_block.cacheObj.dirty = false;
        if (const int ret = blockDir.set(dpp, y, &block, txn); 0 > ret) {
          return fail(ret, "mark latest block clean");
        }
        if (const int ret = blockDir.set(dpp, y, &null_block, txn); 0 > ret) {
          return fail(ret, "mark null-version block clean");
        }
      }
    }

    if (const int ret = mark_version_head_clean(); 0 > ret) {
      return fail(ret, "mark version-specific HEAD clean");
    }
    return remove_version(false);
  }

  CacheBlock instance_block {};
  instance_block.cacheObj.bucketName = entry.bucket_id;
  instance_block.cacheObj.objName = "null" == object.get_instance()
    ? rgw::sal::get_versioned_head_block_name("null", object.get_name()) : object.get_oid();
  std::string dirty = "false";
  if (const int ret = blockDir.update_field(dpp, y, &instance_block, "dirty", dirty, txn);
      0 > ret) {
    return fail(ret, "mark instance block clean");
  }
  if ("null" == object.get_instance()) {
    if (const int ret = mark_version_head_clean(); 0 > ret) {
      return fail(ret, "mark version-specific HEAD clean");
    }
  }

  CacheBlock latest_block = block;
  if (const int ret = blockDir.get(dpp, y, &latest_block, txn); 0 > ret) {
    return fail(ret, "get latest block directory entry");
  }
  if (entry.version != latest_block.version) {
    return false;
  }

  if (const int ret = blockDir.del(dpp, y, &latest_block, txn); 0 > ret) {
    return fail(ret, "delete latest block directory entry");
  }
  if (const int ret = bucketDir.remove_object(dpp, y, entry.bucket_id, object.get_name(), txn);
      0 > ret) {
    return fail(ret, "remove bucket object entry");
  }
  return remove_version(false);
}

/* As part of the cleaning process, this member function reads an object from the cache
 * and writes it to the backend store. It marks the object clean in the directory
 * It is also responsible for correctly updating the version in the directory.
*/
int LFUDAPolicy::do_writeback(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): writing back key=" << e->key << dendl;

  // Step 1: Pre-flight check - verify object is still dirty
  rgw::d4n::CacheBlock ver_head_check {
    .cacheObj = {
      .objName = rgw::sal::get_versioned_head_block_name(e->version, e->obj_key.name),
      .bucketName = e->bucket_id,
    },
    .blockID = 0,
    .size = 0,
  };

  int ret = blockDir.get(dpp, y, &ver_head_check, std::nullopt);
  if (ret == 0) {
    if (!ver_head_check.cacheObj.dirty) {
      // Object already cleaned by another RGW
      // The shared versioned HEAD block shows dirty=false, but our local cache
      // blocks may still have dirty="1" xattr. Mark them clean to maintain
      // consistency and allow eviction.
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                         << "(): object already clean (cleaned by another RGW)"
                         << ", marking local blocks clean, version=" << e->version
                         << ", key=" << e->key << dendl;

      // The cleaning RGW already updated the SHARED directory; reconcile LOCAL state only.
      mark_local_blocks_clean(dpp, e, y);

      return 0;  // Already cleaned by another RGW
    }
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): versioned HEAD not found, object may be deleted, key=" << e->key << dendl;
    return ret;
  } else {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                      << "(): Failed to get versioned HEAD, ret=" << ret << dendl;
    return ret;
  }

  // Step 2: Check if another RGW is already cleaning this object
  if (!lease) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                      << "(): Lease system not available" << dendl;
    return -EINVAL;
  }

  std::string lease_resource = rgw::sal::get_lease_resource_prefix(
    e->bucket_id,
    e->obj_key.name,
    e->version,
    "CLEAN");

  // Check for existing CLEAN lease first
  auto lease_check = lease->any_active(dpp, lease_resource);

  // The lease lookup failed:
  if (!lease_check) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                      << "(): Lease check failed, error=" << lease_check.error()
                      << " for resource=" << lease_resource << dendl;
    // On database error, be conservative: assume lease exists
    return -EEXIST;
  }

  // A CLEAN lease is active:
  if (*lease_check) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): Active CLEAN lease exists for resource=" << lease_resource
                       << " - another RGW is cleaning, key=" << e->key << dendl;
    return -EEXIST;  // Another RGW is cleaning this object
  }

  // Step 3: Try to acquire the lease (no active lease found)
  std::string local_addr = dpp->get_cct()->_conf->rgw_d4n_local_rgw_address;
  std::string lease_token = gen_rand_alphanumeric_plain(dpp->get_cct(), 16);
  uint64_t lease_ttl = rgw::sal::D4N_LEASE_TTL_NANOSECONDS;

  ret = lease->acquire(dpp, lease_resource, local_addr, lease_token, lease_ttl);
  if (ret == -EEXIST) {
    // Race condition: another RGW acquired between check and acquire
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): Race condition - another RGW acquired lease, key=" << e->key << dendl;
    return ret;
  } else if (ret < 0) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                      << "(): Failed to acquire lease, ret=" << ret << dendl;
    return ret;
  }

  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                     << "(): Acquired cleaning lease for key=" << e->key
                     << ", token=" << lease_token << dendl;

  // RAII guard to ensure lease is released on all exit paths
  auto lease_guard = make_scope_guard([&]() {
    int release_ret = lease->release(dpp, lease_resource, local_addr, lease_token);
    if (release_ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                        << "(): Failed to release lease, ret=" << release_ret << dendl;
    } else {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                         << "(): Released cleaning lease for key=" << e->key << dendl;
    }
  });

  // Step 3: Re-check if object was deleted while acquiring lease (TOCTOU protection)
  rgw::d4n::CacheBlock ver_head_recheck {
    .cacheObj = {
      .objName = rgw::sal::get_versioned_head_block_name(e->version, e->obj_key.name),
      .bucketName = e->bucket_id,
    },
    .blockID = 0,
    .size = 0,
  };

  ret = blockDir.get(dpp, y, &ver_head_recheck, std::nullopt);
  if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): Object deleted while acquiring lease (versioned HEAD gone), key=" << e->key << dendl;
    return -ECANCELED;
  } else if (ret < 0) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                      << "(): Failed to re-check versioned HEAD after acquiring lease, ret=" << ret << dendl;
    return ret;
  }

  if (ver_head_recheck.invalid) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): Object marked invalid while acquiring lease (concurrent delete), key=" << e->key
                       << " - aborting writeback" << dendl;
    return -ECANCELED;
  }

  if (!ver_head_recheck.cacheObj.dirty) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): Object marked clean while acquiring lease (concurrent writeback), key=" << e->key
                       << " - reconciling local state" << dendl;
    mark_local_blocks_clean(dpp, e, y);
    return 0;
  }

  rgw::sal::Attrs obj_attrs;
  uint64_t len = 0;
  rgw_user c_rgw_user = e->user; 
  //writing data to the backend
  //we need to create an atomic_writer
  std::unique_ptr<rgw::sal::User> c_user = driver->get_next()->get_user(c_rgw_user);

  std::unique_ptr<rgw::sal::Bucket> c_bucket;
  rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.tenant, e->bucket_name, e->bucket_id);

  RGWBucketInfo c_bucketinfo;
  c_bucketinfo.bucket = c_rgw_bucket;
  c_bucketinfo.owner = c_rgw_user;
  ret = driver->get_next()->load_bucket(dpp, c_rgw_bucket, &c_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> c_obj = c_bucket->get_object(e->obj_key);
  bool null_instance = (c_obj->get_instance() == "null");
  if (null_instance) {
    //clear the instance for backend store
    c_obj->clear_instance();
  }
  ldpp_dout(dpp, 20) << __func__ << "(): c_obj oid =" << c_obj->get_oid() << dendl;

  ACLOwner owner{c_user->get_id(), c_user->get_display_name()};

  ldpp_dout(dpp, 10) << __func__ << "(): e->key=" << e->key << dendl;
  int op_ret;
  if (e->delete_marker) {
    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = c_obj->get_delete_op();
    del_op->params.obj_owner = owner;
    del_op->params.bucket_owner = c_bucket->get_owner();
    del_op->params.versioning_status = c_bucket->get_info().versioning_status();
    //populate marker_version_id only when delete marker is not null
    if (!null_instance) {
      del_op->params.marker_version_id = e->version;
    }
    op_ret = del_op->delete_obj(dpp, y, rgw::sal::FLAG_LOG_OP);
    if (op_ret >= 0) {
      bool delete_marker = del_op->result.delete_marker;
      std::string version_id = del_op->result.version_id;
      ldpp_dout(dpp, 20) << __func__ << "delete_obj delete_marker=" << delete_marker << dendl;
      ldpp_dout(dpp, 20) << __func__ << "delete_obj version_id=" << version_id << dendl;
    } else {
      ldpp_dout(dpp, 20) << __func__ << "delete_obj returned ret=" << op_ret << dendl;
      return op_ret;
    }
  } else { //end-if delete_marker
    std::unique_ptr<rgw::sal::Writer> processor =  driver->get_next()->get_atomic_writer(dpp,
      y,
      c_obj.get(),
      owner,
      NULL,
      0,
      "");

    op_ret = processor->prepare(y);
    if (op_ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << "processor->prepare() returned ret=" << op_ret << dendl;
      return op_ret;
    }

    off_t lst = e->size;
    off_t fst = 0;
    off_t ofs = 0;

    rgw::sal::DataProcessor* filter = processor.get();
    rgw::d4n::CacheBlock block;
    block.cacheObj.objName = e->obj_key.have_null_instance()
        ? rgw::sal::get_versioned_head_block_name("null", e->obj_key.name)
        : e->obj_key.get_oid();
    block.cacheObj.bucketName = e->bucket_id;
    block.blockID = 0;
    block.size = 0;
    auto ret = blockDir.get(dpp, y, &block, std::nullopt);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() blockDir.get() failed: " << ret << dendl;
      return ret;
    }
    obj_attrs = std::move(block.cacheObj.attrs);
    obj_attrs.erase(RGW_CACHE_ATTR_MTIME);
    obj_attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    obj_attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    obj_attrs.erase(RGW_CACHE_ATTR_EPOCH);
    obj_attrs.erase(RGW_CACHE_ATTR_MULTIPART);
    obj_attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    obj_attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    obj_attrs.erase(RGW_CACHE_ATTR_LOCAL_WEIGHT);

    // Calculate number of chunks for lease renewal
    uint64_t chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;
    uint64_t num_chunks = (e->size + chunk_size - 1) / chunk_size;  // Ceiling division

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): Object size=" << e->size
                       << ", chunks=" << num_chunks << dendl;

    uint64_t renewal_count = 0;

    while (fst < lst) {
      // Renew lease for each chunk
      ret = lease->renew(dpp, lease_resource, local_addr, lease_token, lease_ttl, num_chunks);
      renewal_count++;

      if (ret == -EINVAL) {
        // max_ticks exceeded - should not happen if num_chunks calculated correctly
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                          << "(): Lease renewal max_ticks exceeded, stopping writeback" << dendl;
        return ret;
      } else if (ret == -ENOENT) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                          << "(): Lease expired or stolen, stopping writeback" << dendl;
        return ret;
      } else if (ret < 0) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__
                          << "(): Failed to renew lease, ret=" << ret << dendl;
        return ret;
      }

      ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                         << "(): Renewed lease, renewal=" << renewal_count
                         << "/" << num_chunks << dendl;

      off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
      off_t cur_len = cur_size - fst;
      std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
      ldpp_dout(dpp, 10) << __func__ << "(): oid_in_cache=" << oid_in_cache << dendl;

      ceph::bufferlist data;
      rgw::sal::Attrs attrs;
      op_ret = cacheDriver->get(dpp, oid_in_cache, 0, cur_len, data, attrs, y);
      if (op_ret < 0) {
        ldpp_dout(dpp, 20) << __func__ << "cacheDriver->get returned ret=" << op_ret << dendl;
        return op_ret;
      }
      len = data.length();
      if (std::cmp_not_equal(cur_len, len)) {
        const auto whence = std::source_location::current();
        detail::log_message(dpp, whence,
          "writeback read expected {} bytes, got {}, key={}", cur_len, len, oid_in_cache);
        return -EIO;
      }
      fst += len;

      op_ret = filter->process(std::move(data), ofs);
      if (op_ret < 0) {
        ldpp_dout(dpp, 20) << __func__ << "processor->process() returned ret=" << op_ret << dendl;
        return op_ret;
      }
      ofs += len;
    }

    op_ret = filter->process({}, ofs);
    if (op_ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << "processor->process() flush returned ret=" << op_ret << dendl;
      return op_ret;
    }

    const req_context rctx{dpp, y, nullptr};
    ceph::real_time mtime = e->creationTime;
    op_ret = processor->complete(lst, e->etag, &mtime, e->creationTime, obj_attrs,
          std::nullopt, ceph::real_time(), nullptr, nullptr,
          nullptr, nullptr, nullptr,
          rctx, rgw::sal::FLAG_LOG_OP);

    if (op_ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << "processor->complete() returned ret=" << op_ret << dendl;
      return op_ret;
    }
  } //end-else if delete_marker

  if (null_instance) {
    c_obj->set_instance("null");
  }

  bool delete_after_commit = false;
  // Preserve one directory attempt; backend work above must never be replayed here.
  const int transaction_error = driver->execute_transaction(dpp, y,
      [this, dpp, e, y, &c_obj, &delete_after_commit](Transaction& txn) {
        auto staged = stage_writeback_directory(dpp, *e, *c_obj, txn, y);
        if (!staged) {
          return staged.error();
        }
        delete_after_commit = *staged;
        return 0;
      }, 1);

  // Handle FoundationDB unknown_commit: retain dirty local state unless completion is confirmed.
  if (0 > transaction_error) {
    return transaction_error;
  }

  mark_local_blocks_clean(dpp, e, y);
  if (delete_after_commit) {
    auto del_op = c_obj->get_delete_op();
    if (const int ret = del_op->delete_obj(dpp, y, 0); 0 > ret && -ENOENT != ret) {
      const auto whence = std::source_location::current();
      detail::log_message(dpp, whence, "failed to delete from backend, ret={}", ret);
    }
  }

  return 0;
}

static bool is_transient_error(int ret)
{
  switch (-ret) {
    case ETIMEDOUT:
    case ECONNREFUSED:
    case EIO:
    case EAGAIN:
    case EEXIST:
      // Lease conflict - another RGW is cleaning this object.
      // This is transient: the other RGW will finish (success or failure),
      // and our retry will verify completion via pre-flight check.
      // If object becomes clean → pre-flight returns 0 (verified success)
      // If lease released but object still dirty → we acquire and clean
      // If still being cleaned → returns -EEXIST again → retry continues
      return true;
    case ECANCELED:
      // Concurrent delete detected during writeback - object was marked invalid.
      // Retry will transition state to INVALID and call do_delete to clean up cache blocks.
      return true;
    case ENOENT:
    case EACCES:
    case EINVAL:
    default:
      return false;
  }
}

/*
 * cleaning() - Background thread that cleans dirty objects by writing them to backend
 *
 * This method processes dirty objects from
 * the object_heap (min-heap ordered by creationTime). It handles both:
 * 1. VALID dirty objects -> writeback to backend (do_writeback)
 * 2. INVALID tombstoned objects -> delete from cache (do_delete)
 *
 * Key Features:
 * - Wait-based scheduling: Holds entries and waits for expiration/watermark
 * - Retry with deferral: Transient errors retry with time-based backoff
 * - Lease-based coordination: Prevents multiple RGWs from cleaning same object
 * - Version promotion: After cleaning one version, promotes next version to heap
 * - Watermark-aware: Can interrupt waits to clean urgently when cache is full
 *
 * State Transitions (in o_entries_map):
 *   INIT -> IN_PROGRESS -> (success) -> erased
 *                       -> (retry)   -> INIT -> back to heap
 *                       -> (failure) -> erased + added to failed_entries
 *   INVALID -> IN_PROGRESS -> (success) -> erased
 *                          -> (retry)   -> INVALID -> back to heap
 *
 * Lock Pattern:
 *   1. Acquire lock -> pop entry from heap -> release during waits
 *   2. Re-acquire lock (via continue) -> check state -> mark IN_PROGRESS -> release
 *   3. Process WITHOUT lock (do_writeback/do_delete are potentially long operations)
 *   4. Re-acquire lock for retry/failure handling
 *   5. Re-acquire lock for version promotion
 *
 */
void LFUDAPolicy::cleaning(const DoutPrefixProvider* dpp, optional_yield y)
{
  // Entry is held across loop iterations to implement wait-based scheduling:
  // - Pop entry once, hold it during wait, then process it
  // - Prevents other threads from picking the same entry
  // - Reset to nullptr after processing (success/failure/retry)
  std::optional<LFUDAObjEntry*> e;

  // Per-coroutine timer used for the wait-based scheduling below. Registered in watermark_timers so the eviction worker / shutdown
  // can wake this coroutine early via wake_cleaning_timers(). shared_ptr ensures the
  // timer outlives any in-flight posted cancel, even after this coroutine deregisters.
  auto timer = std::make_shared<boost::asio::steady_timer>(y.get_yield_context().get_executor());
  {
    std::lock_guard<std::mutex> l(watermark_timers_lock);
    watermark_timers.push_back(timer);
  }
  auto timer_guard = make_scope_guard([this, &timer] {
    std::lock_guard<std::mutex> l(watermark_timers_lock);
    std::erase(watermark_timers, timer);
  });

  while(!quit) {
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;

    // Track whether this entry is tombstoned (invalid=true means do_delete, false means do_writeback)
    bool invalid = false;

    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "" << __LINE__ << "(): Before acquiring cleaning-lock" << dendl;

    // Acquire lock for heap and state operations
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);

    // Pop entry from heap if we don't already have one
    // (we hold entry across iterations during wait periods)
    if (!e.has_value()) {
      // Wait for heap to be non-empty
      while (!quit && object_heap.empty()) {
        boost::system::error_code ec;
        cond->async_wait(l, y.get_yield_context()[ec]);
      }
      if (quit) break;

      // Pop entry with earliest next_retry_time (min-heap ordered by next_retry_time)
      // next_retry_time is initialized to creationTime, so this gives us age-based ordering
      // For retried entries, next_retry_time is set to future time for deferral
      e = object_heap.top();
      object_heap.pop();
    }

    // State variable for retry logic - tracks whether entry is INIT or INVALID
    // This is set to match the entry's state and is used to restore state on retry
    State s = State::INIT;

    // Debug logging for entry details
    ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): e->key=" << (*e)->key << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->delete_marker=" << (*e)->delete_marker << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->version=" << (*e)->version << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->bucket_name=" << (*e)->bucket_name << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->bucket_id=" << (*e)->bucket_id << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->user=" << (*e)->user << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->obj_key=" << (*e)->obj_key << dendl;

    // STEP 1: Check if entry is ready to process
    // For new entries (retry_count == 0): next_retry_time = creationTime, ready when now >= creationTime + interval
    // For retried entries (retry_count > 0): next_retry_time = target retry time, ready when now >= next_retry_time
    auto now = ceph::real_clock::now();
    const int interval = dpp->get_cct()->_conf->rgw_d4n_cache_cleaning_interval;

    // Note: age is calculated from creationTime (actual time since object became dirty)
    // for informational/logging purposes only. Scheduling decision uses next_retry_time + interval.
    int age = std::chrono::duration_cast<std::chrono::milliseconds>(now - (*e)->creationTime).count();

    // Calculate target time when entry should be processed
    ceph::real_time target_time;
    if ((*e)->retry_count == 0) {
      // New entry: ready after cleaning interval from creation
      target_time = (*e)->next_retry_time + std::chrono::seconds(interval);
    } else {
      // Retried entry: next_retry_time already set to target retry time
      target_time = (*e)->next_retry_time;
    }

    bool entry_ready = (now >= target_time) || above_watermark;

    // If entry not ready AND cache not full, wait for one of:
    // - target_time to arrive
    // - Watermark exceeded (cache full, need urgent cleaning)
    // - Quit signal
    if (!entry_ready) {
      auto wait_duration = std::chrono::ceil<std::chrono::milliseconds>(
          target_time - now).count();
      ldpp_dout(dpp, 10) << __LINE__ << " " << __func__
                         << "(): entry not ready and below watermark, waiting on=" << (*e)->key
                         << " age=" << age << "ms"
                         << " retry_count=" << (*e)->retry_count
                         << " target_time=" << target_time
                         << " wait_ms=" << wait_duration << dendl;
      l.unlock();
      while (!quit && !above_watermark) {
        now = ceph::real_clock::now();
        if (now >= target_time) break;  // Target time reached - proceed to clean

        // Sleep until target_time or watermark exceeds
        // Note: watermark check in loop condition allows early wakeup for urgent cleaning
        // Round up so the wait always reaches/passes target_time: a truncating cast
        // to seconds would floor sub-second remainders to 0, firing the timer
        // immediately and busy-spinning until the target second ticks over.
        auto remaining = std::chrono::ceil<std::chrono::milliseconds>(target_time - now);
        boost::system::error_code ec;
        timer->expires_after(remaining);
        timer->async_wait(y.get_yield_context()[ec]);
      }
      continue;
    }
    // STEP 2: Entry is ready to process (next_retry_time arrived or watermark exceeded)
    if (!(*e)->key.empty()) { // Sanity check - should always be true
      ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): entry ready to process= " << (*e)->key
                         << " age=" << age << "s" << dendl;

      // Validate entry still exists and check its state
      auto p = o_entries_map.find((*e)->key);
      if (p == o_entries_map.end()) {
        e.reset();
        l.unlock();
        continue;
      }

      // Check state to determine processing type
      if (p->second.second == State::INVALID) {
        // Entry is tombstoned (deleted object) - needs cache cleanup (do_delete)
        invalid = true;
      } else if (p->second.second == State::IN_PROGRESS) {
        // NOTE: This should NEVER happen because entry was popped from heap
        // (only cleaning threads pop from heap, and entry is held in 'e')
        e.reset();
        l.unlock();
        continue;
      }

      p->second.second = State::IN_PROGRESS;

      // Release lock BEFORE processing - do_delete/do_writeback are potentially long operations:
      // - do_writeback: fetch bucket, acquire lease, read/write chunks, backend PUT
      // - do_delete: check GET leases, delete data blocks, delete HEAD blocks
      // Holding lock during these would block all cache operations for seconds/minutes
      l.unlock();

      // STEP 3: Process entry (WITHOUT holding lock)
      int ret = 0;
      if (invalid) {
        // Tombstoned entry (object was deleted) - remove from cache
        s = State::INVALID;
        ret = do_delete(dpp, *e, interval, y);
      } else {
        // Dirty entry (object data cached but not on backend) - writeback to backend
        s = State::INIT;
        ret = do_writeback(dpp, *e, y);
      }

      // STEP 4: Handle processing result
      // Special case: -EALREADY means entry was deferred by do_delete()
      // Entry moved to later creationTime in per_obj_versions - allow version promotion
      if (ret == -EALREADY) {
        std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
        ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                            << "() entry deferred (active lease), moved in per_obj_versions"
                            << dendl;
        // Reset state from IN_PROGRESS back to INVALID so it can be re-processed when promoted
        auto p = o_entries_map.find((*e)->key);
        if (p != o_entries_map.end()) {
          p->second.second = s;  // Back to INVALID
        }
        queue_oldest(per_obj_versions.at(object_key(**e)));
        cond->notify(l);
        e.reset();
        continue;
      } else {
        // ret < 0 (error) or ret == 0 (success)
        if (ret < 0) {
          // Processing failed - determine if we should retry or mark as permanently failed

          if (is_transient_error(ret) && (*e)->retry_count < MAX_CLEANING_RETRY) {
            // STEP 4a: Transient error - retry with deferral
            std::unique_lock<std::mutex> retry_lock(lfuda_cleaning_lock);

            // Reset entry state from IN_PROGRESS back to INIT/INVALID
            // This allows the entry to be processed again when retried
            // State transition: IN_PROGRESS -> INIT/INVALID (allows re-processing)
            auto p = o_entries_map.find((*e)->key);
            if (p != o_entries_map.end()) {
              // Special case: -ECANCELED means concurrent delete detected during writeback
              // Transition to INVALID so retry calls do_delete to clean up cache blocks
              if (ret == -ECANCELED) {
                p->second.second = State::INVALID;
                ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                                   << "() transitioning to INVALID state after -ECANCELED, retry will call do_delete"
                                   << " key=" << (*e)->key << dendl;
              } else {
                p->second.second = s;  // s is INIT (for writeback) or INVALID (for delete)
              }
            }

            // Set retry deferral time to prevent tight retry loop on small heaps
            // Why use next_retry_time instead of modifying creationTime?
            // - creationTime is used as map key in per_obj_versions
            // - Modifying it would reorder versions incorrectly
            (*e)->next_retry_time = ceph::real_clock::now() + std::chrono::seconds(interval / 2);

            // Re-queue entry for retry
            (*e)->retry_count++;
            ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                               << "() deferring cleaning retry due to transient error ret=" << ret
                               << " retry_count=" << (*e)->retry_count
                               << " next_retry_time=" << (*e)->next_retry_time << dendl;

            auto handle = object_heap.push(*e);
            (*e)->set_handle(handle);
            cond->notify(retry_lock);

            // Reset e so it will be re-popped from heap on next iteration
            // The retry deferral wait (STEP 2a) will ensure we don't process it too soon
            e.reset();
            continue;

          } else {
            // STEP 4b: Permanent failure or retry limit exceeded
            // Action: Save to failed_entries for manual review/debugging
            // Entry will still be removed from active tracking (erased below)

            std::unique_lock<std::mutex> fail_lock(lfuda_cleaning_lock);
            failed_entries.push_back(*(*e));
            // An unconfirmed marker cleanup retains its invalid cached HEAD for restart recovery.
            // Note: Entry falls through to erase_dirty_object below
          }
        }
        // Retire this entry and promote the next version under the same lock.
        erase_dirty_object(dpp, (*e)->key, y);

        // Clear our local reference since entry is now deleted
        e.reset();
      }
    } // end-if !e->key.empty()
  } // end-while !quit
}

void LFUDAPolicy::localweight_writer(const DoutPrefixProvider* dpp)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Starting thread " << dendl;
  auto TIMEOUT_DURATION = std::chrono::seconds(dpp->get_cct()->_conf->rgw_d4n_localweight_processing_interval);
  while (!lw_quit.load()) {
    std::unordered_map<std::string, uint64_t> temp;
    bool woke_up = false;
    //sleep for some duration or till size crosses 10K before processing
    {
      std::unique_lock<std::mutex> wait_lock(lfuda_lock);
      woke_up = lw_cond.wait_for(wait_lock, TIMEOUT_DURATION, [this] {
                return updated_blocks.size() >= LOCALWEIGHT_BATCH_SIZE || lw_quit.load();
      });
      if (lw_quit.load()) {
          ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Quit signal received, exiting" << dendl;
          break;
      }
      if (!updated_blocks.empty()) {
          updated_blocks.swap(temp);
          if (woke_up) {
              ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Woke up due to size threshold, processing " << temp.size() << " items" << dendl;
          } else {
              ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Woke up due to timeout, processing " << temp.size() << " items" << dendl;
          }
      }
    } //lock released here
    if (!temp.empty()) {
      ldpp_dout(dpp, 5) << "LFUDAPolicy::" << __func__ << "(): Processing batch of " << temp.size() << " items" << dendl;
      for (auto& it : temp) {
        if (lw_quit.load()) {
          ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Quit signal received, exiting" << lw_quit << dendl;
          break;
        }
        auto& key = it.first;
        auto localWeight = it.second;
        ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method called for key: " << key << dendl;
        int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_LOCAL_WEIGHT, std::to_string(localWeight), y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed, ret=" << ret << dendl;
        }
      } //end-for
      ldpp_dout(dpp, 5) << "LFUDAPolicy::" << __func__ << "(): Finished processing batch" << dendl;
    } //end-if
  }//end-while
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Thread exiting" << dendl;
}

int LRUPolicy::exist_key(const std::string& key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  uint64_t freeSpace = cacheDriver->get_free_space(dpp, y);

  while (freeSpace < size) {
    auto p = entries_lru_list.front();
    entries_map.erase(entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    auto ret = cacheDriver->delete_data(dpp, p.key, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to delete data from the cache backend, ret=" << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp, y);
  }

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, const rgw_user user, const std::string& bucketName, uint8_t op, optional_yield y, rgw::d4n::CacheBlock* block, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  bool is_dirty = false;
  if (dirty.has_value()) {
    is_dirty = dirty.value();
  }
  Entry* e = new Entry(key, offset, len, version, is_dirty, 0, user, bucketName);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void LRUPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, ceph::real_time creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  ObjEntry* e = new ObjEntry(key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key);
  o_entries_map.emplace(key, e);
  return;
}


bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  return _erase(dpp, key, y);
}

bool LRUPolicy::erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }
  o_entries_map.erase(p);
  return true;
}

bool LRUPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}


} // namespace rgw::d4n
