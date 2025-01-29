#include "d4n_policy.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/split.h"
#include "rgw_perf_counters.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = asio::consign(std::move(handler), conn);
    return asio::dispatch(get_executor(),
        [c=conn, &req, &resp, h=std::move(h)] () mutable {
          c->async_exec(req, resp, std::move(h));
        });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename... Types>
static inline void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int LFUDAPolicy::init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) {
  response<int, int, int, int> resp;
  static auto obj_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    double creationTime, const rgw_user user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, optional_yield y, std::string& restore_val) {
    update_dirty_object(dpp, key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, RefCount::NOOP, y, restore_val);
  };

  static auto block_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, optional_yield y, std::string& restore_val) {
    update(dpp, key, offset, len, version, dirty, RefCount::NOOP, y, restore_val);
  };

  cacheDriver->restore_blocks_objects(dpp, obj_callback, block_callback);

  driver = _driver;
  if (dpp->get_cct()->_conf->d4n_writecache_enabled) {
    quit = false;
    tc = std::thread(&CachePolicy::cleaning, this, dpp);
  }

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>>
    > resp;
    request req;
    req.push("MULTI");
    req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum), /* New cache node will always have the minimum average weight */
              "minLocalWeights_size", std::to_string(entries_map.size()), 
              "minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);
    req.push("HSETNX", "lfuda", "age", age); /* Only set maximum age if it doesn't exist */
    req.push("EXEC");
  
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  asio::co_spawn(io_context.get_executor(),
		   redis_sync(dpp, y), asio::detached);

  return 0;
}

int LFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  response< std::optional<std::string> > resp;

  try { 
    boost::system::error_code ec;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    return -EINVAL;
  }

  if (std::get<0>(resp).value().value().empty() || age > std::stoi(std::get<0>(resp).value().value())) { /* Set new maximum age */
    try { 
      boost::system::error_code ec;
      response<ignore_t> ret;
      request req;
      req.push("HSET", "lfuda", "age", age);

      redis_exec(conn, ec, req, ret, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  } else {
    age = std::stoi(std::get<0>(resp).value().value());
  }

  return 0;
}

int LFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {
    response<std::vector<std::string>> resp;

    try { 
      boost::system::error_code ec;
      request req;
      req.push("HMGET", "lfuda", "minLocalWeights_sum", "minLocalWeights_size");
	
      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  
    float minAvgWeight = std::stof(std::get<0>(resp).value()[0]) / std::stof(std::get<0>(resp).value()[1]);
    float localAvgWeight = 0;
    if (entries_map.size())
      localAvgWeight = static_cast<float>(weightSum) / static_cast<float>(entries_map.size());

    if (localAvgWeight < minAvgWeight) { /* Set new minimum weight */
      try { 
	boost::system::error_code ec;
	response<ignore_t> resp;
	request req;
	req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum), 
                  "minLocalWeights_size", std::to_string(entries_map.size()), 
                  "minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      } catch (std::exception &e) {
	return -EINVAL;
      }
    } else {
      weightSum = std::stoi(std::get<0>(resp).value()[0]);
      postedSum = std::stoi(std::get<0>(resp).value()[0]);
    }
  }

  try { /* Post update for local cache */
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push("HSET", dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, "avgLocalWeight_sum", std::to_string(weightSum), 
              "avgLocalWeight_size", std::to_string(entries_map.size()));

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    return 0;
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

asio::awaitable<void> LFUDAPolicy::redis_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  rthread_timer.emplace(co_await asio::this_coro::executor);
  co_await asio::this_coro::throw_if_cancelled(true);
  co_await asio::this_coro::reset_cancellation_state(
    asio::enable_terminal_cancellation());

  for (;;) try {
    /* Update age */
    if (int ret = age_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ret << dendl;
    }
    
    /* Update minimum local weight sum */
    if (int ret = local_weight_sync(dpp, y) < 0) {
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

/* Changes state to INVALID for dirty objects. An INVALID state indicates that a delete request has been
 issued on an object and it must be deleted rather than written to the backend. This lazy deletion occurs
 in the Cleaning method and prevents data races during concurrent requests. The method below returns "false"
 if the state has not been set to INVALID, and "true" if it has. The state is not set to INVALID when
 cleaning is in progress, a process which writes the object to the backend store. */
bool LFUDAPolicy::invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) {
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
    int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_INVALID, "1", y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Failed to set xattr, ret=" << ret << dendl;
      return false;
    }
    return true;
  } else if (p->second.second == State::IN_PROGRESS) {
    state_cond.wait(l, [this, &key]{ return (o_entries_map.find(key) == o_entries_map.end()); });
  }

  return false;
}

CacheBlock* LFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) {
  if (entries_heap.empty())
    return nullptr;

  /* Get victim cache block */
  LFUDAEntry* entry = entries_heap.top();
  std::string key = entry->key;
  CacheBlock* victim = new CacheBlock();

  auto parts = split(key, "#");
  std::vector<std::string> block_info;
  block_info.assign(parts.begin(), parts.end());
  
  if (block_info.size() != 5) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Key of the top entry in the min heap has not been constructed correctly." << dendl;
    return nullptr;
  }

  victim->cacheObj.bucketName = block_info[0]; 
  victim->version = block_info[1]; 
  victim->cacheObj.objName = block_info[2]; 
  victim->blockID = std::stoull(block_info[3]); 
  victim->size = std::stoull(block_info[4]); 

  /* check dirty flag of entry to be evicted, if the flag is dirty, all entries on the local node are dirty
    check refcount also, if refcount > 0 then no entries are available for eviction */
  if (entry->dirty || entry->refcount > 0) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Top entry in min heap is dirty or with positive refcount, no entry is available for eviction!" << dendl;
    return nullptr;
  }

  return victim;
}

int LFUDAPolicy::exist_key(const std::string& key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  int ret = -1;
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) { // TODO: Think about parallel reads and writes; can this turn into an infinite loop?
    std::unique_lock<std::mutex> l(lfuda_lock);
    CacheBlock* victim = get_victim_block(dpp, y);
  
    if (victim == nullptr) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
      delete victim;
      l.unlock();
      return -ENOSPC;
    }

    std::string key = entries_heap.top()->key;
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      delete victim;
      l.unlock();
      return -ENOENT;
    }

    int avgWeight = weightSum / entries_map.size();
    /* the following part takes care of updating the weight (globalWeight) of the block if this is the last copy in a remote setup
       and is pushed out to a remote cache where space is available */
#if 0
    if (victim->cacheObj.hostsList.size() == 1 && *(victim->cacheObj.hostsList.begin()) == dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address) { /* Last copy */
      if (victim->globalWeight) {
	it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight = it->second->localWeight;
	entries_heap.decrease(it->second->handle); // larger value means node must be decreased to maintain min heap 
	if ((ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_LOCAL_WEIGHT, std::to_string(it->second->localWeight), y)) < 0) { 
	  delete victim;
	  return ret;
        }

	victim->globalWeight = 0;
      }

      if (it->second->localWeight > avgWeight) {
	// TODO: push victim block to remote cache
	// add remote cache host to host list
      }
    }

    victim->globalWeight += it->second->localWeight;
    if ((ret = blockDir->update_field(dpp, victim, "globalWeight", std::to_string(victim->globalWeight), y)) < 0) {
      delete victim;
      return ret;
    }
#endif
    //erase also updates weightSum, is the following needed?
    weightSum = (avgWeight * entries_map.size()) - it->second->localWeight;

    age = std::max(it->second->localWeight, age);
    _erase(dpp, key, y);

    l.unlock();

    //Need to get and then update the host atomically in a remote setup
    if ((ret = blockDir->remove_host(dpp, victim, dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, y)) < 0) {
      delete victim;
      return ret;
    }

    delete victim;

    if ((ret = cacheDriver->delete_data(dpp, key, y)) < 0) {
      return ret;
    }

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    if (perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_evictions);
    }
    freeSpace = cacheDriver->get_free_space(dpp);
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
  }
  if (op == RefCount::DECR) {
    if (refcount > 1) {
      refcount -= 1;
    }
  }
  (*entry->handle)->refcount = refcount;
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << (*entry->handle)->refcount << dendl;
	entries_heap.update(entry->handle);

  return true;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint8_t op, optional_yield y, std::string& restore_val)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updating entry: " << key << dendl;
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  const std::lock_guard l(lfuda_lock);
  int localWeight = age;
  auto entry = find_entry(key);
  bool updateLocalWeight = true;
  uint64_t refcount = 0;

  if (!restore_val.empty()) {
    updateLocalWeight = false;
    localWeight = std::stoull(restore_val);
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): restored localWeight is: " << localWeight << dendl;
  }

  /* check the dirty flag in the existing entry for the key and the incoming dirty flag. If the
     incoming dirty flag is false, that means update() is invoked as part of cleaning process,
     so we must not update its localWeight. */
  if (entry != nullptr) {
    refcount = entry->refcount;
    if ((entry->dirty && !dirty)) {
      localWeight = entry->localWeight;
      updateLocalWeight = false;
    } else {
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
  _erase(dpp, key, y);
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << refcount << dendl;
  LFUDAEntry* e = new LFUDAEntry(key, offset, len, version, dirty, refcount, localWeight);
  handle_type handle = entries_heap.push(e);
  e->set_handle(handle);
  entries_map.emplace(key, e);

  if (updateLocalWeight) {
    int ret = -1;
    if ((ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_LOCAL_WEIGHT, std::to_string(localWeight), y)) < 0) 
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed, ret=" << ret << dendl;
  }

  weightSum += ((localWeight < 0) ? 0 : localWeight);
}

void LFUDAPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id, const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>::handle_type;
  State state{State::INIT};
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Before acquiring lock, adding entry: " << key << dendl;

  if (!restore_val.empty() && restore_val == "1") { // No need to set the xattr because this case only occurs when the state has
    state = State::INVALID;                         // been retrieved from the xattr itself.
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): State restored to INVALID." << dendl;
  } else {
    state = State::INIT;
  }

  const std::lock_guard l(lfuda_cleaning_lock);
  LFUDAObjEntry* e = new LFUDAObjEntry{key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key};
  handle_type handle = object_heap.push(e);
  e->set_handle(handle);
  o_entries_map.emplace(key, std::make_pair(e, state));
  cond.notify_one();
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
  const std::lock_guard l(lfuda_cleaning_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  object_heap.erase(p->second.first->handle);
  delete p->second.first;
  p->second.first = nullptr;
  o_entries_map.erase(p);
  state_cond.notify_one();

  return true;
}

int LFUDAPolicy::delete_data_blocks(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y) {
  off_t lst = e->size, fst = 0;

  do {
    if (fst >= lst) {
      break;
    }
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;
    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));

    int ret = -1;
    std::unique_lock<std::mutex> ll(lfuda_lock);
    auto it = entries_map.find(oid_in_cache);
    if (it != entries_map.end()) {
      if (it->second->refcount > 0) {
        return -EBUSY;//better error code?
      }
    }
    ll.unlock();
    if ((ret = cacheDriver->delete_data(dpp, oid_in_cache, y)) == 0) {
      if (!(ret = erase(dpp, oid_in_cache, y))) {
	ldpp_dout(dpp, 0) << "Failed to delete policy entry for: " << oid_in_cache << ", ret=" << ret << dendl;
        return -EINVAL;
      }
    } else {
      ldpp_dout(dpp, 0) << "Failed to delete data block " << oid_in_cache << ", ret=" << ret << dendl;
      return -EINVAL;
    }

    fst += cur_len;
  } while (fst < lst);

  return 0;
}

void LFUDAPolicy::cleaning(const DoutPrefixProvider* dpp)
{
  const int interval = dpp->get_cct()->_conf->rgw_d4n_cache_cleaning_interval;
  while(!quit) {
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;
    uint64_t len = 0;
    rgw::sal::Attrs obj_attrs;
    bool invalid = false;
  
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "" << __LINE__ << "(): Before acquiring cleaning-lock" << dendl;
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
    LFUDAObjEntry* e;
    if (object_heap.size() > 0) {
      e = object_heap.top();
    } else {
      cond.wait(l, [this]{ return (!object_heap.empty() || quit); });
      continue;
    }
    ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): e->key=" << e->key << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->delete_marker=" << e->delete_marker << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->version=" << e->version << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->bucket_name=" << e->bucket_name << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->bucket_id=" << e->bucket_id << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->user=" << e->user << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->obj_key=" << e->obj_key << dendl;
    l.unlock();

    int diff = std::difftime(time(NULL), e->creationTime);
    if (!e->key.empty() && (diff > interval)) { // if block is dirty and written more than interval seconds ago
      l.lock();
      auto p = o_entries_map.find(e->key);
      if (p == o_entries_map.end()) {
	l.unlock();
	continue;
      }
      if (p->second.second == State::INVALID) {
	invalid = true;
      } else {
        p->second.second = State::IN_PROGRESS;
      }
      l.unlock();
      
      // If the state is invalid, the blocks must be deleted from the cache rather than written to the backend.
      if (invalid) {
	ldpp_dout(dpp, 10) << __func__ << "(): State is INVALID; deleting object." << dendl;
	int ret = -1;
	//check if key exists and get the refcount of block, if greater than zero then modify the creationTime of dirty object to attempt to delete later
	std::unique_lock<std::mutex> ll(lfuda_lock);
	auto it = entries_map.find(e->key);
	if (it != entries_map.end()) {
	  if (it->second->refcount > 0) {
	    l.lock();
	    //deferring the deletion of the invalid object
	    (*e->handle)->creationTime = (*e->handle)->creationTime + interval/2;
	    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated creation time is: " << (*e->handle)->creationTime << dendl;
	    object_heap.update(e->handle);
	    l.unlock();
	    continue;
	  }
	  ll.unlock();
	  if ((ret = cacheDriver->delete_data(dpp, e->key, y)) == 0) {
	    if (!(ret = erase(dpp, e->key, y))) {
	      ldpp_dout(dpp, 0) << "Failed to delete head policy entry for: " << e->key << ", ret=" << ret << dendl; // TODO: what must occur during failure?
	    }
	  } else {
	    ldpp_dout(dpp, 0) << "Failed to delete head object for: " << e->key << ", ret=" << ret << dendl;
	  }
	} else {
	  //ignore if block not found, as it could have been deleted earlier when refcount for it was 0
	  ll.unlock();
	}

	if (!e->delete_marker) {
	  ret = delete_data_blocks(dpp, e, y);
	  if (ret == 0) {
	    erase_dirty_object(dpp, e->key, null_yield);
	  } else if (ret == -EBUSY) {
	    l.lock();
	    //deferring the deletion of the invalid object
	    (*e->handle)->creationTime = (*e->handle)->creationTime + interval/2;
	    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated creation time is: " << (*e->handle)->creationTime << dendl;
	    object_heap.update(e->handle);
	    l.unlock();
	    continue;
	  } else {
	    ldpp_dout(dpp, 0) << "Failed to delete blocks for: " << e->key << ", ret=" << ret << dendl;
	  }
	}
      } else {
	rgw_user c_rgw_user = e->user; 
	//writing data to the backend
	//we need to create an atomic_writer
	std::unique_ptr<rgw::sal::User> c_user = driver->get_user(c_rgw_user);

	std::unique_ptr<rgw::sal::Bucket> c_bucket;
	rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.tenant, e->bucket_name, e->bucket_id);

	RGWBucketInfo c_bucketinfo;
	c_bucketinfo.bucket = c_rgw_bucket;
	c_bucketinfo.owner = c_rgw_user;
	int ret = driver->load_bucket(dpp, c_rgw_bucket, &c_bucket, null_yield);
	if (ret < 0) {
	  ldpp_dout(dpp, 10) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
	  //Remove bucket should be implemented in d4n which will take care of deleting objects belonging to the bucket, and hence we should not reach here
	  erase_dirty_object(dpp, e->key, null_yield);
	  continue;
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
	  bool null_delete_marker = (c_obj->get_instance() == "null");
	  if (null_delete_marker) {
	    //clear the instance for backend store
	    c_obj->clear_instance();
	  }
	  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = c_obj->get_delete_op();
	  del_op->params.obj_owner = owner;
	  del_op->params.bucket_owner = c_bucket->get_owner();
	  del_op->params.versioning_status = c_bucket->get_info().versioning_status();
	  //populate marker_version_id only when delete marker is not null
	  del_op->params.marker_version_id = e->version;
	  op_ret = del_op->delete_obj(dpp, null_yield, rgw::sal::FLAG_LOG_OP);
	  if (op_ret >= 0) {
	    bool delete_marker = del_op->result.delete_marker;
	    std::string version_id = del_op->result.version_id;
	    ldpp_dout(dpp, 20) << __func__ << "delete_obj delete_marker=" << delete_marker << dendl;
	    ldpp_dout(dpp, 20) << __func__ << "delete_obj version_id=" << version_id << dendl;
	  } else {
	    ldpp_dout(dpp, 20) << __func__ << "delete_obj returned ret=" << op_ret << dendl;
	    erase_dirty_object(dpp, e->key, null_yield);
	    continue;
	  }
	  if (null_delete_marker) {
	    //restore instance for directory data processing in later steps
	    c_obj->set_instance("null");
	  }
	} else { //end-if delete_marker

	  std::unique_ptr<rgw::sal::Writer> processor =  driver->get_atomic_writer(dpp,
		  null_yield,
		  c_obj.get(),
		  owner,
		  NULL,
		  0,
		  "");

	  op_ret = processor->prepare(null_yield);
	  if (op_ret < 0) {
	    ldpp_dout(dpp, 20) << __func__ << "processor->prepare() returned ret=" << op_ret << dendl;
	    erase_dirty_object(dpp, e->key, null_yield);
	    continue;
	  }

	  off_t lst = e->size;
	  off_t fst = 0;
	  off_t ofs = 0;

	  rgw::sal::DataProcessor* filter = processor.get();
	  bufferlist bl;
	  op_ret = cacheDriver->get_attrs(dpp, e->key, obj_attrs, null_yield); //get obj attrs from head
	  if (op_ret < 0) {
	    ldpp_dout(dpp, 20) << __func__ << "cacheDriver->get_attrs returned ret=" << op_ret << dendl;
	    erase_dirty_object(dpp, e->key, null_yield);
	    continue;
	  }
	  obj_attrs.erase(RGW_CACHE_ATTR_MTIME);
	  obj_attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
	  obj_attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
	  obj_attrs.erase(RGW_CACHE_ATTR_EPOCH);
	  obj_attrs.erase(RGW_CACHE_ATTR_MULTIPART);
	  obj_attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
	  obj_attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
	  obj_attrs.erase(RGW_CACHE_ATTR_LOCAL_WEIGHT);

	  do {
	    ceph::bufferlist data;
	    if (fst >= lst){
	      break;
	    }
	    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
	    off_t cur_len = cur_size - fst;
	    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
	    ldpp_dout(dpp, 10) << __func__ << "(): oid_in_cache=" << oid_in_cache << dendl;
	    rgw::sal::Attrs attrs;
	    cacheDriver->get(dpp, oid_in_cache, 0, cur_len, data, attrs, null_yield);
	    if (op_ret < 0) {
	      ldpp_dout(dpp, 20) << __func__ << "cacheDriver->get returned ret=" << op_ret << dendl;
	      erase_dirty_object(dpp, e->key, null_yield);
	      continue;
	    }
	    len = data.length();
	    fst += len;

	    if (len == 0) {
	      // TODO: if len of any block is 0 for some reason, we must return from here?
	      break;
	    }

	    op_ret = filter->process(std::move(data), ofs);
	    if (op_ret < 0) {
	      ldpp_dout(dpp, 20) << __func__ << "processor->process() returned ret=" << op_ret << dendl;
	      erase_dirty_object(dpp, e->key, null_yield);
	      continue;
	    }

	    ofs += len;
	  } while (len > 0);

	  op_ret = filter->process({}, ofs);

	  const req_context rctx{dpp, null_yield, nullptr};
	  ceph::real_time mtime = ceph::real_clock::from_time_t(e->creationTime);
	  op_ret = processor->complete(lst, e->etag, &mtime, ceph::real_clock::from_time_t(e->creationTime), obj_attrs,
				  std::nullopt, ceph::real_time(), nullptr, nullptr,
				  nullptr, nullptr, nullptr,
				  rctx, rgw::sal::FLAG_LOG_OP);

	  if (op_ret < 0) {
	    ldpp_dout(dpp, 20) << __func__ << "processor->complete() returned ret=" << op_ret << dendl;
	    erase_dirty_object(dpp, e->key, null_yield);
	    continue;
	  }
	  /* invoke update() with dirty flag set to false, to update in-memory metadata for each block
	     reset values */
	  lst = e->size;
	  fst = 0;
	  do {
	    if (fst >= lst) {
	break;
	    }
	    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
	    off_t cur_len = cur_size - fst;

	    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
	    ldpp_dout(dpp, 20) << __func__ << "(): oid_in_cache =" << oid_in_cache << dendl;
	    //Update in-memory data structure for each block
	    this->update(dpp, oid_in_cache, 0, 0, e->version, false, 0, y);

	    rgw::d4n::CacheBlock block;
	    block.cacheObj.bucketName = c_obj->get_bucket()->get_bucket_id();
	    block.cacheObj.objName = c_obj->get_key().get_oid();
	    block.size = cur_len;
	    block.blockID = fst;
            if ((op_ret = cacheDriver->set_attr(dpp, oid_in_cache, RGW_CACHE_ATTR_DIRTY, "0", y)) == 0) {
        std::string dirty = "false";
	      op_ret = blockDir->update_field(dpp, &block, "dirty", dirty, null_yield);
	      if (op_ret < 0) {
		ldpp_dout(dpp, 0) << __func__ << "updating dirty flag in block directory failed, ret=" << op_ret << dendl;
	      }
            } else {
	      ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty xattr in cache, ret=" << op_ret << dendl;
            }

	    fst += cur_len;
	  } while(fst < lst);
	} //end-else if delete_marker

	//invoke update() with dirty flag set to false, to update in-memory metadata for head
	this->update(dpp, e->key, 0, 0, e->version, false, 0, y);
         
        if ((ret = cacheDriver->set_attr(dpp, e->key, RGW_CACHE_ATTR_DIRTY, "0", y)) < 0) {
	  ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty attr in cache, ret=" << op_ret << dendl;
        }

	if (null_instance) {
	  //restore instance for directory data processing in later steps
	  c_obj->set_instance("null");
	}
	rgw::d4n::CacheBlock block;
	block.cacheObj.bucketName = c_obj->get_bucket()->get_bucket_id();
        ldpp_dout(dpp, 20) << __func__ << "(): bucket name: " << block.cacheObj.bucketName << dendl;
	block.cacheObj.objName = c_obj->get_name();
	block.size = 0;
	block.blockID = 0;
	//non-versioned case
	if (!c_obj->have_instance()) {
	  // hash entry for latest version
	  op_ret = blockDir->get(dpp, &block, y);
	  if (op_ret < 0) {
	    ldpp_dout(dpp, 0) << __func__ << "(): Failed to get latest entry in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
	  } else {
	    // if this entry is not the latest, it could have been overwritten by a newer one
	    if (block.version == e->version) {
	      rgw::d4n::CacheBlock null_block;
	      null_block = block;
	      null_block.cacheObj.objName = "_:null_" + c_obj->get_name();
	      //hash entry for null block
	      op_ret = blockDir->get(dpp, &null_block, y);
	      if (op_ret < 0) {
		ldpp_dout(dpp, 0) << __func__ << "(): Failed to get latest entry in block directory for: " << null_block.cacheObj.objName << ", ret=" << ret << dendl;
	      } else {
		if (null_block.version == e->version) {
		  block.cacheObj.dirty = false;
		  null_block.cacheObj.dirty = false;
		  auto blk_op_ret = blockDir->set(dpp, &block, y);
		  auto null_op_ret = blockDir->set(dpp, &null_block, y);
		  if (blk_op_ret < 0 || null_op_ret < 0) {
		    ldpp_dout(dpp, 0) << __func__ << "(): Failed to Queue update dirty flag for latest entry/null entry in block directory" << dendl;
		  }
		}
	      }
	    } //end-if (block.version == entry->version)
	  } //end - else if op_ret == 0
	  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Removing object name: "<< c_obj->get_name() << " score: " << std::setprecision(std::numeric_limits<double>::max_digits10) << e->creationTime << " from ordered set" << dendl;
	  rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
	    .objName = c_obj->get_name(),
	    .bucketName = c_obj->get_bucket()->get_bucket_id(),
	  };
	  /* remove the entry from the ordered set using its score, as the object is already cleaned
	     need not be part of a transaction as it is being removed based on its score which is its creation time. */
	  ret = objDir->zremrangebyscore(dpp, &dir_obj, e->creationTime, e->creationTime, y);
	  if (ret < 0) {
	    ldpp_dout(dpp, 0) << __func__ << "(): Failed to remove object from ordered set with error: " << ret << dendl;
	  }
	}
	if (c_obj->have_instance()) { //versioned case
	  std::string objName = c_obj->get_oid();
	  if (c_obj->get_instance() == "null") {
	    objName = "_:null_" + c_obj->get_name();
	  }
	  rgw::d4n::CacheBlock instance_block;
	  instance_block.cacheObj.bucketName = c_obj->get_bucket()->get_bucket_id();
	  instance_block.cacheObj.objName = objName;
	  instance_block.size = 0;
	  instance_block.blockID = 0;
    std::string dirty = "false";
	  op_ret = blockDir->update_field(dpp, &instance_block, "dirty", dirty, null_yield);
	  if (op_ret < 0) {
	    ldpp_dout(dpp, 20) << __func__ << "updating dirty flag in block directory for instance block failed!" << dendl;
	  }
	  //the next steps remove the entry from the ordered set and if needed the latest hash entry also in case of versioned buckets
	  rgw::d4n::CacheBlock latest_block = block;
	  latest_block.cacheObj.objName = c_obj->get_name();
	  int retry = 3;
	  while(retry) {
	    retry--;
	    //get latest entry
	    ret = blockDir->get(dpp, &latest_block, y);
	    if (ret < 0) {
	      ldpp_dout(dpp, 0) << __func__ << "(): Failed to get latest entry in block directory for: " << latest_block.cacheObj.objName << ", ret=" << ret << dendl;
	    }
	    if (latest_block.version == e->version) {
	      //remove object entry from ordered set of versions
	      if (c_obj->have_instance()) {
		blockDir->del(dpp, &latest_block, y);
		if (ret < 0) {
		  ldpp_dout(dpp, 0) << __func__ << "(): Failed to queue del for latest hash entry: " << latest_block.cacheObj.objName << ", ret=" << ret << dendl;
		  continue;
		}
	      }
	      //delete entry from ordered set of objects, as older versions would have been written to the backend store
	      ret = bucketDir->zrem(dpp, e->bucket_id, c_obj->get_name(), y, true);
	      if (ret < 0) {
		blockDir->discard(dpp, y);
		ldpp_dout(dpp, 0) << __func__ << "(): Failed to queue zrem for object entry: " << c_obj->get_name() << ", ret=" << ret << dendl;
		continue;
	      }
	    }
	    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Removing object name: "<< c_obj->get_name() << " score: " << std::setprecision(std::numeric_limits<double>::max_digits10) << e->creationTime << " from ordered set" << dendl;
	    rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
	      .objName = c_obj->get_name(),
	      .bucketName = c_obj->get_bucket()->get_bucket_id(),
	    };
	    ret = objDir->zremrangebyscore(dpp, &dir_obj, e->creationTime, e->creationTime, y, true);
	    if (ret < 0) {
	      ldpp_dout(dpp, 0) << __func__ << "(): Failed to remove object from ordered set with error: " << ret << dendl;
	      continue;
	    }
	    break;
	  }//end-while (retry)
	}
	//remove entry from map and queue, erase_dirty_object locks correctly
	erase_dirty_object(dpp, e->key, null_yield);
      }
    } else if (diff < interval) { //end-if std::difftime(time(NULL), e->creationTime) > interval
      std::this_thread::sleep_for(std::chrono::seconds(interval - diff));
    }
  } //end-while true
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
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) {
    auto p = entries_lru_list.front();
    entries_map.erase(entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    auto ret = cacheDriver->delete_data(dpp, p.key, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to delete data from the cache backend, ret=" << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp);
  }

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, uint8_t op, optional_yield y, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  Entry* e = new Entry(key, offset, len, version, dirty, 0);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void LRUPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
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

} } // namespace rgw::d4n
