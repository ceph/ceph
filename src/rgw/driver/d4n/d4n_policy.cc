#include "d4n_policy.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/dout.h" 

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
void redis_exec(std::shared_ptr<connection> conn,
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

int LFUDAPolicy::init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) {
  this->cct = cct;
  dir->init(cct);
  driver = _driver;
  tc = std::thread(&CachePolicy::cleaning, this, dpp);
  tc.detach();
  int result = 0;
  response<int, int, int, int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", "lfuda", "age"); 
    req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum)); /* New cache node will always have the minimum average weight */
    req.push("HSET", "lfuda", "minLocalWeights_size", std::to_string(entries_map.size()));
    req.push("HSET", "lfuda", "minLocalWeights_address", dir->cct->_conf->rgw_d4n_l1_datacache_address);
  
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    result = std::min(std::get<1>(resp).value(), std::min(std::get<2>(resp).value(), std::get<3>(resp).value()));
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  if (!std::get<0>(resp).value()) { /* Only set maximum age if it doesn't exist */
    try {
      boost::system::error_code ec;
      response<int> value;
      request req;
      req.push("HSET", "lfuda", "age", age);
    
      redis_exec(conn, ec, req, value, y);

      if (ec) {
	ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      result = std::min(result, std::get<0>(value).value());
    } catch (std::exception &e) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  }

  asio::co_spawn(io_context.get_executor(),
		   redis_sync(dpp, y), asio::detached);

  return result;
}

int LFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  response<std::string> resp;

  try { 
    boost::system::error_code ec;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    return -EINVAL;
  }

  if (age > std::stoi(std::get<0>(resp).value()) || std::get<0>(resp).value().empty()) { /* Set new maximum age */
    try { 
      boost::system::error_code ec;
      request req;
      response<int> value;
      req.push("HSET", "lfuda", "age", age);
      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	return -ec.value();
      }

      return std::get<0>(value).value();
    } catch (std::exception &e) {
      return -EINVAL;
    }
  } else {
    age = std::stoi(std::get<0>(resp).value());
    return 0;
  }
}

int LFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  int result; 

  if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {
    response<std::string, std::string> resp;

    try { 
      boost::system::error_code ec;
      request req;
      req.push("HGET", "lfuda", "minLocalWeights_sum");
      req.push("HGET", "lfuda", "minLocalWeights_size");
	
      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  
    float minAvgWeight = std::stof(std::get<0>(resp).value()) / std::stof(std::get<1>(resp).value());

    if ((static_cast<float>(weightSum) / static_cast<float>(entries_map.size())) < minAvgWeight) { /* Set new minimum weight */
      try { 
	boost::system::error_code ec;
	request req;
	response<int, int, int> value;
	req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum));
	req.push("HSET", "lfuda", "minLocalWeights_size", std::to_string(entries_map.size()));
	req.push("HSET", "lfuda", "minLocalWeights_address", dir->cct->_conf->rgw_d4n_l1_datacache_address);
	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  return -ec.value();
	}

	result = std::min(std::get<0>(value).value(), std::get<1>(value).value());
	result = std::min(result, std::get<2>(value).value());
      } catch (std::exception &e) {
	return -EINVAL;
      }
    } else {
      weightSum = std::stoi(std::get<0>(resp).value());
      postedSum = std::stoi(std::get<0>(resp).value());
    }
  }

  try { /* Post update for local cache */
    boost::system::error_code ec;
    request req;
    response<int, int> resp;
    req.push("HSET", dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, "avgLocalWeight_sum", std::to_string(weightSum));
    req.push("HSET", dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address, "avgLocalWeight_size", std::to_string(entries_map.size()));
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }

    result = std::min(std::get<0>(resp).value(), std::get<1>(resp).value());
  } catch (std::exception &e) {
    return -EINVAL;
  }
  
  return result;
}

asio::awaitable<void> LFUDAPolicy::redis_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  rthread_timer.emplace(co_await asio::this_coro::executor);
  co_await asio::this_coro::throw_if_cancelled(true);
  co_await asio::this_coro::reset_cancellation_state(
    asio::enable_terminal_cancellation());

  for (;;) try {
    /* Update age */
    if (int ret = age_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: ret=" << ret << dendl;
    }
    
    /* Update minimum local weight sum */
    if (int ret = local_weight_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: ret=" << ret << dendl;
    }

    int interval = dpp->get_cct()->_conf->rgw_lfuda_sync_frequency;
    rthread_timer->expires_after(std::chrono::seconds(interval));
    co_await rthread_timer->async_wait(asio::use_awaitable);
  } catch (sys::system_error& e) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;

    if (e.code() == asio::error::operation_aborted) {
      break;
    } else {
      continue;
    }
  }
}

CacheBlock* LFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) {
  const std::lock_guard l(lfuda_lock);
  if (entries_heap.empty())
    return nullptr;

  /* Get victim cache block */
  std::string key = entries_heap.top()->key;
  CacheBlock* victim = new CacheBlock();

  victim->cacheObj.bucketName = key.substr(0, key.find('_')); 
  key.erase(0, key.find('_') + 1);
  victim->cacheObj.objName = key.substr(0, key.find('_'));
  victim->blockID = entries_heap.top()->offset;
  victim->size = entries_heap.top()->len;

  if (dir->get(dpp, victim, y) < 0) {
    return nullptr;
  }

  return victim;
}

int LFUDAPolicy::exist_key(std::string key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) { // TODO: Think about parallel reads and writes; can this turn into an infinite loop? 
    CacheBlock* victim = get_victim_block(dpp, y);
  
    if (victim == nullptr) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
      delete victim;
      return -ENOENT;
    }

    const std::lock_guard l(lfuda_lock);
    std::string key = entries_heap.top()->key;
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      delete victim;
      return -ENOENT;
    }
    // check dirty flag of entry to be evicted, if the flag is dirty, all entries on the local node are dirty
    if (it->second->dirty) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Top entry in min heap is dirty, no entry is available for eviction!" << dendl;
      return -ENOENT;
    }
    int avgWeight = weightSum / entries_map.size();

    if (victim->hostsList.size() == 1 && victim->hostsList[0] == dir->cct->_conf->rgw_d4n_l1_datacache_address) { /* Last copy */
      if (victim->globalWeight) {
	it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight = it->second->localWeight;
	entries_heap.increase(it->second->handle);

	if (int ret = cacheDriver->set_attr(dpp, key, "user.rgw.localWeight", std::to_string(it->second->localWeight), y) < 0) { 
	  delete victim;
	  return ret;
        }

	victim->globalWeight = 0;
	if (int ret = dir->update_field(dpp, victim, "globalWeight", std::to_string(victim->globalWeight), y) < 0) {
	  delete victim;
	  return ret;
        }
      }

      if (it->second->localWeight > avgWeight) {
	// TODO: push victim block to remote cache
	// add remote cache host to host list
      }
    }

    victim->globalWeight += it->second->localWeight;
    if (int ret = dir->update_field(dpp, victim, "globalWeight", std::to_string(victim->globalWeight), y) < 0) {
      delete victim;
      return ret;
    }

    if (int ret = dir->remove_host(dpp, victim, dir->cct->_conf->rgw_d4n_l1_datacache_address, y) < 0) {
      delete victim;
      return ret;
    }

    delete victim;

    if (int ret = cacheDriver->del(dpp, key, y) < 0) 
      return ret;

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    weightSum = (avgWeight * entries_map.size()) - it->second->localWeight;

    age = std::max(it->second->localWeight, age);

    erase(dpp, key, y);
    freeSpace = cacheDriver->get_free_space(dpp);
  }
  
  return 0;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, optional_yield y)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  const std::lock_guard l(lfuda_lock);
  int localWeight = age;
  auto entry = find_entry(key);
  bool updateLocalWeight = true;
  // check the dirty flag in the existing entry for the key and the incoming dirty flag. If the
  // incoming dirty flag is false, that means update() is invoked as part of cleaning process,
  // so we must not update its localWeight.
  if (entry != nullptr) {
    if (entry->dirty && !dirty) {
      localWeight = entry->localWeight;
      updateLocalWeight = false;
    } else {
      localWeight = entry->localWeight + age;
    }
  }  
  erase(dpp, key, y);
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, dirty, localWeight);
  handle_type handle = entries_heap.push(e);
  e->set_handle(handle);
  entries_map.emplace(key, e);

  std::string oid_in_cache = key;
  if (dirty == true) {
    oid_in_cache = "D_" + key;
  }

  if (updateLocalWeight) {
    if (cacheDriver->set_attr(dpp, oid_in_cache, "user.rgw.localWeight", std::to_string(localWeight), y) < 0) 
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
  }

  weightSum += ((localWeight < 0) ? 0 : localWeight);
}

void LFUDAPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key, optional_yield y)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>::handle_type;
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Before acquiring lock." << dendl;
  const std::lock_guard l(lfuda_cleaning_lock);
  LFUDAObjEntry *e = new LFUDAObjEntry{key, version, dirty, size, creationTime, user, etag, bucket_name, obj_key};
  handle_type handle = object_heap.push(e);
  e->set_handle(handle);
  o_entries_map.emplace(key, e);
  cond.notify_one();
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  weightSum -= ((p->second->localWeight < 0) ? 0 : p->second->localWeight);

  entries_heap.erase(p->second->handle);
  entries_map.erase(p);

  return true;
}

bool LFUDAPolicy::eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_cleaning_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  object_heap.erase(p->second->handle);
  o_entries_map.erase(p);
  delete p->second;

  return true;
}

void LFUDAPolicy::cleaning(const DoutPrefixProvider* dpp)
{
  const int interval = cct->_conf->rgw_d4n_cache_cleaning_interval;
  while(!quit) {
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;
    std::string name = ""; 
    std::string b_name = ""; 
    std::string key = ""; 
    uint64_t len = 0;
    rgw::sal::Attrs obj_attrs;
  
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "" << __LINE__ << "(): Before acquiring cleaning-lock." << dendl;
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
    LFUDAObjEntry* e;
    if (object_heap.size() > 0) {
      e = object_heap.top();
    } else {
      cond.wait(l, [this]{ return (!object_heap.empty() || quit); });
      continue;
    }
    ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): e->key=" << e->key << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->dirty=" << e->dirty << dendl;
    l.unlock();
    if (!e->key.empty() && (e->dirty == true) && (std::difftime(time(NULL), e->creationTime) > interval)) { //if block is dirty and written more than interval seconds ago
      name = e->key;
      rgw_user c_rgw_user = e->user;

      size_t pos = 0;
      std::string delimiter = "_";
      int count = 0;
      while ((pos = name.find(delimiter)) != std::string::npos) {
        if (count == 0) {
          b_name = name.substr(0, pos);
          ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): b_name=" << b_name << dendl;
              name.erase(0, pos + delimiter.length());
          ldpp_dout(dpp, 10) << __LINE__ <<  " " << __func__ << "(): name=" << name << dendl;
          break;
        }
        count++;
        ldpp_dout(dpp, 10) << __LINE__ <<  " " << __func__ << "(): count=" << b_name << dendl;
      }
      key = name;
      //writing data to the backend
      //we need to create an atomic_writer
      std::unique_ptr<rgw::sal::User> c_user = driver->get_user(c_rgw_user);

      std::unique_ptr<rgw::sal::Bucket> c_bucket;
      rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.tenant, e->bucket_name, "");

      RGWBucketInfo c_bucketinfo;
      c_bucketinfo.bucket = c_rgw_bucket;
      c_bucketinfo.owner = c_rgw_user;
      int ret = driver->load_bucket(dpp, c_rgw_bucket, &c_bucket, null_yield);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
        break;
      }

      std::unique_ptr<rgw::sal::Object> c_obj = c_bucket->get_object(e->obj_key);

      ACLOwner owner{c_user->get_id(), c_user->get_display_name()};

      std::unique_ptr<rgw::sal::Writer> processor =  driver->get_atomic_writer(dpp,
              null_yield,
              c_obj.get(),
              owner,
              NULL,
              0,
              "");

      int op_ret = processor->prepare(null_yield);
      if (op_ret < 0) {
          ldpp_dout(dpp, 20) << __func__ << "processor->prepare() returned ret=" << op_ret << dendl;
          break;
      }

      std::string prefix = b_name + "_" + e->version + "_" + c_obj->get_name();
      off_t lst = e->size;
      off_t fst = 0;
      off_t ofs = 0;

      rgw::sal::DataProcessor *filter = processor.get();
      std::string head_oid_in_cache = "D_" + prefix;
      std::string new_head_oid_in_cache = prefix;
      ldpp_dout(dpp, 10) << __func__ << "(): head_oid_in_cache=" << head_oid_in_cache << dendl;
      ldpp_dout(dpp, 10) << __func__ << "(): new_head_oid_in_cache=" << new_head_oid_in_cache << dendl;
      bufferlist bl;
      cacheDriver->get_attrs(dpp, head_oid_in_cache, obj_attrs, null_yield); //get obj attrs from head
      obj_attrs.erase("user.rgw.mtime");
      obj_attrs.erase("user.rgw.object_size");
      obj_attrs.erase("user.rgw.accounted_size");
      obj_attrs.erase("user.rgw.epoch");

      do {
        ceph::bufferlist data;
        if (fst >= lst){
            break;
        }
        off_t cur_size = std::min<off_t>(fst + cct->_conf->rgw_max_chunk_size, lst);
        off_t cur_len = cur_size - fst;
        std::string oid_in_cache = "D_" + prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_len);
        ldpp_dout(dpp, 10) << __func__ << "(): oid_in_cache=" << oid_in_cache << dendl;
        rgw::sal::Attrs attrs;
        cacheDriver->get(dpp, oid_in_cache, 0, cur_len, data, attrs, null_yield);
        len = data.length();
        fst += len;

        if (len == 0) {
          // TODO: if len of any block is 0 for some reason, we must return from here?
          break;
        }

        op_ret = filter->process(std::move(data), ofs);
        if (op_ret < 0) {
            ldpp_dout(dpp, 20) << __func__ << "processor->process() returned ret="
            << op_ret << dendl;
            return;
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

      //invoke update() with dirty flag set to false, to update in-memory metadata for each block
      // reset values
      lst = e->size;
      fst = 0;
      do {
        if (fst >= lst) {
            break;
        }
        off_t cur_size = std::min<off_t>(fst + cct->_conf->rgw_max_chunk_size, lst);
        off_t cur_len = cur_size - fst;

        std::string oid_in_cache = "D_" + prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_len);
        ldpp_dout(dpp, 20) << __func__ << "(): oid_in_cache =" << oid_in_cache << dendl;
        std::string new_oid_in_cache = prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_len);
        //Rename block to remove "D" prefix
        cacheDriver->rename(dpp, oid_in_cache, new_oid_in_cache, null_yield);
        //Update in-memory data structure for each block
        this->update(dpp, new_oid_in_cache, 0, 0, e->version, false, y);

        rgw::d4n::CacheBlock block;
        block.cacheObj.bucketName = c_obj->get_bucket()->get_name();
        block.cacheObj.objName = c_obj->get_key().get_oid();
        block.size = cur_len;
        block.blockID = fst;
        op_ret = dir->update_field(dpp, &block, "dirty", "false", null_yield);
        if (op_ret < 0) {
            ldpp_dout(dpp, 5) << __func__ << "updating dirty flag in Block directory failed!" << dendl;
            return;
        }
        fst += cur_len;
      } while(fst < lst);

      cacheDriver->rename(dpp, head_oid_in_cache, new_head_oid_in_cache, null_yield);
      //data is clean now, updating in-memory metadata for an object
      e->dirty = false;
      //invoke update() with dirty flag set to false, to update in-memory metadata for head
      this->update(dpp, new_head_oid_in_cache, 0, 0, e->version, false, y);

      rgw::d4n::CacheBlock block;
      block.cacheObj.bucketName = c_obj->get_bucket()->get_name();
      block.cacheObj.objName = c_obj->get_name();
      block.size = 0;
      block.blockID = 0;
      op_ret = dir->update_field(dpp, &block, "dirty", "false", null_yield);
      if (op_ret < 0) {
          ldpp_dout(dpp, 20) << __func__ << "updating dirty flag in block directory for head failed!" << dendl;
          return;
      }

      //remove entry from map and queue, eraseObj locks correctly
      eraseObj(dpp, e->key, null_yield);
    } else { //end-if std::difftime(time(NULL), e->creationTime) > interval
      std::this_thread::sleep_for(std::chrono::milliseconds(interval)); //TODO:: should this time be optimised?
    }
  } //end-while true
}


int LRUPolicy::exist_key(std::string key)
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
      ldpp_dout(dpp, 10) << __func__ << "(): Failed to delete data from the cache backend: " << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp);
  }

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  Entry *e = new Entry(key, offset, len, version, dirty);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void LRUPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, const rgw_obj_key& obj_key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  ObjEntry *e = new ObjEntry(key, version, dirty, size, creationTime, user, etag, bucket_name, obj_key);
  o_entries_map.emplace(key, e);
  return;
}


bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  return _erase(dpp, key, y);
}

bool LRUPolicy::eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
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
