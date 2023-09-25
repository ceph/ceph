#include "../../../common/async/yield_context.h"
#include "d4n_policy.h"
#include "common/async/blocked_completion.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;
  boost::redis::request req;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, Response& resp)
  {
    conn->async_exec(req, resp, boost::asio::consign(std::move(handler), conn));
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn), req}, token, resp);
}

template <typename T>
void redis_exec(std::shared_ptr<connection> conn, boost::system::error_code& ec, boost::redis::request& req, boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int LFUDAPolicy::set_age(int age, optional_yield y) {
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "age", std::to_string(age));

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::get_age(optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", "lfuda", "age");

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return -1;
  } catch(std::exception &e) {
    return -1;
  }

  if (!std::get<0>(resp).value()) {
    if (!set_age(0, y)) /* Initialize age */
      return 0;
    else
      return -1;
  }

  try { 
    boost::system::error_code ec;
    response<std::string> value;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn, ec, req, value, y);

    if (ec)
      return -1;

    return std::stoi(std::get<0>(value).value());
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::set_global_weight(std::string key, int weight, optional_yield y) {
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", key, "globalWeight", std::to_string(weight));

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::get_global_weight(std::string key, optional_yield y) {
  try { 
    boost::system::error_code ec;
    response<std::string> resp;
    request req;
    req.push("HGET", key, "globalWeight");
      
    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return -1;

    return std::stoi(std::get<0>(resp).value());
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::set_min_avg_weight(size_t weight, std::string cacheLocation, optional_yield y) {
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "minAvgWeight:cache", cacheLocation);

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};
  } catch(std::exception &e) {
    return -1;
  }
  
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "minAvgWeight:weight", cacheLocation);

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::get_min_avg_weight(optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", "lfuda", "minAvgWeight:cache");

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return -1;
  } catch(std::exception &e) {
    return -1;
  }

  if (!std::get<0>(resp).value()) {
    if (!set_min_avg_weight(INT_MAX, ""/* local cache location or keep empty? */, y)) { /* Initialize minimum average weight */
      return INT_MAX;
    } else {
      return -1;
    }
  }

  try { 
    boost::system::error_code ec;
    response<std::string> value;
    request req;
    req.push("HGET", "lfuda", "minAvgWeight:weight");
      
    redis_exec(conn, ec, req, value, y);

    if (ec)
      return -1;

    return std::stoi(std::get<0>(value).value());
  } catch(std::exception &e) {
    return -1;
  }
}

CacheBlock LFUDAPolicy::find_victim(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) {
  #if 0
  std::vector<rgw::cache::Entry> entries = cacheNode->list_entries(dpp);
  std::string victimName;
  int minWeight = INT_MAX;

  for (auto it = entries.begin(); it != entries.end(); ++it) {
    std::string localWeightStr = cacheNode->get_attr(dpp, it->key, "localWeight", y); // should represent block -Sam

  /* Get victim cache block */
  CacheBlock victim;
  victim.cacheObj.objName = it->second->key;
  victim.cacheObj.bucketName = cacheNode->get_attr(dpp, victim.cacheObj.objName, "bucket_name", y); // generalize for other cache backends -Sam
  victim.blockID = 0; // find way to get ID -Sam 

      if (ret < 0)
	return {};
    } else if (std::stoi(localWeightStr) < minWeight) {
      minWeight = std::stoi(localWeightStr);
      victimName = it->key;
    }
  }

  /* Get victim cache block */
  CacheBlock victimBlock;
  victimBlock.cacheObj.objName = victimName;
  BlockDirectory blockDir(io);
  blockDir.init(cct, dpp);

  int ret = blockDir.get(&victimBlock, y);

  if (ret < 0)
    return {};
  #endif
  CacheBlock victimBlock;
  return victimBlock;
}

int LFUDAPolicy::exist_key(std::string key, optional_yield y) {
  response<int> resp;

  try { 
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);
  
    redis_exec(conn, ec, req, resp, y);
    
    if (ec)
      return false;
  } catch(std::exception &e) {}

  return std::get<0>(resp).value();
}

int LFUDAPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) {
  std::string key = "rgw-object:" + block->cacheObj.objName + ":directory";
  std::string localWeightStr = cacheNode->get_attr(dpp, block->cacheObj.objName, "localWeight", y); // change to block name eventually -Sam
  int localWeight = -1;
  response<std::string> resp;

  if (localWeightStr.empty()) { // figure out where to set local weight -Sam
    int ret = cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(get_age(y)), y); 
    localWeight = get_age(y);

    if (ret < 0)
      return -1;
  } else {
    localWeight = std::stoi(localWeightStr);
  }

  int age = get_age(y);

  if (exist_key(key, y)) { /* Local copy */ 
    localWeight += age;
  } else {
    uint64_t freeSpace = cacheNode->get_free_space(dpp);

    while (freeSpace < block->size) /* Not enough space in local cache */
      freeSpace += eviction(dpp, cacheNode, y);

    if (exist_key(key, y)) { /* Remote copy */
      try {
	boost::system::error_code ec;
	request req;
	req.push("HGET", key, "blockHosts");
	  
	redis_exec(conn, ec, req, resp, y);

	if (ec)
	  return -1;
      } catch(std::exception &e) {
	return -1;
      }
    } else {
      return -2; 
    }

    // should not hold local cache IP if in this else statement -Sam
    if (std::get<0>(resp).value().length() > 0) { /* Remote copy */
      int globalWeight = get_global_weight(key, y);
      globalWeight += age;
      
      if (set_global_weight(key, globalWeight, y))
	return -1;
    } else { /* No remote copy */
      // do I need to add the block to the local cache here? -Sam
      // update hosts list for block as well? check read workflow -Sam
      localWeight += age;
      return cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(localWeight), y);
    }
  } 

  return 0;
}

uint64_t LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) {
  CacheBlock victim = find_victim(dpp, cacheNode, y);

  if (victim.cacheObj.objName.empty()) {
    ldpp_dout(dpp, 10) << "RGW D4N Policy: Could not find victim block" << dendl;
    return -1;
  }

  std::string key = "rgw-object:" + victim.cacheObj.objName + ":directory";
  int globalWeight = get_global_weight(key, y);
  int localWeight = std::stoi(cacheNode->get_attr(dpp, victim.cacheObj.objName, "localWeight", y)); // change to block name eventually -Sam
  int avgWeight = get_min_avg_weight(y);
  response<std::string> resp;

  if (exist_key(key, y)) {
    try {
      boost::system::error_code ec;
      request req;
      req.push("HGET", key, "blockHosts");
	
      redis_exec(conn, ec, req, resp, y);

      if (ec)
	return -1;
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2; 
  }

  if (std::get<0>(resp).value().empty()) { /* Last copy */
    if (globalWeight > 0) {
      localWeight += globalWeight;
      int ret = cacheNode->set_attr(dpp, victim.cacheObj.objName, "localWeight", std::to_string(localWeight), y);

      if (!ret)
        ret = set_global_weight(key, 0, y);
      else
        return -1;

      if (ret)
	return -1;
    }

    if (avgWeight < 0)
      return -1;

    if (localWeight > avgWeight) {
      // push block to remote cache
    }
  }

  globalWeight += localWeight;

  if (set_global_weight(key, globalWeight, y))
    return -2;

  ldpp_dout(dpp, 10) << "RGW D4N Policy: Block " << victim.cacheObj.objName << " has been evicted." << dendl;
  int ret = cacheNode->del(dpp, victim.cacheObj.objName, y);

  if (!ret) {
    //ret = set_min_avg_weight(avgWeight - (localWeight/entries_map.size()), ""/*local cache location*/, y); // Where else must this be set? -Sam

    if (!ret) {
      int age = get_age(y);
      age = std::max(localWeight, age);
      ret = set_age(age, y);
      
      if (ret)
	return -1;
    } else {
      return -1;
    }
  } else {
    return -1;
  }

  return victim.size;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{

}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key)
{
  return false;
}

void LFUDAPolicy::shutdown()
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
}

int LRUPolicy::exist_key(std::string key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{
  uint64_t freeSpace = cacheNode->get_free_space(dpp);
  while(freeSpace < block->size) {
    freeSpace = eviction(dpp, cacheNode, y);
  }
  return 0;
}

uint64_t LRUPolicy::eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = entries_lru_list.front();
  entries_map.erase(entries_map.find(p.key));
  entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
  cacheNode->delete_data(dpp, p.key, null_yield);
  return cacheNode->get_free_space(dpp);
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{
  erase(dpp, key);

  Entry *e = new Entry(key, offset, len, ""); // update version later -Sam
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key)
{
  const std::lock_guard l(lru_lock);
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}

int PolicyDriver::init() {
  if (policyName == "lfuda") {
    cachePolicy = new LFUDAPolicy(io);
    return 0;
  } else if (policyName == "lru") {
    cachePolicy = new LRUPolicy();
    return 0;
  }
  return -1;
}

} } // namespace rgw::d4n
