#include <algorithm>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <cstring>
#include <memory>
#include <stop_token>
//#include <string>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = boost::asio::consign(std::move(handler), conn);
    return boost::asio::dispatch(get_executor(),
        [c = conn, &req, &resp, h = std::move(h)] () mutable {
            return c->async_exec(req, resp, std::move(h));
    });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
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

template <typename... Types>
void redis_exec_cp(const DoutPrefixProvider* dpp,
                std::shared_ptr<rgw::d4n::RedisPool> pool,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp,
		optional_yield y)
{
//purpose: Execute a Redis command using a connection from the pool
	std::shared_ptr<connection> conn = pool->acquire(dpp);
	try {

  		if (y) {
    		auto yield = y.get_yield_context();
    		async_exec(conn, req, resp, yield[ec]);
  		} else {
    		async_exec(conn, req, resp, ceph::async::use_blocked[ec]);
  		}
	} catch (const std::exception& e) {
		//release the connection upon exception
    		pool->release(conn);
    		throw;
	}
	//release the connection back to the pool after execution
	pool->release(conn);
}

void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
    boost::redis::generic_response& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

void redis_exec_cp(const DoutPrefixProvider* dpp,
                std::shared_ptr<rgw::d4n::RedisPool> pool,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::generic_response& resp, optional_yield y)
{
	//purpose: Execute a Redis command using a connection from the pool
	std::shared_ptr<connection> conn = pool->acquire(dpp);

	try {
  		if (y) {
    			auto yield = y.get_yield_context();
    			async_exec(conn, req, resp, yield[ec]);
  		} else {
    			async_exec(conn, req, resp, ceph::async::use_blocked[ec]);
  		}	
	} catch (const std::exception& e) {
    			pool->release(conn);
    			throw;
	}
	//release the connection back to the pool after execution
	pool->release(conn);
}

int check_bool(std::string str) {
  if (str == "true" || str == "1") {
    return 1;
  } else if (str == "false" || str == "0") {
    return 0;
  } else {
    return -EINVAL;
  }
}

void redis_exec_connection_pool(const DoutPrefixProvider* dpp,
				std::shared_ptr<RedisPool> redis_pool,
				std::shared_ptr<connection> conn,
				boost::system::error_code& ec,
				const boost::redis::request& req,
				boost::redis::generic_response& resp,
				optional_yield y)
{
    if(!redis_pool)[[unlikely]]
    {
	redis_exec(conn, ec, req, resp, y);
	ldpp_dout(dpp, 0) << "Directory::" << __func__ << " not using connection-pool, it's using the shared connection " << dendl;
    }
    else[[likely]]
    	redis_exec_cp(dpp, redis_pool, ec, req, resp, y);
}

template <typename... Types>
void redis_exec_connection_pool(const DoutPrefixProvider* dpp,
				std::shared_ptr<RedisPool> redis_pool,
				std::shared_ptr<connection> conn,
				boost::system::error_code& ec,
				const boost::redis::request& req,
				boost::redis::response<Types...>& resp,
				optional_yield y)
{
    if(!redis_pool)[[unlikely]]
    {
	redis_exec(conn, ec, req, resp, y);
	ldpp_dout(dpp, 0) << "Directory::" << __func__ << " not using connection-pool, it's using the shared connection " << dendl;
    }
    else[[likely]]
    	redis_exec_cp(dpp, redis_pool, ec, req, resp, y);
}

#ifndef dout_subsys
#define dout_subsys ceph_subsys_rgw
#endif

int BucketDirectory::zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline)
{
  try {
    boost::system::error_code ec;
    if (pipeline && pipeline->is_pipeline()) {
      request& req = pipeline->get_request();
      req.push("ZADD", bucket_id, "CH", std::to_string(0), member);
    } else {
      request req;
      req.push("ZADD", bucket_id, "CH", std::to_string(0), member);

    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

      if (ec) {
        ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
        return -ec.value();
      }

      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "BucketDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}

int BucketDirectory::zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", bucket_id, member);
    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 10) << "BucketDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -ENOENT;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BucketDirectory::zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    if (offset == 0 && count == 0) {
      req.push("ZRANGE", bucket_id, start, stop, "bylex");
    } else {
      req.push("ZRANGE", bucket_id, start, stop, "bylex", "LIMIT", offset, count);
    }

    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "BucketDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BucketDirectory::zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;

    req.push("ZSCAN", bucket_id, cursor, "MATCH", pattern, "COUNT", count);

    boost::redis::generic_response resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    std::vector<boost::redis::resp3::basic_node<std::__cxx11::basic_string<char> > > root_array;
    if (resp.has_value()) {
      root_array = resp.value();
      ldpp_dout(dpp, 20) << "BucketDirectory::" << __func__ << "() aggregate size is: " << root_array.size() << dendl;
      auto size = root_array.size();
      if (size >= 2) {
        //Nothing of interest at index 0, index 1 has the next cursor value
        next_cursor = std::stoull(root_array[1].value);

        //skip the first 3 values to get the actual member, score
        for (uint64_t i = 3; i < size; i = i+2) {
          members.emplace_back(root_array[i].value);
          ldpp_dout(dpp, 20) << "BucketDirectory::" << __func__ << "() member is: " << root_array[i].value << dendl;
        }
      }
    } else {
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BucketDirectory::zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;

    req.push("ZRANK", bucket_id, member);

    response<int> resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    rank = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

void D4NTransaction::create_rw_temp_keys(std::string key)
{
	//TODO get_trx_id( ) could be called here, upon transaction start
  if(trxState == TrxState::STARTED) {
    m_original_key = key;
    // in case the content of m_original_key was changed(by other transaction),the temp keys are deleted on end-transaction phase.
    std::string temp_key = create_unique_temp_keys(m_original_key);
 
    // the temp key structure is as follows: _<original_key>_<transaction_id>_temp_<read|write|test_write>
    m_temp_key_read = temp_key + "_read";
    m_temp_key_write = temp_key + "_write";  
    m_temp_key_test_write = temp_key + "_test_write";
  
    };
}

std::string ObjectDirectory::build_index(CacheObj* object)
{ 
  std::string key = object->bucketName + "_" + object->objName;
  
  if(m_d4n_trx) 
      m_d4n_trx->create_rw_temp_keys(key);
  
  return key;   
}  

std::string D4NTransaction::create_unique_temp_keys(std::string key) 
{
  if(m_trx_id.empty()) {
	//note: there are cases where the trx_id is empty (such as update_field), this is a temporary solution.
	//TODO: what is flow for this case?
	m_trx_id = std::to_string(99999);
  }
  // the trx_id is a 5 digit number, it should be unique for each transaction.
  m_trx_id.insert(0, 5 - m_trx_id.size(), '0');
  return key + "_" + m_trx_id + "_temp";
}

int ObjectDirectory::exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP, y);

  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

int ObjectDirectory::set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  /* For existing keys, call get method beforehand. 
     Sets completely overwrite existing values. */
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::WRITE_OP, y);

  std::string endpoint;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("objName");
  redisValues.push_back(object->objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(object->bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(object->creationTime); 
  redisValues.push_back("dirty");
  int ret = -1;
  if ((ret = check_bool(std::to_string(object->dirty))) != -EINVAL) {
    object->dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(object->dirty));
  redisValues.push_back("hosts");

  for (auto const& host : object->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint);
  redisValues.push_back("etag");
  redisValues.push_back(object->etag);
  redisValues.push_back("objSize");
  redisValues.push_back(std::to_string(object->size));
  redisValues.push_back("userId");
  redisValues.push_back(object->user_id);
  redisValues.push_back("displayName");
  redisValues.push_back(object->display_name);

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push_range("HSET", key, redisValues);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  	}

  return 0;
}

int ObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP,y);

  std::vector<std::string> fields;
  ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");
  fields.push_back("etag");
  fields.push_back("objSize");
  fields.push_back("userId");
  fields.push_back("displayName");

  try {
    boost::system::error_code ec;
    response< std::vector<std::string> > resp;
    request req;
    req.push_range("HMGET", key, fields);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values returned." << dendl;
      return -ENOENT;
    }

    using Fields = rgw::d4n::ObjectFields;
    object->objName = std::get<0>(resp).value()[std::size_t(Fields::ObjName)];
    object->bucketName = std::get<0>(resp).value()[std::size_t(Fields::BucketName)];
    object->creationTime = std::get<0>(resp).value()[std::size_t(Fields::CreationTime)];
    object->dirty = (std::stoi(std::get<0>(resp).value()[std::size_t(Fields::Dirty)]) != 0);
    boost::split(object->hostsList, std::get<0>(resp).value()[std::size_t(Fields::Hosts)], boost::is_any_of("_"));
    object->etag = std::get<0>(resp).value()[std::size_t(Fields::Etag)];
    object->size = std::stoull(std::get<0>(resp).value()[std::size_t(Fields::ObjSize)]);
    object->user_id = std::get<0>(resp).value()[std::size_t(Fields::UserID)];
    object->display_name = std::get<0>(resp).value()[std::size_t(Fields::DisplayName)];
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
int ObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{//TODO can we skip it?
  if(m_d4n_trx)
    m_d4n_trx->get_trx_id(dpp,conn,y);
  
  std::string key = build_index(object);
  auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };
  std::string copyKey = build_index(&copyObj);

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
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values copied." << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int ObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{//TODO it is not cover (rename , and to delete upon end-trx)
  std::string key = build_index(object);
  ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("DEL", key);//rename the key to a temp key, and delete the temp key upon end transaction, or rename back to original key name.

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values deleted." << dendl;
      return -ENOENT;
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0; 
}

int ObjectDirectory::update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y)
{//TODO what should be done here? (temp-read and temp-write)
  int ret = -1;
  if(m_d4n_trx)
    m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp, conn, key, D4NTransaction::redis_operation_type::READ_OP, y);

  if ((ret = exist_key(dpp, object, y))) {
    try {
      if (field == "hosts") {
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "ObjectDirectory::" << __func__ << "(): Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response<std::string> resp;
	request req;
	req.push("HGET", key, field);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	/* If entry exists, it should have at least one host */
	std::get<0>(resp).value() += "_";
	std::get<0>(resp).value() += value;
	value = std::get<0>(resp).value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
          bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      m_d4n_trx->is_trx_started(dpp, conn, key, D4NTransaction::redis_operation_type::WRITE_OP, y);

      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): Object does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
}

int ObjectDirectory::zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP,y);


  try {
    boost::system::error_code ec;
    if (pipeline && pipeline->is_pipeline()) {
      request& req = pipeline->get_request();
      req.push("ZADD", key, "CH", std::to_string(score), member);
    } else {
      request req;
      req.push("ZADD", key, "CH", std::to_string(score), member);

      response<std::string> resp;
      redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

      if (ec) {
        ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
        return -ec.value();
      }

      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}

int ObjectDirectory::zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", key, start, stop);

    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::WRITE_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", key, member);
    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -ENOENT;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::WRITE_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREMRANGEBYSCORE", key, std::to_string(min), std::to_string(max));
    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() == "0") {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() No element removed!" << dendl;
      return -ENOENT;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{//TODO : skip it?
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  key = key + "_versioned_epoch";
  uint64_t value;
  try {
    boost::system::error_code ec;
    request req;
    req.push("INCR", key);
    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    value = std::stoull(std::get<0>(resp).value());

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return value;
}

int ObjectDirectory::zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(object);
  m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP,y);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANK", key, member);
    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    index = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

int D4NTransaction::clone_key_for_transaction(std::string key_source, std::string key_destination, std::shared_ptr<connection> conn, optional_yield y)
{
  // TODO to validate that cloned key do not exists
  //running the loaded script 
  try {//this should be done only once, the first time the transaction is started. 
	boost::system::error_code ec;
	response<std::string> resp;
	request req;
	//TODO the key should clone only once, and the key should be deleted upon end transaction.
	req.push("EVALSHA", m_evalsha_clone_key, "2", key_source, key_destination);

	redis_exec(conn, ec, req, resp, y);

	// to handle the case where the source key is not exists --> no need to clone it.
	// in case the destination key already exists, it should not happend since the key is unique for each transaction.
	if (ec) {
	    ldout(g_ceph_context, 0) << "D4NTransaction::" << __func__ << "() ERROR: " << ec.value() << dendl;
	    return -ec.value();
	}

	} catch (std::exception &e) {
		ldout(g_ceph_context, 0) << "D4NTransaction::" << __func__ << "() ERROR: " << e.what() << dendl;
		return -EINVAL;
	}

  return 0;
}

void D4NTransaction::clear_temp_keys()
{
  m_original_key.clear();
  m_temp_key_read.clear();
  m_temp_key_write.clear();
  m_temp_key_test_write.clear();

  m_temp_read_keys.clear();
  m_temp_write_keys.clear();
  m_temp_test_write_keys.clear();
}

bool D4NTransaction::is_trx_started(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn,std::string &key,redis_operation_type op, optional_yield y)
{//TODO this method could reuse the ObjectDirectory::is_trx_started, sould placed on the base class.
	if(trxState != TrxState::STARTED) {
		return false;
	}

	init_trx(dpp,conn,y);

	if(op == redis_operation_type::READ_OP){
	  auto rc = clone_key_for_transaction(m_original_key, m_temp_key_read, conn, y);
	  if (rc != 0 ) {
	    //if the key is not exists, it should not be cloned.
	    //TODO to handle the case where the clone key already exists.(it should not happend since the key is unique for each transaction).
	    ldpp_dout(dpp, 0) << "Directory::is_trx_started failed to clone key for read operation" << dendl;
	    return false;
	  }

	  //m_temp_read_keys stores the temp key that is used for read operations. a single transaction can have multiple read keys.
	  m_temp_read_keys.insert(m_temp_key_read);
	  key = m_temp_key_read;
	}
	else if(op == redis_operation_type::WRITE_OP){
	  //upon end transaction, the m_temp_key_write should be renamed to the original key.
	  ldpp_dout(dpp, 0) << "Directory::is_trx_started cloning " << m_original_key << " into " << m_temp_key_write << dendl;

	  clone_key_for_transaction(m_original_key, m_temp_key_write, conn, y);
	  //m_temp_write_keys stores the temp key that is used for write operations. a single transaction can have multiple write keys.
	  m_temp_write_keys.insert(m_temp_key_write);

	  //upon end transaction, the m_temp_key_test_write should be compared to the originl key.
	  ldpp_dout(dpp, 0) << "Directory::is_trx_started cloning " << m_original_key << " into " << m_temp_key_test_write << dendl;
	  clone_key_for_transaction(m_original_key, m_temp_key_test_write, conn, y);
	  // the same as m_temp_write_keys, but for test write operations.
	  m_temp_test_write_keys.insert(m_temp_key_test_write);
	 
	  //the key that is used for write operations. 
	  key = m_temp_key_write;
	}
	return true;
}

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  std::string key = block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);

  if(m_d4n_trx)
      m_d4n_trx->create_rw_temp_keys(key);

  return key;
}

int BlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  if(m_d4n_trx)
    m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  if(m_d4n_trx)
    m_d4n_trx->is_trx_started(dpp,conn,key, D4NTransaction::redis_operation_type::READ_OP, y);

  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return false;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}


int save_trx_info(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn, std::string key, std::string value, optional_yield y)
{
  //return 0;//NOTE: skip for now (investigate blocking issues)
  // the key contains debug information about the transaction, and the loaded script sha.
  try {
    if(dpp){ldpp_dout(dpp, 0) << "save_trx_info" << "saving " << key << ":" << value << dendl;}
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push("HSET", "trx_debug", key, value);

    redis_exec(conn, ec, req, resp, y);
    if (ec) {
      if(dpp){ldpp_dout(dpp, 0) << "save_trx_info" << "had failed to save data " << ec.what() << dendl;}
      return -ec.value();
    }
    } catch (std::exception &e) {
      if(dpp){ldpp_dout(dpp, 0) << "save_trx_info raise an exception "  << e.what() << dendl;}
      return -EINVAL;
    }

  return 0;
}

void D4NTransaction::start_trx()
{
	
  if(trxState == TrxState::STARTED) {
    ldout(g_ceph_context, 0) << "D4NTransaction::" << __func__ << "(): Transaction state should not be active (STARTED)" << dendl;
    return;
  }
  // NOTE: check whether at this point its better to get the transaction id from the redis server or generate a unique id.
  trxState = TrxState::STARTED;
}

//the lua sript should check whether the destination key exists or not.
//in case the destination key exists, the script should return -1.
//in case the destination key does not exists, it should clone the source key into the destination key.
std::string lua_script_clone_keys = R"(
local function clone_key(key_source, key_destination)
		local keyType_source = redis.call('TYPE', key_source).ok
		if keyType_source == 'none' then
				redis.log(redis.LOG_NOTICE,"key source does not exists: " .. key_source .. " cannot clone to: " .. key_destination)
				return -2
		end
		local keyType = redis.call('TYPE', key_destination).ok
		if keyType == 'none' then
				redis.log(redis.LOG_NOTICE,"key does not exists: " .. key_destination .. " cloning key: " .. key_source)
				redis.call('COPY', key_source, key_destination)
				local exist_status = redis.call('EXISTS', key_destination)
				redis.log(redis.LOG_NOTICE,"key exists: " .. key_destination .. " status: " .. exist_status)
				return 0
		else
				redis.log(redis.LOG_NOTICE,"key already exists: " .. key_destination)
				return -1
		end
end

return clone_key(KEYS[1], KEYS[2])
)";

int D4NTransaction::get_clone_script(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn,optional_yield y)
{
  //get m_evalsha_clone_key from redis server. 
  try {
      boost::system::error_code ec;
      response< std::optional<std::vector<std::string>> > resp;
      request req;

      req.push("HGET", "trx_debug", "clone_key_sha");

      redis_exec(conn, ec, req, resp, y);
      if (ec) {
	ldpp_dout(dpp, 0) << "Directory::start_trx" << "failed to get clone_key_sha ec = " << ec.value() << dendl;
	return -ec.value();
      }

      if (std::get<0>(resp).value().has_value()) {
	m_evalsha_clone_key = std::get<0>(resp).value().value()[0];
	ldpp_dout(dpp, 0) << "Directory::start_trx got clone_key_sha = " << m_evalsha_clone_key << dendl;
	return 0;
      }

  } catch (std::exception &e) {
	ldpp_dout(dpp, 0) << "Directory::start_trx" << "failed to get clone_key_sha " << "() ERROR: " << e.what() << dendl;
	return -EINVAL;
  }

  return 0;
}

int D4NTransaction::init_trx(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn,optional_yield y)
{
	 //TODO a singletone pattern should be used to load the lua script only once per process lifetime.
	
  if(trxState != TrxState::STARTED) {
    return 0;
  }
  
  ldpp_dout(dpp, 0) << "Directory::start_trx this = " << this << dendl;

  // this function is called each time a read or write operation is done, thus, the lua script should be loaded only once.
  if(m_evalsha_clone_key.empty()) {
  	//get m_evalsha_clone_key from redis server. 
  	get_clone_script(dpp,conn,y);
  	if(!m_evalsha_clone_key.empty()) return 0;
	//else load the script.
  } else {
  	//the script is already loaded.
  	return 0;
  }

  //it is not loaded yet, load it now.
  try{
      // loading the lua script for cloning the keys.
      boost::system::error_code ec;
      response< std::optional<std::vector<std::string>> > resp;
      request req;

      req.push("script","load", lua_script_clone_keys); 

      redis_exec(conn, ec, req, resp, y);
      if (ec) {
	ldpp_dout(dpp, 0) << "Directory::start_trx" << "failed to load copy script ec = " << ec.value() << dendl;
	return -ec.value();
      }

      if (std::get<0>(resp).value().has_value()) {
	m_evalsha_clone_key = std::get<0>(resp).value().value()[0];
    	//save the loaded script sha in redis server, so it could be retrieved later.
    	save_trx_info(dpp, conn, "clone_key_sha", m_evalsha_clone_key, y);
	return 0;
      } else {
	ldpp_dout(dpp, 0) << "Directory::start_trx" << "failed to load script, no sha returned" << dendl;
	return -EINVAL;
      }

    } catch (std::exception &e) {
    	ldpp_dout(dpp, 0) << "Directory::start_trx" << "failed to load script " << "() ERROR: " << e.what() << dendl;
    	return -EINVAL;
    }
 
  return 0;
}

std::string lua_script_end_trx  = R"(

--- this LUA script is used to compare the cloned keys with the original keys, in case keys are different, it means that other request has updated the key.
--- the script recives the keys that are related to the unique transaction.
--- according to the key suffix (_read, _write, _test_write), the script will compare the keys and decide whether to rollback the transaction or not.

--- it should note that each key is unique for each transaction.
--- thus, upon writing to the key it is gurenteed that no other transaction is writing to the same key.
--- the same is with reading the key, it is gurenteed that no other transaction is writing to the same key.
--- upon end of the transaction, cloned key are compared to the original key, in case they are different, it means that other transaction has updated the key.

--- log message : redis.log(redis.LOG_NOTICE,"message"), are written to the log file, the log-file is defined in the redis.conf file.
--- redis monitor can be used to monitor redis server, "redis/valkey-cli monitor"

local allComparisonsSuccessful = true

local function log_message(message)
--- TODO use runtime configuration to enable/disable logging (the runtime configuration setting should retrieve once and stored in a global variable)
	redis.log(redis.LOG_NOTICE,message)
end

local function compareTables(tbl1, tbl2)
    if not tbl1 then
      log_message("compareTables : tbl1 is nil")
      return false
    end

    if not tbl2 then
      log_message("compareTables : tbl2 is nil")
      return false
    end

    if #tbl1 ~= #tbl2 then 
	log_message("tables are not equal in size")
	return false 
    end
    local set1, set2 = {}, {}
    for _, v in ipairs(tbl1) do set1[v] = (set1[v] or 0) + 1 end
    for _, v in ipairs(tbl2) do set2[v] = (set2[v] or 0) + 1 end
    for k, v in pairs(set1) do
        if set2[k] ~= v then
	  log_message("tables are not equal in values k,v: " .. k .. " " .. v)
	  return false 
	end
    end
    return true
end

local function getKeyValues(key)
    local keyType = redis.call('TYPE', key).ok
    if keyType == 'string' then
        return {redis.call('GET', key)}
    elseif keyType == 'list' then
        return redis.call('LRANGE', key, 0, -1)
    elseif keyType == 'set' then
        return redis.call('SMEMBERS', key)
    elseif keyType == 'zset' then
        return redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
    elseif keyType == 'hash' then
        return redis.call('HGETALL', key)
    else
	log_message("keyType is not supported: " .. keyType .. " key: " .. key)
        return nil
    end
end

--- for debuging purposes
local function create_timestamp()
  local time = redis.call('TIME')
  return time[1] .. "." .. time[2]
end

local function save_trx_info(key, value)
  redis.call('HSET', "trx_debug", key, value)
end


-- delete keys from the input keys(set by D4N application), delete by suffix
local function deleteKeysWithSuffix(suffix)
    for _, key in ipairs(KEYS) do --KEYS conatain keys that are related to the unique transaction send by D4N application
        if key:match(suffix .. "$") then
            redis.call('DEL', key)
	    log_message("deleted key: " .. key)
        end
    end
end

local function rename_all_write_keys()
    for _, key in ipairs(KEYS) do
      if key:match("_temp_write$") then
	local baseKey = key:gsub("_%d%d%d%d%d_temp_write$", "")
	local trx_id = string.sub(key,string.find(key,"_%d%d%d%d%d_"))
	local tempWriteKey = baseKey .. trx_id .. "temp_write"
	local testWriteKey = baseKey .. trx_id .. "temp_test_write"
	if redis.call('EXISTS', tempWriteKey) ~= 0 then
		-- the clone key replaces the original key
	  redis.call('RENAME', tempWriteKey, baseKey)
	end
	redis.call('DEL', testWriteKey)
      end
    end
end

log_message("START: end transaction")
for _, key in ipairs(KEYS) do

    log_message("IN: the for-loop -- key: " .. key)
    
    if allComparisonsSuccessful == false then
	log_message("allComparisonsSuccessful is false, breaking the loop")
	break
    end

    if key:match("_temp_read$") then
-- cut the suffix from the key
	local baseKey = key:gsub("_%d%d%d%d%d_temp_read$", "")

	if redis.call('EXISTS', baseKey) == 0 then
	  log_message("base key does not exist for <KEY>_temp_read")
	  break
	end

-- cut the transaction id from the key
	local trx_id = string.sub(key,string.find(key,"_%d%d%d%d%d_"))
        local values1 = getKeyValues(key)
        local values2 = getKeyValues(baseKey)
	log_message("read operation" .. " baseKey: " .. baseKey .. " key: " .. key .. " trx_id: " .. trx_id)

	log_message("compring 2 keys of baseKey: " .. baseKey .. " key: " .. key .. " trx_id: " .. trx_id)
        if not compareTables(values1, values2) then
-- in case the read key is not the same as the base key, it means the base key has been written to
-- the transaction should be rolled back
	      log_message("<KEY>_temp_read **NOT EQUAL** to " .. "baseKey: " .. baseKey .. " key: " .. key .. " trx_id: " .. trx_id)
	      allComparisonsSuccessful = false
	      log_message("allComparisonsSuccessful is false, breaking the loop" .. " baseKey: " .. baseKey .. " key: " .. key .. " trx_id: " .. trx_id)
        end

	if allComparisonsSuccessful == true then
	  log_message("<KEY>_temp_read is OK " .. "baseKey: " .. baseKey .. " key: " .. key .. " trx_id: " .. trx_id)
	end	

    elseif key:match("_temp_test_write$") then
	    -- skip the _temp_test_write keys, the _temp_write keys is the one that should be processed
	    log_message(" do nothing with _temp_test_write key: " .. key)
    elseif key:match("_temp_write$") then

	    -- NOTE: is it possible to have only _temp_test_write without _temp_write? (no,both are set by D4N application, at the same time)

        local baseKey = key:gsub("_%d%d%d%d%d_temp_write$", "")
	local trx_id = string.sub(key,string.find(key,"_%d%d%d%d%d_"))
	-- testkey is the key that is used to test if the base key has been written to
        local testKey = baseKey .. trx_id .. "temp_test_write"
	log_message("write operation" .. " baseKey: " .. baseKey .. " key: " .. key .. " trx_id: " .. trx_id)

        log_message("in _temp_write :  baseKey: " .. baseKey .. " testKey: " .. testKey .. " trx_id: " .. trx_id)

-- in case the base key does not exist, we can rename the write keys to the base key
	if redis.call('EXISTS', baseKey) == 0 then 
	  log_message("base key does not exist for <KEY>_temp_write")
	else

	  local values1 = getKeyValues(baseKey)
	  local values2 = getKeyValues(testKey)

	  log_message("compring 2 keys of baseKey: " .. baseKey .. " testKey: " .. testKey .. " trx_id: " .. trx_id)
	  if compareTables(values1, values2) then
-- the test-write key is the same as the base key, it means no one has written to the base key
	      log_message("<KEY>_temp_write is safe for commit " .. "baseKey: " .. baseKey .. " testKey: " .. testKey .. " trx_id: " .. trx_id)
	  else
-- in case the test-write key is not the same as the base key, it means the base key has been written to
-- the transaction should be rolled back
	      log_message("temp_write branch **NOT EQUAL** " .. "baseKey: " .. baseKey .. " testKey: " .. testKey .. " trx_id: " .. trx_id)
	      allComparisonsSuccessful = false
	      log_message("allComparisonsSuccessful is false, breaking the loop" .. " baseKey: " .. baseKey .. " testKey: " .. testKey .. " trx_id: " .. trx_id)
	  end -- end of if values1 and values2 and compareTables(values1, values2)
	end -- end of if redis.call('EXISTS', baseKey) == 0
    end -- end of if key:match("_temp_write$")
end -- end of for _, key in ipairs(KEYS)


if allComparisonsSuccessful == true then
-- the rename of the write keys should be done only if all keys are consistent

	log_message("allComparisonsSuccessful is true, deleting all temp-read keys")
	deleteKeysWithSuffix("_temp_read") 

	log_message("allComparisonsSuccessful is true, renaming all write keys")
	rename_all_write_keys()
	return {true, "Processing complete - commit transaction"}
else
-- the transaction should be rolled back, all temp keys should be deleted
	log_message("allComparisonsSuccessful is false")
	deleteKeysWithSuffix("_temp_write")
	deleteKeysWithSuffix("_temp_test_write")
	deleteKeysWithSuffix("_temp_read") 
	return {false, "Processing failed - rolling back"}
end


)";

std::string D4NTransaction::get_trx_id(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn, optional_yield y)
{

  //purpose : the trx_id makes sure that the transaction is unique, and it is used to create the temporary keys.
  if(!m_trx_id.empty()) {
    return m_trx_id;
  }

  try {
    boost::system::error_code ec;
    response<std::string> resp;
    request req;
    req.push("INCR", "trx_id");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "Directory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return std::to_string(-ec.value());
    }

    m_trx_id = std::get<0>(resp).value();

    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "Directory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return std::string("-EINVAL");
    }

return m_trx_id;
}

std::string D4NTransaction::get_end_trx_script(const DoutPrefixProvider* dpp, std::shared_ptr<connection> conn, optional_yield y)
{
  try {
    boost::system::error_code ec;
    response<std::string> resp;
    request req;
    req.push("HGET", "trx_debug", "m_evalsha_end_trx");
  
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldout(g_ceph_context,0) << "get_end_trx_script::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return std::string("");
    }
      
    if (std::get<0>(resp).value().empty()) {
      ldout(g_ceph_context,0) << "get_end_trx_script:: m_evalsha_end_trx is empty " << dendl;
      return std::string("");
    }

    m_evalsha_end_trx = std::get<0>(resp).value();

    } catch (std::exception &e) {
      ldout(g_ceph_context,0) << "get_end_trx_script::" << __func__ << "() ERROR: " << e.what() << dendl;
      return std::string("");
    }

  return m_evalsha_end_trx;
}

int D4NTransaction::end_trx(const DoutPrefixProvider* dpp, std::shared_ptr<connection> conn, optional_yield y)
{
	//TODO upon calling to a LUA script, it should be guaranteed that no other transaction is running at the same time with the same trx-id.
	//or it should be guaranteed that the transaction is unique across multiple D4N transactions.
	
  if(trxState != TrxState::STARTED) {
    ldout(g_ceph_context, 0) << "Directory::end_trx trx is not started, skipping end_trx" << dendl;
    return 0;
    //TODO : it should not happen, to consider throwing an exception.
  }
  trxState = TrxState::ENDED;

  
   //the end_trx is currently called from the destructor, and the dpp is not available.
  if(!dpp) {ldout(g_ceph_context, 0) << "Directory::end_trx this = " << this << dendl;}
  if(!dpp) {ldout(g_ceph_context, 0) << "Directory::end_trx evalsha " << m_evalsha_end_trx << dendl;}
  //save_trx_info(dpp,conn, "test_debug_key", "test_debug_value", y);

    // load the lua script that implements the end of the transaction.
    if(get_end_trx_script(dpp,conn,y).empty()) {
      try{
	  boost::system::error_code ec;
	  response< std::optional<std::vector<std::string>> > resp;
	  request req;
	  req.push("script","load", lua_script_end_trx);
	  redis_exec(conn, ec, req, resp, y);
	  if (ec) {
	    if(!dpp){ldout(g_ceph_context, 0) << "Directory::end_trx failed to load script " << " ERROR: " << ec.what() << dendl;}
	    return -ec.value();
	  }
	  m_evalsha_end_trx = std::get<0>(resp).value().value()[0];
	  if(!dpp){ldout(g_ceph_context, 0) << "Directory::end_trx loading evalsha script = " << "evalsha " << m_evalsha_end_trx << dendl;}
   
	  save_trx_info(dpp,conn, "m_evalsha_end_trx", m_evalsha_end_trx, y);	
      } catch(std::exception &e){
	if(!dpp){ldout(g_ceph_context, 0) << "Directory::end_trx failed to load script " << " ERROR: " << e.what() << dendl;}
	return -EINVAL;
      }
    }

  //running the loaded script 
  try {
    boost::system::error_code ec;
    response<bool,std::string> resp; //the response RESP3 should contain a tuple of <bool status, string result>
    request req;

    unsigned int num_keys = m_temp_read_keys.size() + m_temp_write_keys.size() + m_temp_test_write_keys.size();
		
    std::string debug_all_keys;
    std::list<std::string> trx_keys;
    trx_keys.push_back(std::to_string(num_keys));
    //concatenate all keys into a single string into trx_keys, the end-trx script will use this string to compare the keys (read,write,test-write).
    for(auto const& key : m_temp_read_keys) {
      debug_all_keys += key + " ";
      trx_keys.push_back(key);
    }
    for (auto const& key : m_temp_write_keys) {
      debug_all_keys += key + " ";
      trx_keys.push_back(key);
    }
    for (auto const& key : m_temp_test_write_keys) {
      debug_all_keys += key + " ";
      trx_keys.push_back(key);
    }		

    if(num_keys == 0) {
	    //TODO how it happens that no keys are set?
      ldout(g_ceph_context, 0) << "Directory::end_trx no keys to compare, skipping end_trx script" << dendl;
      return 0;
    }

    ldout(g_ceph_context, 0) << "Directory::end_trx running evalsha script = " 
	    << "evalsha " << m_evalsha_end_trx << " num of keys " << num_keys 
	    << " with the following keys " << debug_all_keys << dendl;

    //the keys that are passed to the script are the keys that are related to the unique transaction.
    req.push_range("EVALSHA",m_evalsha_end_trx, trx_keys);
    redis_exec(conn, ec, req, resp, y);

    //error handling
      if (ec) {
	std::ostringstream err_msg;
	err_msg	<< "Directory::end_trx the end-trx script had failed this = " << this ;

	//system level error
	if (ec.category() == boost::system::system_category()) {
        		ldout(g_ceph_context, 0) << err_msg.str() << " System error: " << ec.message()
                  		<< " (errno=" << ec.value() << dendl; 
	//boost redis error
    	} else if (ec.category().name() == std::string("boost.redis")) {
        		ldout(g_ceph_context,0) << err_msg.str() << " Redis error: " << ec.message()
                  		<< " (boost.redis code=" << ec.value() << dendl;

   	} else {//TODO what are the other error categories?
        		ldout(g_ceph_context,0) << err_msg.str() << " Other error: " << ec.message()
                  		<< " (category=" << ec.category().name()
                  		<< ", value=" << ec.value() << dendl;
    	}

	return -ec.value();
      }

      // the response contain whether the transaction was successful or not <bool status, string result>
      // Extract values
	bool status = std::get<0>(resp).value();
	std::string result = std::get<1>(resp).value();

      //could be compile-time error or no matching script.
      if (result.starts_with("ERR") || result.starts_with("NOSCRIPT") || result.starts_with("WRONGTYPE")) {
	 ldout(g_ceph_context, 0) << "Directory::end_trx the end-trx script had failed this = " << this << "with result =" << result << dendl;
         return -EINVAL;
      }

      if(status == 0) {
	//the transaction had failed, it was rolled back.
	ldout(g_ceph_context, 0) << "Directory::end_trx the end-trx script had rolled back the transaction this = " << this << "  " << result << dendl;
	return -EINVAL;
      }


    } catch (std::exception &e) {
      ldout(g_ceph_context, 0) << "Directory::end_trx the end-trx script had failed this = " << this << "with exception = " << e.what() << dendl;
      return -EINVAL;
    }

    ldout(g_ceph_context, 0) << "Directory::end_trx the end-trx script had finished successfully this = " << this << dendl;

return 0;
}

template<SeqContainer Container>
int BlockDirectory::set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y)
{
  std::string hosts;
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("blockID");
  redisValues.push_back(std::to_string(block.blockID));
  redisValues.push_back("version");
  redisValues.push_back(block.version);
  redisValues.push_back("deleteMarker");
  int ret = -1;
  if ((ret = check_bool(std::to_string(block.deleteMarker))) != -EINVAL) {
    block.deleteMarker = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value for delete marker" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block.deleteMarker));
  redisValues.push_back("size");
  redisValues.push_back(std::to_string(block.size));
  redisValues.push_back("globalWeight");
  redisValues.push_back(std::to_string(block.globalWeight));
  redisValues.push_back("objName");
  redisValues.push_back(block.cacheObj.objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(block.cacheObj.bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(block.cacheObj.creationTime);
  redisValues.push_back("dirty");
  if ((ret = check_bool(std::to_string(block.cacheObj.dirty))) != -EINVAL) {
    block.cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block.cacheObj.dirty));
  redisValues.push_back("hosts");

  hosts.clear();
  for (auto const& host : block.cacheObj.hostsList) {
    if (hosts.empty())
    hosts = host + "_";
    else
    hosts = hosts + host + "_";
  }

  if (!hosts.empty())
  hosts.pop_back();

  redisValues.push_back(hosts);
  redisValues.push_back("etag");
  redisValues.push_back(block.cacheObj.etag);
  redisValues.push_back("objSize");
  redisValues.push_back(std::to_string(block.cacheObj.size));
  redisValues.push_back("userId");
  redisValues.push_back(block.cacheObj.user_id);
  redisValues.push_back("displayName");
  redisValues.push_back(block.cacheObj.display_name);

  return 0;
}

int BlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline)
{
  /* For existing keys, call get method beforehand. 
     Sets completely overwrite existing values. */
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  std::vector<std::string> redisValues;

  auto ret = set_values(dpp, *block, redisValues, y);
  if (ret < 0) {
    return ret;
  }

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    if (pipeline && pipeline->is_pipeline()) {
      request& req = pipeline->get_request();
      req.push_range("HSET", key, redisValues);
    } else {
      request req;
      req.push_range("HSET", key, redisValues);

      redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);
      if (ec) {
        ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
        return -ec.value();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  request req;
  for (auto block : blocks) {
    std::string key = build_index(&block);
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    //std::string hosts;
    std::list<std::string> redisValues;
    auto ret = set_values(dpp, block, redisValues, y);
    if (ret < 0) {
      return ret;
    }
    req.push_range("HSET", key, redisValues);
  }

  try {
    boost::system::error_code ec;
    boost::redis::generic_response resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

template<typename T, typename Seq>
struct expander;

template<typename T, std::size_t... Is>
struct expander<T, std::index_sequence<Is...>> {
template<typename E, std::size_t>
using elem = E;

using type = boost::redis::response<elem<T, Is>...>;
};

template <size_t N, class Type>
struct redis_response
{
  using type = typename expander<Type, std::make_index_sequence<N>>::type;
};

template <typename Integer, Integer ...I, typename F>
constexpr void constexpr_for_each(std::integer_sequence<Integer, I...>, F &&func)
{
    (func(std::integral_constant<Integer, I>{}) , ...);
}

template <auto N, typename F>
constexpr void constexpr_for(F &&func)
{
    if constexpr (N > 0)
    {
        constexpr_for_each(std::make_integer_sequence<decltype(N), N>{}, std::forward<F>(func));
    }
}

template <typename T>
void parse_response(T t, std::vector<std::vector<std::string>>& responses)
{
    constexpr_for<std::tuple_size_v<T>>([&](auto index)
    {
      std::vector<std::string> empty_vector;
      constexpr auto i = index.value;
      if (std::get<i>(t).value().has_value()) {
        if (std::get<i>(t).value().value().empty()) {
          responses.emplace_back(empty_vector);
        } else {
          responses.emplace_back(std::get<i>(t).value().value());
        }
      } else {
        responses.emplace_back(empty_vector);
      }
    });
}

//explicit instantiation for 100 elements
template int BlockDirectory::get<100>(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);

template <size_t N>
int BlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{//TODO : what is that? does it mean multiple transactions (as a single one)?
  request req;
  typename redis_response<N, std::optional<std::vector<std::string>>>::type resp;
  for (auto block : blocks) {
    std::string key = build_index(&block);
    std::vector<std::string> fields;
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    fields.push_back("blockID");
    fields.push_back("version");
    fields.push_back("deleteMarker");
    fields.push_back("size");
    fields.push_back("globalWeight");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("hosts");
    fields.push_back("etag");
    fields.push_back("objSize");
    fields.push_back("userId");
    fields.push_back("displayName");

    try {
      req.push_range("HMGET", key, fields);
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } //end - for

  try {
    boost::system::error_code ec;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  std::vector<std::vector<std::string>> responses;
  parse_response<decltype(resp)>(resp, responses);

  for (size_t i = 0; i < blocks.size(); i++) {
    CacheBlock* block = &blocks[i];
    auto vec = responses[i];
    if (vec.empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned for key=" << build_index(block) << dendl;
      continue;
    }

    using Fields = rgw::d4n::BlockFields;
    block->blockID = std::stoull(vec[std::size_t(Fields::BlockID)]);
    block->version = vec[std::size_t(Fields::Version)];
    block->deleteMarker = (std::stoi(vec[std::size_t(Fields::DeleteMarker)]) != 0);
    block->size = std::stoull(vec[std::size_t(Fields::Size)]);
    block->globalWeight = std::stoull(vec[std::size_t(Fields::GlobalWeight)]);
    block->cacheObj.objName = vec[std::size_t(Fields::ObjName)];
    block->cacheObj.bucketName = vec[std::size_t(Fields::BucketName)];
    block->cacheObj.creationTime = vec[std::size_t(Fields::CreationTime)];
    block->cacheObj.dirty = (std::stoi(vec[std::size_t(Fields::Dirty)]) != 0);
    boost::split(block->cacheObj.hostsList, vec[std::size_t(Fields::Hosts)], boost::is_any_of("_"));
    block->cacheObj.etag = vec[std::size_t(Fields::Etag)];
    block->cacheObj.size = std::stoull(vec[std::size_t(Fields::ObjSize)]);
    block->cacheObj.user_id = vec[std::size_t(Fields::UserID)];
    block->cacheObj.display_name = vec[std::size_t(Fields::DisplayName)];
  }

  return 0;
}

int BlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::READ_OP,y);

  std::vector<std::string> fields;
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  fields.push_back("blockID");
  fields.push_back("version");
  fields.push_back("deleteMarker");
  fields.push_back("size");
  fields.push_back("globalWeight");

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");
  fields.push_back("etag");
  fields.push_back("objSize");
  fields.push_back("userId");
  fields.push_back("displayName");

  try {
    boost::system::error_code ec;
    response< std::optional<std::vector<std::string>> > resp;
    request req;
    req.push_range("HMGET", key, fields);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().value().empty()) {
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values returned for key=" << key << dendl;
      return -ENOENT;
    } 

    block->blockID = std::stoull(std::get<0>(resp).value().value()[0]);
    block->version = std::get<0>(resp).value().value()[1];
    block->deleteMarker = (std::stoi(std::get<0>(resp).value().value()[2]) != 0);
    block->size = std::stoull(std::get<0>(resp).value().value()[3]);
    block->globalWeight = std::stoull(std::get<0>(resp).value().value()[4]);
    block->cacheObj.objName = std::get<0>(resp).value().value()[5];
    block->cacheObj.bucketName = std::get<0>(resp).value().value()[6];
    block->cacheObj.creationTime = std::get<0>(resp).value().value()[7];
    block->cacheObj.dirty = (std::stoi(std::get<0>(resp).value().value()[8]) != 0);
    boost::split(block->cacheObj.hostsList, std::get<0>(resp).value().value()[9], boost::is_any_of("_"));
    block->cacheObj.etag = std::get<0>(resp).value().value()[10];
    block->cacheObj.size = std::stoull(std::get<0>(resp).value().value()[11]);
    block->cacheObj.user_id = std::get<0>(resp).value().value()[12];
    block->cacheObj.display_name = std::get<0>(resp).value().value()[13];
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  boost::redis::generic_response resp;
  request req;
  for (auto block : blocks) {
    std::string key = build_index(&block);
    std::vector<std::string> fields;
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    fields.push_back("blockID");
    fields.push_back("version");
    fields.push_back("deleteMarker");
    fields.push_back("size");
    fields.push_back("globalWeight");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("hosts");
    fields.push_back("etag");
    fields.push_back("objSize");
    fields.push_back("userId");
    fields.push_back("displayName");

    try {
      req.push("HGETALL", key);
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } //end - for

  try {
    boost::system::error_code ec;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  //i is used to index blocks
  //j is used to keep a track of number of elements for aggregate type map or array
  auto i = 0, j = 0;
  bool field_key=true, field_val=false;
  std::string key, fieldkey, fieldval, prev_val;
  int num_elements = 0;
  for (auto& element : resp.value()) {
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): i is: " << i << dendl;
    CacheBlock* block = &blocks[i];
    std::string key = build_index(block);
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;
    if (element.data_type == boost::redis::resp3::type::array || element.data_type == boost::redis::resp3::type::map) {
      num_elements = element.aggregate_size;
      if (num_elements == 0) {
        i++;
        j = 0;
      }
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "() num_elements: " << num_elements << dendl;
      continue;
    } else {
      if (j < num_elements) {
        if (field_key && !field_val) {
          if (element.value == "blockID" || element.value == "version" || element.value == "deleteMarker" ||
              element.value == "size" || element.value == "globalWeight" || element.value == "objName" ||
              element.value == "bucketName" || element.value == "creationTime" || element.value == "dirty" ||
              element.value == "hosts" || element.value == "etag" || element.value == "objSize" ||
              element.value == "userId" || element.value == "displayName") {
            prev_val = element.value;
            ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "() field key: " << prev_val << dendl;
            field_key = false;
            field_val = true;
          }
          continue;
        } else {
          ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "() field val: " << element.value << dendl;
          if (prev_val == "blockID") {
            block->blockID = std::stoull(element.value);
          } else if (prev_val == "version") {
            block->version = element.value;
          } else if (prev_val == "deleteMarker") {
            block->deleteMarker = (std::stoi(element.value) != 0);
          } else if (prev_val == "size") {
            block->size = std::stoull(element.value);
          } else if (prev_val == "globalWeight") {
            block->globalWeight = std::stoull(element.value);
          } else if (prev_val == "objName") {
            block->cacheObj.objName = element.value;
          } else if (prev_val == "bucketName") {
            block->cacheObj.bucketName = element.value;
          } else if (prev_val == "creationTime") {
            block->cacheObj.creationTime = element.value;
          } else if (prev_val == "dirty") {
            block->cacheObj.dirty = (std::stoi(element.value) != 0);
          } else if (prev_val == "hosts") {
            boost::split(block->cacheObj.hostsList, element.value, boost::is_any_of("_"));
          } else if (prev_val == "etag") {
            block->cacheObj.etag = element.value;
          } else if (prev_val == "objSize") {
            block->cacheObj.size = std::stoull(element.value);
          } else if (prev_val == "userId") {
            block->cacheObj.user_id = element.value;
          } else if (prev_val == "displayName") {
            block->cacheObj.display_name = element.value;
          }
          j++;
          field_key= true;
          field_val = false;
          prev_val.clear();
        }
      }
      if (j == num_elements) {
        i++;
        j = 0;
      }
    }
  }
  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
int BlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{//TODO : what is the role of COPY in transaction?
  if(m_d4n_trx)
    m_d4n_trx->get_trx_id(dpp,conn,y);

  std::string key = build_index(block);
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>> 
    > resp;
    request req;
    req.push("MULTI");//TODO : obselete?
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values copied." << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int BlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
{
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    request req;
    req.push("DEL", key);
    response<int> resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);
    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values deleted for key=" << key << dendl;
      return -ENOENT;
    }
    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << std::endl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0; 
}

int BlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  int ret = -1;
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::READ_OP,y);

  if ((ret = exist_key(dpp, block, y))) {
    try {
      if (field == "hosts") { 
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "BlockDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response< std::optional<std::string> > resp;
	request req;
	req.push("HGET", key, field);

    	redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	/* If entry exists, it should have at least one host */
	std::get<0>(resp).value().value() += "_";
	std::get<0>(resp).value().value() += value;
	value = std::get<0>(resp).value().value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
          bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::WRITE_OP,y);
      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

    	redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
}

int BlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::READ_OP,y);
  //TODO this might be cloned already, thus it should be checked whether the key is cloned or not.
  
  try {
    {
      boost::system::error_code ec;
      response< std::optional<std::string> > resp;
      request req;
      req.push("HGET", key, "hosts");

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      if (std::get<0>(resp).value().value().empty()) {
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
	return -ENOENT;
      }

      std::string result = std::get<0>(resp).value().value();
      auto it = result.find(value);
      if (it != std::string::npos) { 
	result.erase(result.begin() + it, result.begin() + it + value.size());
      } else {
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Host was not found." << dendl;
	return -ENOENT;
      }

      if (result[0] == '_') {
	result.erase(0, 1);
      } else if (result.length() && result[result.length() - 1] == '_') {
	result.erase(result.length() - 1, 1);
      }

      if (result.length() == 0) /* Last host, delete entirely */
	return del(dpp, block, y); 

  value = result;
    }

    m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::WRITE_OP,y);

    {
      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, "hosts", value);

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::WRITE_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZADD", key, "CH", std::to_string(score), member);

    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}

int BlockDirectory::zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::READ_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Empty response" << dendl;
      return -EINVAL;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::READ_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Empty response" << dendl;
      return -EINVAL;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y)
{
  m_d4n_trx->get_trx_id(dpp,conn,y);
  std::string key = build_index(block);
  m_d4n_trx->is_trx_started(dpp,conn,key,D4NTransaction::redis_operation_type::WRITE_OP,y);

  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", key, member);
    response<std::string> resp;

    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int Pipeline::execute(const DoutPrefixProvider* dpp, optional_yield y)
{
  boost::redis::generic_response resp;
  try {
    boost::system::error_code ec;
    pipeline_mode = false;
    redis_exec_connection_pool(dpp, redis_pool, conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "Directory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "Directory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

} } // namespace rgw::d4n
