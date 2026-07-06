#pragma once

#include "rgw_common.h"
#include "rgw_asio_thread.h"
#include "driver/d4n/d4n_connection.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <boost/version.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <concepts>

namespace rgw::d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

inline int check_bool(std::string_view str) {
  if (str == "true" || str == "1") {
    return 1;
  } else if (str == "false" || str == "0") {
    return 0;
  } else {
    return -EINVAL;
  }
}

//FIXME: should be moved to redis directory
class RedisPool {
public:
    RedisPool(boost::asio::io_context* ioc, const boost::redis::config& cfg, std::size_t size)
        :  m_ioc(ioc),m_cfg(cfg) {
        for (std::size_t i = 0; i < size; ++i) {
            // Each connection gets its own strand
            auto strand = boost::asio::make_strand(*m_ioc);
            auto conn = std::make_shared<boost::redis::connection>(strand);
            m_pool.push_back(conn);
        }
    }

    ~RedisPool() {
      cancel_all();
    }

    std::shared_ptr<boost::redis::connection> acquire(const DoutPrefixProvider* dpp = nullptr) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);

	if (!m_is_pool_connected) {
		for(auto& it:m_pool) {
	    		auto conn = it;
#if BOOST_VERSION >= 108900
	    		conn->async_run(m_cfg, boost::asio::consign(boost::asio::detached, conn));
#else
	    		conn->async_run(m_cfg, {}, boost::asio::consign(boost::asio::detached, conn));
#endif
		}
	    m_is_pool_connected = true;
	}

        if (m_pool.empty()) {
		if (dpp) {
			maybe_warn_about_blocking(dpp);
		}
		//wait until m_pool is not empty
		m_cond_var.wait(lock, [this] { return !m_pool.empty(); });
        }
        auto conn = m_pool.front();
        m_pool.pop_front();
        return conn;
    }

    void release(std::shared_ptr<boost::redis::connection> conn) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        m_pool.push_back(conn);
	// Notify one waiting thread that a connection is available
	m_cond_var.notify_one();
    }

    int current_pool_size() const {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        return m_pool.size();
    }

    void cancel_all() {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        if(m_is_pool_connected) {
	for(auto& conn : m_pool) {
		conn->cancel();
        }
      }
    }

private:
    boost::asio::io_context* m_ioc;
    boost::redis::config m_cfg;
    std::deque<std::shared_ptr<boost::redis::connection>> m_pool;
    mutable std::mutex m_aquire_release_mtx;
    std::condition_variable m_cond_var;
    bool m_is_pool_connected{false};
};

class Pipeline {
  public:
    Pipeline(std::shared_ptr<boost::redis::connection>& conn, std::shared_ptr<RedisPool> redis_pool) : REDISconn(conn), redis_pool(redis_pool) {}
    void start() { pipeline_mode = true; }
    //executes all commands and sets pipeline mode to false
    int execute(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_pipeline() { return pipeline_mode; }
    request& get_request() { return req; }

  private:
    std::shared_ptr<boost::redis::connection> REDISconn;
    std::shared_ptr<RedisPool> redis_pool{nullptr};
    request req;
    bool pipeline_mode{false};
};

template<typename T>
concept SeqContainer =
requires(T& t, typename T::value_type v) {
    t.insert(v);
} || requires(T& t, typename T::value_type v) {
    t.push_back(v);
};

template<typename C>
concept AssociativeContainer = requires(C c, typename C::key_type k) {
    typename C::key_type;
    { c.find(k) } -> std::convertible_to<typename C::iterator>;
    { c.count(k) } -> std::convertible_to<std::size_t>;
};

enum class ObjectFields { // Fields stored in object directory 
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName,
  Acl
};

enum class BlockFields { // Fields stored in block directory 
  BlockID,
  Version, 
  DeleteMarker,
  Size,
  GlobalWeight,
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName,
  Acl
};

//Represents an object entry for ListObjects
struct CacheObject {
  std::string objName;
  std::string bucketId;
  std::string etag;
  uint64_t size; //total object size
  std::string creationTime;
  bool deleteMarker{false};
};

//Represents an Object version entry for ListObjectVersions
struct CacheObjectVersion {
  std::string objName;
  std::string bucketId;
  std::string version;
  std::string user_id;
  std::string display_name;
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty{false};
  std::unordered_set<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
  std::string etag; //etag needed for list objects
  uint64_t size; //total object size (and not block size), needed for list objects
  std::string user_id; // id of user, needed for list object versions
  std::string display_name; // display name of owner, needed for list object versions
  std::string acl;
  rgw::sal::Attrs attrs; // attrs for a head block
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID;
  std::string version;
  bool deleteMarker{false};
  uint64_t size; /* Block size in bytes */
  uint64_t globalWeight = 0; /* LFUDA policy variable */
  /* Blocks use the cacheObj's dirty and hostsList metadata to store their dirty flag values and locations in the block directory. */
};

class Directory {
public:
    Directory() = default;
    virtual ~Directory() = default;

    // Single field get/set
    virtual int get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val) = 0;

    virtual int set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       const std::string& val) = 0;

    // Multi-field get/set
    virtual int get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& key,
                            const std::vector<std::string>& fields,
                            std::map<std::string, std::string>& out_vals) = 0;

    virtual int set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& key,
                            const std::map<std::string, std::string>& vals) = 0;

    virtual int set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                     const std::string& key,
                                     const std::string& field,
                                     const std::string& val) = 0;

};


//Namespace to lexicographically order objects belonging to a bucket
//Should we rename to ObjectDirectory?
class BucketDirectory: virtual public Directory {
  public:
    BucketDirectory() = default;
    virtual ~BucketDirectory() = default;

    virtual int exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y) = 0;
    virtual int add_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, optional_yield y, Pipeline* pipeline=nullptr) = 0;
    virtual int remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y) = 0;
    virtual int list_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, optional_yield y) = 0;

  private:
};


//Namespace to order versions of an object in the order in which they were added, with the latest
//version appearing first
//Should we rename to ObjectVersionDirectory?
class ObjectDirectory: virtual public Directory {
  public:
    ObjectDirectory() = default;
    virtual ~ObjectDirectory() = default;
	
    virtual int exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) = 0;
    //version ordering is a function of creation time, hence adding creation time to the interface
    virtual int add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline=nullptr) = 0;
    virtual int remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y) = 0;
    //this can be removed and remove_version can be used instead
    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, optional_yield y) = 0;
    virtual int list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, optional_yield y) = 0;

  private:

  protected:
    //should this be made virtual override as derived classes may want to override it
    std::string build_index(const std::string& bucket_id, const std::string& obj_name);
};

//Namespace to store key, value pairs of a block
class BlockDirectory: virtual public Directory {
  public:
    BlockDirectory() = default;
    virtual ~BlockDirectory() = default;
    
	
    virtual int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) = 0;

    //Pipelined version of set
    virtual int set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) = 0;
    virtual int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline=nullptr) = 0;
    virtual int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) = 0;
    virtual int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) = 0;

    virtual int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) = 0;
    virtual int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y) = 0;
	
    virtual int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y) = 0;
	
  private:

  protected:
    std::string build_index(CacheBlock* block);
};

} // namespace rgw::d4n
