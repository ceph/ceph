#pragma once

#include "rgw_common.h"
#include "rgw_asio_thread.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <concepts>
#include <set>

namespace rgw { namespace d4n {

template<typename T>
  concept SeqContainer = requires(T& t, typename T::value_type v) {
      t.push_back(v);
  };

using boost::redis::connection;
class RedisPool {
public:
    RedisPool(boost::asio::io_context* ioc, const boost::redis::config& cfg, std::size_t size)
        :  m_ioc(ioc),m_cfg(cfg) {
        for (std::size_t i = 0; i < size; ++i) {
            // Each connection gets its own strand
            auto strand = boost::asio::make_strand(*m_ioc);
            auto conn = std::make_shared<connection>(strand);
            m_pool.push_back(conn);
        }
    }

    ~RedisPool() {
      cancel_all();
    }

    std::shared_ptr<connection> acquire(const DoutPrefixProvider* dpp = nullptr) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);

	if (!m_is_pool_connected) {
		for(auto& it:m_pool) {
	    		auto conn = it;
	    		conn->async_run(m_cfg, {}, boost::asio::consign(boost::asio::detached, conn));
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

    void release(std::shared_ptr<connection> conn) {
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
    std::deque<std::shared_ptr<connection>> m_pool;
    mutable std::mutex m_aquire_release_mtx;
    std::condition_variable m_cond_var;
    bool m_is_pool_connected{false};
};


namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

enum class ObjectFields { // Fields stored in object directory 
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName
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
  DisplayName
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
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID;
  std::string version;
  bool deleteMarker{false};
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  /* Blocks use the cacheObj's dirty and hostsList metadata to store their dirty flag values and locations in the block directory. */
};

class D4NTransaction {
  //purpose: to provide a transactional interface per Redis operations, i.e. to allow multiple Redis operations to be executed atomically.
  //this object instance is created per each request, and it is used to manage multiple Redis operations as a single transaction.
  //this object is shared among all the directories objects (object, block, bucket)
  //assumption: all the directories objects belong to the same s3-request context.
public:

    enum class redis_operation_type {
	NONE,READ_OP,WRITE_OP
    };

    enum class CloneStatus {
	SUCCESS = 0,           // Key cloned successfully
	SOURCE_NOT_EXIST = -1, // Source key does not exist
	DEST_ALREADY_EXIST = -2 // Destination key already exists
    };

    enum class TrxState {
			NONE,
			STARTED,
			ENDED,
			CANCELLED
		} trxState{TrxState::NONE};

    std::string m_trx_id;
    void start_trx();
    int create_clone_lua_script(const DoutPrefixProvider* dpp,std::shared_ptr<connection> , optional_yield y);
    int end_trx(const DoutPrefixProvider* dpp,std::shared_ptr<connection> , optional_yield y);
    int get_clone_script(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn,optional_yield y);
    bool set_transaction_key(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn,std::string &key,redis_operation_type op, optional_yield y);
    std::string get_end_trx_script(const DoutPrefixProvider* dpp, std::shared_ptr<connection> conn, optional_yield y);
    std::string get_trx_id(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn, optional_yield y);
    bool is_transaction_active() const { return trxState == TrxState::STARTED;}
    void cancel_transaction(const DoutPrefixProvider* dpp);

    // Debug function: Log transaction start time to Valkey
    void log_transaction_start_time(const DoutPrefixProvider* dpp, std::shared_ptr<connection> conn, optional_yield y);

    void create_rw_temp_keys(std::string key);
    std::string create_unique_temp_keys(std::string key);
  
    CloneStatus clone_key_for_transaction(std::string key_source, std::string key_destination, std::shared_ptr<connection> conn, optional_yield y);
    void clear_temp_keys();
    std::string m_evalsha_clone_key;
    std::string m_evalsha_end_trx;

    std::set<std::string> m_temp_read_keys;//unique temporary keys that are used in transactions(read operations)
    std::set<std::string> m_temp_write_keys;//unique temporary keys for use in transactions(write operations)
    std::set<std::string> m_temp_test_write_keys;// each key that is used for write, is clone for later test, to ensure that the key is not changed by other transactions (test write operation)
    std::string m_original_key;//original key, used to restore the key from Redis
    std::string m_temp_key_read;//temporary key for use in transactions
    std::string m_temp_key_write;//temporary key for use in transactions
    std::string m_temp_key_test_write;//temporary key for use in transactions
};

class Directory {
  public:
	std::shared_ptr<RedisPool> redis_pool{nullptr}; // Redis connection pool
    	void set_redis_pool(std::shared_ptr<RedisPool> pool) {
      	redis_pool = pool;
    }
    Directory() {}
   
    //m_d4n_trx is used to manage the transaction state and operations and it is shared among all the Directory objects.
    D4NTransaction* m_d4n_trx{nullptr};//TODO: private
    void set_d4n_trx(D4NTransaction* d4n_trx) {m_d4n_trx = d4n_trx;}
    D4NTransaction* get_d4n_trx() const { return m_d4n_trx; }
};

class Pipeline {
  public:
    Pipeline(std::shared_ptr<connection>& conn, std::shared_ptr<RedisPool> redis_pool) : conn(conn), redis_pool(redis_pool) {}
    void start() { pipeline_mode = true; }
    //executes all commands and sets pipeline mode to false
    int execute(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_pipeline() { return pipeline_mode; }
    request& get_request() { return req; }

  private:
    std::shared_ptr<connection> conn;
    std::shared_ptr<RedisPool> redis_pool{nullptr};
    request req;
    bool pipeline_mode{false};
};

class BucketDirectory: public Directory {
  public:
    BucketDirectory(std::shared_ptr<connection> conn) : conn(conn) {}
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y);
    int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y);
    int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y);

  private:
    std::shared_ptr<connection> conn;
};

class ObjectDirectory: public Directory {
  public:
    ObjectDirectory(std::shared_ptr<connection> conn) : conn(conn) {}

    //get a connection
    std::shared_ptr<connection> get_connection() { return conn; } 

    int exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

    int set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y); /* If nx is true, set only if key doesn't exist */
    int get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y);
    int zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y);
    int zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y);
    //Return value is the incremented value, else return error
    int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    std::string build_index(CacheObj* object);

  private:
    std::shared_ptr<connection> conn;

};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(std::shared_ptr<connection> conn) : conn(conn) {}
    
    int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);

    //Pipelined version of set
    int set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline=nullptr);
    int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    //Pipelined version of get using boost::redis::response for list bucket
    template <size_t N = 100>
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    //Pipelined version of get using boost::redis::generic_response
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y);
    int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y);
    int zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y);
    int zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y);

    std::shared_ptr<connection> get_connection() {return conn;}	
    std::string build_index(CacheBlock* block);

  private:
    std::shared_ptr<connection> conn;

    template<SeqContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y);
};

  class RedisTransactionHandling {
	//purpose: upon redis operation, transaction-id is allocated, clone keys are created for read/write operations.
	//this object is created/destroyed per each redis operation.
  public:
      // Constructor for ObjectDirectory
      RedisTransactionHandling(ObjectDirectory* obj_dir,
                           const DoutPrefixProvider* dpp,
                           std::shared_ptr<connection> conn,
                           CacheObj* object,
                           D4NTransaction::redis_operation_type op_type,
                           optional_yield y)
          : m_obj_dir(obj_dir), m_block_dir(nullptr), m_bucket_dir(nullptr), m_object(object), m_block(nullptr), m_bucket_key("")
      {
          initialize_transaction(dpp, conn, op_type, y);
      }

      // Constructor for BlockDirectory
      RedisTransactionHandling(BlockDirectory* block_dir,
                           const DoutPrefixProvider* dpp,
                           std::shared_ptr<connection> conn,
                           CacheBlock* block,
                           D4NTransaction::redis_operation_type op_type,
                           optional_yield y)
          : m_obj_dir(nullptr), m_block_dir(block_dir), m_bucket_dir(nullptr), m_object(nullptr), m_block(block), m_bucket_key("")
      {
          initialize_transaction(dpp, conn, op_type, y);
      }

      // Constructor for BucketDirectory
      RedisTransactionHandling(BucketDirectory* bucket_dir,
                           const DoutPrefixProvider* dpp,
                           std::shared_ptr<connection> conn,
                           const std::string& bucket_id,
                           D4NTransaction::redis_operation_type op_type,
                           optional_yield y)
          : m_obj_dir(nullptr), m_block_dir(nullptr), m_bucket_dir(bucket_dir), m_object(nullptr), m_block(nullptr), m_bucket_key(bucket_id)
      {
          initialize_transaction_for_bucket(dpp, conn, op_type, y);
      }

      std::string build_temp_key() const {
	  //purpose: build the temp key, the key is unique per transaction (multiple redis operations). [ start --> end ]
          if (m_obj_dir && m_object) {
              return m_obj_dir->build_index(m_object);
          } else if (m_block_dir && m_block) {
              return m_block_dir->build_index(m_block);
          } else if (m_bucket_dir && !m_bucket_key.empty()) {
              return m_bucket_key;  // For bucket, the key IS the bucket_id
          }
          return "";
      }

      // transaction validity
      bool is_valid() const { return m_is_valid; }

      // Get the Redis key to use for operations (transaction key if valid, original key otherwise)
      std::string get_redis_key() const {
          if (m_is_valid && !m_temp_key.empty()) {
              return m_temp_key;  // Use transaction key
          }
          // Fall back to original key
          return build_temp_key();
      }

  private:
      ObjectDirectory* m_obj_dir;
      BlockDirectory* m_block_dir;
      BucketDirectory* m_bucket_dir;
      CacheObj* m_object;
      CacheBlock* m_block;
      std::string m_bucket_key;
      bool m_is_valid = false;
      std::string m_temp_key;

      void initialize_transaction(const DoutPrefixProvider* dpp,
                                std::shared_ptr<connection> conn,
                                D4NTransaction::redis_operation_type op_type,
                                optional_yield y)
      {
          D4NTransaction* d4n_trx = nullptr;

          // Get the transaction object from the appropriate directory
          if (m_obj_dir) {
              d4n_trx = m_obj_dir->m_d4n_trx;
          } else if (m_block_dir) {
              d4n_trx = m_block_dir->m_d4n_trx;
          }

          if (!d4n_trx) {
              m_is_valid = false;
              return;
          }

          try {
              // Get transaction ID(once per request)
              d4n_trx->get_trx_id(dpp, conn, y);

              // Build the key using the directory's build_index method
              std::string key = build_temp_key();
              if (key.empty()) {
                  m_is_valid = false;
                  return;
              }

              // clone the key for read/write operations
              // Note: set_transaction_key modifies 'key' to the transaction key
              m_is_valid = d4n_trx->set_transaction_key(dpp, conn, key, op_type, y);
	      if (m_is_valid) {
		  m_temp_key = key; // key is now the transaction key (modified by set_transaction_key)
	      }

          } catch (const std::exception& e) {
              m_is_valid = false;
          }
      }

      void initialize_transaction_for_bucket(const DoutPrefixProvider* dpp,
                                            std::shared_ptr<connection> conn,
                                            D4NTransaction::redis_operation_type op_type,
                                            optional_yield y)
      {
          D4NTransaction* d4n_trx = nullptr;

          // Get the transaction object from BucketDirectory
          if (m_bucket_dir) {
              d4n_trx = m_bucket_dir->m_d4n_trx;
          }

          if (!d4n_trx) {
              m_is_valid = false;
              return;
          }

          try {
              // Get transaction ID (once per request)
              d4n_trx->get_trx_id(dpp, conn, y);

              // Use bucket_id as the key
              std::string key = m_bucket_key;
              if (key.empty()) {
                  m_is_valid = false;
                  return;
              }

              // Note: Create temp keys structure BEFORE set_transaction_key
              // This creates m_temp_key_read, m_temp_key_write, and m_temp_key_test_write
              d4n_trx->create_rw_temp_keys(key);

              // Clone the key for read/write operations
              // Note: set_transaction_key modifies 'key' to the transaction key
              m_is_valid = d4n_trx->set_transaction_key(dpp, conn, key, op_type, y);
              if (m_is_valid) {
                  m_temp_key = key; // key is now the transaction key (modified by set_transaction_key)
              }

          } catch (const std::exception& e) {
              m_is_valid = false;
          }
      }
  };

} } // namespace rgw::d4n
