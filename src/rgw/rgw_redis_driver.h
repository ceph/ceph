#ifndef CEPH_REDISDRIVER_H
#define CEPH_REDISDRIVER_H

//#include <aedis.hpp>
#include <aio.h>
#include "common/async/completion.h"
#include "rgw_common.h"
#include "rgw_cache_driver.h"

#include <cpp_redis/cpp_redis>
#include "driver/d4n/d4n_directory.h"

namespace rgw { namespace cache {

class RedisDriver;

class RedisCacheAioRequest: public CacheAioRequest {
  public:
    RedisCacheAioRequest(RedisDriver* cache_driver) : cache_driver(cache_driver) {}
    virtual ~RedisCacheAioRequest() = default;
    virtual void cache_aio_read(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, off_t ofs, uint64_t len, rgw::Aio* aio, rgw::AioResult& r) override;
    virtual void cache_aio_write(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, bufferlist& bl, uint64_t len, rgw::Aio* aio, rgw::AioResult& r) override;
  private:
    RedisDriver* cache_driver;
};

class RedisDriver : public CacheDriver {
  public:
    RedisDriver(Partition& _partition_info) : partition_info(_partition_info),
                                              free_space(_partition_info.size), 
				              outstanding_write_size(0) 
    {
      add_partition_info(_partition_info);
    }
    virtual ~RedisDriver()
    {
      remove_partition_info(partition_info);
    }

    /* Entry */
    virtual bool key_exists(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual std::vector<Entry> list_entries(const DoutPrefixProvider* dpp) override;
    virtual size_t get_num_entries(const DoutPrefixProvider* dpp) override;
    //int update_local_weight(const DoutPrefixProvider* dpp, std::string key, int localWeight); // may need to exist for base class -Sam

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override { return free_space; } // how to get this from redis server? -Sam
    static std::optional<Partition> get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type);
    static std::vector<Partition> list_partitions(const DoutPrefixProvider* dpp);

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) override;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) override;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) override;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key) override;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) override;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) override;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) override;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val) override;

    virtual std::unique_ptr<CacheAioRequest> get_cache_aio_request_ptr(const DoutPrefixProvider* dpp) override;
    virtual rgw::AioResultList get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;

    struct libaio_handler { // should this be the same as SSDDriver? -Sam
      rgw::Aio* throttle = nullptr;
      rgw::AioResult& r;

      // read callback
      void operator()(boost::system::error_code ec, bufferlist bl) const {
	r.result = -ec.value();
	r.data = std::move(bl);
	throttle->put(r);
      }
    };
    template <typename ExecutionContext, typename CompletionToken>
    auto get_async(const DoutPrefixProvider *dpp, ExecutionContext& ctx, const std::string& key,
                  off_t read_ofs, off_t read_len, CompletionToken&& token);

  protected:
    cpp_redis::client client;
    rgw::d4n::Address addr;
    static std::unordered_map<std::string, Partition> partitions;
    std::unordered_map<std::string, Entry> entries;
    Partition partition_info;
    uint64_t free_space;
    uint64_t outstanding_write_size;
    CephContext* cct;

    int find_client(const DoutPrefixProvider* dpp);
    int insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len);
    std::optional<Entry> get_entry(const DoutPrefixProvider* dpp, std::string key);
    int remove_entry(const DoutPrefixProvider* dpp, std::string key);
    int add_partition_info(Partition& info);
    int remove_partition_info(Partition& info);

  private:
    // unique_ptr with custom deleter for struct aiocb
    struct libaio_aiocb_deleter {
      void operator()(struct aiocb* c) {
        if(c->aio_fildes > 0) {
          if( ::close(c->aio_fildes) != 0) {
          }
        }
        delete c;
      }
    };

    using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

    struct AsyncReadOp {
      bufferlist result;
      unique_aio_cb_ptr aio_cb;
      using Signature = void(boost::system::error_code, bufferlist);
      using Completion = ceph::async::Completion<Signature, AsyncReadOp>;

      int init(const DoutPrefixProvider *dpp, CephContext* cct, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg);
      static void libaio_cb_aio_dispatch(sigval sigval);

      template <typename Executor1, typename CompletionHandler>
      static auto create(const Executor1& ex1, CompletionHandler&& handler);
    };
};

} } // namespace rgw::cal
    
#endif
