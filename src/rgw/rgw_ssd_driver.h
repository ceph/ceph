#pragma once

#include <aio.h>
#include "rgw_common.h"
#include "rgw_cache_driver.h"
#include <filesystem>
#include <shared_mutex>
#include <boost/asio.hpp>

namespace efs = std::filesystem;
namespace rgw { namespace cache {

static constexpr size_t XATTR_OVERHEAD_ESTIMATE = 4096;

class SSDDriver : public CacheDriver {
public:
  SSDDriver(Partition& partition_info, boost::asio::io_context& io_context, bool admin) : partition_info(partition_info), io_context(io_context), admin(admin) {}
  virtual ~SSDDriver() { quit = true; }

  virtual int initialize(const DoutPrefixProvider* dpp) override;
  virtual int put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
  virtual rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) override;
  virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) override;
  virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;
  virtual int rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y) override;
  virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
  virtual int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override;
  virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;
  int delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name);

  /* Partition */
  virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
  virtual uint64_t get_free_space(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int reserve_space(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
  virtual int check_and_reserve_space(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
  virtual int release_reservation(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;

  virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) override;

private:
  Partition partition_info;
  uint64_t free_space{0};
  uint64_t reserved_space{0};
  boost::asio::io_context& io_context;
  CephContext* cct;
  std::shared_mutex cache_lock;
  std::optional<boost::asio::steady_timer> free_space_timer;
  std::promise<void> free_space_done_promise;
  inline static std::atomic<bool> quit{false};
  bool admin;

  struct libaio_read_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // read callback
    void operator()(boost::system::error_code ec, bufferlist bl) const {
      r.result = -ec.value();
      r.data = std::move(bl);
      throttle->put(r);
    }
  };

  struct libaio_write_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // write callback
    void operator()(boost::system::error_code ec) const {
      r.result = -ec.value();
      throttle->put(r);
    }
  };

  // unique_ptr with custom deleter for struct aiocb
  struct libaio_aiocb_deleter {
    void operator()(struct aiocb* c) {
      if(c->aio_fildes > 0) {
	      TEMP_FAILURE_RETRY(::close(c->aio_fildes));
      }
      c->aio_buf = nullptr;
      delete c;
    }
  };

  void background_free_space_sync_worker(const DoutPrefixProvider* dpp, optional_yield y);

  template <typename Executor, typename CompletionToken>
    auto get_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
		    off_t read_ofs, off_t read_len, CompletionToken&& token);
  
  template <typename Executor, typename CompletionToken>
  void put_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                  const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);
  
  rgw::Aio::OpFunc ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
				  off_t read_ofs, off_t read_len, const std::string& key);

  rgw::Aio::OpFunc ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key);

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

  struct AsyncReadOp {
    bufferlist result;
    unique_aio_cb_ptr aio_cb;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, AsyncReadOp>;

    int prepare_libaio_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg);
    static void libaio_cb_aio_dispatch(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  struct AsyncWriteRequest {
    const DoutPrefixProvider* dpp;
	  std::string file_path;
    std::string temp_file_path;
	  void *data;
	  int fd;
	  unique_aio_cb_ptr cb;
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, AsyncWriteRequest>;

	  int prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path);
    static void libaio_write_cb(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  //Helper methods to restore blocks and objects from cache data
  void restore_bucket_objects(const DoutPrefixProvider* dpp,
                              const std::filesystem::path& bucket_path,
                              const std::string& bucket_id,
                              ObjectDataCallback obj_func,
                              BlockDataCallback block_func);
  void restore_object_files(const DoutPrefixProvider* dpp,
                            const std::filesystem::path& object_path,
                            const std::string& bucket_id,
                            const std::string& object_name,
                            ObjectDataCallback obj_func,
                            BlockDataCallback block_func);
  void process_cache_file(const DoutPrefixProvider* dpp,
                          const std::filesystem::path& file_path,
                          const std::string& bucket_id,
                          const std::string& object_name,
                          ObjectDataCallback obj_func,
                          BlockDataCallback block_func);
  bool get_dirty_flag(const DoutPrefixProvider* dpp, const std::filesystem::path& file_path);
  //TODO - check if this can be removed
  void process_clean_head_block(const DoutPrefixProvider* dpp,
                                         const std::filesystem::path& file_path,
                                         const std::string& key,
                                         const std::string& version,
                                         const rgw::sal::Attrs& attrs,
                                         ObjectDataCallback obj_func,
                                         BlockDataCallback block_func);
  //For delete markers only
  void process_dirty_head_block(const DoutPrefixProvider* dpp,
                                         const std::filesystem::path& file_path,
                                         const std::string& key,
                                         const std::string& version,
                                         const std::string& bucket_id,
                                         const std::string& object_name,
                                         const rgw::sal::Attrs& attrs,
                                         ObjectDataCallback obj_func,
                                         BlockDataCallback block_func);

  bool extract_delete_marker(const DoutPrefixProvider* dpp,
                                      const rgw::sal::Attrs& attrs);
  rgw_user extract_user_from_attrs(const DoutPrefixProvider* dpp,
                                            const rgw::sal::Attrs& attrs);
  std::string extract_attr_string(const DoutPrefixProvider* dpp,
                                           const rgw::sal::Attrs& attrs,
                                           const std::string& attr_name);
  void process_data_block(const DoutPrefixProvider* dpp,
                            const std::filesystem::path& file_path,
                            const std::string& base_key,
                            const std::string& version,
                            const std::string& bucket_id,
                            const std::string& object_name,
                            const std::vector<std::string>& parts,
                            bool dirty,
                            const rgw::sal::Attrs& attrs,
                            ObjectDataCallback obj_func,
                            BlockDataCallback block_func);
  rgw_obj_key build_obj_key(const std::string& object_name,
                                     const rgw::sal::Attrs& attrs);

};

} } // namespace rgw::cache

