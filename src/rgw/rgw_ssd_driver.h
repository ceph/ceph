#pragma once

#include <aio.h>
#include "rgw_common.h"
#include "rgw_cache_driver.h"

namespace rgw { namespace cache {

class SSDDriver : public CacheDriver {
public:
  SSDDriver(Partition& partition_info) : partition_info(partition_info) {}
  virtual ~SSDDriver() {}

  virtual int initialize(const DoutPrefixProvider* dpp) override;
  virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int del(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override { return -1; } // TODO: implement
  virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
  virtual int put_async(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
  virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) override;
  virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;
  virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
  virtual int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override;
  virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;
  int delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name);

  /* Partition */
  virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
  virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override { return free_space; }

private:
  Partition partition_info;
  uint64_t free_space;
  CephContext* cct;

  struct libaio_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // read callback
    void operator()(boost::system::error_code ec, bufferlist bl) const {
      r.result = -ec.value();
      r.data = std::move(bl);
      throttle->put(r);
    }
  };

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

  template <typename ExecutionContext, typename CompletionToken>
    auto get_async(const DoutPrefixProvider *dpp, ExecutionContext& ctx, const std::string& key,
		    off_t read_ofs, off_t read_len, CompletionToken&& token);

  rgw::Aio::OpFunc ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
				  off_t read_ofs, off_t read_len, const std::string& key);

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

  struct AsyncReadOp {
    bufferlist result;
    unique_aio_cb_ptr aio_cb;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, AsyncReadOp>;

  int init(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg);
  static void libaio_cb_aio_dispatch(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  struct AsyncWriteRequest {
    const DoutPrefixProvider* dpp;
	  std::string key;
	  void *data;
	  int fd;
	  struct aiocb *cb;
    SSDDriver *priv_data;

	  AsyncWriteRequest(const DoutPrefixProvider* dpp) : dpp(dpp) {}
	  int prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string key, std::string cache_location);
    static void libaio_write_cb(sigval sigval);

    ~AsyncWriteRequest() {
      ::close(fd);
		  cb->aio_buf = nullptr;
		  delete(cb);
    }
  };

  void libaio_write_completion_cb(AsyncWriteRequest* c);
};

} } // namespace rgw::cache

