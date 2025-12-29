// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <aio.h>
#include "rgw_common.h"
#include "rgw_cache_driver.h"

#if defined(HAVE_LIBURING)
#include <liburing.h>

namespace rgw { namespace cache {

// Alignment for O_DIRECT I/O (typically 512 or 4096 bytes for modern devices)
constexpr size_t IO_BUFFER_ALIGNMENT = 4096;

// Round up size to alignment boundary
inline size_t iouring_align_size(size_t size, size_t alignment = IO_BUFFER_ALIGNMENT) {
  return (size + alignment - 1) & ~(alignment - 1);
}

} } // namespace rgw::cache
#endif // HAVE_LIBURING

namespace rgw { namespace cache {

class SSDDriver : public CacheDriver {
public:
  SSDDriver(Partition& partition_info, bool admin) : partition_info(partition_info), admin(admin) {}
  virtual ~SSDDriver() {}

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
  void set_free_space(const DoutPrefixProvider* dpp, uint64_t free_space);

  virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) override;

private:
  Partition partition_info;
  uint64_t free_space;
  CephContext* cct;
  std::mutex cache_lock;
  bool admin;

  // io backend selection - true = io_uring, false = libaio
  bool use_io_uring = false;

#if defined(HAVE_LIBURING)
  int64_t IoUringQueueDepth;  // set from ceph.conf in SSDDriver::initialize()

  int init_per_thread_uring(const DoutPrefixProvider* dpp, struct io_uring** ring_out) const;
#endif

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

#if defined(HAVE_LIBURING)

  // async read operation using io_uring
  struct IoUringAsyncReadOp {
    const DoutPrefixProvider* dpp = nullptr;
    bufferlist result;
    int fd = -1;
    off_t offset = 0;       // original requested offset
    size_t length = 0;      // original requested length
    void* buffer = nullptr;
    bool direct_io = false;  // whether O_DIRECT was used (needs result trimming)
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, IoUringAsyncReadOp>;

    int prepare_io_uring_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, size_t read_len, void* sqe_user_data, struct io_uring* ring);
    static void io_uring_read_completion(struct io_uring_cqe* cqe, IoUringAsyncReadOp* op);
    // CQE reaper callback: processes CQE and dispatches the async completion
    static void handle_cqe(struct io_uring_cqe* cqe, void* arg);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  // async write operation using io_uring
  class IoUringAsyncWriteRequest {
  public:
    const DoutPrefixProvider* dpp;
    std::string file_path;
    std::string temp_file_path;
    void* data;
    int fd;
    size_t length;          // original (unaligned) data length
    bool direct_io = false; // whether O_DIRECT was used (needs ftruncate)
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, IoUringAsyncWriteRequest>;

    int prepare_io_uring_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path, void* sqe_user_data, struct io_uring* ring);
    static void io_uring_write_completion(struct io_uring_cqe* cqe, IoUringAsyncWriteRequest* op);
    // CQE reaper callback: processes CQE and dispatches the async completion
    static void handle_cqe(struct io_uring_cqe* cqe, void* arg);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);

    IoUringAsyncWriteRequest() : dpp(nullptr), data(nullptr), fd(-1), length(0), priv_data(nullptr) {}
    ~IoUringAsyncWriteRequest() = default;
  };
#endif // HAVE_LIBURING

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

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

  struct LibaioAsyncReadOp {
    bufferlist result;
    unique_aio_cb_ptr aio_cb;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, LibaioAsyncReadOp>;

    int prepare_libaio_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg);
    static void libaio_cb_aio_dispatch(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  struct LibaioAsyncWriteRequest {
    const DoutPrefixProvider* dpp;
    std::string file_path;
    std::string temp_file_path;
    void *data;
    int fd;
    unique_aio_cb_ptr cb;
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, LibaioAsyncWriteRequest>;

    int prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path);
    static void libaio_write_cb(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

#if defined(HAVE_LIBURING)
  // io_uring async operations
  template <typename Executor, typename CompletionToken>
  auto get_async_uring(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 off_t read_ofs, off_t read_len, CompletionToken&& token);

  template <typename Executor, typename CompletionToken>
  void put_async_uring(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);
#endif

  // libaio async operations
  template <typename Executor, typename CompletionToken>
  auto get_async_libaio(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 off_t read_ofs, off_t read_len, CompletionToken&& token);

  template <typename Executor, typename CompletionToken>
  void put_async_libaio(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);

  // Dispatch to appropriate backend based on use_io_uring flag
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
};

} } // namespace rgw::cache
