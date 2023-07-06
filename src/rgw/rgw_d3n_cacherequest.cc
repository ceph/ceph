#include "rgw_d3n_cacherequest.h"

#include <fcntl.h>
#include <stdlib.h>
#include <aio.h>

#include "common/async/completion.h"
#include "common/error_code.h"
#include "common/errno.h"


namespace rgw::d3n {

// unique_ptr with custom deleter for struct aiocb
struct libaio_aiocb_deleter {
  void operator()(struct aiocb* c) {
    if(c->aio_fildes > 0) {
      if( ::close(c->aio_fildes) != 0) {
        lsubdout(g_ceph_context, rgw_datacache, 2) << "D3nDataCache: " << __func__ << "(): Error - can't close file, errno=" << -errno << dendl;
      }
    }
    delete c;
  }
};

using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

using aio_notify_fn = void(*)(union sigval); // sigevent::sigev_notify_function

struct AsyncFileReadOp {
  bufferlist result;
  unique_aio_cb_ptr aio_cb;

  int init_async_read(const DoutPrefixProvider *dpp, const std::string& location,
                      off_t read_ofs, off_t read_len, aio_notify_fn notify_cb,
                      void* arg)
  {
    ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
    aio_cb.reset(new struct aiocb);
    memset(aio_cb.get(), 0, sizeof(struct aiocb));
    aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(location.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
    if(aio_cb->aio_fildes < 0) {
      int err = errno;
      ldpp_dout(dpp, 1) << "ERROR: D3nDataCache: " << __func__ << "(): can't open " << location << " : " << cpp_strerror(err) << dendl;
      return -err;
    }
    if (g_conf()->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
      posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d3n_l1_fadvise);

    bufferptr bp(read_len);
    aio_cb->aio_buf = bp.c_str();
    result.append(std::move(bp));

    aio_cb->aio_nbytes = read_len;
    aio_cb->aio_offset = read_ofs;
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = notify_cb;
    aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

    return 0;
  }
};

using Signature = void(boost::system::error_code, bufferlist);
using Completion = ceph::async::Completion<Signature, AsyncFileReadOp>;

static void on_notify_yield(sigval sigval)
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
  auto op = std::move(p->user_data);
  const int ret = -aio_error(op.aio_cb.get());
  boost::system::error_code ec;
  if (ret < 0) {
    ec.assign(-ret, boost::system::system_category());
  }

  ceph::async::dispatch(std::move(p), ec, std::move(op.result));
}

template <typename ExecutionContext, typename CompletionToken>
static auto async_read(const DoutPrefixProvider *dpp, ExecutionContext& ctx,
                       const std::string& location, off_t ofs, off_t len,
                       CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = Completion::create(ctx.get_executor(),
                              std::move(init.completion_handler));
  auto& op = p->user_data;

  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
  int ret = op.init_async_read(dpp, location, ofs, len,
                               on_notify_yield, p.get());
  if(0 == ret) {
    ret = ::aio_read(op.aio_cb.get());
  }
  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
  if(ret < 0) {
    auto ec = boost::system::error_code{-ret, boost::system::system_category()};
    ceph::async::post(std::move(p), ec, bufferlist{});
  } else {
    // coverity[leaked_storage:SUPPRESS]
    (void)p.release();
  }
  return init.result.get();
}

struct d3n_libaio_handler {
  Aio* throttle = nullptr;
  AioResult& r;
  // read callback
  void operator()(boost::system::error_code ec, bufferlist bl) const {
    r.result = -ec.value();
    r.data = std::move(bl);
    throttle->put(r);
  }
};

static void cache_read(const DoutPrefixProvider *dpp,
                       boost::asio::io_context& context, yield_context yield,
                       const std::string& dir, off_t ofs, off_t len,
                       Aio* aio, AioResult& r)
{
  using namespace boost::asio;
  async_completion<yield_context, void()> init(yield);
  auto ex = get_associated_executor(init.completion_handler);

  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): oid=" << r.obj.oid << dendl;
  async_read(dpp, context, dir+"/"+url_encode(r.obj.oid, true), ofs, len,
             bind_executor(ex, d3n_libaio_handler{aio, r}));
}

Aio::OpFunc cache_read_op(const DoutPrefixProvider *dpp, optional_yield y,
                          off_t ofs, off_t len, const std::string& cache_dir)
{
  return [dpp, y, ofs, len, cache_dir] (Aio* aio, AioResult& r) mutable {
    // d3n data cache requires yield context (rgw_beast_enable_async=true)
    ceph_assert(y);
    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: d3n_cache_aio_abstract(): libaio Read From Cache, oid=" << r.obj.oid << dendl;
    cache_read(dpp, y.get_io_context(), y.get_yield_context(),
               cache_dir, ofs, len, aio, r);
  };
}

} // namespace rgw::d3n
