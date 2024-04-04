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
  void operator()(aiocb* c) {
    if (c->aio_fildes > 0) {
      TEMP_FAILURE_RETRY(::close(c->aio_fildes));
    }
    delete c;
  }
};
using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

// aio state stored in AioResult::user_data
struct state {
  Aio* aio = nullptr;
  unique_aio_cb_ptr aio_cb;

  explicit state(Aio* aio) : aio(aio) {}
};
static_assert(sizeof(AioResult::user_data) >= sizeof(state));

static void on_complete(AioResult& r)
{
  auto s = reinterpret_cast<state*>(&r.user_data);
  Aio* aio = s->aio;
  // manually destroy the state that was constructed with placement new
  s->~state();
  aio->put(r);
}


// for sigevent::sigev_notify_function
using aio_notify_fn = void(*)(union sigval);

static int init_async_read(const DoutPrefixProvider *dpp, aiocb* aio_cb,
                           const std::string& path, off_t ofs, off_t len,
                           void* buffer, aio_notify_fn notify_cb, void* arg)
{
  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): location=" << path << dendl;
  aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
  if (aio_cb->aio_fildes < 0) {
    int err = errno;
    ldpp_dout(dpp, 1) << "ERROR: D3nDataCache: " << __func__ << "(): can't open " << path << " : " << cpp_strerror(err) << dendl;
    return -err;
  }
  if (g_conf()->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
    posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d3n_l1_fadvise);

  aio_cb->aio_buf = buffer;
  aio_cb->aio_nbytes = len;
  aio_cb->aio_offset = ofs;
  aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  aio_cb->aio_sigevent.sigev_notify_function = notify_cb;
  aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
  aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

  return 0;
}


// yielding completion

using Signature = void(boost::system::error_code);
using Completion = ceph::async::Completion<Signature, state*>;

// aio_notify_fn for yield_context overload
static void on_notify_yield(sigval sigval)
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
  const int ret = -::aio_error(p->user_data->aio_cb.get());
  boost::system::error_code ec;
  if (ret < 0) {
    ec.assign(-ret, boost::system::system_category());
  }

  ceph::async::dispatch(std::move(p), ec);
}

template <typename ExecutionContext, typename CompletionToken>
static auto async_read(const DoutPrefixProvider *dpp, ExecutionContext& ctx,
                       const std::string& path, off_t ofs, off_t len,
                       AioResult& r, void* buffer, CompletionToken&& token)
{
  auto s = reinterpret_cast<state*>(&r.user_data);

  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = Completion::create(ctx.get_executor(),
                              std::move(init.completion_handler), s);

  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): location=" << path << dendl;
  int ret = init_async_read(dpp, s->aio_cb.get(), path, ofs, len,
                            buffer, on_notify_yield, p.get());
  if(0 == ret) {
    ret = ::aio_read(s->aio_cb.get());
  }
  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
  if (ret < 0) {
    auto ec = boost::system::error_code{-ret, boost::system::system_category()};
    r.data.clear();
    ceph::async::post(std::move(p), ec);
  } else {
    // coverity[leaked_storage:SUPPRESS]
    (void)p.release();
  }
  return init.result.get();
}

struct d3n_libaio_handler {
  AioResult& r;
  // read callback
  void operator()(boost::system::error_code ec) const {
    r.result = -ec.value();
    on_complete(r);
  }
};

static void cache_read(const DoutPrefixProvider *dpp,
                       boost::asio::io_context& context, spawn::yield_context yield,
                       const std::string& path, off_t ofs, off_t len,
                       void* buffer, AioResult& r)
{
  using namespace boost::asio;
  async_completion<spawn::yield_context, void()> init(yield);
  auto ex = get_associated_executor(init.completion_handler);

  async_read(dpp, context, path, ofs, len, r, buffer,
             bind_executor(ex, d3n_libaio_handler{r}));
}


// non-yielding completion

static void on_notify(sigval sigval)
{
  auto& r = *(static_cast<AioResult*>(sigval.sival_ptr));
  auto s = reinterpret_cast<state*>(&r.user_data);
  r.result = -::aio_error(s->aio_cb.get());
  on_complete(r);
}

static void cache_read(const DoutPrefixProvider *dpp, const std::string& path,
                       off_t ofs, off_t len, void* buffer, AioResult& r)
{
  auto s = reinterpret_cast<state*>(&r.user_data);

  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): location=" << path << dendl;
  int ret = init_async_read(dpp, s->aio_cb.get(), path, ofs, len,
                            buffer, on_notify, &r);
  if (0 == ret) {
    ret = ::aio_read(s->aio_cb.get());
  }
  ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
  if (ret < 0) {
    r.result = -::aio_error(s->aio_cb.get());
    r.data.clear();
    on_complete(r);
  }
}


Aio::OpFunc cache_read_op(const DoutPrefixProvider *dpp, optional_yield y,
                          off_t ofs, off_t len, const std::string& cache_dir)
{
  return [dpp, y, ofs, len, cache_dir] (Aio* aio, AioResult& r) mutable {
    // use placement new to construct the aio state inside of user_data
    auto s = new (&r.user_data) state(aio);

    // allocate/zero the aiocb
    s->aio_cb.reset(new aiocb);
    memset(s->aio_cb.get(), 0, sizeof(aiocb));

    // preallocate the data buffer and manage its lifetime in AioResult::data
    bufferptr bp(len);
    void* buffer = bp.c_str();
    r.data.append(std::move(bp));

    std::string path = cache_dir+"/"+url_encode(r.obj.oid, true);

    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: d3n_cache_aio_abstract(): libaio Read From Cache, oid=" << r.obj.oid << dendl;
    if (y) {
      cache_read(dpp, y.get_io_context(), y.get_yield_context(),
                 path, ofs, len, buffer, r);
    } else {
      cache_read(dpp, path, ofs, len, buffer, r);
    }
  };
}

} // namespace rgw::d3n
