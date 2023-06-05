#include "common/async/completion.h"
#include "rgw_ssd_driver.h"
#if defined(__linux__)
#include <features.h>
#endif

#if __has_include(<filesystem>)
#include <filesystem>
namespace efs = std::filesystem;
#else
#include <experimental/filesystem>
namespace efs = std::experimental::filesystem;
#endif

namespace rgw { namespace cache {

std::optional<Partition> SSDDriver::get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type)
{
    std::string key = name + type;
    auto iter = partitions.find(key);
    if (iter != partitions.end()) {
        return iter->second;
    }

    return std::nullopt;
}

std::vector<Partition> SSDDriver::list_partitions(const DoutPrefixProvider* dpp)
{
    std::vector<Partition> partitions_v;
    for (auto& it : SSDDriver::partitions) {
        partitions_v.emplace_back(it.second);
    }
    return partitions_v;
}

int SSDDriver::add_partition_info(Partition& info)
{
    std::string key = info.name + info.type;
    auto ret = partitions.emplace(key, info);
    return ret.second;
}

int SSDDriver::remove_partition_info(Partition& info)
{
    std::string key = info.name + info.type;
    return partitions.erase(key);
}

int SSDDriver::insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len)
{
    auto ret = entries.emplace(key, Entry(key, offset, len));
    return ret.second;
}

int SSDDriver::remove_entry(const DoutPrefixProvider* dpp, std::string key)
{
    return entries.erase(key);
}

std::optional<Entry> SSDDriver::get_entry(const DoutPrefixProvider* dpp, std::string key)
{
    auto iter = entries.find(key);
    if (iter != entries.end()) {
        return iter->second;
    }

    return std::nullopt;
}

std::vector<Entry> SSDDriver::list_entries(const DoutPrefixProvider* dpp)
{
    std::vector<Entry> entries_v;
    for (auto& it : entries) {
        entries_v.emplace_back(it.second);
    }
    return entries_v;
}

SSDDriver::SSDDriver(Partition& partition_info) : partition_info(partition_info),
                                                    free_space(partition_info.size), outstanding_write_size(0)
{
    add_partition_info(partition_info);
}

SSDDriver::~SSDDriver()
{
    remove_partition_info(partition_info);
}

int SSDDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp)
{
    this->cct = cct;

    if(partition_info.location.back() != '/') {
      partition_info.location += "/";
    }
    try {
        if (efs::exists(partition_info.location)) {
            if (cct->_conf->rgw_d3n_l1_evict_cache_on_start) {
                ldpp_dout(dpp, 5) << "initialize: evicting the persistent storage directory on start" << dendl;
                for (auto& p : efs::directory_iterator(partition_info.location)) {
                    efs::remove_all(p.path());
                }
            }
        } else {
            ldpp_dout(dpp, 5) << "initialize:: creating the persistent storage directory on start" << dendl;
            efs::create_directories(partition_info.location);
        }
    } catch (const efs::filesystem_error& e) {
        ldpp_dout(dpp, 0) << "initialize::: ERROR initializing the cache storage directory '" << partition_info.location <<
                                "' : " << e.what() << dendl;
        //return -EINVAL; Should return error from here?
    }

    #if defined(HAVE_LIBAIO) && defined(__GLIBC__)
    // libaio setup
    struct aioinit ainit{0};
    ainit.aio_threads = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_threads");
    ainit.aio_num = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_num");
    ainit.aio_idle_time = 120;
    aio_init(&ainit);
    #endif

    return 0;
}

int SSDDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs)
{
    if (key_exists(dpp, key)) {
        return 0;
    }

    std::string location = partition_info.location + key;

    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "w+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: put::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    nbytes = fwrite(bl.c_str(), 1, len, cache_file);
    if (nbytes != len) {
        ldpp_dout(dpp, 0) << "ERROR: put::io_write: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: put::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }

    efs::space_info space = efs::space(location);
    this->free_space = space.available;

    return insert_entry(dpp, key, 0, len);
}

int SSDDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs)
{
    if (!key_exists(dpp, key)) {
        return -ENOENT;
    }

    char buffer[len];
    std::string location = partition_info.location + key;

    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "r+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: put::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    nbytes = fread(buffer, sizeof(buffer), 1 , cache_file);
    if (nbytes != len) {
        ldpp_dout(dpp, 0) << "ERROR: put::io_read: fread has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: put::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }

    ceph::encode(buffer, bl);

    return 0;
}

std::unique_ptr<CacheAioRequest> SSDDriver::get_cache_aio_request_ptr(const DoutPrefixProvider* dpp)
{
    return std::make_unique<SSDCacheAioRequest>(this);
}

rgw::AioResultList SSDDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, rgw::Aio::cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

int SSDDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key)
{
    std::string location = partition_info.location + key;

    if (!efs::remove(location)) {
        ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the file: " << location << dendl;
        return -EIO;
    }

    return remove_entry(dpp, key);
}

int SSDDriver::AsyncReadOp::init(const DoutPrefixProvider *dpp, CephContext* cct, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): file_path=" << file_path << dendl;
    aio_cb.reset(new struct aiocb);
    memset(aio_cb.get(), 0, sizeof(struct aiocb));
    aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
    if(aio_cb->aio_fildes < 0) {
        int err = errno;
        ldpp_dout(dpp, 1) << "ERROR: SSDCache: " << __func__ << "(): can't open " << file_path << " : " << " error: " << err << dendl;
        return -err;
    }
    if (cct->_conf->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL) {
        posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d3n_l1_fadvise);
    }

    bufferptr bp(read_len);
    aio_cb->aio_buf = bp.c_str();
    result.append(std::move(bp));

    aio_cb->aio_nbytes = read_len;
    aio_cb->aio_offset = read_ofs;
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = libaio_cb_aio_dispatch;
    aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

    return 0;
}

void SSDDriver::AsyncReadOp::libaio_cb_aio_dispatch(sigval sigval)
{
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    const int ret = -aio_error(op.aio_cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
    }

    ceph::async::dispatch(std::move(p), ec, std::move(op.result));
}

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::AsyncReadOp::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}

template <typename ExecutionContext, typename CompletionToken>
auto SSDDriver::get_async(const DoutPrefixProvider *dpp, ExecutionContext& ctx, const std::string& key,
                off_t read_ofs, off_t read_len, CompletionToken&& token)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    using Op = AsyncReadOp;
    using Signature = typename Op::Signature;
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto p = Op::create(ctx.get_executor(), init.completion_handler);
    auto& op = p->user_data;

    int ret = op.init(dpp, cct, location, read_ofs, read_len, p.get());
    if(0 == ret) {
        ret = ::aio_read(op.aio_cb.get());
    }
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
    if(ret < 0) {
        auto ec = boost::system::error_code{-ret, boost::system::system_category()};
        ceph::async::post(std::move(p), ec, bufferlist{});
    } else {
        (void)p.release();
    }
    return init.result.get();
}

void SSDCacheAioRequest::cache_aio_read(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, off_t ofs, uint64_t len, rgw::Aio* aio, rgw::AioResult& r)
{
    using namespace boost::asio;
    async_completion<spawn::yield_context, void()> init(y.get_yield_context());
    auto ex = get_associated_executor(init.completion_handler);

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    cache_driver->get_async(dpp, y.get_io_context(), key, ofs, len, bind_executor(ex, SSDDriver::libaio_handler{aio, r}));
}

} } // namespace rgw::cache
