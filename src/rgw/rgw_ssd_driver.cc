#include "common/async/completion.h"
#include "rgw_ssd_driver.h"
#if defined(__linux__)
#include <features.h>
#include <sys/xattr.h>
#endif

#if __has_include(<filesystem>)
#include <filesystem>
namespace efs = std::filesystem;
#else
#include <experimental/filesystem>
namespace efs = std::experimental::filesystem;
#endif

namespace rgw { namespace cache {

constexpr std::string_view ATTR_PREFIX = "user.rgw.";

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

SSDDriver::SSDDriver(Partition& partition_info) : partition_info(partition_info),
                                                    free_space(partition_info.size)
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

int SSDDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y)
{
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

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    if (attrs.size() > 0) {
        r = set_attrs(dpp, key, attrs, y);
        if (r < 0) {
            ldpp_dout(dpp, 0) << "ERROR: put::set_attrs: failed to set attrs, r = " << r << dendl;
            return r;
        }
    }

    return 0;
}

int SSDDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
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

    if (attrs.size() > 0) {
        r = get_attrs(dpp, key, attrs, y);
        if (r < 0) {
            ldpp_dout(dpp, 0) << "ERROR: put::get_attrs: failed to get attrs, r = " << r << dendl;
            return r;
        }
    }
    return 0;
}

int SSDDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y)
{
    std::string location = partition_info.location + key;

    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "a+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: put::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    nbytes = fwrite(bl_data.c_str(), 1, bl_data.length(), cache_file);
    if (nbytes != bl_data.length()) {
        ldpp_dout(dpp, 0) << "ERROR: append_data: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << bl_data.length() << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: append_data::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
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

rgw::Aio::OpFunc SSDDriver::ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                off_t read_ofs, off_t read_len, const std::string& key) {
  return [this, dpp, y, read_ofs, read_len, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_read_op(): Read From Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    spawn::yield_context yield = y.get_yield_context();
    async_completion<spawn::yield_context, void()> init(yield);
    auto ex = get_associated_executor(init.completion_handler);

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->get_async(dpp, y.get_io_context(), key, read_ofs, read_len, bind_executor(ex, SSDDriver::libaio_handler{aio, r}));
  };
}

rgw::AioResultList SSDDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, ssd_cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

void SSDDriver::libaio_write_completion_cb(AsyncWriteRequest* c)
{
    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;
}

int SSDDriver::put_async(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Write To Cache, oid=" << key << ", len=" << len << dendl;
    struct AsyncWriteRequest* wr = new struct AsyncWriteRequest(dpp);
    int r = 0;
    if ((r = wr->prepare_libaio_write_op(dpp, bl, len, key, partition_info.location)) < 0) {
        ldpp_dout(dpp, 0) << "ERROR: SSDCache: " << __func__ << "() prepare libaio write op r=" << r << dendl;
        return r;
    }
    wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    wr->cb->aio_sigevent.sigev_notify_function = SSDDriver::AsyncWriteRequest::libaio_write_cb;
    wr->cb->aio_sigevent.sigev_notify_attributes = nullptr;
    wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
    wr->key = key;
    wr->priv_data = this;

    if ((r = ::aio_write(wr->cb)) != 0) {
        ldpp_dout(dpp, 0) << "ERROR: SSDCache: " << __func__ << "() aio_write r=" << r << dendl;
        delete wr;
        return r;
    }
    return 0;
}

int SSDDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y)
{
    std::string location = partition_info.location + key;

    if (!efs::remove(location)) {
        ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the file: " << location << dendl;
        return -EIO;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::AsyncWriteRequest::prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string key, std::string cache_location)
{
    std::string location = cache_location + key;
    int r = 0;

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Write To Cache, location=" << location << dendl;
    cb = new struct aiocb;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    memset(cb, 0, sizeof(struct aiocb));
    r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
        ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << location.c_str() << "'" << dendl;
        return r;
    }
    if (dpp->get_cct()->_conf->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(fd, 0, 0, dpp->get_cct()->_conf->rgw_d3n_l1_fadvise);
    cb->aio_fildes = fd;

    data = malloc(len);
    if (!data) {
        ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: memory allocation failed" << dendl;
        ::close(fd);
        return r;
    }
    cb->aio_buf = data;
    memcpy((void*)data, bl.c_str(), len);
    cb->aio_nbytes = len;
    return r;
}

void SSDDriver::AsyncWriteRequest::libaio_write_cb(sigval sigval)
{
  SSDDriver::AsyncWriteRequest* c = static_cast<SSDDriver::AsyncWriteRequest*>(sigval.sival_ptr);
  c->priv_data->libaio_write_completion_cb(c);
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

int SSDDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : attrs) {
        std::string attr_name, attr_val;
        ceph::decode(attr_val, it.second);
        attr_name = it.first;
        auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_REPLACE);
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not modify attr value for attr name: " << attr_name << " key: " << key << dendl;
            return ret;
        }
    }
    return 0;
}

int SSDDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : del_attrs) {
        auto ret = delete_attr(dpp, key, it.first);
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << it.first << " key: " << key << dendl;
            return ret;
        }
    }
    return 0;
}

int SSDDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    char namebuf[64 * 1024];
    int ret;
    ssize_t buflen = listxattr(location.c_str(), namebuf, sizeof(namebuf));
    if (buflen < 0) {
        ret = errno;
        ldpp_dout(dpp, 0) << "ERROR: could not get attributes for key: " << key << ": " << ret << dendl;
        return -ret;
    }
    char *keyptr = namebuf;
    while (buflen > 0) {
        ssize_t keylen;

        keylen = strlen(keyptr) + 1;
        std::string attr_name(keyptr);
        std::string::size_type prefixloc = key.find(ATTR_PREFIX);
        if (prefixloc == std::string::npos) {
            buflen -= keylen;
            keyptr += keylen;
            continue;
        }
        std::string attr_value = get_attr(dpp, key, attr_name, y);
        bufferlist bl_value;
        ceph::encode(attr_value, bl_value);
        attrs.emplace(std::move(attr_name), std::move(bl_value));

    }
    return 0;
}

int SSDDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& [attr_name, attr_val_bl] : attrs) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): attr_name = " << attr_name << " attr_val_bl length: " << attr_val_bl.length() << dendl;
        if (attr_val_bl.length() != 0) {
            auto ret = set_attr(dpp, key, attr_name, attr_val_bl.c_str(), y);
            if (ret < 0) {
                ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << " key: " << key << dendl;
                return ret;
            }
        }
    }
    return 0;
}

std::string SSDDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): get_attr: key: " << attr_name << dendl;

    int attr_size = getxattr(location.c_str(), attr_name.c_str(), nullptr, 0);

    std::string attr_val;
    attr_val.reserve(attr_size + 1);
    char* attr_val_ptr =  &attr_val[0];

    attr_size = getxattr(location.c_str(), attr_name.c_str(), attr_val_ptr, attr_size);
    if (attr_size < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not get attr value for attr name: " << attr_name << " key: " << key << dendl;
    }

    return attr_val;
}

int SSDDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << attr_val << dendl;

    return setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), 0);
}

int SSDDriver::delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    return removexattr(location.c_str(), attr_name.c_str());
}

} } // namespace rgw::cache
