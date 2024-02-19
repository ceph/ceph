#include "common/async/completion.h"
#include "common/errno.h"
#include "common/async/blocked_completion.h"
#include "rgw_ssd_driver.h"
#if defined(__linux__)
#include <features.h>
#include <sys/xattr.h>
#endif

#include <filesystem>
namespace efs = std::filesystem;

namespace rgw { namespace cache {

constexpr std::string_view ATTR_PREFIX = "user.rgw.";

int SSDDriver::initialize(const DoutPrefixProvider* dpp)
{
    if(partition_info.location.back() != '/') {
      partition_info.location += "/";
    }
    try {
        if (efs::exists(partition_info.location)) {
            if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
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
    ainit.aio_threads = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_threads");
    ainit.aio_num = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_num");
    ainit.aio_idle_time = 120;
    aio_init(&ainit);
    #endif

    efs::space_info space = efs::space(partition_info.location);
    //currently partition_info.size is unused
    this->free_space = space.available;

    return 0;
}

int SSDDriver::put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    boost::system::error_code ec;
    if (y) {
        using namespace boost::asio;
        spawn::yield_context yield = y.get_yield_context();
        this->put_async(dpp, y.get_io_context(), key, bl, len, attrs, yield[ec]);
    } else {
        this->put_async(dpp, y.get_io_context(), key, bl, len, attrs, ceph::async::use_blocked[ec]);
    }
    if (ec) {
        return ec.value();
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
        ldpp_dout(dpp, 0) << "ERROR: get::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    fseek(cache_file, offset, SEEK_SET);

    nbytes = fread(buffer, 1, len, cache_file);
    if (nbytes != len) {
        fclose(cache_file);
        ldpp_dout(dpp, 0) << "ERROR: get::io_read: fread has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }

    bl.append(buffer, len);

    r = get_attrs(dpp, key, attrs, y);
    if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::get_attrs: failed to get attrs, r = " << r << dendl;
        return r;
    }

    return 0;
}

int SSDDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y)
{
    bufferlist src = bl_data;
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

    nbytes = fwrite(src.c_str(), 1, src.length(), cache_file);
    if (nbytes != src.length()) {
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

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::AsyncWriteRequest::create(const Executor1& ex1, CompletionHandler&& handler)
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

    int ret = op.prepare_libaio_read_op(dpp, location, read_ofs, read_len, p.get());
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

template <typename ExecutionContext, typename CompletionToken>
void SSDDriver::put_async(const DoutPrefixProvider *dpp, ExecutionContext& ctx, const std::string& key,
                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    using Op = AsyncWriteRequest;
    using Signature = typename Op::Signature;
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto p = Op::create(ctx.get_executor(), init.completion_handler);
    auto& op = p->user_data;

    int r = 0;
    bufferlist src = bl;
    std::string temp_key = key + "_" + std::to_string(index++);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): temp key=" << temp_key << dendl;
    r = op.prepare_libaio_write_op(dpp, src, len, temp_key, partition_info.location);
    op.cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    op.cb->aio_sigevent.sigev_notify_function = SSDDriver::AsyncWriteRequest::libaio_write_cb;
    op.cb->aio_sigevent.sigev_notify_attributes = nullptr;
    op.cb->aio_sigevent.sigev_value.sival_ptr = (void*)p.get();
    op.key = key;
    op.temp_key = temp_key;
    op.dpp = dpp;
    op.priv_data = this;
    op.attrs = std::move(attrs);
    if (r >= 0) {
        r = ::aio_write(op.cb.get());
    } else {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::prepare_libaio_write_op(), r=" << r << dendl;
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_write(), r=" << r << dendl;
    if(r < 0) {
        auto ec = boost::system::error_code{-r, boost::system::system_category()};
        ceph::async::post(std::move(p), ec);
    } else {
        (void)p.release();
    }
    init.result.get();
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
    this->get_async(dpp, y.get_io_context(), key, read_ofs, read_len, bind_executor(ex, SSDDriver::libaio_read_handler{aio, r}));
  };
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key) {
  return [this, dpp, y, bl, len, attrs, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_write_op(): Write to Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    spawn::yield_context yield = y.get_yield_context();
    async_completion<spawn::yield_context, void()> init(yield);
    auto ex = get_associated_executor(init.completion_handler);

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->put_async(dpp, y.get_io_context(), key, bl, len, attrs, bind_executor(ex, SSDDriver::libaio_write_handler{aio, r}));
  };
}

rgw::AioResultList SSDDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, ssd_cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

rgw::AioResultList SSDDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, ssd_cache_write_op(dpp, y, this, bl, len, attrs, key), cost, id);
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
    cb.reset(new struct aiocb);
    memset(cb.get(), 0, sizeof(struct aiocb));
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    r = fd = TEMP_FAILURE_RETRY(::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode));
    if (fd < 0) {
        ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << location.c_str() << "'" << dendl;
        return r;
    }
    if (dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(fd, 0, 0, dpp->get_cct()->_conf->rgw_d4n_l1_fadvise);
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

void SSDDriver::AsyncWriteRequest::libaio_write_cb(sigval sigval) {
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_cb: key: " << op.key << dendl;
    int ret = -aio_error(op.cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
        ceph::async::dispatch(std::move(p), ec);
    }
    int attr_ret = 0;
    if (op.attrs.size() > 0) {
        //TODO - fix yield_context
        optional_yield y{null_yield};
        attr_ret = op.priv_data->set_attrs(op.dpp, op.temp_key, op.attrs, y);
        if (attr_ret < 0) {
            ldpp_dout(op.dpp, 0) << "ERROR: AsyncWriteRequest::libaio_write_yield_cb::set_attrs: failed to set attrs, ret = " << attr_ret << dendl;
            ec.assign(-ret, boost::system::system_category());
            ceph::async::dispatch(std::move(p), ec);
        }
    }

    Partition partition_info = op.priv_data->get_current_partition_info(op.dpp);
    efs::space_info space = efs::space(partition_info.location);
    op.priv_data->set_free_space(op.dpp, space.available);

    std::string new_path = partition_info.location + op.key;
    std::string old_path = partition_info.location + op.temp_key;

    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_yield_cb: temp_key: " << op.temp_key << dendl;

    ret = rename(old_path.c_str(), new_path.c_str());
    if (ret < 0) {
        ret = errno;
        ldpp_dout(op.dpp, 0) << "ERROR: put::rename: failed to rename file: " << ret << dendl;
        ec.assign(-ret, boost::system::system_category());
    }
    ceph::async::dispatch(std::move(p), ec);
}

int SSDDriver::AsyncReadOp::prepare_libaio_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg)
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
    if (dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL) {
        posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d4n_l1_fadvise);
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

int SSDDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : attrs) {
        std::string attr_name = it.first;
        std::string attr_val = it.second.to_str();
        std::string old_attr_val;
        auto ret = get_attr(dpp, key, attr_name, old_attr_val, y);
        if (old_attr_val.empty()) {
            ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_CREATE);
        } else {
            ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_REPLACE);
        }
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not modify attr value for attr name: " << attr_name << " key: " << key << " ERROR: " << cpp_strerror(errno) <<dendl;
            return ret;
        }
    }
    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;
    return 0;
}

int SSDDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : del_attrs) {
        auto ret = delete_attr(dpp, key, it.first);
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << it.first << " key: " << key << cpp_strerror(errno) << dendl;
            return ret;
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

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
        std::string::size_type prefixloc = attr_name.find(ATTR_PREFIX);
        buflen -= keylen;
        keyptr += keylen;
        if (prefixloc == std::string::npos) {
            continue;
        }
        std::string attr_value;
        get_attr(dpp, key, attr_name, attr_value, y);
        bufferlist bl_value;
        bl_value.append(attr_value);
        attrs.emplace(std::move(attr_name), std::move(bl_value));
    }
    return 0;
}

int SSDDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& [attr_name, attr_val_bl] : attrs) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): attr_name = " << attr_name << " attr_val_bl length: " << attr_val_bl.length() << dendl;
        if (attr_val_bl.length() != 0) {
            auto ret = set_attr(dpp, key, attr_name, attr_val_bl.to_str(), y);
            if (ret < 0) {
                ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
                return ret;
            }
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): get_attr: key: " << attr_name << dendl;

    int attr_size = getxattr(location.c_str(), attr_name.c_str(), nullptr, 0);
    if (attr_size < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << attr_name << ": " << cpp_strerror(errno) << dendl;
        attr_val = "";
        return errno;
    }

    if (attr_size == 0) {
        ldpp_dout(dpp, 0) << "ERROR: no attribute value found for attr_name: " << attr_name << dendl;
        attr_val = "";
        return 0;
    }

    attr_val.resize(attr_size);
    attr_size = getxattr(location.c_str(), attr_name.c_str(), attr_val.data(), attr_size);
    if (attr_size < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not get attr value for attr name: " << attr_name << " key: " << key << dendl;
        attr_val = "";
        return errno;
    }

    return 0;
}

int SSDDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << attr_val << dendl;

    auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), 0);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name)
{
    std::string location = partition_info.location + key;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    auto ret = removexattr(location.c_str(), attr_name.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

} } // namespace rgw::cache
