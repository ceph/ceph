#include "common/async/completion.h"
#include "common/errno.h"
#include "common/async/blocked_completion.h"
#include "rgw_ssd_driver.h"
#if defined(__linux__)
#include <features.h>
#include <sys/xattr.h>
#endif

#include <filesystem>
#include <errno.h>
namespace efs = std::filesystem;

namespace rgw { namespace cache {

int SSDDriver::initialize(const DoutPrefixProvider* dpp)
{
    if(partition_info.location.back() != '/') {
      partition_info.location += "/";
    }

    try {
        if (efs::exists(partition_info.location)) {
            if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
                ldpp_dout(dpp, 5) << "initialize: evicting the persistent storage directory on start" << dendl;

		uid_t uid = dpp->get_cct()->get_set_uid();
		gid_t gid = dpp->get_cct()->get_set_gid();

		ldpp_dout(dpp, 5) << "initialize:: uid is " << uid << " and gid is " << gid << dendl;
		ldpp_dout(dpp, 5) << "initialize:: changing permissions for datacache directory." << dendl;

		if (uid) { 
                  if (chown(partition_info.location.c_str(), uid, gid) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chown return error: " << strerror(errno) << dendl;
                  }

                  if (chmod(partition_info.location.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chmod return error: " << strerror(errno) << dendl;
                  }
		}

                for (auto& p : efs::directory_iterator(partition_info.location)) {
                    efs::remove_all(p.path());
                }
            }
        } else {
            ldpp_dout(dpp, 5) << "initialize:: creating the persistent storage directory on start: " << partition_info.location << dendl;
            std::error_code ec;
            if (!efs::create_directories(partition_info.location, ec)) {
                ldpp_dout(dpp, 0) << "initialize::: ERROR initializing the cache storage directory: '" << partition_info.location <<
                                "' : " << ec.value() << dendl;
            } else {
		uid_t uid = dpp->get_cct()->get_set_uid();
		gid_t gid = dpp->get_cct()->get_set_gid();

		ldpp_dout(dpp, 5) << "initialize:: uid is " << uid << " and gid is " << gid << dendl;
		ldpp_dout(dpp, 5) << "initialize:: changing permissions for datacache directory." << dendl;
		
		if (uid) { 
                  if (chown(partition_info.location.c_str(), uid, gid) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chown return error: " << strerror(errno) << dendl;
                  }

                  if (chmod(partition_info.location.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chmod return error: " << strerror(errno) << dendl;
                  }
		}
            }
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

int SSDDriver::restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func)
{
    if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
        return 0; //don't do anything as the cache directory must have been evicted during start-up
    }
    for (auto const& dir_entry : std::filesystem::directory_iterator{partition_info.location}) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): dir_entry.path: " << dir_entry.path() << dendl;
        std::string file_name = dir_entry.path().filename();
        std::vector<std::string> parts;
        std::string part;
        bool parsed = false;
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): filename: " << file_name << dendl;
        try {
            std::stringstream ss(file_name);
            while (std::getline(ss, part, '_')) {
                parts.push_back(part);
            }
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): parts.size(): " << parts.size() << dendl;
            //non-dirty or clean blocks - bucket_id, version, object_name in head block and offset, len in data blocks
            if (parts.size() == 3 || parts.size() == 5) {
                std::string key = file_name;
                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): file_name: " << file_name << dendl;

                std::string version = parts[1];
                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): version: " << version << dendl;

                uint64_t offset = 0, len = 0;
                if (parts.size() == 5) {
                    offset = std::stoull(parts[3]);
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): offset: " << offset << dendl;

                    len = std::stoull(parts[4]);
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): len: " << len << dendl;
                }
                std::string localWeightStr;
                auto ret = get_attr(dpp, file_name, RGW_CACHE_ATTR_LOCAL_WEIGHT, localWeightStr, null_yield);
                if (ret < 0) {
                    ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_LOCAL_WEIGHT << dendl;
                } else {
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
                }
                block_func(dpp, key, offset, len, version, false, null_yield, localWeightStr);
                parsed = true;
            }
            //dirty blocks - "D", bucket_id, version, object_name in head block and offset, len in data blocks
            if ((parts.size() == 4 || parts.size() == 6) && parts[0] == "D") {
                std::string prefix = "D_";
                if (file_name.starts_with(prefix)) {
                    std::string key = file_name.substr(prefix.length());
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key: " << key << dendl;

                    bool dirty = true;

                    std::string bucket_id = parts[1];
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): bucket_id: " << bucket_id << dendl;

                    std::string version = parts[2];
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): version: " << version << dendl;

                    std::string obj_name = parts[3];
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): obj_name: " << obj_name << dendl;

                    uint64_t len = 0, offset = 0;
                    std::string localWeightStr;
                    if (parts.size() == 4) {
                        rgw::sal::Attrs attrs;
                        get_attrs(dpp, file_name, attrs, null_yield);
                        std::string etag, bucket_name;
                        uint64_t size = 0;
                        time_t creationTime = time_t(nullptr);
                        rgw_user user;
                        rgw_obj_key obj_key;
                        if (attrs.find(RGW_ATTR_ETAG) != attrs.end()) {
                            etag = attrs[RGW_ATTR_ETAG].to_str();
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): etag: " << etag << dendl;
                        }
                        if (attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE) != attrs.end()) {
                            size = std::stoull(attrs[RGW_CACHE_ATTR_OBJECT_SIZE].to_str());
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): size: " << size << dendl;
                        }
                        if (attrs.find(RGW_CACHE_ATTR_MTIME) != attrs.end()) {
                            creationTime = ceph::real_clock::to_time_t(ceph::real_clock::from_double(std::stod(attrs[RGW_CACHE_ATTR_MTIME].to_str())));
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): creationTime: " << creationTime << dendl;
                        }
                        if (attrs.find(RGW_ATTR_ACL) != attrs.end()) {
                            bufferlist bl_acl = attrs[RGW_ATTR_ACL];
                            RGWAccessControlPolicy policy;
                            auto iter = bl_acl.cbegin();
                            try {
                                policy.decode(iter);
                            } catch (buffer::error& err) {
                                ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
                                continue;
                            }
                            user = std::get<rgw_user>(policy.get_owner().id);
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_user: " << user.to_str() << dendl;
                        }
                        obj_key.name = obj_name;
                        if (attrs.find(RGW_CACHE_ATTR_VERSION_ID) != attrs.end()) {
                            std::string instance = attrs[RGW_CACHE_ATTR_VERSION_ID].to_str();
                            if (instance != "null") {
                                obj_key.instance = instance;
                            }
                        }
                        if (attrs.find(RGW_CACHE_ATTR_OBJECT_NS) != attrs.end()) {
                            obj_key.ns = attrs[RGW_CACHE_ATTR_OBJECT_NS].to_str();
                        }
                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_obj_key: " << obj_key.get_oid() << dendl;
                        if (attrs.find(RGW_CACHE_ATTR_BUCKET_NAME) != attrs.end()) {
                            bucket_name = attrs[RGW_CACHE_ATTR_BUCKET_NAME].to_str();
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): bucket_name: " << bucket_name << dendl;
                        }

                        if (attrs.find(RGW_CACHE_ATTR_LOCAL_WEIGHT) != attrs.end()) {
                            localWeightStr = attrs[RGW_CACHE_ATTR_LOCAL_WEIGHT].to_str();
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
                        }

                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): calling func for: " << key << dendl;
                        obj_func(dpp, key, version, dirty, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, null_yield);
                        block_func(dpp, key, offset, len, version, dirty, null_yield, localWeightStr);
                        parsed = true;
                    } //end-if part.size() == 4
                    if (parts.size() == 6) {
                        offset = std::stoull(parts[4]);
                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): offset: " << offset << dendl;

                        len = std::stoull(parts[5]);
                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): len: " << len << dendl;
                        std::string localWeightStr;
                        auto ret = get_attr(dpp, file_name, RGW_CACHE_ATTR_LOCAL_WEIGHT, localWeightStr, null_yield);
                        if (ret < 0) {
                            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_LOCAL_WEIGHT << dendl;
                        } else {
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
                        }
                        block_func(dpp, key, offset, len, version, dirty, null_yield, localWeightStr);
                        parsed = true;
                    }
                } //end-if file_name.starts_with
            } //end-if parts.size() == 4 || parts.size() == 6
            if (!parsed) {
                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Unable to parse file_name: " << file_name << dendl;
                continue;
            }
        }//end-try
        catch(...) {
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Execption while parsing file_name: " << file_name << dendl;
            continue;
        }
    }

    return 0;
}

int SSDDriver::put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    boost::system::error_code ec;
    if (y) {
        using namespace boost::asio;
        yield_context yield = y.get_yield_context();
        auto ex = yield.get_executor();
        this->put_async(dpp, ex, key, bl, len, attrs, yield[ec]);
    } else {
      auto ex = boost::asio::system_executor{};
      this->put_async(dpp, ex, key, bl, len, attrs, ceph::async::use_blocked[ec]);
    }
    if (ec) {
        return ec.value();
    }
    return 0;
}

int SSDDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
    char buffer[len];
    std::string location = partition_info.location + "/" + url_encode(key, true);

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
    std::string location = partition_info.location + "/" + url_encode(key, true);

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

template <typename Executor, typename CompletionToken>
auto SSDDriver::get_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                off_t read_ofs, off_t read_len, CompletionToken&& token)
{
  using Op = AsyncReadOp;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const std::string& key,
              off_t read_ofs, off_t read_len) {
    auto p = Op::create(ex, handler);
    auto& op = p->user_data;

    std::string location = partition_info.location + "/" + url_encode(key, true);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

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
  }, token, dpp, ex, key, read_ofs, read_len);
}

template <typename Executor, typename CompletionToken>
void SSDDriver::put_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token)
{
  using Op = AsyncWriteRequest;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const std::string& key, const bufferlist& bl,
              uint64_t len, const rgw::sal::Attrs& attrs) {
    auto p = Op::create(ex, handler);
    auto& op = p->user_data;

    std::string location = partition_info.location + "/" + url_encode(key, true);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

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
  }, token, dpp, ex, key, bl, len, attrs);
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                off_t read_ofs, off_t read_len, const std::string& key) {
  return [this, dpp, y, read_ofs, read_len, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_read_op(): Read From Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    yield_context yield = y.get_yield_context();
    auto ex = yield.get_executor();

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->get_async(dpp, ex, key, read_ofs, read_len, bind_executor(ex, SSDDriver::libaio_read_handler{aio, r}));
  };
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key) {
  return [this, dpp, y, bl, len, attrs, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_write_op(): Write to Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    yield_context yield = y.get_yield_context();
    auto ex = yield.get_executor();

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->put_async(dpp, ex, key, bl, len, attrs, bind_executor(ex, SSDDriver::libaio_write_handler{aio, r}));
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
    std::string location = partition_info.location + "/" + url_encode(key, true);

    if (!efs::remove(location)) {
        ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the file: " << location << dendl;
        return -EIO;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y)
{ 
    std::string location = partition_info.location;

    int ret = std::rename((location + "/" + url_encode(oldKey, true)).c_str(), (location + "/" + url_encode(newKey, true)).c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDDriver: ERROR: failed to rename the file: " << location + "/" + url_encode(oldKey, true) << dendl;
        return ret;
    }

    return 0;
}


int SSDDriver::AsyncWriteRequest::prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string key, std::string cache_location)
{
    std::string location = cache_location + "/" + url_encode(key, true);
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
        return;
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
            return;
        }
    }

    Partition partition_info = op.priv_data->get_current_partition_info(op.dpp);
    efs::space_info space = efs::space(partition_info.location);
    op.priv_data->set_free_space(op.dpp, space.available);

    std::string new_path = partition_info.location + "/" + url_encode(op.key, true);
    std::string old_path = partition_info.location + "/" + url_encode(op.temp_key, true);

    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_yield_cb: temp_key: " << op.temp_key << dendl;

    ret = std::rename(old_path.c_str(), new_path.c_str());
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
        std::string::size_type prefixloc = attr_name.find(RGW_ATTR_PREFIX);
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
    std::string location = partition_info.location + "/" + url_encode(key, true);
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
