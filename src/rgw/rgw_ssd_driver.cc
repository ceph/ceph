#include <boost/asio/system_executor.hpp>
#include <boost/container/small_vector.hpp>

#include "common/async/completion.h"
#include "common/errno.h"
#include "common/async/blocked_completion.h"
#include "rgw_ssd_driver.h"
#if defined(__linux__)
#include <features.h>
#include <sys/xattr.h>
#endif

#include <errno.h>
namespace efs = std::filesystem;

namespace rgw { namespace cache {

constexpr auto CHECK_INTERVAL = std::chrono::minutes(10);  // Check every 10 minutes

static std::atomic<uint64_t> index{0};
static std::atomic<uint64_t> dir_index{0};

constexpr auto calculate_free_space(std::unsigned_integral auto available_space, std::unsigned_integral auto reserve_space)
{
    return available_space < reserve_space ? 0 : available_space - reserve_space;
}

constexpr auto adjust_free_space(std::unsigned_integral auto free_space, std::unsigned_integral auto size)
{
    return free_space < size ? 0 : free_space - size;
}

constexpr auto adjust_reserved_space(std::unsigned_integral auto reserved_space, std::unsigned_integral auto size)
{
    return reserved_space < size ? 0 : reserved_space - size;
}

static std::vector<std::string> tokenize_key(std::string_view key)
{
    std::vector<std::string> tokens;
    size_t start = 0, end = 0;
    while ((end = key.find(CACHE_DELIM, start)) != std::string_view::npos) {
        tokens.emplace_back(key.substr(start, end - start));
        start = end + 1;
    }
    // Add the last token
    if (start < key.length()) {
        tokens.emplace_back(key.substr(start));
    }
    return tokens;
}

/*
* Parses key to return directory path and file name
*/
static void parse_key(const DoutPrefixProvider* dpp, const std::string& location, const std::string& key, std::string& dir_path, std::string& file_name, bool temp = false) {
    ldpp_dout(dpp, 10) << __func__ << "() key is: " << key << dendl;
    std::string bucket_id, object, version;
    std::vector<std::string> parts = tokenize_key(key);

    ldpp_dout(dpp, 10) << __func__ << "() parts.size() is " << parts.size() << dendl;

    if (parts.size() == 3 || parts.size() == 5) {
        bucket_id = parts[0];
        ldpp_dout(dpp, 10) <<  __func__ << "() bucket_id is " << bucket_id << dendl;
        object = parts[2];
        ldpp_dout(dpp, 10) <<  __func__ << "() object is " << object << dendl;
        version = parts[1];
        if (parts.size() == 5) { //has offset and length
            version += CACHE_DELIM + parts[3] + CACHE_DELIM + parts[4];
        }
        if (temp) {
            version += "_" + std::to_string(index++);
        }
        ldpp_dout(dpp, 10) <<  __func__ << "() version is " << version << dendl;
        dir_path = location + "/" + bucket_id + "/" + object;
        file_name = version;
        ldpp_dout(dpp, 10) <<  __func__ << "() dir_path is " << dir_path << dendl;
    }
    return;
}

static void create_directories(const DoutPrefixProvider* dpp, const std::string& dir_path)
{
    std::error_code ec;
    std::string temp_dir_path = dir_path + "_" + std::to_string(dir_index++);
    if (!efs::exists(dir_path, ec)) {
        if (!efs::create_directories(temp_dir_path, ec)) {
            ldpp_dout(dpp, 0) << "create_directories::: ERROR creating directory: '" << temp_dir_path <<
                            "' : " << ec.value() << dendl;
        } else {
            efs::rename(temp_dir_path, dir_path, ec);
            if (ec) {
                ldpp_dout(dpp, 0) << "create_directories::: ERROR renaming directory: '" << temp_dir_path <<
                            "' : " << ec.value() << dendl;
                efs::remove(temp_dir_path, ec);
            } else {
                uid_t uid = dpp->get_cct()->get_set_uid();
                gid_t gid = dpp->get_cct()->get_set_gid();

                ldpp_dout(dpp, 5) << "create_directories:: uid is " << uid << " and gid is " << gid << dendl;
                ldpp_dout(dpp, 5) << "create_directories:: changing permissions for directory: " << dendl;

                if (uid) {
                    if (chown(dir_path.c_str(), uid, gid) == -1) {
                        ldpp_dout(dpp, 5) << "create_directories: chown return error: " << strerror(errno) << dendl;
                    }

                    if (chmod(dir_path.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
                        ldpp_dout(dpp, 5) << "create_directories: chmod return error: " << strerror(errno) << dendl;
                    }
                }
            }
        }
    }
}

static inline std::string get_file_path(const DoutPrefixProvider* dpp, const std::string& dir_path, const std::string& file_name)
{
    return dir_path + "/" + file_name;
}

static std::string create_dirs_get_filepath_from_key(const DoutPrefixProvider* dpp, const std::string& location, const std::string& key, bool temp=false)
{
    std::string dir_path, file_name;
    parse_key(dpp, location, key, dir_path, file_name, temp);
    create_directories(dpp, dir_path);
    return get_file_path(dpp, dir_path, file_name);

}

/* This coroutine operates in a background thread to sync the free_space variable with efs::space.
 * The efs::space call is expensive (especially under a lock) so this sync occurs every 10 minutes. */
void SSDDriver::background_free_space_sync_worker(const DoutPrefixProvider* dpp, optional_yield y)
{
  ldpp_dout(dpp, 10) << "Background free_space co-routine started" << dendl;
  while (!quit) {
    free_space_timer->expires_after(CHECK_INTERVAL);
    boost::system::error_code ec;
    free_space_timer->async_wait(y.get_yield_context()[ec]);

    if (ec == boost::asio::error::operation_aborted || quit.load()) {
      break;
    }

    std::unique_lock<std::shared_mutex> l(cache_lock);
    efs::space_info space = efs::space(partition_info.location);
    free_space = calculate_free_space(space.available, partition_info.reserve_size);

    ldpp_dout(dpp, 20) << "SSDDriver:: " << __func__ << "(): free_space: " << free_space << dendl;
  }
}

int SSDDriver::initialize(const DoutPrefixProvider* dpp)
{
    if(partition_info.location.back() != '/') {
      partition_info.location += "/";
    }

    if (!admin) { // Only initialize or evict cache if radosgw-admin is not responsible for call 
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
      return -EINVAL;
      }
    }

    #if defined(HAVE_LIBAIO) && defined(__GLIBC__)
    // libaio setup
    struct aioinit ainit{0};
    ainit.aio_threads = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_threads");
    ainit.aio_num = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_num");
    ainit.aio_idle_time = 120;
    aio_init(&ainit);
    #endif

    /* The free_space variable is to be set using efs::space only upon initialization in the main thread. Afterwards, it will 
     * be updated during cache ops that handle data (not attributes) by adding/subtracting the size of the data itself.
     * Recalibration of free_space occurs in a background thread every 10 minutes with additional efs::space calls to improve 
     * cache performance while maintaining partition correctedness. */
    efs::space_info space = efs::space(partition_info.location);
    partition_info.size = space.capacity - partition_info.reserve_size;
    free_space = calculate_free_space(space.available, partition_info.reserve_size);
    reserved_space = 0;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): reserved_space=" << reserved_space << dendl;

    free_space_timer.emplace(io_context);
    boost::asio::spawn(
        io_context,
        [this, dpp](boost::asio::yield_context yield) {
          optional_yield y{yield};
          background_free_space_sync_worker(dpp, y);
        },
        [this, dpp](std::exception_ptr e) {
          if (e) {
            free_space_done_promise.set_exception(e);
          } else {
            free_space_done_promise.set_value();
          }
          ldpp_dout(dpp, 10) << "Background free_space co-routine stopped" << dendl;
      }
    );
    return 0;
}

//Helper method to parse the filename in the cache, sample format is version#offset#block_len
static std::vector<std::string> parse_cache_filename(const DoutPrefixProvider* dpp, const std::filesystem::path& filename)
{
    std::vector<std::string> parts;
    std::string file_str = filename.string();
    std::string part;
    std::stringstream ss(file_str);

    while (std::getline(ss, part, CACHE_DELIM)) {
        parts.push_back(part);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): parts.size(): " << parts.size() << dendl;
    return parts;
}

//Helper method to build cache key which is used in d4n filter driver and policy methods
static std::string build_cache_key(const std::string& bucket_id,
                            const std::string& version,
                            const std::string& object_name)
{
    return url_encode(bucket_id, true) + CACHE_DELIM +
           url_encode(version, true) + CACHE_DELIM +
           url_encode(object_name, true);
}

//Helper method to extract an attr and convert to a sting
std::string SSDDriver::extract_attr_string(const DoutPrefixProvider* dpp,
                                           const rgw::sal::Attrs& attrs,
                                           const std::string& attr_name)
{
    if (attrs.contains(attr_name)) {
        return attrs.at(attr_name).to_str();
    }
    ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << attr_name << dendl;
    return {};
}

//Helper method to build rgw_obj_key from instance and ns
rgw_obj_key SSDDriver::build_obj_key(const std::string& object_name,
                                     const rgw::sal::Attrs& attrs)
{
    rgw_obj_key obj_key;
    obj_key.name = object_name;

    if (attrs.contains(RGW_CACHE_ATTR_VERSION_ID)) {
        std::string instance = attrs.at(RGW_CACHE_ATTR_VERSION_ID).to_str();
        if (instance != "null") {
            obj_key.instance = instance;
        }
    }

    if (attrs.contains(RGW_CACHE_ATTR_OBJECT_NS)) {
        obj_key.ns = attrs.at(RGW_CACHE_ATTR_OBJECT_NS).to_str();
    }

    return obj_key;
}

//Helper method to decode ACL and extract user name from it
rgw_user SSDDriver::extract_user_from_attrs(const DoutPrefixProvider* dpp,
                                            const rgw::sal::Attrs& attrs)
{
    rgw_user user;

    if (attrs.contains(RGW_ATTR_ACL)) {
        try {
            bufferlist bl_acl = attrs.at(RGW_ATTR_ACL);
            RGWAccessControlPolicy policy;
            auto iter = bl_acl.cbegin();
            policy.decode(iter);
            user = std::get<rgw_user>(policy.get_owner().id);
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_user: " << user.to_str() << dendl;
        } catch (buffer::error& err) {
            ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
        }
    }

    return user;
}

//Helper method to check if the block is a delete marker
bool SSDDriver::extract_delete_marker(const DoutPrefixProvider* dpp,
                                      const rgw::sal::Attrs& attrs)
{
    if (attrs.contains(RGW_CACHE_ATTR_DELETE_MARKER)) {
        std::string deleteMarkerStr = attrs.at(RGW_CACHE_ATTR_DELETE_MARKER).to_str();
        return (deleteMarkerStr == "1");
    }
    return false;
}

//Helper method to process data blocks, of the format version#offset#len
void SSDDriver::process_data_block(const DoutPrefixProvider* dpp,
                                   const std::filesystem::path& file_path,
                                   const std::string& base_key,
                                   const std::string& version,
                                   const std::string& bucket_id,
                                   const std::string& object_name,
                                   const std::vector<std::string>& parts,
                                   bool dirty,
                                   const rgw::sal::Attrs& attrs,
                                   ObjectDataCallback obj_func,
                                   BlockDataCallback block_func)
{
    uint64_t offset = std::stoull(parts[1]);
    uint64_t len = std::stoull(parts[2]);

    std::string key = base_key + CACHE_DELIM + std::to_string(offset) + CACHE_DELIM + std::to_string(len);

    rgw_obj_key obj_key = build_obj_key(object_name, attrs);
    std::string instance = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_VERSION_ID);
    std::string invalidStr = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_INVALID);
    std::string localWeightStr = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_LOCAL_WEIGHT);
    rgw_user user = extract_user_from_attrs(dpp, attrs);
    std::string bucket_name = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_BUCKET_NAME);

    // Mark invalid blocks as clean for eviction
    if (invalidStr == "1") {
        dirty = false;
    }

    block_func(dpp, key, offset, len, version, dirty, user, bucket_name, null_yield, localWeightStr);
    if (dirty) {
        obj_func(dpp, key, version, false, bucket_id, obj_key, instance, null_yield, invalidStr);
    }
}

//TODO - check if this can be removed
void SSDDriver::process_clean_head_block(const DoutPrefixProvider* dpp,
                                         const std::filesystem::path& file_path,
                                         const std::string& key,
                                         const std::string& version,
                                         const rgw::sal::Attrs& attrs,
                                         ObjectDataCallback obj_func,
                                         BlockDataCallback block_func)
{
    std::string localWeightStr = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_LOCAL_WEIGHT);
    rgw_user user = extract_user_from_attrs(dpp, attrs);
    std::string bucket_name = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_BUCKET_NAME);

    uint64_t offset = 0, len = 0;
    block_func(dpp, key, offset, len, version, false, user, bucket_name, null_yield, localWeightStr);
}

/* Helper method to process a dirty head block, cached only for delete markers since they do not
 * have data, format: version (there is no offset and len attached as it is a 0 sized block)
 */
void SSDDriver::process_dirty_head_block(const DoutPrefixProvider* dpp,
                                         const std::filesystem::path& file_path,
                                         const std::string& key,
                                         const std::string& version,
                                         const std::string& bucket_id,
                                         const std::string& object_name,
                                         const rgw::sal::Attrs& attrs,
                                         ObjectDataCallback obj_func,
                                         BlockDataCallback block_func)
{
    rgw_obj_key obj_key = build_obj_key(object_name, attrs);
    std::string instance = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_VERSION_ID);
    bool deleteMarker = extract_delete_marker(dpp, attrs);
    std::string invalidStr = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_INVALID);
    std::string localWeightStr = extract_attr_string(dpp, attrs, RGW_CACHE_ATTR_LOCAL_WEIGHT);
    rgw_user user = extract_user_from_attrs(dpp, attrs);

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): calling func for: " << key << dendl;
    obj_func(dpp, key, version, deleteMarker, bucket_id, obj_key, instance, null_yield, invalidStr);

    uint64_t offset = 0, len = 0;
    std::string bucket_name;
    block_func(dpp, key, offset, len, version, true, user, bucket_name, null_yield, localWeightStr);
}

//Helper method to check if a block is dirty
bool SSDDriver::get_dirty_flag(const DoutPrefixProvider* dpp, const std::filesystem::path& file_path)
{
    std::string dirtyStr;
    auto ret = get_attr(dpp, file_path, RGW_CACHE_ATTR_DIRTY, dirtyStr, null_yield);

    if (ret == 0 && dirtyStr == "1") {
        ldpp_dout(dpp, 10) << "SSDCache: " << __func__ << "(): Dirty xattr retrieved" << dendl;
        return true;
    } else if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: "
                         << RGW_CACHE_ATTR_DIRTY << ", ret=" << ret << dendl;
    }
    return false;
}

//Helper method to process cache files, could be a head block(for a delete marker only) or a data block
void SSDDriver::process_cache_file(const DoutPrefixProvider* dpp,
                                   const std::filesystem::path& file_path,
                                   const std::string& bucket_id,
                                   const std::string& object_name,
                                   ObjectDataCallback obj_func,
                                   BlockDataCallback block_func)
{
    std::vector<std::string> parts = parse_cache_filename(dpp, file_path.filename());

    if (parts.size() != 1 && parts.size() != 3) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Unable to parse: "
                          << file_path.filename() << dendl;
        return;
    }

    rgw::sal::Attrs attrs;
    get_attrs(dpp, file_path, attrs, null_yield);

    std::string version = url_decode(parts[0]);
    std::string key = build_cache_key(bucket_id, version, object_name);
    bool dirty = get_dirty_flag(dpp, file_path);

    if (parts.size() == 1) {
        if (dirty) {
            process_dirty_head_block(dpp, file_path, key, version, bucket_id, object_name, attrs, obj_func, block_func);
        } else {
            process_clean_head_block(dpp, file_path, key, version, attrs, obj_func, block_func);
        }
    } else {
        process_data_block(dpp, file_path, key, version, bucket_id, object_name, parts, dirty, attrs, obj_func, block_func);
    }
}

//Helper method to iterate through object subdirectories
void SSDDriver::restore_object_files(const DoutPrefixProvider* dpp,
                                     const std::filesystem::path& object_path,
                                     const std::string& bucket_id,
                                     const std::string& object_name,
                                     ObjectDataCallback obj_func,
                                     BlockDataCallback block_func)
{
    for (auto const& file_entry : efs::directory_iterator{object_path}) {
        if (!file_entry.is_regular_file()) continue;
        try {
            process_cache_file(dpp, file_entry.path(), bucket_id, object_name, obj_func, block_func);
        } catch (...) {
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Exception parsing: "
                              << file_entry.path() << dendl;
        }
    }
}

/* Helper method to iterate through bucket directories.
 * format of a file is cache_location/bucket-id/object-name/cache-file,
 * where cache-file can be a head/data block. Head blocks are only for
 * delete markers
 */
void SSDDriver::restore_bucket_objects(const DoutPrefixProvider* dpp,
                                       const std::filesystem::path& bucket_path,
                                       const std::string& bucket_id,
                                       ObjectDataCallback obj_func,
                                       BlockDataCallback block_func)
{
    for (auto const& object_dir : efs::directory_iterator{bucket_path}) {
        if (!object_dir.is_directory()) continue;
        std::string object_name = object_dir.path().filename();
        restore_object_files(dpp, object_dir.path(), bucket_id, object_name, obj_func, block_func);
    }
}

/* Method to restore blocks and objects from cache data upon rgw restart
 * and is called from LFUDAPolicy::init() method
 */
int SSDDriver::restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func)
{
    if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
        return 0;
    }

    std::string cache_location = partition_info.location;
    if (cache_location.back() == '/') {
        cache_location.pop_back();
    }

    for (auto const& bucket_dir : efs::directory_iterator{cache_location}) {
        if (!bucket_dir.is_directory()) continue;
        
        std::string bucket_id = bucket_dir.path().filename();
        restore_bucket_objects(dpp, bucket_dir.path(), bucket_id, obj_func, block_func);
    }

    return 0;
}

uint64_t SSDDriver::get_free_space(const DoutPrefixProvider* dpp, optional_yield y)
{
	std::shared_lock<std::shared_mutex> l(cache_lock);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): free_space=" << free_space << dendl;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): reserved_space=" << reserved_space << dendl;
	return (free_space < reserved_space) ? 0 : (free_space - reserved_space);
}

int SSDDriver::reserve_space(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) 
{
    std::unique_lock<std::shared_mutex> l(cache_lock);
	reserved_space += size;
	return 0;
}

int SSDDriver::check_and_reserve_space(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) 
{
    std::unique_lock<std::shared_mutex> l(cache_lock);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): free_space=" << free_space << dendl;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): reserved_space=" << reserved_space << dendl;
	uint64_t visible = (free_space < reserved_space) ? 0 : (free_space - reserved_space);
	if (visible < size) {
		return -ENOSPC;
	}
	reserved_space += size;
	return 0;
}

int SSDDriver::release_reservation(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) 
{
    std::unique_lock<std::shared_mutex> l(cache_lock);
	reserved_space = adjust_reserved_space(reserved_space, size);
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

    uint64_t size = len;
    if (attrs.size()) {
      size += XATTR_OVERHEAD_ESTIMATE;
    } 
	std::unique_lock<std::shared_mutex> l(cache_lock);
    free_space = adjust_free_space(free_space, size);
    reserved_space = adjust_reserved_space(reserved_space, size);
	return 0;
}

int SSDDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y)
{
    bufferlist src = bl_data;
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);

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
	std::unique_lock<std::shared_mutex> l(cache_lock);
    free_space = adjust_free_space(free_space, bl_data.length());
    reserved_space = adjust_reserved_space(reserved_space, bl_data.length());
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

    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
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
        // coverity[leaked_storage:SUPPRESS]
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

    op.file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.file_path=" << op.file_path << dendl;

    op.temp_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, key, true);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.temp_file_path=" << op.temp_file_path << dendl;

    int r = 0;
    bufferlist src = bl;
    r = op.prepare_libaio_write_op(dpp, src, len, op.temp_file_path);
    op.cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    op.cb->aio_sigevent.sigev_notify_function = SSDDriver::AsyncWriteRequest::libaio_write_cb;
    op.cb->aio_sigevent.sigev_notify_attributes = nullptr;
    op.cb->aio_sigevent.sigev_value.sival_ptr = (void*)p.get();
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
        ceph::async::dispatch(std::move(p), ec);
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

int SSDDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;

    if (y) {
        using namespace boost::asio;
        boost::system::error_code ec;
        yield_context yield = y.get_yield_context();
        auto ex = yield.get_executor();
        bl = this->get_async(dpp, ex, key, offset, len, boost::asio::bind_executor(ex, yield[ec]));
        if (ec) {
            ldpp_dout(dpp, 0) << "ERROR: SSDCache: get_async failed, ec=" << ec.message() << dendl;
            return -ec.value();
        }
    } else {
        boost::container::small_vector<char, 1024> buffer(len);
        auto deleter = [](FILE* f) { fclose(f); };
        auto cache_file = std::unique_ptr<FILE, decltype(deleter)>(
            fopen(location.c_str(), "r+"), deleter);

        if (cache_file == nullptr) {
            ldpp_dout(dpp, 0) << "ERROR: get::fopen file has return error, errno=" << errno << dendl;
            return -errno;
        }

        if (fseek(cache_file.get(), offset, SEEK_SET) != 0) {
            ldpp_dout(dpp, 0) << "ERROR: get::fseek failed, errno=" << errno << dendl;
            return -errno;
        }

        size_t nbytes = fread(buffer.data(), 1, len, cache_file.get());
        if (nbytes != len) {
            ldpp_dout(dpp, 0) << "ERROR: get::io_read: fread has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
            return -EIO;
        }

        bl.append(buffer.data(), len);
    }

    ldpp_dout(dpp, 20) << "INFO: get::bl length: = " << bl.length() << dendl;
    if (auto r = get_attrs(dpp, location, attrs, y); r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::get_attrs: failed to get attrs, r = " << r << dendl;
        return r;
    }
    return 0;
}

int SSDDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y)
{
    std::string dir_path, file_name;
    parse_key(dpp, partition_info.location, key, dir_path, file_name);
    std::string location = get_file_path(dpp, dir_path, file_name);
    ldpp_dout(dpp, 20) << "INFO: delete_data::file to remove: " << location << dendl;
    std::error_code ec;
	uint64_t size = 0;

	if (efs::is_regular_file(location)) {
		size = efs::file_size(location);
	}

    //Remove file
    if (!efs::remove(location, ec)) {
        ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the file: " << location << dendl;
        return -ec.value();
    }

    //Remove directory if empty, removes object directory
    if (efs::is_empty(dir_path, ec)) {
        ldpp_dout(dpp, 20) << "INFO: delete_data::object directory to remove: " << dir_path << " :" << ec.value() << dendl;
        if (!efs::remove(dir_path, ec)) {
            //another version could have been written between the check and removal, hence not returning error from here
            ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the directory: " << dir_path  << " :" << ec.value() << dendl;
        }
    }
    auto pos = dir_path.find_last_of('/');
    if (pos != std::string::npos) {
        dir_path.erase(pos, (dir_path.length() - pos));

        //Remove bucket directory
        if (efs::is_empty(dir_path, ec)) {
            ldpp_dout(dpp, 20) << "INFO: delete_data::bucket directory to remove: " << dir_path << " :" << ec.value() << dendl;
            if (!efs::remove(dir_path, ec)) {
                //another object could have been written between the check and removal, hence not returning error from here
                ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the directory: " << dir_path << " :" << ec.value() << dendl;
            }
        }
    }
	std::unique_lock<std::shared_mutex> l(cache_lock);
    free_space += size;
	return 0;
}

int SSDDriver::rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y)
{ 
    std::string old_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, oldKey);
    std::string new_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, newKey);
    int ret = std::rename(old_file_path.c_str(), new_file_path.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDDriver: ERROR: failed to rename the file: " << old_file_path << dendl;
        return ret;
    }

    return 0;
}


int SSDDriver::AsyncWriteRequest::prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path)
{
    int r = 0;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Write To Cache, location=" << file_path << dendl;
    cb.reset(new struct aiocb);
    memset(cb.get(), 0, sizeof(struct aiocb));
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    r = fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode));
    if (fd < 0) {
        //directories might have been deleted by a parallel delete of the last version of an object
        if (errno == ENOENT) {
            //retry after creating directories
            std::string dir_path = file_path;
            auto pos = dir_path.find_last_of('/');
            if (pos != std::string::npos) {
                dir_path.erase(pos, (dir_path.length() - pos));
            }
            ldpp_dout(dpp, 20) << "INFO: AsyncWriteRequest::prepare_libaio_write_op: dir_path for creating directories=" << dir_path << dendl;
            create_directories(dpp, dir_path);
            r = fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode));
            if (fd < 0) {
                ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
                return r;
            }
        } else {
            ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
            return r;
        }
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
    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_cb: key: " << op.file_path << dendl;
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
        attr_ret = op.priv_data->set_attrs(op.dpp, op.temp_file_path, op.attrs, y);
        if (attr_ret < 0) {
            ldpp_dout(op.dpp, 0) << "ERROR: AsyncWriteRequest::libaio_write_yield_cb::set_attrs: failed to set attrs, ret = " << attr_ret << dendl;
            ec.assign(-ret, boost::system::system_category());
            ceph::async::dispatch(std::move(p), ec);
            return;
        }
    }

    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_yield_cb: new_path: " << op.file_path << dendl;
    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_yield_cb: old_path: " << op.temp_file_path << dendl;

    ret = std::rename(op.temp_file_path.c_str(), op.file_path.c_str());
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
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : attrs) {
        std::string attr_name = it.first;
        std::string attr_val = it.second.to_str();
        auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_REPLACE);
        if (ret < 0 && errno == ENODATA) {
            ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_CREATE);
        }
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not modify attr value for attr name: " << attr_name << " key: " << key << " ERROR: " << cpp_strerror(errno) <<dendl;
            return ret;
        }
    }

    return 0;
}

int SSDDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : del_attrs) {
        auto ret = delete_attr(dpp, key, it.first);
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << it.first << " key: " << key << cpp_strerror(errno) << dendl;
            return ret;
        }
    }

    return 0;
}

int SSDDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location;
    // a hack to avoid calling create_dirs_get_filepath_from_key in case the path is already formed
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

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
        get_attr(dpp, location, attr_name, attr_value, y);
        bufferlist bl_value;
        bl_value.append(attr_value);
        attrs.emplace(std::move(attr_name), std::move(bl_value));
    }

    return 0;
}

int SSDDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location;
    // a hack to avoid calling create_dirs_get_filepath_from_key in case the path is already formed
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

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

    return 0;
}

int SSDDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
    std::string location;
    // a hack to avoid calling create_dirs_get_filepath_from_key in case the path is already formed
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): get_attr: key: " << attr_name << dendl;

    size_t buffer_size = 256;
    while (true) {
        attr_val.resize(buffer_size);
        ssize_t attr_size = getxattr(location.c_str(), attr_name.c_str(), attr_val.data(), attr_val.size());
        if (attr_size < 0) {
            if (errno == ERANGE) {
                // Buffer too small, get actual size needed
                attr_size = getxattr(location.c_str(), attr_name.c_str(), nullptr, 0);
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
                // Resize and try again
                buffer_size = static_cast<size_t>(attr_size);
                continue;
            }
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not get attribute " << attr_name << ": " << cpp_strerror(errno) << dendl;
            attr_val = "";
            return errno;
        } //end-if result < 0
        if (attr_size == 0) {
            ldpp_dout(dpp, 0) << "ERROR: no attribute value found for attr_name: " << attr_name << dendl;
            attr_val = "";
            return 0;
        } //end-if result == 0
        // Success - resize buffer to actual data size and return
        ldpp_dout(dpp, 20) << "INFO: attr_size is: " << attr_size << dendl;
        attr_val.resize(static_cast<size_t>(attr_size));
        return 0;
    }
}

int SSDDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
    std::string location;
    // a hack to avoid calling create_dirs_get_filepath_from_key in case the path is already formed
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    if (attr_name == RGW_ATTR_ACL) {
      if (dpp->get_cct()->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
        std::string policy_json;
        RGWAccessControlPolicy policy;
        bufferlist bl;
        bl.append(attr_val);
        auto bliter = bl.cbegin();
        try {
          policy.decode(bliter);
          Formatter *f = Formatter::create("json");
          policy.dump(f);
          std::stringstream ss;
          f->flush(ss);
          policy_json = ss.str();
          delete f;
        } catch (buffer::error& err) {
          ldpp_dout(dpp, 0) << "ERROR: decode policy failed" << err.what() << dendl;
          policy_json = "ERROR: decode policy failed";
        }
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << policy_json << dendl;
      }
    } else {
      ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << attr_val << dendl;
    }

    auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), 0);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    return 0;
}

int SSDDriver::delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    auto ret = removexattr(location.c_str(), attr_name.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    return 0;
}

} } // namespace rgw::cache
