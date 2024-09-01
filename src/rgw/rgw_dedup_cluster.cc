#include "rgw_dedup_cluster.h"
#include "include/ceph_assert.h"
#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "svc_zone.h"
#include "common/config.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "rgw_common.h"
#include "include/denc.h"
#include "rgw_sal.h"
#include "driver/rados/rgw_sal_rados.h"
#include "cls/cmpxattr/client.h"
#include "cls/cmpxattr/server.h"
#include <random>
#include <cstdlib>
#include <ctime>
#include <string>
static constexpr auto dout_subsys = ceph_subsys_rgw;

using namespace ::cls::cmpxattr;
//using namespace librados;
namespace rgw::dedup {
#define DEDUP_CLUSTER_LEADER  "LEADER_TOKEN"

  static const std::string KEY_NAME("cluster_lock");
  //static constexpr unsigned MAX_LOCK_DURATION_SEC = 3600; // 1 hour
  static constexpr unsigned MAX_LOCK_DURATION_SEC = 300;
  static const ceph::bufferlist null_bl;

  //---------------------------------------------------------------------------
  int init_dedup_pool_ioctx(RGWRados                 *rados,
			    const DoutPrefixProvider *dpp,
			    librados::IoCtx          *p_ioctx)
  {
    rgw_pool dedup_pool(DEDUP_POOL_NAME);
    std::string pool_name(DEDUP_POOL_NAME);
    // using Replica-1 for the intermediate data
    // since it can be regenerated in case of a failure
    std::string replica_count(std::to_string(1));
    librados::bufferlist inbl;
    std::string output;
    std::string command = R"(
    {
      "prefix": "osd pool create",
      "pool": ")" + pool_name +
      R"(",
      "pool_type": "replicated",
      "size": )" + replica_count +
      R"(
    })";
    int ret = rados->get_rados_handle()->mon_command(command, inbl, nullptr, &output);
    if (output.length()) {
      if (output != "pool 'rgw_dedup_pool' already exists") {
	ldpp_dout(dpp, 0) << __func__ << "::" << output << dendl;
      }
    }
    if (ret != 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 0) << __func__ << "::failed to create pool " << DEDUP_POOL_NAME
			<< " with: " << cpp_strerror(ret) << ", ret=" << ret << dendl;
    }

    ret = rgw_init_ioctx(dpp, rados->get_rados_handle(), dedup_pool, *p_ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to initialize pool for listing with: "
			<< cpp_strerror(ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  void cluster::assign_cluster_id()
  {
    // choose a 14 Bytes Random ID (so strings won't use dynamic allocation)
    // Seed with a real random value, if available
    std::random_device r;
    // Choose a random mean between 1 ULLONG_MAX
    std::default_random_engine e1(r());
    std::uniform_int_distribution<uint64_t> uniform_dist(1, std::numeric_limits<uint64_t>::max());
    uint64_t rand_high = uniform_dist(e1);
    char buff[16];
    memset(buff, 0, sizeof(buff));
    int n = snprintf(buff, sizeof(buff), "%014lx%c", rand_high, '\0');
    d_cluster_id.insert(0, buff, n);

    ldpp_dout(dpp, 1) << __func__ << "::" << d_cluster_id << "::" << dendl;
  }

  //---------------------------------------------------------------------------
  cluster::cluster(const DoutPrefixProvider *_dpp) : dpp(_dpp)
  {
    d_was_initialized = false;
    d_curr_md5_shard = 0;
    d_curr_worker_shard = 0;
    memset(d_completed_workers, 0, sizeof(d_completed_workers));
    // use current time as seed for random generator
    std::srand(std::time(nullptr));
    assign_cluster_id();
  }

  //---------------------------------------------------------------------------
  int cluster::init(rgw::sal::RadosStore *store, librados::IoCtx *p_ioctx)
  {
    if (set_epoch(store) != 0) {
      return -1;
    }

    cleanup_prev_run(p_ioctx);
    create_shard_tokens(p_ioctx, MAX_WORK_SHARD, WORKER_SHARD_PREFIX);
    create_shard_tokens(p_ioctx, MAX_MD5_SHARD, MD5_SHARD_PREFIX);

    d_was_initialized = true;

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::cleanup_prev_run(librados::IoCtx *p_ioctx)
  {
    constexpr uint32_t max = 100;
    std::string marker;
    bool truncated = false;
    rgw::AccessListFilter filter{};
    do {
      std::vector<std::string> oids;
      int ret = rgw_list_pool(dpp, *p_ioctx, max, filter, marker, &oids, &truncated);
      if (ret == -ENOENT) {
	ret = 0;
	ldpp_dout(dpp, 0) << __func__ << "::-ENOENT"<< dendl;
	break;
      }
      else if (ret < 0) {
	ldpp_dout(dpp, 1) << "failed rgw_list_pool()! ret=" << ret
			  << "::" << cpp_strerror(ret) << dendl;
	return ret;
      }
      unsigned deleted_count = 0, skipped_count = 0, failed_count= 0;;
      for (const std::string& oid : oids) {
	uint64_t size;
	struct timespec tspec;
	if (p_ioctx->stat2(oid, &size, &tspec) != 0) {
	  ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.stat( " << oid << " )" << dendl;
	  failed_count++;
	  continue;
	}
	utime_t mtime(tspec);
	if (d_epoch < mtime) {
	  ldpp_dout(dpp, 10) << __func__ << "::skipping new obj! "
			     << "::EPOCH={" << d_epoch.tv.tv_sec << ":" << d_epoch.tv.tv_nsec << "} "
			     << "::mtime={" << mtime.tv.tv_sec << ":" << mtime.tv.tv_nsec << "}" << dendl;
	  skipped_count++;
	  continue;
	}
	ldpp_dout(dpp, 10) << __func__ << "::removing object: " << oid << dendl;
	if (p_ioctx->remove(oid) == 0) {
	  deleted_count++;
	}
	else {
	  failed_count++;
	  ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.remove( " << oid << " ), ret="
			     << ret << "::" << cpp_strerror(ret) << dendl;
	}
      }
      ldpp_dout(dpp, 0) << __func__ << "::oids.size()=" << oids.size()
			<< "::deleted="     << deleted_count
			<< "::failed="      << failed_count
			<< "::skipped_new=" << skipped_count << dendl;

    } while (truncated);
    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::get_epoch(rgw::sal::RadosStore *store,
			 const DoutPrefixProvider *dpp,
			 utime_t *p_epoch /* OUT */ )
  {
    auto& pool = store->svc()->zone->get_zone_params().log_pool;
    librados::IoCtx ioctx;
    int ret = rgw_init_ioctx(dpp, store->getRados()->get_rados_handle(), pool, ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to get IO context for logging object from data pool:"
			<< pool.to_str() << dendl;
      return -EIO;
    }

    std::string oid(DEDUP_CLUSTER_LEADER);
    bufferlist bl;
    ret = ioctx.getxattr(oid, "epoch", bl);
    if (ret > 0) {
      try {
	auto p = bl.cbegin();
	decode(*p_epoch, p);
      }catch (const buffer::error&) {
	ldpp_dout(dpp, 0) << __func__ << "::failed epoch decode!" << dendl;
      }
      ldpp_dout(dpp, 10) << __func__ << "::EPOCH={" << p_epoch->tv.tv_sec
			 << ":" << p_epoch->tv.tv_nsec << "}" << dendl;
      return 0;
    }
    else {
      ldpp_dout(dpp, 1) << __func__ << "::failed ioctx.getxattr() ret="
			<< ret << dendl;
      return -1;
    }
  }

  //---------------------------------------------------------------------------
  int cluster::set_epoch(rgw::sal::RadosStore *store)
  {
    std::string oid(DEDUP_CLUSTER_LEADER);
    auto sysobj = store->svc()->sysobj;
    auto& pool  = store->svc()->zone->get_zone_params().log_pool;
    bufferlist null_bl;
    ldpp_dout(dpp, 10) << __func__ << "::oid=" << oid << dendl;
    bool exclusive = true; // block overwrite of old objects
    int ret = rgw_put_system_obj(dpp, sysobj, pool, oid, null_bl, exclusive, nullptr, real_time(), null_yield);
    if (ret == 0) {
      ldpp_dout(dpp, 0) << __func__ << "::successfully created leader object!" << dendl;
      // now try and take ownership
    }
    else if (ret == -EEXIST) {
      ldpp_dout(dpp, 0) << __func__ << "::leader object exists -> trying to take over" << dendl;
      // try and take ownership
    }
    else{
      ldpp_dout(dpp, 0) << "ERROR: failed to write log obj " << oid << " with: "
			<< cpp_strerror(ret) << ", ret=" << ret << dendl;
      return ret;
    }

    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK |
			       operation_flags_t::LOCK_UPDATE_OP_SET_EPOCH);
    ret = lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration, op_flags, null_bl);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << __func__ << "::ERR: failed lock_update()" << dendl;
      return -1;
    }

    librados::IoCtx ioctx;
    ret = rgw_init_ioctx(dpp, store->getRados()->get_rados_handle(), pool, ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to get IO context for logging object from data pool:"
			<< pool.to_str() << dendl;
      return -EIO;
    }

    ldpp_dout(dpp, 10) << __func__ << "::send Cluster Lock CLS" << dendl;
    ret = ioctx.operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::I'm Cluster Leader" << dendl;
    }
    else if (ret == -EBUSY) {
      ldpp_dout(dpp, 10) << __func__ << "::I'm Cluster Member" << dendl;
      ret = 0;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::failed ioctx.operate() ret="
			<< ret << dendl;
    }

    return get_epoch(store, dpp, &d_epoch);
  }

  //---------------------------------------------------------------------------
  int cluster::create_shard_tokens(librados::IoCtx *p_ioctx,
				   unsigned shards_count,
				   const char *prefix)
  {
    char buff[16];
    int prefix_len = snprintf(buff, sizeof(buff), "%s", prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);
      ldpp_dout(dpp, 10) << __func__ << "::creating object: " << oid << dendl;
      bool exclusive = true;
      int ret = p_ioctx->create(oid, exclusive);
      if (ret >= 0) {
	ldpp_dout(dpp, 10) << __func__ << "::oid=" << oid << " was created!" << dendl;
      }
      else if (ret == -EEXIST) {
	ldpp_dout(dpp, 10) << "failed ioctx.create(" << oid << ") -EEXIST!" << dendl;
      }
      else {
	ldpp_dout(dpp, 1) << "failed ioctx.create(" << oid << ") with: " << ret
			  << "::" << cpp_strerror(ret) << dendl;
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::update_shard_token_heartbeat(librados::IoCtx *p_ioctx,
					    unsigned shard,
					    uint64_t count_a,
					    uint64_t count_b,
					    const char *prefix)
  {
    ceph_assert(d_was_initialized);
    char buff[16];
    int n = snprintf(buff, sizeof(buff), "%s%02x", prefix, shard);
    std::string oid(buff, n);
    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK);
    int ret = lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration,
			  op_flags, null_bl, count_a, count_b);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << __func__ << "::Failed lock_update()::" << oid << dendl;
      return -1;
    }

    ret = p_ioctx->operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::success! " << oid << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  int cluster::get_next_shard_token(librados::IoCtx *p_ioctx,
				    unsigned start_shard,
				    unsigned max_shard,
				    const char *prefix)
  {
    ceph_assert(d_was_initialized);
    char buff[16];
    int prefix_len = snprintf(buff, sizeof(buff), "%s", prefix);

    for (auto shard = start_shard; shard < max_shard; shard++) {
      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);
      //ldpp_dout(dpp, 0) << __func__ << "::try garbbing " << oid << dendl;
      librados::ObjectWriteOperation op;
      utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
      operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK);
      int ret = lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration, op_flags, null_bl);
      if (ret != 0) {
	ldpp_dout(dpp, 0) << __func__ << "::ERR: failed lock_update()" << dendl;
	return -1;
      }

      ret = p_ioctx->operate(oid, &op);
      if (ret == 0) {
	ldpp_dout(dpp, 0) << __func__ << "::" << oid << dendl;
	return shard;
      }
    }

    return -1;
  }

  //---------------------------------------------------------------------------
  work_shard_t cluster::get_next_work_shard_token(librados::IoCtx *p_ioctx)
  {
    int shard = get_next_shard_token(p_ioctx, d_curr_worker_shard, MAX_WORK_SHARD, WORKER_SHARD_PREFIX);
    if (shard >= 0 && shard < MAX_WORK_SHARD) {
      d_curr_worker_shard = shard + 1;
      return shard;
    }
    else {
      return NULL_WORK_SHARD;
    }
  }

  //---------------------------------------------------------------------------
  md5_shard_t cluster::get_next_md5_shard_token(librados::IoCtx *p_ioctx)
  {
    int shard = get_next_shard_token(p_ioctx, d_curr_md5_shard, MAX_MD5_SHARD, MD5_SHARD_PREFIX);
    if (shard >= 0 && shard < MAX_MD5_SHARD) {
      d_curr_md5_shard = shard + 1;
      return shard;
    }
    else {
      return NULL_MD5_SHARD;
    }
  }

  //---------------------------------------------------------------------------
  int cluster::mark_shard_token_completed(librados::IoCtx *p_ioctx,
					  unsigned shard,
					  uint64_t obj_count,
					  const char *prefix,
					  const bufferlist &bl)
  {
    ceph_assert(d_was_initialized);
    char buff[16];
    int n = snprintf(buff, sizeof(buff), "%s%02x", prefix, shard);
    std::string oid(buff,  n);
    ldpp_dout(dpp, 20) << __func__ << "::" << prefix << "::" << oid << dendl;

    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK |
			       operation_flags_t::LOCK_UPDATE_OP_MARK_COMPLETED);
    int ret = lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration,
			  op_flags, bl, obj_count, ULLONG_MAX);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << __func__ << "::ERR: failed lock_update()" << dendl;
      return -1;
    }

    ret = p_ioctx->operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 0) << __func__ << "::Done ioctx.operate(" << oid << ")" << dendl;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::Failed ioctx.operate(" << oid << ")" << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  bool cluster::all_shard_tokens_completed(librados::IoCtx *p_ioctx,
					   unsigned shards_count,
					   const char *prefix,
					   uint32_t *ttl,
					   uint64_t *p_total_ingressed)
  {
    ceph_assert(d_was_initialized);
    *ttl = 60; // wait 60 seconds before retry if no one finished yet
    unsigned count = 0;
    char buff[16];
    int prefix_len = snprintf(buff, sizeof(buff), "%s", prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      if (d_completed_workers[shard]) {
	count++;
	continue;
      }

      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);
      ldpp_dout(dpp, 0) << __func__ << "::checking object: " << oid << dendl;
      bufferlist bl;
      int ret = p_ioctx->getxattr(oid, "cluster_lock", bl);
      if (unlikely(ret <= 0)) {
	if (ret != -ENODATA) {
	  ldpp_dout(dpp, 0) << __func__ << "::failed ioctx.getxattr() ret="
			    << ret << ", ttl=" << *ttl << dendl;
	}
	continue;
      }

      named_time_lock_t ntl;
      try {
	auto p = bl.cbegin();
	decode(ntl, p);
      }
      catch (const buffer::error&) {
	ldpp_dout(dpp, 0) << __func__ << "::failed worker_stats_t decode!" << dendl;
	return false;
      }

      if (ntl.progress_b == ULLONG_MAX) {
	utime_t duration = ntl.completion_time - ntl.creation_time;
	// mark token completed;
	d_num_completed_workers++;
	d_total_completed_time += duration;
	d_completed_workers[shard] = 0xFF;
	d_total_ingressed_obj += ntl.progress_a;
	ldpp_dout(dpp, 20) << __func__ << "::" << oid
			   << "::completed! duration=" << duration << dendl;
	count++;
      }
      else {
	utime_t time_elapsed = ntl.lock_time - ntl.creation_time;
	if (time_elapsed > ntl.max_lock_duration) {
	  // lock expired -> try and break lock
	  ldpp_dout(dpp, 0) << __func__ << "::" << oid << "::expired lock, skipping" << dendl;
	  d_completed_workers[shard] = 0xDD;
	  d_num_failed_workers++;
	  count++;
	  continue;
	}
	else {
	  // TBD: check progress rate to set ttl
	  // default 3 seconds ttl;
	  *ttl = 3;
	  if (d_num_completed_workers) {
	    // set ttl to average completion time
	    uint32_t avg_time = (d_total_completed_time.tv.tv_sec / d_num_completed_workers);
	    if (avg_time > time_elapsed.tv.tv_sec) {
	      *ttl = avg_time - time_elapsed.tv.tv_sec;
	    }
	  }
	  else {
	    *ttl = std::max(*ttl, (uint32_t)time_elapsed.tv.tv_sec/100);
	  }
	}
      }
    } // loop

    *p_total_ingressed = d_total_ingressed_obj;
    return (count == shards_count);
  }

  //---------------------------------------------------------------------------
  int cluster::collect_shard_stats(librados::IoCtx *p_ioctx,
				   const DoutPrefixProvider *dpp,
				   unsigned shards_count,
				   const char *prefix,
				   bufferlist bl_arr[],
				   named_time_lock_t *ntl_arr)
  {
    unsigned count = 0;
    char buff[16];
    int prefix_len = snprintf(buff, sizeof(buff), "%s", prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);
      ldpp_dout(dpp, 10) << __func__ << "::checking object: " << oid << dendl;
      int ret = p_ioctx->getxattr(oid, "completion_stats", bl_arr[shard]);
      if (ret > 0) {
	count++;
      }
      else if (ret == -ENODATA) {
	ldpp_dout(dpp, 10) << __func__ << "::shard is not completed yet " << oid << dendl;
	continue;
      }
      else {
	ldpp_dout(dpp, 0) << __func__ << "::failed ioctx.getxattr() ret=" << ret << dendl;
	continue;
      }

      bufferlist bl;
      ret = p_ioctx->getxattr(oid, "cluster_lock", bl);
      if (ret > 0) {
	named_time_lock_t ntl;
	try {
	  auto p = bl.cbegin();
	  decode(ntl, p);
	  ntl_arr[shard] = ntl;
	}
	catch (const buffer::error&) {
	  ldpp_dout(dpp, 0) << __func__ << "::failed worker_stats_t decode!" << dendl;
	  continue;
	}
      }
      else if (ret != -ENODATA) {
	ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.getxattr() ret=" << ret << dendl;
	continue;
      }
    }

    if (count != shards_count) {
      ldpp_dout(dpp, 10) << __func__ << "::missing shards stats! we got " << count
			 << " / " << (int)shards_count << dendl;
    }

    return count;
  }

  struct member_time_t {
    utime_t start_time;
    utime_t end_time;
    utime_t aggregated_time;
  };

  //---------------------------------------------------------------------------
  static void show_time_func(const utime_t &start_time, bool show_time,
			     const std::map<std::string, member_time_t> &owner_map)
  {
    member_time_t all_members_time;
    all_members_time.start_time = start_time;

    for (const auto& [key, value] : owner_map) {
      uint32_t sec = value.end_time.tv.tv_sec - value.start_time.tv.tv_sec;
      std::cout << key << "::start time = [" << value.start_time.tv.tv_sec % 1000
		<< ":" << value.start_time.tv.tv_nsec / (1000*1000) << "] "
		<< "::aggregated time = " << value.aggregated_time.tv.tv_sec
		<< "(" << sec << ") seconds " << std::endl;

      all_members_time.aggregated_time += value.aggregated_time;
      if (all_members_time.end_time < value.end_time) {
	all_members_time.end_time = value.end_time;
      }
    }
    if (show_time) {
      uint32_t sec = all_members_time.end_time.tv.tv_sec - all_members_time.start_time.tv.tv_sec;
      std::cout << "All work-shard start      time = " << all_members_time.start_time << std::endl;
      std::cout << "All work-shard end        time = " << all_members_time.end_time
		<< " (" << sec << " seconds)" << std::endl;
      std::cout << "All work-shard aggregated time = "
		<< all_members_time.aggregated_time.tv.tv_sec << std::endl;
    }
  }

  //---------------------------------------------------------------------------
  int cluster::collect_all_shard_stats(rgw::sal::RadosStore *store, const DoutPrefixProvider *dpp)
  {
    utime_t epoch;
    if (get_epoch(store, dpp, &epoch) != 0) {
      return -1;
    }

    RGWRados *rados = store->getRados();
    librados::IoCtx ioctx, *p_ioctx = &ioctx;
    if (init_dedup_pool_ioctx(rados, dpp, p_ioctx) != 0) {
      return -1;
    }
    const utime_t null_time;
    {
      std::map<std::string, member_time_t> owner_map;
      bool show_time = true;
      worker_stats_t wrk_stats_sum;
      bufferlist bl_arr[MAX_WORK_SHARD];
      named_time_lock_t ntl_arr[MAX_WORK_SHARD];
      int cnt = collect_shard_stats(p_ioctx, dpp, MAX_WORK_SHARD, WORKER_SHARD_PREFIX, bl_arr, ntl_arr);
      if (cnt != MAX_WORK_SHARD) {
	std::cerr << "Partial stats recived " << cnt << " / " << (int)MAX_WORK_SHARD << std::endl;
      }

      for (unsigned shard = 0; shard < MAX_WORK_SHARD; shard++) {
	if (bl_arr[shard].length() == 0) {
	  continue;
	}
	worker_stats_t stats;
	try {
	  auto p = bl_arr[shard].cbegin();
	  decode(stats, p);
	  wrk_stats_sum += stats;
	  if (ntl_arr[shard].creation_time == null_time || ntl_arr[shard].completion_time == null_time) {
	    show_time = false;
	    continue;
	  }
	  const named_time_lock_t &ntl = ntl_arr[shard];
	  const std::string &owner = ntl.owner;
	  utime_t duration = ntl.completion_time - ntl.creation_time;
	  if (owner_map.find(owner) != owner_map.end()) {
	    owner_map[owner].aggregated_time += duration;
	    owner_map[owner].end_time = ntl.completion_time;
	  }
	  else {
	    owner_map[owner].start_time = ntl.creation_time;
	    owner_map[owner].aggregated_time = duration;
	    owner_map[owner].end_time = ntl.completion_time;
	  }
	}catch (const buffer::error&) {
	  ldpp_dout(dpp, 0) << __func__ << "::failed worker_stats_t decode #" << (int)shard << dendl;
	  continue;
	}
	ldpp_dout(dpp, 10) << __func__ << "::Got worker_stats for shard #" << (int)shard << dendl;
      }
      std::cout << "Aggreagted work-shard stats counters:\n" << wrk_stats_sum << std::endl;
      show_time_func(epoch, show_time, owner_map);
    }

    {
      std::map<std::string, member_time_t> owner_map;
      bool show_time = true;
      md5_stats_t md5_stats_sum;
      bufferlist bl_arr[MAX_WORK_SHARD];
      named_time_lock_t ntl_arr[MAX_WORK_SHARD];
      int cnt = collect_shard_stats(p_ioctx, dpp, MAX_MD5_SHARD, MD5_SHARD_PREFIX, bl_arr, ntl_arr);
      if (cnt != MAX_MD5_SHARD) {
	std::cerr << "Partial stats recived " << cnt << " / " << (int)MAX_MD5_SHARD << std::endl;
      }

      utime_t start_time;
      for (unsigned shard = 0; shard < MAX_MD5_SHARD; shard++) {
	if (bl_arr[shard].length() == 0) {
	  continue;
	}
	md5_stats_t stats;
	try {
	  auto p = bl_arr[shard].cbegin();
	  decode(stats, p);
	  md5_stats_sum += stats;
	  if (ntl_arr[shard].creation_time == null_time || ntl_arr[shard].completion_time == null_time) {
	    show_time = false;
	    continue;
	  }
	  const named_time_lock_t &ntl = ntl_arr[shard];
	  const std::string &owner = ntl.owner;
	  utime_t duration = ntl.completion_time - ntl.creation_time;
	  if (shard == 0) {
	    start_time = ntl.creation_time;
	  }
	  if (owner_map.find(owner) != owner_map.end()) {
	    owner_map[owner].aggregated_time += duration;
	    owner_map[owner].end_time = ntl.completion_time;
	  }
	  else {
	    owner_map[owner].start_time = ntl.creation_time;
	    owner_map[owner].aggregated_time = duration;
	    owner_map[owner].end_time = ntl.completion_time;
	  }
	}catch (const buffer::error&) {
	  ldpp_dout(dpp, 0) << __func__ << "::failed md5_stats_t decode #" << (int)shard << dendl;
	  continue;
	}
	ldpp_dout(dpp, 10) << __func__ << "::Got md5_stats for shard #" << (int)shard << dendl;
      }
      std::cout << "Aggreagted md5-shard stats counters:\n" << md5_stats_sum << std::endl;
      show_time_func(start_time, show_time, owner_map);
    }

    return 0;
  }

} // namespace rgw::dedup
