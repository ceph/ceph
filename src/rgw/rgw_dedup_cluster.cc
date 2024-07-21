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
namespace rgw::dedup {
#define DEDUP_EPOCH_TOKEN  "EPOCH_TOKEN"

  static const std::string KEY_NAME("cluster_lock");
  //static constexpr unsigned MAX_LOCK_DURATION_SEC = 3600; // 1 hour
  static constexpr unsigned EPOCH_MAX_LOCK_DURATION_SEC = 30;
  static constexpr unsigned MAX_LOCK_DURATION_SEC = 60;
  static const ceph::bufferlist null_bl;

  const uint64_t URGENT_MSG_ID_VAL = 0x00FFFFFFFFFFFFFFULL;

  static int get_epoch(rgw::sal::RadosStore *store,
		       const DoutPrefixProvider *dpp,
		       dedup_epoch_t *p_epoch, /* OUT */
		       const char *caller);
  static int set_epoch(rgw::sal::RadosStore *store,
		       const std::string &cluster_id,
		       const DoutPrefixProvider *dpp);
  static int collect_shard_stats(librados::IoCtx *p_ioctx,
				 const DoutPrefixProvider *dpp,
				 utime_t epoch_time,
				 unsigned shards_count,
				 const char *prefix,
				 bufferlist bl_arr[],
				 named_time_lock_t *ntl_arr);

  //---------------------------------------------------------------------------
  static int set_cluster_id(char buff[], unsigned buff_size, uint64_t id)
  {
    char hex_string[sizeof(id)*2+1];
    memset(hex_string, 0, sizeof(hex_string));
    unsigned n = snprintf(hex_string, sizeof(hex_string), "%016lx", id);
    if (n != 16) {
      std::cerr << "n=" << n << std::endl;
    }
    ceph_assert(buff_size <= n);
    memset(buff, 0, buff_size);
    unsigned copy_size = std::min(buff_size, n) - 1;
    memcpy(buff, hex_string, copy_size);
    return copy_size;
  }

  //---------------------------------------------------------------------------
  static std::string get_urgent_msg_cluster_id()
  {
    char buff[15];
    int n = set_cluster_id(buff, sizeof(buff), URGENT_MSG_ID_VAL);
    std::string cluster_id(buff, n);
    return cluster_id;
  }

  //---------------------------------------------------------------------------
  static void assign_cluster_id(std::string & cluster_id,
				const DoutPrefixProvider *dpp)
  {
    // choose a 14 Bytes Random ID (so strings won't use dynamic allocation)
    // Seed with a real random value, if available
    std::random_device r;
    // Choose a random mean between 1 ULLONG_MAX
    std::default_random_engine e1(r());
    std::uniform_int_distribution<uint64_t> uniform_dist(1, std::numeric_limits<uint64_t>::max());
    uint64_t rand_high = uniform_dist(e1);
    while (rand_high == URGENT_MSG_ID_VAL) {
      rand_high = uniform_dist(e1);
    }
    char buff[15];
    int n = set_cluster_id(buff, sizeof(buff), rand_high);
    cluster_id.insert(0, buff, n);

    ldpp_dout(dpp, 1) << __func__ << "::" << cluster_id << "::" << dendl;
  }

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
  cluster::cluster(const DoutPrefixProvider *_dpp) : dpp(_dpp)
  {
    d_was_initialized = false;
    // use current time as seed for random generator
    std::srand(std::time(nullptr));
    assign_cluster_id(d_cluster_id, dpp);
  }

  //---------------------------------------------------------------------------
  void cluster::reset()
  {
    d_was_initialized = false;

    d_curr_md5_shard = 0;
    d_curr_worker_shard = 0;

    d_num_completed_workers = 0;
    d_num_completed_md5 = 0;

    memset(d_completed_workers, TOKEN_STATE_PENDING, sizeof(d_completed_workers));
    memset(d_completed_md5, TOKEN_STATE_PENDING, sizeof(d_completed_md5));

    d_total_ingressed_obj = 0;
    d_num_failed_workers = 0;
  }

  //---------------------------------------------------------------------------
  int cluster::init(rgw::sal::RadosStore *store,
		    librados::IoCtx *p_ioctx,
		    dedup_epoch_t *p_epoch,
		    bool init_epoch)
  {
    int ret;
    reset();
    if (init_epoch) {
      ret = set_epoch(store, d_cluster_id, dpp);
      if (ret != 0) {
	ldpp_dout(dpp, 1) << "failed set_epoch()! ret=" << ret
			  << "::" << cpp_strerror(ret) << dendl;
	return ret;
      }
    }
    ret = get_epoch(store, dpp, p_epoch, __func__);
    if (ret != 0) {
      return ret;
    }
    d_epoch_time = p_epoch->time;
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
	if (d_epoch_time < mtime) {
	  ldpp_dout(dpp, 10) << __func__ << "::skipping new obj! "
			     << "::EPOCH={" << d_epoch_time.tv.tv_sec << ":" << d_epoch_time.tv.tv_nsec << "} "
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
  static int get_epoch(rgw::sal::RadosStore *store,
		       const DoutPrefixProvider *dpp,
		       dedup_epoch_t *p_epoch, /* OUT */
		       const char *caller)
  {
    auto& pool = store->svc()->zone->get_zone_params().log_pool;
    librados::IoCtx ioctx;
    int ret = rgw_init_ioctx(dpp, store->getRados()->get_rados_handle(), pool, ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to get IO context for logging object from data pool:"
			<< pool.to_str() << dendl;
      return -EIO;
    }

    std::string oid(DEDUP_EPOCH_TOKEN);
    bufferlist bl;
    ret = ioctx.getxattr(oid, RGW_DEDUP_ATTR_EPOCH, bl);
    if (ret > 0) {
      try {
	auto p = bl.cbegin();
	decode(*p_epoch, p);
      }catch (const buffer::error&) {
	ldpp_dout(dpp, 0) << __func__ << "::failed epoch decode!" << dendl;
      }
      if (caller) {
	ldpp_dout(dpp, 1) << __func__ << "::" << caller<< "::" << *p_epoch << dendl;
      }
      return 0;
    }
    else {
      ldpp_dout(dpp, 1) << __func__ << "::" << (caller ? caller : "")
			<< "::failed ioctx.getxattr() with: "
			<< cpp_strerror(ret) << ", ret=" << ret << dendl;
      return ret;
    }
  }

  //---------------------------------------------------------------------------
  static int set_epoch(rgw::sal::RadosStore *store,
		       const std::string &cluster_id,
		       const DoutPrefixProvider *dpp)
  {
    std::string oid(DEDUP_EPOCH_TOKEN);
    auto sysobj = store->svc()->sysobj;
    auto& pool  = store->svc()->zone->get_zone_params().log_pool;
    bufferlist null_bl;
    ldpp_dout(dpp, 10) << __func__ << "::oid=" << oid << dendl;
    bool exclusive = true; // block overwrite of old objects
    int ret = rgw_put_system_obj(dpp, sysobj, pool, oid, null_bl, exclusive, nullptr, real_time(), null_yield);
    if (ret == 0) {
      ldpp_dout(dpp, 0) << __func__ << "::successfully created Epoch object!" << dendl;
      // now try and take ownership
    }
    else if (ret == -EEXIST) {
      ldpp_dout(dpp, 0) << __func__ << "::Epoch object exists -> trying to take over" << dendl;
      // try and take ownership
    }
    else{
      ldpp_dout(dpp, 0) << __func__ << "::ERROR: failed to write log obj " << oid
			<<" with: "<< cpp_strerror(ret) << ", ret=" << ret << dendl;
      return ret;
    }

    // TBD: CMP-SWAP with null CMP
    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(EPOCH_MAX_LOCK_DURATION_SEC, 0);
    operation_flags_t epoch_flag;
    epoch_flag = operation_flags_t::LOCK_UPDATE_OP_SET_EPOCH;
    ldpp_dout(dpp, 0) << __func__ << "::LOCK_UPDATE_OP_SET_EPOCH" << dendl;
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK | epoch_flag);
    lock_update(op, cluster_id, KEY_NAME, max_lock_duration, op_flags, null_bl,
		0, 0, URGENT_MSG_NONE);
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
      ldpp_dout(dpp, 1) << __func__ << "::Epoch object was written" << dendl;
    }
    else if (ret == -EBUSY) {
      ldpp_dout(dpp, 1) << __func__ << "::Accept existing Epoch object" << dendl;
      ret = 0;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::failed ioctx.operate() with: "
			<< cpp_strerror(ret) << ", ret=" << ret << dendl;
    }
    return ret;
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
	ldpp_dout(dpp, 5) << __func__ << "::oid=" << oid << " was created!" << dendl;
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
  static void ntl_display_progress(const named_time_lock_t &ntl, unsigned shard)
  {
    if (ntl.progress_a == 0 && ntl.progress_b == 0) {
      return;
    }

    std::cout << std::hex << "0x" << std::setw(2) << std::setfill('0') << shard << std::dec << "] ";
    if (ntl.progress_b == NTL_ALL_OBJECTS) {
      std::cout << "Token is marked completed! obj_count="
		<< ntl.progress_a << std::endl;
    }
    else if (ntl.progress_a == 0 && ntl.progress_b == 0) {
      std::cout << "Token was not started yet!" << std::endl;
    }
    else if (ntl.progress_b == 0) {
      std::cout << "Token is incomplete: progress = "
		<< ntl.progress_a <<  std::endl;
    }
    else {
      std::cout << "Token is incomplete: progress = [" << ntl.progress_a
		<< ", " << ntl.progress_b << "]" <<  std::endl;
    }
  }

  //---------------------------------------------------------------------------
  static int check_urgent_msg(librados::IoCtx *p_ioctx,
			      const DoutPrefixProvider *dpp,
			      const std::string &cluster_id,
			      const std::string &oid,
			      int *p_urgent_msg /* OUT PARAM */,
			      bool silent = false)
  {
    if (!silent) {
      ldpp_dout(dpp, 10) << __func__ << "::oid=" << oid << dendl;
    }
    bufferlist bl;
    int ret = p_ioctx->getxattr(oid, "cluster_lock", bl);
    if (ret == -ENODATA) {
      ldpp_dout(dpp, 0) << __func__ << "::Resume request removed lock on oid::" << oid << dendl;
    }

    if (unlikely(ret <= 0)) {
      ldpp_dout(dpp, 0) << __func__ << "::>" << oid
			<< "::failed ioctx.getxattr() ret=" << ret
			<< "::" << cpp_strerror(ret) << dendl;
      return -1;
    }

    named_time_lock_t ntl;
    try {
      auto p = bl.cbegin();
      decode(ntl, p);
    }
    catch (const buffer::error&) {
      ldpp_dout(dpp, 0) << __func__ << "::(3)failed named_time_lock_t decode!::" << oid << dendl;
      return -1;
    }
    if (ntl.urgent_msg == URGENT_MSG_NONE) {
      ldpp_dout(dpp, 10) << __func__ << "::URGENT_MSG_NONE" << dendl;
      *p_urgent_msg = URGENT_MSG_NONE;
      return 0;
    }

    if (ntl.is_urgent_stop_msg()) {
      if (!silent) {
	ldpp_dout(dpp, 0) << __func__ << "::>" << oid << get_urgent_msg_names(ntl.urgent_msg) << dendl;
      }
      *p_urgent_msg = ntl.urgent_msg;
    }
    else if (ntl.urgent_msg == URGENT_MSG_RESUME) {
      ldpp_dout(dpp, 0) << __func__ << "::" << oid << "::URGENT_MSG_RESUME" << dendl;
      *p_urgent_msg = ntl.urgent_msg;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::SKIP" << dendl;
      *p_urgent_msg = URGENT_MSG_SKIP;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::get_urgent_msg_state(librados::IoCtx *p_ioctx,
				    int *urgent_msg /* OUT-PARAM */)
  {
    char buff[16];
    // use the first work-shard to test urgent-messages
    int n = snprintf(buff, sizeof(buff), "%s%02x", WORKER_SHARD_PREFIX, 0);
    const std::string oid(buff, n);
    return check_urgent_msg(p_ioctx, dpp, d_cluster_id, oid, urgent_msg, true);
  }

  //---------------------------------------------------------------------------
  int cluster::update_shard_token_heartbeat(librados::IoCtx *p_ioctx,
					    unsigned shard,
					    uint64_t count_a,
					    uint64_t count_b,
					    const char *prefix,
					    int *p_urgent_msg /* IN-OUT PARAM */)
  {
    ceph_assert(d_was_initialized);
    char buff[16];
    int n = snprintf(buff, sizeof(buff), "%s%02x", prefix, shard);
    std::string oid(buff, n);
    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK);
    // TBD: can use a simple attribute write since we hold the lock ??
    lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration, op_flags,
		null_bl, count_a, count_b, *p_urgent_msg);
    *p_urgent_msg = URGENT_MSG_NONE;
    int ret = p_ioctx->operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::success!::" << oid << dendl;
      return 0;
    }

    return check_urgent_msg(p_ioctx, dpp, d_cluster_id, oid, p_urgent_msg);
  }

  //---------------------------------------------------------------------------
  int cluster::get_next_shard_token(librados::IoCtx *p_ioctx,
				    unsigned start_shard,
				    unsigned max_shard,
				    const char *prefix,
				    int *p_urgent_msg /* IN-OUT PARAM */)
  {
    ceph_assert(d_was_initialized);
    char buff[16];
    int prefix_len = snprintf(buff, sizeof(buff), "%s", prefix);
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    for (auto shard = start_shard; shard < max_shard; shard++) {
      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);

      for (unsigned ii = 0; ii < 2; ii++) {
	ldpp_dout(dpp, 20) << __func__ << "::try garbbing " << oid << dendl;
	librados::ObjectWriteOperation op;
	operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK);
	// TBD: maybe use RGW/CLS existing lock mechanism
	lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration, op_flags, null_bl,
		    0, 0, URGENT_MSG_NONE);

	int ret = p_ioctx->operate(oid, &op);
	if (ret == 0) {
	  ldpp_dout(dpp, 0) << __func__ << "::SUCCESS!::" << oid << dendl;
	  return shard;
	}

	// check if token is marked for an urgent message
	*p_urgent_msg = URGENT_MSG_NONE;
	ret = check_urgent_msg(p_ioctx, dpp, d_cluster_id, oid, p_urgent_msg);

	// Token is set for an URGENT-STOP
	if (*p_urgent_msg == URGENT_MSG_ABORT || *p_urgent_msg == URGENT_MSG_PASUE) {
	  return -1;
	}

	if (ret != 0 || *p_urgent_msg != URGENT_MSG_NONE) {
	  // might be a recoverable issue -> retry one more time
	  // most likely scenario: token lock failed because of an urgent messages
	  // which has been removed since and now we can take the token
	  ldpp_dout(dpp, 1) << __func__ << "::retry garbbing " << oid << dendl;
	}
	else {
	  // someone else took this token -> move to the next one
	  break;
	}
      }
    }

    return -1;
  }

  //---------------------------------------------------------------------------
  work_shard_t cluster::get_next_work_shard_token(librados::IoCtx *p_ioctx,
						  int *p_urgent_msg /* IN-OUT PARAM */)
  {
    int shard = get_next_shard_token(p_ioctx, d_curr_worker_shard, MAX_WORK_SHARD,
				     WORKER_SHARD_PREFIX, p_urgent_msg);
    if (shard >= 0 && shard < MAX_WORK_SHARD) {
      d_curr_worker_shard = shard + 1;
      return shard;
    }
    else {
      return NULL_WORK_SHARD;
    }
  }

  //---------------------------------------------------------------------------
  md5_shard_t cluster::get_next_md5_shard_token(librados::IoCtx *p_ioctx,
						int *p_urgent_msg /* IN-OUT PARAM */)
  {
    int shard = get_next_shard_token(p_ioctx, d_curr_md5_shard, MAX_MD5_SHARD,
				     MD5_SHARD_PREFIX, p_urgent_msg);
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
    ldpp_dout(dpp, 10) << __func__ << "::" << prefix << "::" << oid << dendl;

    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK |
			       operation_flags_t::LOCK_UPDATE_OP_MARK_COMPLETED);
    // TBD: can probably be done without compare since we hold lock
    lock_update(op, d_cluster_id, KEY_NAME, max_lock_duration, op_flags,
		bl, obj_count, NTL_ALL_OBJECTS, URGENT_MSG_NONE);

    int ret = p_ioctx->operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 10) << __func__ << "::Done ioctx.operate(" << oid << ")" << dendl;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::Failed ioctx.operate(" << oid << ") ret="
			<< ret << "::" << cpp_strerror(ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  bool cluster::all_shard_tokens_completed(librados::IoCtx *p_ioctx,
					   unsigned shards_count,
					   const char *prefix,
					   uint16_t *p_num_completed,
					   uint8_t completed_arr[],
					   uint32_t *ttl,
					   uint64_t *p_total_ingressed)
  {
    ceph_assert(d_was_initialized);
    // default 3 seconds ttl;
    *ttl = 3;

    unsigned count = 0;
    char buff[16];
    int prefix_len = snprintf(buff, sizeof(buff), "%s", prefix);
    for (unsigned shard = 0; shard < shards_count; shard++) {
      if (completed_arr[shard] != TOKEN_STATE_PENDING) {
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
	ldpp_dout(dpp, 0) << __func__ << "::failed named_time_lock_t decode!" << dendl;
	return false;
      }

      if (ntl.progress_b == NTL_ALL_OBJECTS) {
	utime_t duration = ntl.completion_time - ntl.creation_time;
	// mark token completed;
	(*p_num_completed)++;
	completed_arr[shard] = TOKEN_STATE_COMPLETED;
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
	  completed_arr[shard] = TOKEN_STATE_TIMED_OUT;
	  d_num_failed_workers++;
	  count++;
	  continue;
	}
	else {
	  return false;
	}
      }
    } // loop

    *p_total_ingressed = d_total_ingressed_obj;
    return (count == shards_count);
  }

  //---------------------------------------------------------------------------
  static int collect_shard_stats(librados::IoCtx *p_ioctx,
				 const DoutPrefixProvider *dpp,
				 utime_t epoch_time,
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

      uint64_t size;
      struct timespec tspec;
      if (p_ioctx->stat2(oid, &size, &tspec) != 0) {
	ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.stat( " << oid << " )" << dendl;
	continue;
      }
      utime_t mtime(tspec);
      if (epoch_time > mtime) {
	ldpp_dout(dpp, 10) << __func__ << "::skipping old obj! "
			   << "::EPOCH={" << epoch_time.tv.tv_sec << ":" << epoch_time.tv.tv_nsec << "} "
			   << "::mtime={" << mtime.tv.tv_sec << ":" << mtime.tv.tv_nsec << "}" << dendl;
	continue;
      }

      bufferlist bl;
      int ret = p_ioctx->getxattr(oid, "cluster_lock", bl);
      if (ret > 0) {
	named_time_lock_t ntl;
	try {
	  auto p = bl.cbegin();
	  decode(ntl, p);
	  if (ntl.urgent_msg != URGENT_MSG_NONE) {
	    // TBD: should we abort opertions??
	    ldpp_dout(dpp, 0) << __func__ << "::shard=" << shard
			      << " urgent message=" << ntl.urgent_msg << dendl;
	  }
	  ntl_arr[shard] = ntl;
	}
	catch (const buffer::error&) {
	  ldpp_dout(dpp, 0) << __func__ << "::(1)failed named_time_lock_t decode!" << dendl;
	  //continue;
	}
      }
      else if (ret != -ENODATA) {
	ldpp_dout(dpp, 10) << __func__ << "::failed ioctx.getxattr() ret=" << ret << dendl;
	//continue;
      }

      ret = p_ioctx->getxattr(oid, "completion_stats", bl_arr[shard]);
      if (ret > 0) {
	count++;
      }
      else if (ret == -ENODATA) {
	ldpp_dout(dpp, 10) << __func__ << "::shard is not completed yet " << oid << dendl;
	continue;
      }
      else {
	ldpp_dout(dpp, 0) << __func__ << "::" << oid << "::failed ioctx.getxattr() ret="
			  << ret << "::" << cpp_strerror(ret) << dendl;
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
  static utime_t show_time_func(const utime_t &start_time, bool show_time,
				const std::map<std::string, member_time_t> &owner_map)
  {
    std::string urgent_msg_cluster_id = get_urgent_msg_cluster_id();
    member_time_t all_members_time;
    all_members_time.start_time = start_time;
    all_members_time.end_time   = start_time;
    all_members_time.aggregated_time = utime_t();

    for (const auto& [owner, value] : owner_map) {
      uint32_t sec = value.end_time.tv.tv_sec - value.start_time.tv.tv_sec;
      std::cout << ((owner != urgent_msg_cluster_id) ? owner : "")
		<< "::start time = [" << value.start_time.tv.tv_sec % 1000
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

    return all_members_time.end_time;
  }

  //---------------------------------------------------------------------------
  static void collect_single_shard_stats(const DoutPrefixProvider *dpp,
					 std::map<std::string, member_time_t> &owner_map,
					 const named_time_lock_t ntl_arr[],
					 unsigned shard,
					 bool *p_show_time,
					 const char *name)
  {
    const utime_t null_time;
    const named_time_lock_t &ntl = ntl_arr[shard];
    if (ntl.creation_time == null_time || ntl.completion_time == null_time) {
      *p_show_time = false;
      return;
    }

    if (ntl.progress_b != NTL_ALL_OBJECTS && ntl.is_urgent_stop_msg()) {
      const int32_t &msg = ntl.urgent_msg;
      std::cout << name << " Shard #" << shard << " is marked for urgent message"
		<< msg << std::endl;
      std::cout << __func__ << "::" << get_urgent_msg_names(msg) << std::endl;
      *p_show_time = false;
      return;
    }

    std::string urgent_msg_cluster_id = get_urgent_msg_cluster_id();
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
    ldpp_dout(dpp, 10) << __func__ << "::Got " << name
		       << " stats for shard #" << (int)shard << dendl;
  }

  //---------------------------------------------------------------------------
  static void show_dedup_ratio(const worker_stats_t &wrk_stats_sum,
			       const md5_stats_t    &md5_stats_sum)
  {
    uint64_t rados_bytes_before = md5_stats_sum.rados_bytes_before_dedup;
    uint64_t s3_bytes_before    = wrk_stats_sum.ingress_obj_bytes;
    uint64_t s3_dedup_bytes     = md5_stats_sum.duplicated_blocks_bytes;
    uint64_t s3_bytes_after     = s3_bytes_before - s3_dedup_bytes;
    // skipped objects should be accounted for
    // TBD: double check the logic!
    uint64_t skipped_bytes = (wrk_stats_sum.ingress_skip_too_small_bytes +
			      wrk_stats_sum.non_default_storage_class_objs_bytes);
    s3_bytes_after -= skipped_bytes;
    if (rados_bytes_before > s3_bytes_after && s3_bytes_after) {
      double dedup_ratio = (double)rados_bytes_before/s3_bytes_after;
      std::cout << "rados_bytes_before = " << rados_bytes_before << "\n";
      std::cout << "s3_bytes_before    = " << s3_bytes_before << "\n";
      std::cout << "s3_bytes_after     = " << s3_bytes_after << "\n";
      std::cout << "dedup_ratio        = " << dedup_ratio << "\n";
    }
  }

  //---------------------------------------------------------------------------
  int cluster::collect_all_shard_stats(rgw::sal::RadosStore *store,
				       const DoutPrefixProvider *dpp)
  {
    dedup_epoch_t epoch;
    if (get_epoch(store, dpp, &epoch, nullptr) != 0) {
      return -1;
    }

    RGWRados *rados = store->getRados();
    librados::IoCtx ioctx, *p_ioctx = &ioctx;
    if (init_dedup_pool_ioctx(rados, dpp, p_ioctx) != 0) {
      return -1;
    }
    unsigned completed_work_shards_count = 0;
    unsigned completed_md5_shards_count  = 0;
    utime_t md5_start_time;
    worker_stats_t wrk_stats_sum;
    {
      std::map<std::string, member_time_t> owner_map;
      bool show_time = true;
      bufferlist bl_arr[MAX_WORK_SHARD];
      named_time_lock_t ntl_arr[MAX_WORK_SHARD];
      int cnt = collect_shard_stats(p_ioctx, dpp, epoch.time, MAX_WORK_SHARD, WORKER_SHARD_PREFIX, bl_arr, ntl_arr);
      if (cnt != MAX_WORK_SHARD) {
	std::cerr << ">>>Partial work shard stats recived " << cnt << " / "
		  << (int)MAX_WORK_SHARD << "\n" << std::endl;
      }

      for (unsigned shard = 0; shard < MAX_WORK_SHARD; shard++) {
	if (bl_arr[shard].length() == 0) {
	  ntl_display_progress(ntl_arr[shard], shard);
	  continue;
	}
	completed_work_shards_count++;
	worker_stats_t stats;
	try {
	  auto p = bl_arr[shard].cbegin();
	  decode(stats, p);
	  wrk_stats_sum += stats;
	}catch (const buffer::error&) {
	  std::cerr << __func__ << "::(2)failed worker_stats_t decode #" << (int)shard << std::endl;
	  continue;
	}
	collect_single_shard_stats(dpp, owner_map, ntl_arr, shard, &show_time, "WORKER");
      }
      std::cout << "Aggreagted work-shard stats counters:\n" << wrk_stats_sum << std::endl;
      md5_start_time = show_time_func(epoch.time, show_time, owner_map);
    }

    if (completed_work_shards_count == MAX_WORK_SHARD) {
      std::map<std::string, member_time_t> owner_map;
      bool show_time = true;
      md5_stats_t md5_stats_sum;
      bufferlist bl_arr[MAX_WORK_SHARD];
      named_time_lock_t ntl_arr[MAX_WORK_SHARD];
      int cnt = collect_shard_stats(p_ioctx, dpp, epoch.time, MAX_MD5_SHARD, MD5_SHARD_PREFIX, bl_arr, ntl_arr);
      if (cnt != MAX_MD5_SHARD) {
	std::cerr << ">>>Partial MD5_SHARD stats recived " << cnt << " / "
		  << (int)MAX_MD5_SHARD << "\n" << std::endl;
      }

      for (unsigned shard = 0; shard < MAX_MD5_SHARD; shard++) {
	if (bl_arr[shard].length() == 0) {
	  ntl_display_progress(ntl_arr[shard], shard);
	  continue;
	}
	completed_md5_shards_count++;
	md5_stats_t stats;
	try {
	  auto p = bl_arr[shard].cbegin();
	  decode(stats, p);
	  md5_stats_sum += stats;
	}catch (const buffer::error&) {
	  std::cerr << __func__ << "::failed md5_stats_t decode #" << (int)shard << std::endl;
	  continue;
	}
	collect_single_shard_stats(dpp, owner_map, ntl_arr, shard, &show_time, "MD5");
      }
      std::cout << "Aggreagted md5-shard stats counters:\n" << md5_stats_sum << std::endl;
      show_dedup_ratio(wrk_stats_sum, md5_stats_sum);
      show_time_func(md5_start_time, show_time, owner_map);
    }

    if (completed_md5_shards_count == MAX_MD5_SHARD) {
      std::cout << "DEDUP WORK WAS COMPLETED" << std::endl;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::dedup_control(rgw::sal::RadosStore *store,
			     const DoutPrefixProvider *dpp,
			     int urgent_msg)
  {
    std::cout << __func__ << "::dedup_control=" << urgent_msg << std::endl;
    ldpp_dout(dpp, 0) << __func__ << "::dedup_control req " << urgent_msg << "::"
		      << get_urgent_msg_names(urgent_msg) << dendl;
    if (urgent_msg != URGENT_MSG_RESUME &&
	urgent_msg != URGENT_MSG_PASUE  &&
	urgent_msg != URGENT_MSG_ABORT) {
      std::cerr << __func__ << "::illegal urgent_msg=" << urgent_msg << std::endl;
      ceph_abort();
      return -1;
    }

    std::string cluster_id = get_urgent_msg_cluster_id();
    RGWRados *rados = store->getRados();
    librados::IoCtx ioctx, *p_ioctx = &ioctx;
    if (init_dedup_pool_ioctx(rados, dpp, p_ioctx) != 0) {
      return -1;
    }

    // 1 year lock
    utime_t max_lock_duration(60*60*24*365, 0);
    operation_flags_t op_flags(operation_flags_t::LOCK_UPDATE_OP_SET_LOCK |
			       operation_flags_t::LOCK_UPDATE_OP_URGENT_MSG);

    unsigned failed_worker_tokens = 0;
    char buff[16];
    memset(buff, 0, sizeof(buff));
    int prefix_len = snprintf(buff, sizeof(buff), "%s", WORKER_SHARD_PREFIX);
    for (unsigned shard = 0; shard < MAX_WORK_SHARD; shard++) {
      librados::ObjectWriteOperation op;
      // TBD: maybe use RGW built-in control mechanism ??
      lock_update(op, cluster_id, KEY_NAME, max_lock_duration, op_flags,
		  null_bl, 0, 0, urgent_msg);
      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);

      int ret = p_ioctx->operate(oid, &op);
      if (ret != 0) {
	failed_worker_tokens++;
	std::cerr << __func__ << "::Failed::" << oid << std::endl;
      }
    }
    unsigned failed_md5_tokens = 0;
    memset(buff, 0, sizeof(buff));
    prefix_len = snprintf(buff, sizeof(buff), "%s", MD5_SHARD_PREFIX);
    for (unsigned shard = 0; shard < MAX_MD5_SHARD; shard++) {
      librados::ObjectWriteOperation op;
      // TBD: maybe use RGW built-in control mechanism ??
      lock_update(op, cluster_id, KEY_NAME, max_lock_duration, op_flags,
		  null_bl, 0, 0, urgent_msg);
      int n = snprintf(buff + prefix_len, sizeof(buff), "%02x", shard);
      std::string oid(buff, prefix_len + n);

      int ret = p_ioctx->operate(oid, &op);
      if (ret != 0) {
	failed_md5_tokens++;
	std::cerr << __func__ << "::Failed::" << oid << std::endl;
      }
    }

    if (!failed_worker_tokens && !failed_md5_tokens) {
      return 0;
    }
    if (failed_worker_tokens) {
      std::cerr << __func__ << "::Failed setting " << failed_worker_tokens << " / "
		<< MAX_WORK_SHARD << " worker-tokens" << std::endl;
    }
    if (failed_md5_tokens) {
      std::cerr << __func__ << "::Failed setting " << failed_md5_tokens << " / "
		<< MAX_MD5_SHARD << " md5-tokens" << std::endl;
    }
    return -1;
  }

  //---------------------------------------------------------------------------
  int cluster::dedup_restart_scan(rgw::sal::RadosStore *store,
				  bool dry_run,
				  const DoutPrefixProvider *dpp)
  {
    dedup_epoch_t old_epoch;
    // store the previous epoch for cmp-swap
    int ret = get_epoch(store, dpp, &old_epoch, __func__);
    if (ret != 0) {
      return ret;
    }

    // first abort all dedup work!
    dedup_control(store, dpp, URGENT_MSG_ABORT);
    // wait 6 second for all workers to process the abort request
    std::this_thread::sleep_for(std::chrono::seconds(6));
    auto& pool = store->svc()->zone->get_zone_params().log_pool;
    librados::IoCtx ioctx;
    ret = rgw_init_ioctx(dpp, store->getRados()->get_rados_handle(), pool, ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << "::failed rgw_init_ioctx" << dendl;
      return -EIO;
    }

    ldpp_dout(dpp, 1) << __func__ << (dry_run ?"::DRY RUN":"::FULL DEDUP") << dendl;
    int dedup_type = (dry_run ? DEDUP_TYPE_DRY_RUN : DEDUP_TYPE_FULL);
    dedup_epoch_t new_epoch = { old_epoch.serial + 1, dedup_type, ceph_clock_now()};
    bufferlist old_epoch_bl, new_epoch_bl;
    encode(old_epoch, old_epoch_bl);
    encode(new_epoch, new_epoch_bl);
    ComparisonMap cmp_pairs = {{RGW_DEDUP_ATTR_EPOCH, old_epoch_bl}};
    std::map<std::string, bufferlist> set_pairs = {{RGW_DEDUP_ATTR_EPOCH, new_epoch_bl}};
    librados::ObjectWriteOperation op;
    ret = cmp_vals_set_vals(op, Mode::String, Op::EQ, cmp_pairs, set_pairs);
    ldpp_dout(dpp, 1) << __func__ << "::send EPOCH CLS" << dendl;
    std::string oid(DEDUP_EPOCH_TOKEN);
    ret = ioctx.operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 1) << __func__ << "::Epoch object was reset" << dendl;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::Failed to reset EPOCH object with: "
			<< cpp_strerror(ret) << ", ret=" << ret << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  bool cluster::can_start_new_scan(rgw::sal::RadosStore *store,
				   const utime_t &epoch_time,
				   const DoutPrefixProvider *dpp)
  {
    ldpp_dout(dpp, 10) << __func__ << "::epoch=" << epoch_time << dendl;
    dedup_epoch_t new_epoch;
    if (get_epoch(store, dpp, &new_epoch, nullptr) != 0) {
      ldpp_dout(dpp, 1) << __func__ << "::No Epoch Object::"
			<< "::scan can be restarted!\n\n\n" << dendl;
      // no epoch object exists -> we should start a new scan
      return true;
    }

    if (new_epoch.time <= epoch_time) {
      if (new_epoch.time == epoch_time) {
	ldpp_dout(dpp, 10) << __func__ << "::Epoch hasn't change - > Do not restart scan!!" << dendl;
      }
      else {
	ldpp_dout(dpp, 1) << __func__ << "::Do not restart scan!\n    epoch="
			  << epoch_time << "\nnew_epoch="<< new_epoch.time << dendl;
      }
      return false;
    }
    // allow members to join within a 30 sec limit
    utime_t limit = {30, 0};
    utime_t now = ceph_clock_now();
    ldpp_dout(dpp, 1) << __func__ << "\n::new_epoch=" << new_epoch.time
		      << "\n::now      =" << now << dendl;
    if ((now > new_epoch.time) && ((now - new_epoch.time) < limit)) {
      ldpp_dout(dpp, 1) << __func__ << "::Epoch is less than 30 seconds old!"
			<< " Restart scan\n\n\n" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "\n::new_epoch - now = " << (new_epoch.time - now)
		      << "\n::limit           = " << limit << dendl;

    if (new_epoch.time > now) {
      ldpp_dout(dpp, 1) << ":new_epoch > now = TRUE " << dendl;
    }
    return false;
  }
} // namespace rgw::dedup
