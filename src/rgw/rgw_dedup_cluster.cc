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
#include <random>


static constexpr auto dout_subsys = ceph_subsys_rgw;

using namespace ::cls::cmpxattr;
namespace rgw::dedup {
#define DEDUP_CLUSTER_PREFIX      "DEDUP.CLUSTER."
#define DEDUP_CLUSTER_COORDINATOR DEDUP_CLUSTER_PREFIX "COORDINATOR"
#define MD5_SHARD_PREFIX          DEDUP_CLUSTER_PREFIX "MD5.SHARD."
#define WORKER_SHARD_PREFIX       DEDUP_CLUSTER_PREFIX "WORKER.SHARD."

  static const std::string KEY_NAME("cluster_lock");
  //static constexpr unsigned MAX_LOCK_DURATION_SEC = 3600; // 1 hour
  static constexpr unsigned MAX_LOCK_DURATION_SEC = 10;
  struct coordinator_info_t {

  };

  //---------------------------------------------------------------------------
  cluster::cluster(rgw::sal::RadosStore       *_store,
		   const DoutPrefixProvider   *_dpp) :
    store(_store), dpp(_dpp)
  {
    assign_cluster_id();
    connect();
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
    char buff[15];
    int n = snprintf(buff, sizeof(buff), "%014lx", rand_high);
    cluster_id.insert(0, buff, n);

    ldpp_dout(dpp, 1) << __func__ << "::connect::" << cluster_id << dendl;
  }

  //---------------------------------------------------------------------------
  static int get_ioctx_by_pool(const DoutPrefixProvider* const dpp,
			       RGWRados* rados,
			       const rgw_pool &pool,
			       const std::string& oid,
			       librados::IoCtx *p_ioctx)
  {
    rgw_raw_obj obj(pool, oid);
    int ret = rgw_init_ioctx(dpp, rados->get_rados_handle(), pool, *p_ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to get IO context for logging object from data pool:"
			<< pool.to_str() << dendl;
      return -EIO;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int cluster::connect()
  {
    std::string oid(DEDUP_CLUSTER_COORDINATOR);
    auto sysobj = store->svc()->sysobj;
    auto& pool  = store->svc()->zone->get_zone_params().log_pool;
    bufferlist bl;
    ldpp_dout(dpp, 0) << __func__ << "::oid=" << oid << dendl;
    bool exclusive = true; // block overwrite of old objects
    int ret = rgw_put_system_obj(dpp, sysobj, pool, oid, bl, exclusive, nullptr, real_time(), null_yield);
    if (ret == 0) {
      ldpp_dout(dpp, 0) << __func__ << "::successfully created coordinator object!" << dendl;
      // now try and take ownership
    }
    else if (ret == -EEXIST) {
      ldpp_dout(dpp, 0) << __func__ << "::coordinator object exists -> trying to take over" << dendl;
      // try and take ownership
    }
    else{
      ldpp_dout(dpp, 0) << "ERROR: failed to write log obj " << oid << " with: "
			<< cpp_strerror(ret) << ", ret=" << ret << dendl;
      return ret;
    }

    librados::ObjectWriteOperation op;
    utime_t max_lock_duration(MAX_LOCK_DURATION_SEC, 0);
    ret = lock_update(op, cluster_id, KEY_NAME, max_lock_duration);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << __func__ << "::ERR: failed lock_update()" << dendl;
      return -1;
    }

    librados::IoCtx ioctx;
    ret = get_ioctx_by_pool(dpp, store->getRados(), pool, oid, &ioctx);
    if (unlikely(ret != 0)) {
      return -1;
    }

    ldpp_dout(dpp, 0) << __func__ << "::send Cluster Lock CLS" << dendl;
    ret = ioctx.operate(oid, &op);
    if (ret == 0) {
      ldpp_dout(dpp, 0) << __func__ << "::I'm Cluster Cooridinator" << dendl;
    }
    else if (ret == -EBUSY) {
      ldpp_dout(dpp, 0) << __func__ << "::I'm Cluster Member" << dendl;
      ret = 0;
    }
    else {
      ldpp_dout(dpp, 0) << __func__ << "::failed rgw_rados_operate() ret=" << ret << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  work_shard_t cluster::get_next_work_shard()
  {
    return NULL_WORK_SHARD;
  }
  //---------------------------------------------------------------------------
  void cluster::mark_work_shard_completed()
  {

  }

  //---------------------------------------------------------------------------
  bool cluster::all_work_shards_completed(uint32_t *ttl)
  {

    return false;
  }

  //---------------------------------------------------------------------------
  md5_shard_t cluster::get_next_md5_shard()
  {
    return NULL_MD5_SHARD;
  }

  //---------------------------------------------------------------------------
  void cluster::mark_md5_shard_completed()
  {

  }

  //---------------------------------------------------------------------------
  void cluster::cleanup_prev_run()
  {

  }

} // namespace rgw::dedup
