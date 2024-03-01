// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>
#include <boost/assign/list_of.hpp>
#include <stddef.h>

#include "include/neorados/RADOS.hpp"

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/Timer.h"

#include "librbd/AsioEngine.h"
#include "librbd/AsyncRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/LibrbdAdminSocketHook.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/PluginRegistry.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/exclusive_lock/AutomaticPolicy.h"
#include "librbd/exclusive_lock/StandardPolicy.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ImageDispatcher.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/QosImageDispatch.h"
#include "librbd/io/IoOperations.h"
#include "librbd/io/Utils.h"
#include "librbd/journal/StandardPolicy.h"
#include "librbd/operation/ResizeRequest.h"

#include "osdc/Striper.h"
#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageCtx: "

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;

namespace librbd {

namespace {

class SafeTimerSingleton : public CommonSafeTimer<ceph::mutex> {
public:
  ceph::mutex lock = ceph::make_mutex("librbd::SafeTimerSingleton::lock");

  explicit SafeTimerSingleton(CephContext *cct)
      : SafeTimer(cct, lock, true) {
    init();
  }
  ~SafeTimerSingleton() {
    std::lock_guard locker{lock};
    shutdown();
  }
};

librados::IoCtx duplicate_io_ctx(librados::IoCtx& io_ctx) {
  librados::IoCtx dup_io_ctx;
  dup_io_ctx.dup(io_ctx);
  return dup_io_ctx;
}

} // anonymous namespace

  const string ImageCtx::METADATA_CONF_PREFIX = "conf_";

  ImageCtx::ImageCtx(const string &image_name, const string &image_id,
		     const char *snap, IoCtx& p, bool ro)
    : cct((CephContext*)p.cct()),
      config(cct->_conf),
      perfcounter(NULL),
      snap_id(CEPH_NOSNAP),
      snap_exists(true),
      read_only(ro),
      read_only_flags(ro ? IMAGE_READ_ONLY_FLAG_USER : 0U),
      exclusive_locked(false),
      name(image_name),
      asio_engine(std::make_shared<AsioEngine>(p)),
      rados_api(asio_engine->get_rados_api()),
      data_ctx(duplicate_io_ctx(p)),
      md_ctx(duplicate_io_ctx(p)),
      image_watcher(NULL),
      journal(NULL),
      owner_lock(ceph::make_shared_mutex(util::unique_lock_name("librbd::ImageCtx::owner_lock", this))),
      image_lock(ceph::make_shared_mutex(util::unique_lock_name("librbd::ImageCtx::image_lock", this))),
      timestamp_lock(ceph::make_shared_mutex(util::unique_lock_name("librbd::ImageCtx::timestamp_lock", this))),
      async_ops_lock(ceph::make_mutex(util::unique_lock_name("librbd::ImageCtx::async_ops_lock", this))),
      copyup_list_lock(ceph::make_mutex(util::unique_lock_name("librbd::ImageCtx::copyup_list_lock", this))),
      extra_read_flags(0),
      old_format(false),
      order(0), size(0), features(0),
      format_string(NULL),
      id(image_id), parent(NULL),
      stripe_unit(0), stripe_count(0), flags(0),
      readahead(),
      total_bytes_read(0),
      state(new ImageState<>(this)),
      operations(new Operations<>(*this)),
      exclusive_lock(nullptr), object_map(nullptr),
      op_work_queue(asio_engine->get_work_queue()),
      plugin_registry(new PluginRegistry<ImageCtx>(this)),
      event_socket_completions(32),
      asok_hook(nullptr),
      trace_endpoint("librbd")
  {
    ldout(cct, 10) << this << " " << __func__ << ": "
                   << "image_name=" << image_name << ", "
                   << "image_id=" << image_id << dendl;

    if (snap)
      snap_name = snap;

    rebuild_data_io_context();

    // FIPS zeroization audit 20191117: this memset is not security related.
    memset(&header, 0, sizeof(header));

    io_image_dispatcher = new io::ImageDispatcher<ImageCtx>(this);
    io_object_dispatcher = new io::ObjectDispatcher<ImageCtx>(this);

    if (cct->_conf.get_val<bool>("rbd_auto_exclusive_lock_until_manual_request")) {
      exclusive_lock_policy = new exclusive_lock::AutomaticPolicy(this);
    } else {
      exclusive_lock_policy = new exclusive_lock::StandardPolicy(this);
    }
    journal_policy = new journal::StandardPolicy(this);
  }

  ImageCtx::ImageCtx(const string &image_name, const string &image_id,
		     uint64_t snap_id, IoCtx& p, bool ro)
    : ImageCtx(image_name, image_id, "", p, ro) {
    open_snap_id = snap_id;
  }

  ImageCtx::~ImageCtx() {
    ldout(cct, 10) << this << " " << __func__ << dendl;

    ceph_assert(config_watcher == nullptr);
    ceph_assert(image_watcher == NULL);
    ceph_assert(exclusive_lock == NULL);
    ceph_assert(object_map == NULL);
    ceph_assert(journal == NULL);
    ceph_assert(asok_hook == NULL);

    if (perfcounter) {
      perf_stop();
    }
    delete[] format_string;

    md_ctx.aio_flush();
    if (data_ctx.is_valid()) {
      data_ctx.aio_flush();
    }

    delete io_object_dispatcher;
    delete io_image_dispatcher;

    delete journal_policy;
    delete exclusive_lock_policy;
    delete operations;
    delete state;

    delete plugin_registry;
  }

  void ImageCtx::init() {
    ceph_assert(!header_oid.empty());
    ceph_assert(old_format || !id.empty());

    asok_hook = new LibrbdAdminSocketHook(this);

    string pname = string("librbd-") + id + string("-") +
      md_ctx.get_pool_name() + string("-") + name;
    if (!snap_name.empty()) {
      pname += "-";
      pname += snap_name;
    }

    trace_endpoint.copy_name(pname);
    perf_start(pname);

    ceph_assert(image_watcher == NULL);
    image_watcher = new ImageWatcher<>(*this);
  }

  void ImageCtx::shutdown() {
    delete image_watcher;
    image_watcher = nullptr;

    delete asok_hook;
    asok_hook = nullptr;
  }

  void ImageCtx::init_layout(int64_t pool_id)
  {
    if (stripe_unit == 0 || stripe_count == 0) {
      stripe_unit = 1ull << order;
      stripe_count = 1;
    }

    vector<uint64_t> alignments;
    alignments.push_back(stripe_count << order); // object set (in file striping terminology)
    alignments.push_back(stripe_unit * stripe_count); // stripe
    alignments.push_back(stripe_unit); // stripe unit
    readahead.set_alignments(alignments);

    layout = file_layout_t();
    layout.stripe_unit = stripe_unit;
    layout.stripe_count = stripe_count;
    layout.object_size = 1ull << order;
    layout.pool_id = pool_id;  // FIXME: pool id overflow?

    delete[] format_string;
    size_t len = object_prefix.length() + 16;
    format_string = new char[len];
    if (old_format) {
      snprintf(format_string, len, "%s.%%012llx", object_prefix.c_str());
    } else {
      snprintf(format_string, len, "%s.%%016llx", object_prefix.c_str());
    }

    ldout(cct, 10) << "init_layout stripe_unit " << stripe_unit
		   << " stripe_count " << stripe_count
		   << " object_size " << layout.object_size
		   << " prefix " << object_prefix
		   << " format " << format_string
		   << dendl;
  }

  void ImageCtx::perf_start(string name) {
    auto perf_prio = PerfCountersBuilder::PRIO_DEBUGONLY;
    if (child == nullptr) {
      // ensure top-level IO stats are exported for librbd daemons
      perf_prio = PerfCountersBuilder::PRIO_USEFUL;
    }

    PerfCountersBuilder plb(cct, name, l_librbd_first, l_librbd_last);

    plb.add_u64_counter(l_librbd_rd, "rd", "Reads", "r", perf_prio);
    plb.add_u64_counter(l_librbd_rd_bytes, "rd_bytes", "Data size in reads",
                        "rb", perf_prio, unit_t(UNIT_BYTES));
    plb.add_time_avg(l_librbd_rd_latency, "rd_latency", "Latency of reads",
                     "rl", perf_prio);
    plb.add_u64_counter(l_librbd_wr, "wr", "Writes", "w", perf_prio);
    plb.add_u64_counter(l_librbd_wr_bytes, "wr_bytes", "Written data",
                        "wb", perf_prio, unit_t(UNIT_BYTES));
    plb.add_time_avg(l_librbd_wr_latency, "wr_latency", "Write latency",
                     "wl", perf_prio);
    plb.add_u64_counter(l_librbd_discard, "discard", "Discards");
    plb.add_u64_counter(l_librbd_discard_bytes, "discard_bytes", "Discarded data", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_time_avg(l_librbd_discard_latency, "discard_latency", "Discard latency");
    plb.add_u64_counter(l_librbd_flush, "flush", "Flushes");
    plb.add_time_avg(l_librbd_flush_latency, "flush_latency", "Latency of flushes");
    plb.add_u64_counter(l_librbd_ws, "ws", "WriteSames");
    plb.add_u64_counter(l_librbd_ws_bytes, "ws_bytes", "WriteSame data", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_time_avg(l_librbd_ws_latency, "ws_latency", "WriteSame latency");
    plb.add_u64_counter(l_librbd_cmp, "cmp", "CompareAndWrites");
    plb.add_u64_counter(l_librbd_cmp_bytes, "cmp_bytes", "Data size in cmps", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_time_avg(l_librbd_cmp_latency, "cmp_latency", "Latency of cmps");
    plb.add_u64_counter(l_librbd_snap_create, "snap_create", "Snap creations");
    plb.add_u64_counter(l_librbd_snap_remove, "snap_remove", "Snap removals");
    plb.add_u64_counter(l_librbd_snap_rollback, "snap_rollback", "Snap rollbacks");
    plb.add_u64_counter(l_librbd_snap_rename, "snap_rename", "Snap rename");
    plb.add_u64_counter(l_librbd_notify, "notify", "Updated header notifications");
    plb.add_u64_counter(l_librbd_resize, "resize", "Resizes");
    plb.add_u64_counter(l_librbd_readahead, "readahead", "Read ahead");
    plb.add_u64_counter(l_librbd_readahead_bytes, "readahead_bytes", "Data size in read ahead", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_u64_counter(l_librbd_invalidate_cache, "invalidate_cache", "Cache invalidates");

    plb.add_time(l_librbd_opened_time, "opened_time", "Opened time",
                 "ots", perf_prio);
    plb.add_time(l_librbd_lock_acquired_time, "lock_acquired_time",
                 "Lock acquired time", "lats", perf_prio);

    perfcounter = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perfcounter);

    perfcounter->tset(l_librbd_opened_time, ceph_clock_now());
  }

  void ImageCtx::perf_stop() {
    ceph_assert(perfcounter);
    cct->get_perfcounters_collection()->remove(perfcounter);
    delete perfcounter;
  }

  void ImageCtx::set_read_flag(unsigned flag) {
    extra_read_flags |= flag;
  }

  int ImageCtx::get_read_flags(snap_t snap_id) {
    int flags = librados::OPERATION_NOFLAG | read_flags;
    if (flags != 0)
      return flags;

    flags = librados::OPERATION_NOFLAG | extra_read_flags;
    if (snap_id == LIBRADOS_SNAP_HEAD)
      return flags;

    if (config.get_val<bool>("rbd_balance_snap_reads"))
      flags |= librados::OPERATION_BALANCE_READS;
    else if (config.get_val<bool>("rbd_localize_snap_reads"))
      flags |= librados::OPERATION_LOCALIZE_READS;
    return flags;
  }

  int ImageCtx::snap_set(uint64_t in_snap_id) {
    ceph_assert(ceph_mutex_is_wlocked(image_lock));
    auto it = snap_info.find(in_snap_id);
    if (in_snap_id != CEPH_NOSNAP && it != snap_info.end()) {
      snap_id = in_snap_id;
      snap_namespace = it->second.snap_namespace;
      snap_name = it->second.name;
      snap_exists = true;
      if (data_ctx.is_valid()) {
        data_ctx.snap_set_read(snap_id);
        rebuild_data_io_context();
      }
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::snap_unset()
  {
    ceph_assert(ceph_mutex_is_wlocked(image_lock));
    snap_id = CEPH_NOSNAP;
    snap_namespace = {};
    snap_name = "";
    snap_exists = true;
    if (data_ctx.is_valid()) {
      data_ctx.snap_set_read(snap_id);
      rebuild_data_io_context();
    }
  }

  snap_t ImageCtx::get_snap_id(const cls::rbd::SnapshotNamespace& in_snap_namespace,
                               const string& in_snap_name) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    auto it = snap_ids.find({in_snap_namespace, in_snap_name});
    if (it != snap_ids.end()) {
      return it->second;
    }
    return CEPH_NOSNAP;
  }

  const SnapInfo* ImageCtx::get_snap_info(snap_t in_snap_id) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    map<snap_t, SnapInfo>::const_iterator it =
      snap_info.find(in_snap_id);
    if (it != snap_info.end())
      return &it->second;
    return nullptr;
  }

  int ImageCtx::get_snap_name(snap_t in_snap_id,
			      string *out_snap_name) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *out_snap_name = info->name;
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::get_snap_namespace(snap_t in_snap_id,
				   cls::rbd::SnapshotNamespace *out_snap_namespace) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *out_snap_namespace = info->snap_namespace;
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::get_parent_spec(snap_t in_snap_id,
				cls::rbd::ParentImageSpec *out_pspec) const
  {
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *out_pspec = info->parent.spec;
      return 0;
    }
    return -ENOENT;
  }

  uint64_t ImageCtx::get_current_size() const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    return size;
  }

  uint64_t ImageCtx::get_object_size() const
  {
    return 1ull << order;
  }

  string ImageCtx::get_object_name(uint64_t num) const {
    return util::data_object_name(this, num);
  }

  uint64_t ImageCtx::get_stripe_unit() const
  {
    return stripe_unit;
  }

  uint64_t ImageCtx::get_stripe_count() const
  {
    return stripe_count;
  }

  uint64_t ImageCtx::get_stripe_period() const
  {
    return stripe_count * (1ull << order);
  }

  utime_t ImageCtx::get_create_timestamp() const
  {
    return create_timestamp;
  }

  utime_t ImageCtx::get_access_timestamp() const
  {
    return access_timestamp;
  }

  utime_t ImageCtx::get_modify_timestamp() const
  {
    return modify_timestamp;
  }

  void ImageCtx::set_access_timestamp(utime_t at)
  {
    ceph_assert(ceph_mutex_is_wlocked(timestamp_lock));
    access_timestamp = at;
  }

  void ImageCtx::set_modify_timestamp(utime_t mt)
  {
    ceph_assert(ceph_mutex_is_locked(timestamp_lock));
    modify_timestamp = mt;
  }

  int ImageCtx::is_snap_protected(snap_t in_snap_id,
				  bool *is_protected) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *is_protected =
	(info->protection_status == RBD_PROTECTION_STATUS_PROTECTED);
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::is_snap_unprotected(snap_t in_snap_id,
				    bool *is_unprotected) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      *is_unprotected =
	(info->protection_status == RBD_PROTECTION_STATUS_UNPROTECTED);
      return 0;
    }
    return -ENOENT;
  }

  void ImageCtx::add_snap(cls::rbd::SnapshotNamespace in_snap_namespace,
			  string in_snap_name,
			  snap_t id, uint64_t in_size,
			  const ParentImageInfo &parent,
                          uint8_t protection_status, uint64_t flags,
                          utime_t timestamp)
  {
    ceph_assert(ceph_mutex_is_wlocked(image_lock));
    snaps.push_back(id);
    SnapInfo info(in_snap_name, in_snap_namespace,
		  in_size, parent, protection_status, flags, timestamp);
    snap_info.insert({id, info});
    snap_ids.insert({{in_snap_namespace, in_snap_name}, id});
  }

  void ImageCtx::rm_snap(cls::rbd::SnapshotNamespace in_snap_namespace,
			 string in_snap_name,
			 snap_t id)
  {
    ceph_assert(ceph_mutex_is_wlocked(image_lock));
    snaps.erase(std::remove(snaps.begin(), snaps.end(), id), snaps.end());
    snap_info.erase(id);
    snap_ids.erase({in_snap_namespace, in_snap_name});
  }

  uint64_t ImageCtx::get_image_size(snap_t in_snap_id) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    if (in_snap_id == CEPH_NOSNAP) {
      if (!resize_reqs.empty() &&
          resize_reqs.front()->shrinking()) {
        return resize_reqs.front()->get_image_size();
      }
      return size;
    }

    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info) {
      return info->size;
    }
    return 0;
  }

  uint64_t ImageCtx::get_area_size(io::ImageArea area) const {
    // image areas are defined only for the "opened at" snap_id
    // (i.e. where encryption may be loaded)
    uint64_t raw_size = get_image_size(snap_id);
    if (raw_size == 0) {
      return 0;
    }

    auto size = io::util::raw_to_area_offset(*this, raw_size);
    ceph_assert(size.first <= raw_size && size.second == io::ImageArea::DATA);

    switch (area) {
    case io::ImageArea::DATA:
      return size.first;
    case io::ImageArea::CRYPTO_HEADER:
      // CRYPTO_HEADER area ends where DATA area begins
      return raw_size - size.first;
    default:
      ceph_abort();
    }
  }

  uint64_t ImageCtx::get_object_count(snap_t in_snap_id) const {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    uint64_t image_size = get_image_size(in_snap_id);
    return Striper::get_num_objects(layout, image_size);
  }

  bool ImageCtx::test_features(uint64_t features) const
  {
    std::shared_lock l{image_lock};
    return test_features(features, image_lock);
  }

  bool ImageCtx::test_features(uint64_t in_features,
                               const ceph::shared_mutex &in_image_lock) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    return ((features & in_features) == in_features);
  }

  bool ImageCtx::test_op_features(uint64_t in_op_features) const
  {
    std::shared_lock l{image_lock};
    return test_op_features(in_op_features, image_lock);
  }

  bool ImageCtx::test_op_features(uint64_t in_op_features,
                                  const ceph::shared_mutex &in_image_lock) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    return ((op_features & in_op_features) == in_op_features);
  }

  int ImageCtx::get_flags(librados::snap_t _snap_id, uint64_t *_flags) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    if (_snap_id == CEPH_NOSNAP) {
      *_flags = flags;
      return 0;
    }
    const SnapInfo *info = get_snap_info(_snap_id);
    if (info) {
      *_flags = info->flags;
      return 0;
    }
    return -ENOENT;
  }

  int ImageCtx::test_flags(librados::snap_t in_snap_id,
                           uint64_t flags, bool *flags_set) const
  {
    std::shared_lock l{image_lock};
    return test_flags(in_snap_id, flags, image_lock, flags_set);
  }

  int ImageCtx::test_flags(librados::snap_t in_snap_id,
                           uint64_t flags,
                           const ceph::shared_mutex &in_image_lock,
                           bool *flags_set) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    uint64_t snap_flags;
    int r = get_flags(in_snap_id, &snap_flags);
    if (r < 0) {
      return r;
    }
    *flags_set = ((snap_flags & flags) == flags);
    return 0;
  }

  int ImageCtx::update_flags(snap_t in_snap_id, uint64_t flag, bool enabled)
  {
    ceph_assert(ceph_mutex_is_wlocked(image_lock));
    uint64_t *_flags;
    if (in_snap_id == CEPH_NOSNAP) {
      _flags = &flags;
    } else {
      map<snap_t, SnapInfo>::iterator it = snap_info.find(in_snap_id);
      if (it == snap_info.end()) {
        return -ENOENT;
      }
      _flags = &it->second.flags;
    }

    if (enabled) {
      (*_flags) |= flag;
    } else {
      (*_flags) &= ~flag;
    }
    return 0;
  }

  const ParentImageInfo* ImageCtx::get_parent_info(snap_t in_snap_id) const
  {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    if (in_snap_id == CEPH_NOSNAP)
      return &parent_md;
    const SnapInfo *info = get_snap_info(in_snap_id);
    if (info)
      return &info->parent;
    return NULL;
  }

  int64_t ImageCtx::get_parent_pool_id(snap_t in_snap_id) const
  {
    const auto info = get_parent_info(in_snap_id);
    if (info)
      return info->spec.pool_id;
    return -1;
  }

  string ImageCtx::get_parent_image_id(snap_t in_snap_id) const
  {
    const auto info = get_parent_info(in_snap_id);
    if (info)
      return info->spec.image_id;
    return "";
  }

  uint64_t ImageCtx::get_parent_snap_id(snap_t in_snap_id) const
  {
    const auto info = get_parent_info(in_snap_id);
    if (info)
      return info->spec.snap_id;
    return CEPH_NOSNAP;
  }

  int ImageCtx::get_parent_overlap(snap_t in_snap_id,
                                   uint64_t* raw_overlap) const {
    const auto info = get_parent_info(in_snap_id);
    if (info) {
      *raw_overlap = info->overlap;
      return 0;
    }
    return -ENOENT;
  }

  std::pair<uint64_t, io::ImageArea> ImageCtx::reduce_parent_overlap(
      uint64_t raw_overlap, bool migration_write) const {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    if (migration_write) {
      // don't reduce migration write overlap -- it may be larger as
      // it's the largest overlap across snapshots by construction
      return io::util::raw_to_area_offset(*this, raw_overlap);
    }
    if (raw_overlap == 0 || parent == nullptr) {
      // image opened with OPEN_FLAG_SKIP_OPEN_PARENT -> no overlap
      return io::util::raw_to_area_offset(*this, 0);
    }
    // DATA area in the parent may be smaller than the part of DATA
    // area in the clone that is still within the overlap (e.g. for
    // LUKS2-encrypted parent + LUKS1-encrypted clone, due to LUKS2
    // header usually being bigger than LUKS1 header)
    auto overlap = io::util::raw_to_area_offset(*this, raw_overlap);
    std::shared_lock parent_image_locker(parent->image_lock);
    overlap.first = std::min(overlap.first,
                             parent->get_area_size(overlap.second));
    return overlap;
  }

  uint64_t ImageCtx::prune_parent_extents(io::Extents& image_extents,
                                          io::ImageArea area,
                                          uint64_t raw_overlap,
                                          bool migration_write) const {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    ldout(cct, 10) << __func__ << ": image_extents=" << image_extents
                   << " area=" << area << " raw_overlap=" << raw_overlap
                   << " migration_write=" << migration_write << dendl;
    if (raw_overlap == 0) {
      image_extents.clear();
      return 0;
    }

    auto overlap = reduce_parent_overlap(raw_overlap, migration_write);
    if (area == overlap.second) {
      // drop extents completely beyond the overlap
      while (!image_extents.empty() &&
             image_extents.back().first >= overlap.first) {
        image_extents.pop_back();
      }
      if (!image_extents.empty()) {
        // trim final overlapping extent
        auto& last_extent = image_extents.back();
        if (last_extent.first + last_extent.second > overlap.first) {
          last_extent.second = overlap.first - last_extent.first;
        }
      }
    } else if (area == io::ImageArea::DATA &&
               overlap.second == io::ImageArea::CRYPTO_HEADER) {
      // all extents completely beyond the overlap
      image_extents.clear();
    } else {
      // all extents completely within the overlap
      ceph_assert(area == io::ImageArea::CRYPTO_HEADER &&
                  overlap.second == io::ImageArea::DATA);
    }

    uint64_t overlap_bytes = 0;
    for (auto [_, len] : image_extents) {
      overlap_bytes += len;
    }
    ldout(cct, 10) << __func__ << ": overlap=" << overlap.first
                   << "/" << overlap.second
                   << " got overlap_bytes=" << overlap_bytes
                   << " at " << image_extents << dendl;
    return overlap_bytes;
  }

  void ImageCtx::register_watch(Context *on_finish) {
    ceph_assert(image_watcher != NULL);
    image_watcher->register_watch(on_finish);
  }

  void ImageCtx::cancel_async_requests() {
    C_SaferCond ctx;
    cancel_async_requests(&ctx);
    ctx.wait();
  }

  void ImageCtx::cancel_async_requests(Context *on_finish) {
    {
      std::lock_guard async_ops_locker{async_ops_lock};
      if (!async_requests.empty()) {
        ldout(cct, 10) << "canceling async requests: count="
                       << async_requests.size() << dendl;
        for (auto req : async_requests) {
          ldout(cct, 10) << "canceling async request: " << req << dendl;
          req->cancel();
        }
        async_requests_waiters.push_back(on_finish);
        return;
      }
    }

    on_finish->complete(0);
  }

  void ImageCtx::apply_metadata(const std::map<std::string, bufferlist> &meta,
                                bool thread_safe) {
    ldout(cct, 20) << __func__ << dendl;

    std::unique_lock image_locker(image_lock);

    // reset settings back to global defaults
    config_overrides.clear();
    config.set_config_values(cct->_conf.get_config_values());

    // extract config overrides
    for (auto meta_pair : meta) {
      if (!boost::starts_with(meta_pair.first, METADATA_CONF_PREFIX)) {
        continue;
      }

      std::string key = meta_pair.first.substr(METADATA_CONF_PREFIX.size());
      if (!boost::starts_with(key, "rbd_")) {
        // ignore non-RBD configuration keys
        // TODO use option schema to determine applicable subsystem
        ldout(cct, 0) << __func__ << ": ignoring config " << key << dendl;
        continue;
      }

      if (config.find_option(key) != nullptr) {
        std::string val(meta_pair.second.c_str(), meta_pair.second.length());
        int r = config.set_val(key, val);
        if (r >= 0) {
          ldout(cct, 20) << __func__ << ": " << key << "=" << val << dendl;
          config_overrides.insert(key);
        } else {
          lderr(cct) << __func__ << ": failed to set config " << key << " "
                     << "with value " << val << ": " << cpp_strerror(r)
                     << dendl;
        }
      }
    }

    image_locker.unlock();

#define ASSIGN_OPTION(param, type)              \
    param = config.get_val<type>("rbd_"#param)

    bool skip_partial_discard = true;
    ASSIGN_OPTION(non_blocking_aio, bool);
    ASSIGN_OPTION(cache, bool);
    ASSIGN_OPTION(sparse_read_threshold_bytes, Option::size_t);
    ASSIGN_OPTION(clone_copy_on_read, bool);
    ASSIGN_OPTION(enable_alloc_hint, bool);
    ASSIGN_OPTION(mirroring_replay_delay, uint64_t);
    ASSIGN_OPTION(mtime_update_interval, uint64_t);
    ASSIGN_OPTION(atime_update_interval, uint64_t);
    ASSIGN_OPTION(skip_partial_discard, bool);
    ASSIGN_OPTION(discard_granularity_bytes, uint64_t);
    ASSIGN_OPTION(blkin_trace_all, bool);

    auto cache_policy = config.get_val<std::string>("rbd_cache_policy");
    if (cache_policy == "writethrough" || cache_policy == "writeback") {
      ASSIGN_OPTION(readahead_max_bytes, Option::size_t);
      ASSIGN_OPTION(readahead_disable_after_bytes, Option::size_t);
    }

#undef ASSIGN_OPTION

    if (sparse_read_threshold_bytes == 0) {
      sparse_read_threshold_bytes = get_object_size();
    }

    bool dirty_cache = test_features(RBD_FEATURE_DIRTY_CACHE);
    if (!skip_partial_discard || dirty_cache) {
      discard_granularity_bytes = 0;
    }

    alloc_hint_flags = 0;
    auto compression_hint = config.get_val<std::string>("rbd_compression_hint");
    if (compression_hint == "compressible") {
      alloc_hint_flags |= librados::ALLOC_HINT_FLAG_COMPRESSIBLE;
    } else if (compression_hint == "incompressible") {
      alloc_hint_flags |= librados::ALLOC_HINT_FLAG_INCOMPRESSIBLE;
    }

    librados::Rados rados(md_ctx);
    int8_t require_osd_release;
    int r = rados.get_min_compatible_osd(&require_osd_release);
    if (r == 0 && require_osd_release >= CEPH_RELEASE_OCTOPUS) {
      read_flags = 0;
      auto read_policy = config.get_val<std::string>("rbd_read_from_replica_policy");
      if (read_policy == "balance") {
        read_flags |= librados::OPERATION_BALANCE_READS;
      } else if (read_policy == "localize") {
        read_flags |= librados::OPERATION_LOCALIZE_READS;
      }
    }

    io_image_dispatcher->apply_qos_schedule_tick_min(
      config.get_val<uint64_t>("rbd_qos_schedule_tick_min"));

    io_image_dispatcher->apply_qos_limit(
      io::IMAGE_DISPATCH_FLAG_QOS_IOPS_THROTTLE,
      config.get_val<uint64_t>("rbd_qos_iops_limit"),
      config.get_val<uint64_t>("rbd_qos_iops_burst"),
      config.get_val<uint64_t>("rbd_qos_iops_burst_seconds"));
    io_image_dispatcher->apply_qos_limit(
      io::IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE,
      config.get_val<uint64_t>("rbd_qos_bps_limit"),
      config.get_val<uint64_t>("rbd_qos_bps_burst"),
      config.get_val<uint64_t>("rbd_qos_bps_burst_seconds"));
    io_image_dispatcher->apply_qos_limit(
      io::IMAGE_DISPATCH_FLAG_QOS_READ_IOPS_THROTTLE,
      config.get_val<uint64_t>("rbd_qos_read_iops_limit"),
      config.get_val<uint64_t>("rbd_qos_read_iops_burst"),
      config.get_val<uint64_t>("rbd_qos_read_iops_burst_seconds"));
    io_image_dispatcher->apply_qos_limit(
      io::IMAGE_DISPATCH_FLAG_QOS_WRITE_IOPS_THROTTLE,
      config.get_val<uint64_t>("rbd_qos_write_iops_limit"),
      config.get_val<uint64_t>("rbd_qos_write_iops_burst"),
      config.get_val<uint64_t>("rbd_qos_write_iops_burst_seconds"));
    io_image_dispatcher->apply_qos_limit(
      io::IMAGE_DISPATCH_FLAG_QOS_READ_BPS_THROTTLE,
      config.get_val<uint64_t>("rbd_qos_read_bps_limit"),
      config.get_val<uint64_t>("rbd_qos_read_bps_burst"),
      config.get_val<uint64_t>("rbd_qos_read_bps_burst_seconds"));
    io_image_dispatcher->apply_qos_limit(
      io::IMAGE_DISPATCH_FLAG_QOS_WRITE_BPS_THROTTLE,
      config.get_val<uint64_t>("rbd_qos_write_bps_limit"),
      config.get_val<uint64_t>("rbd_qos_write_bps_burst"),
      config.get_val<uint64_t>("rbd_qos_write_bps_burst_seconds"));
    io_image_dispatcher->apply_qos_exclude_ops(
      librbd::io::rbd_io_operations_from_string(
        config.get_val<std::string>("rbd_qos_exclude_ops"), nullptr));

    if (!disable_zero_copy &&
        config.get_val<bool>("rbd_disable_zero_copy_writes")) {
      ldout(cct, 5) << this << ": disabling zero-copy writes" << dendl;
      disable_zero_copy = true;
    }
  }

  ExclusiveLock<ImageCtx> *ImageCtx::create_exclusive_lock() {
    return new ExclusiveLock<ImageCtx>(*this);
  }

  ObjectMap<ImageCtx> *ImageCtx::create_object_map(uint64_t snap_id) {
    return new ObjectMap<ImageCtx>(*this, snap_id);
  }

  Journal<ImageCtx> *ImageCtx::create_journal() {
    return new Journal<ImageCtx>(*this);
  }

  uint64_t ImageCtx::get_data_offset() const {
    if (encryption_format != nullptr) {
      return encryption_format->get_crypto()->get_data_offset();
    }
    return 0;
  }

  void ImageCtx::set_image_name(const std::string &image_name) {
    // update the name so rename can be invoked repeatedly
    std::shared_lock owner_locker{owner_lock};
    std::unique_lock image_locker{image_lock};
    name = image_name;
    if (old_format) {
      header_oid = util::old_header_name(image_name);
    }
  }

  void ImageCtx::notify_update() {
    state->handle_update_notification();
    ImageWatcher<>::notify_header_update(md_ctx, header_oid);
  }

  void ImageCtx::notify_update(Context *on_finish) {
    state->handle_update_notification();
    image_watcher->notify_header_update(on_finish);
  }

  exclusive_lock::Policy *ImageCtx::get_exclusive_lock_policy() const {
    ceph_assert(ceph_mutex_is_locked(owner_lock));
    ceph_assert(exclusive_lock_policy != nullptr);
    return exclusive_lock_policy;
  }

  void ImageCtx::set_exclusive_lock_policy(exclusive_lock::Policy *policy) {
    ceph_assert(ceph_mutex_is_wlocked(owner_lock));
    ceph_assert(policy != nullptr);
    delete exclusive_lock_policy;
    exclusive_lock_policy = policy;
  }

  journal::Policy *ImageCtx::get_journal_policy() const {
    ceph_assert(ceph_mutex_is_locked(image_lock));
    ceph_assert(journal_policy != nullptr);
    return journal_policy;
  }

  void ImageCtx::set_journal_policy(journal::Policy *policy) {
    ceph_assert(ceph_mutex_is_wlocked(image_lock));
    ceph_assert(policy != nullptr);
    delete journal_policy;
    journal_policy = policy;
  }

  void ImageCtx::rebuild_data_io_context() {
    auto ctx = std::make_shared<neorados::IOContext>(
      data_ctx.get_id(), data_ctx.get_namespace());
    if (snap_id != CEPH_NOSNAP) {
      ctx->set_read_snap(snap_id);
    }
    if (!snapc.snaps.empty()) {
      ctx->set_write_snap_context(
        {{snapc.seq, {snapc.snaps.begin(), snapc.snaps.end()}}});
    }
    if (data_ctx.get_pool_full_try()) {
      ctx->set_full_try(true);
    }

    // atomically reset the data IOContext to new version
    atomic_store(&data_io_context, ctx);
  }

  IOContext ImageCtx::get_data_io_context() const {
    return atomic_load(&data_io_context);
  }

  IOContext ImageCtx::duplicate_data_io_context() const {
    auto ctx = get_data_io_context();
    return std::make_shared<neorados::IOContext>(*ctx);
  }

  void ImageCtx::get_timer_instance(CephContext *cct, SafeTimer **timer,
                                    ceph::mutex **timer_lock) {
    auto safe_timer_singleton =
      &cct->lookup_or_create_singleton_object<SafeTimerSingleton>(
	"librbd::journal::safe_timer", false, cct);
    *timer = safe_timer_singleton;
    *timer_lock = &safe_timer_singleton->lock;
  }
}
