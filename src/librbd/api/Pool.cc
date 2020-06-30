// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Pool.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "common/Throttle.h"
#include "cls/rbd/cls_rbd_client.h"
#include "osd/osd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/api/Image.h"
#include "librbd/api/Trash.h"
#include "librbd/image/ValidatePoolRequest.h"

#define dout_subsys ceph_subsys_rbd

namespace librbd {
namespace api {

namespace {

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Pool::ImageStatRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class ImageStatRequest {
public:
  ImageStatRequest(librados::IoCtx& io_ctx, SimpleThrottle& throttle,
                   const std::string& image_id, bool scan_snaps,
                   std::atomic<uint64_t>* bytes,
                   std::atomic<uint64_t>* max_bytes,
                   std::atomic<uint64_t>* snaps)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_io_ctx(io_ctx), m_throttle(throttle), m_image_id(image_id),
      m_scan_snaps(scan_snaps), m_bytes(bytes), m_max_bytes(max_bytes),
      m_snaps(snaps) {
    m_throttle.start_op();
  }

  void send() {
    get_head();
  }

protected:
  void finish(int r) {
    (*m_max_bytes) += m_max_size;
    m_throttle.end_op(r);

    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  SimpleThrottle& m_throttle;
  const std::string& m_image_id;
  bool m_scan_snaps;
  std::atomic<uint64_t>* m_bytes;
  std::atomic<uint64_t>* m_max_bytes;
  std::atomic<uint64_t>* m_snaps;
  bufferlist m_out_bl;

  uint64_t m_max_size = 0;
  ::SnapContext m_snapc;

  void get_head() {
    ldout(m_cct, 15) << dendl;

    librados::ObjectReadOperation op;
    cls_client::get_size_start(&op, CEPH_NOSNAP);
    if (m_scan_snaps) {
      cls_client::get_snapcontext_start(&op);
    }

    m_out_bl.clear();
    auto aio_comp = util::create_rados_callback<
      ImageStatRequest<I>, &ImageStatRequest<I>::handle_get_head>(this);
    int r = m_io_ctx.aio_operate(util::header_name(m_image_id), aio_comp, &op,
                                 &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
  }

  void handle_get_head(int r) {
    ldout(m_cct, 15) << "r=" << r << dendl;

    auto it = m_out_bl.cbegin();
    if (r == 0) {
      uint8_t order;
      r = cls_client::get_size_finish(&it, &m_max_size, &order);
      if (r == 0) {
        (*m_bytes) += m_max_size;
      }
    }
    if (m_scan_snaps && r == 0) {
      r = cls_client::get_snapcontext_finish(&it, &m_snapc);
      if (r == 0) {
        (*m_snaps) += m_snapc.snaps.size();
      }
    }

    if (r == -ENOENT) {
      finish(r);
      return;
    } else if (r < 0) {
      lderr(m_cct) << "failed to stat image: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    if (!m_snapc.is_valid()) {
      lderr(m_cct) << "snap context is invalid" << dendl;
      finish(-EIO);
      return;
    }

    get_snaps();
  }

  void get_snaps() {
    if (!m_scan_snaps || m_snapc.snaps.empty()) {
      finish(0);
      return;
    }

    ldout(m_cct, 15) << dendl;
    librados::ObjectReadOperation op;
    for (auto snap_seq : m_snapc.snaps) {
      cls_client::get_size_start(&op, snap_seq);
    }

    m_out_bl.clear();
    auto aio_comp = util::create_rados_callback<
      ImageStatRequest<I>, &ImageStatRequest<I>::handle_get_snaps>(this);
    int r = m_io_ctx.aio_operate(util::header_name(m_image_id), aio_comp, &op,
                                 &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
  }

  void handle_get_snaps(int r) {
    ldout(m_cct, 15) << "r=" << r << dendl;

    auto it = m_out_bl.cbegin();
    for ([[maybe_unused]] auto snap_seq : m_snapc.snaps) {
      uint64_t size;
      if (r == 0) {
        uint8_t order;
        r = cls_client::get_size_finish(&it, &size, &order);
      }
      if (r == 0 && m_max_size < size) {
        m_max_size = size;
      }
    }

    if (r == -ENOENT) {
      ldout(m_cct, 15) << "out-of-sync metadata" << dendl;
      get_head();
    } else if (r < 0) {
      lderr(m_cct) << "failed to retrieve snap size: " << cpp_strerror(r)
                   << dendl;
      finish(r);
    } else {
      finish(0);
    }
  }

};

template <typename I>
void get_pool_stat_option_value(typename Pool<I>::StatOptions* stat_options,
                                rbd_pool_stat_option_t option,
                                uint64_t** value) {
  auto it = stat_options->find(option);
  if (it == stat_options->end()) {
    *value = nullptr;
  } else {
    *value = it->second;
  }
}

template <typename I>
int get_pool_stats(librados::IoCtx& io_ctx, const ConfigProxy& config,
              const std::vector<std::string>& image_ids, uint64_t* image_count,
              uint64_t* provisioned_bytes, uint64_t* max_provisioned_bytes,
              uint64_t* snapshot_count) {

  bool scan_snaps = ((max_provisioned_bytes != nullptr) ||
                     (snapshot_count != nullptr));

  SimpleThrottle throttle(
    config.template get_val<uint64_t>("rbd_concurrent_management_ops"), true);
  std::atomic<uint64_t> bytes{0};
  std::atomic<uint64_t> max_bytes{0};
  std::atomic<uint64_t> snaps{0};
  for (auto& image_id : image_ids) {
    if (throttle.pending_error()) {
      break;
    }

    auto req = new ImageStatRequest<I>(io_ctx, throttle, image_id,
                                       scan_snaps, &bytes, &max_bytes, &snaps);
    req->send();
  }

  int r = throttle.wait_for_ret();
  if (r < 0) {
    return r;
  }

  if (image_count != nullptr) {
    *image_count = image_ids.size();
  }
  if (provisioned_bytes != nullptr) {
    *provisioned_bytes = bytes.load();
  }
  if (max_provisioned_bytes != nullptr) {
    *max_provisioned_bytes = max_bytes.load();
  }
  if (snapshot_count != nullptr) {
    *snapshot_count = snaps.load();
  }

  return 0;
}

} // anonymous namespace

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Pool: " << __func__ << ": "

template <typename I>
int Pool<I>::init(librados::IoCtx& io_ctx, bool force) {
  auto cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 10) << dendl;

  int r = io_ctx.application_enable(pg_pool_t::APPLICATION_NAME_RBD, force);
  if (r < 0) {
    return r;
  }

  ConfigProxy config{cct->_conf};
  api::Config<I>::apply_pool_overrides(io_ctx, &config);
  if (!config.get_val<bool>("rbd_validate_pool")) {
    return 0;
  }

  asio::ContextWQ *op_work_queue;
  ImageCtx::get_work_queue(cct, &op_work_queue);

  C_SaferCond ctx;
  auto req = image::ValidatePoolRequest<I>::create(io_ctx, op_work_queue, &ctx);
  req->send();

  return ctx.wait();
}

template <typename I>
int Pool<I>::add_stat_option(StatOptions* stat_options,
                             rbd_pool_stat_option_t option,
                             uint64_t* value) {
  switch (option) {
  case RBD_POOL_STAT_OPTION_IMAGES:
  case RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES:
  case RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES:
  case RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS:
  case RBD_POOL_STAT_OPTION_TRASH_IMAGES:
  case RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES:
  case RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES:
  case RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS:
    stat_options->emplace(option, value);
    return 0;
  default:
    break;
  }
  return -ENOENT;
}

template <typename I>
int Pool<I>::get_stats(librados::IoCtx& io_ctx, StatOptions* stat_options) {
  auto cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 10) << dendl;

  ConfigProxy config{cct->_conf};
  api::Config<I>::apply_pool_overrides(io_ctx, &config);

  uint64_t* image_count;
  uint64_t* provisioned_bytes;
  uint64_t* max_provisioned_bytes;
  uint64_t* snapshot_count;

  std::vector<trash_image_info_t> trash_entries;
  int r = Trash<I>::list(io_ctx, trash_entries, false);
  if (r < 0 && r != -EOPNOTSUPP) {
    return r;
  }

  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_IMAGES, &image_count);
  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES,
    &provisioned_bytes);
  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES,
    &max_provisioned_bytes);
  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS, &snapshot_count);
  if (image_count != nullptr || provisioned_bytes != nullptr ||
      max_provisioned_bytes != nullptr || snapshot_count != nullptr) {
    typename Image<I>::ImageNameToIds images;
    int r = Image<I>::list_images_v2(io_ctx, &images);
    if (r < 0) {
      return r;
    }

    std::vector<std::string> image_ids;
    image_ids.reserve(images.size() + trash_entries.size());
    for (auto& it : images) {
      image_ids.push_back(std::move(it.second));
    }
    for (auto& it : trash_entries) {
      if (it.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
        image_ids.push_back(std::move(it.id));
      }
    }

    r = get_pool_stats<I>(io_ctx, config, image_ids, image_count,
                          provisioned_bytes, max_provisioned_bytes,
                          snapshot_count);
    if (r < 0) {
      return r;
    }
  }

  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_TRASH_IMAGES, &image_count);
  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES,
    &provisioned_bytes);
  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES,
    &max_provisioned_bytes);
  get_pool_stat_option_value<I>(
    stat_options, RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS, &snapshot_count);
  if (image_count != nullptr || provisioned_bytes != nullptr ||
      max_provisioned_bytes != nullptr || snapshot_count != nullptr) {

    std::vector<std::string> image_ids;
    image_ids.reserve(trash_entries.size());
    for (auto& it : trash_entries) {
      if (it.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
        continue;
      }
      image_ids.push_back(std::move(it.id));
    }

    r = get_pool_stats<I>(io_ctx, config, image_ids, image_count,
                          provisioned_bytes, max_provisioned_bytes,
                          snapshot_count);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Pool<librbd::ImageCtx>;
