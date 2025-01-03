// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/PoolMetadata.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/image/GetMetadataRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::PoolMetadata: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

void update_pool_timestamp(librados::IoCtx& io_ctx) {
  CephContext *cct = (CephContext *)io_ctx.cct();

  auto now = ceph_clock_now();
  std::string cmd =
    R"({)"
      R"("prefix": "config set", )"
      R"("who": "global", )"
      R"("name": "rbd_config_pool_override_update_timestamp", )"
      R"("value": ")" + stringify(now.sec()) + R"(")"
    R"(})";

  librados::Rados rados(io_ctx);
  bufferlist in_bl;
  std::string ss;
  int r = rados.mon_command(cmd, in_bl, nullptr, &ss);
  if (r < 0) {
    lderr(cct) << "failed to notify clients of pool config update: "
               << cpp_strerror(r) << dendl;
  }
}

} // anonymous namespace

template <typename I>
int PoolMetadata<I>::get(librados::IoCtx& io_ctx,
                     const std::string &key, std::string *value) {
  CephContext *cct = (CephContext *)io_ctx.cct();

  int r = cls_client::metadata_get(&io_ctx, RBD_INFO, key, value);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed reading metadata " << key << ": " << cpp_strerror(r)
               << dendl;
  }

  return r;
}

template <typename I>
int PoolMetadata<I>::set(librados::IoCtx& io_ctx, const std::string &key,
                         const std::string &value) {
  CephContext *cct = (CephContext *)io_ctx.cct();

  bool need_update_pool_timestamp = false;

  std::string config_key;
  if (util::is_metadata_config_override(key, &config_key)) {
    if (!librbd::api::Config<I>::is_option_name(io_ctx, config_key)) {
      lderr(cct) << "validation for " << key
                 << " failed: not allowed pool level override" << dendl;
      return -EINVAL;
    }
    int r = ConfigProxy{false}.set_val(config_key.c_str(), value);
    if (r < 0) {
      lderr(cct) << "validation for " << key << " failed: " << cpp_strerror(r)
                 << dendl;
      return -EINVAL;
    }

    need_update_pool_timestamp = true;
  }

  ceph::bufferlist bl;
  bl.append(value);

  int r = cls_client::metadata_set(&io_ctx, RBD_INFO, {{key, bl}});
  if (r < 0) {
    lderr(cct) << "failed setting metadata " << key << ": " << cpp_strerror(r)
               << dendl;
    return r;
  }

  if (need_update_pool_timestamp) {
    update_pool_timestamp(io_ctx);
  }

  return 0;
}

template <typename I>
int PoolMetadata<I>::remove(librados::IoCtx& io_ctx, const std::string &key) {
  CephContext *cct = (CephContext *)io_ctx.cct();

  std::string value;
  int r = cls_client::metadata_get(&io_ctx, RBD_INFO, key, &value);
  if (r < 0) {
    if (r == -ENOENT) {
      ldout(cct, 1) << "metadata " << key << " does not exist" << dendl;
    } else {
      lderr(cct) << "failed reading metadata " << key << ": " << cpp_strerror(r)
                 << dendl;
    }
    return r;
  }

  r = cls_client::metadata_remove(&io_ctx, RBD_INFO, key);
  if (r < 0) {
    lderr(cct) << "failed removing metadata " << key << ": " << cpp_strerror(r)
               << dendl;
    return r;
  }

  std::string config_key;
  if (util::is_metadata_config_override(key, &config_key)) {
    update_pool_timestamp(io_ctx);
  }

  return 0;
}

template <typename I>
int PoolMetadata<I>::list(librados::IoCtx& io_ctx, const std::string &start,
                          uint64_t max,
                          std::map<std::string, ceph::bufferlist> *pairs) {
  CephContext *cct = (CephContext *)io_ctx.cct();

  pairs->clear();
  C_SaferCond ctx;
  auto req = image::GetMetadataRequest<I>::create(
    io_ctx, RBD_INFO, false, "", start, max, pairs, &ctx);
  req->send();

  int r = ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed listing metadata: " << cpp_strerror(r)
               << dendl;
    return r;
  }
  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::PoolMetadata<librbd::ImageCtx>;
