// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "librbd/Utils.h"
#include "include/random.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include "include/neorados/RADOS.hpp"
#include "include/rbd/features.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Features.h"

#include <boost/algorithm/string/predicate.hpp>
#include <bitset>
#include <random>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::util::" << __func__ << ": "

namespace librbd {
namespace util {
namespace {

const std::string CONFIG_KEY_URI_PREFIX{"config://"};

} // anonymous namespace

const std::string group_header_name(const std::string &group_id)
{
  return RBD_GROUP_HEADER_PREFIX + group_id;
}

const std::string id_obj_name(const std::string &name)
{
  return RBD_ID_PREFIX + name;
}

const std::string header_name(const std::string &image_id)
{
  return RBD_HEADER_PREFIX + image_id;
}

const std::string old_header_name(const std::string &image_name)
{
  return image_name + RBD_SUFFIX;
}

std::string unique_lock_name(const std::string &name, void *address) {
  return name + " (" + stringify(address) + ")";
}

librados::AioCompletion *create_rados_callback(Context *on_finish) {
  return create_rados_callback<Context, &Context::complete>(on_finish);
}

// also used for group and group snapshot ids
std::string generate_image_id(librados::IoCtx &ioctx) {
  librados::Rados rados(ioctx);

  uint64_t bid = rados.get_instance_id();
  std::mt19937 generator{random_device_t{}()};
  std::uniform_int_distribution<uint32_t> distribution{0, 0xFFFFFFFF};
  uint32_t extra = distribution(generator);

  std::ostringstream bid_ss;
  bid_ss << std::hex << bid << std::hex << extra;
  std::string id = bid_ss.str();

  // ensure the image id won't overflow the fixed block name size
  if (id.length() > RBD_MAX_IMAGE_ID_LENGTH) {
    id = id.substr(id.length() - RBD_MAX_IMAGE_ID_LENGTH);
  }

  return id;
}

uint64_t get_rbd_default_features(CephContext* cct)
{
  auto value = cct->_conf.get_val<std::string>("rbd_default_features");
  return librbd::rbd_features_from_string(value, nullptr);
}


bool calc_sparse_extent(const bufferptr &bp,
                        size_t sparse_size,
                        uint64_t length,
                        size_t *write_offset,
                        size_t *write_length,
                        size_t *offset) {
  size_t extent_size;
  if (*offset + sparse_size > length) {
    extent_size = length - *offset;
  } else {
    extent_size = sparse_size;
  }

  bufferptr extent(bp, *offset, extent_size);
  *offset += extent_size;

  bool extent_is_zero = extent.is_zero();
  if (!extent_is_zero) {
    *write_length += extent_size;
  }
  if (extent_is_zero && *write_length == 0) {
    *write_offset += extent_size;
  }

  if ((extent_is_zero || *offset == length) && *write_length != 0) {
    return true;
  }
  return false;
}

bool is_metadata_config_override(const std::string& metadata_key,
                                 std::string* config_key) {
  size_t prefix_len = librbd::ImageCtx::METADATA_CONF_PREFIX.size();
  if (metadata_key.size() > prefix_len &&
      metadata_key.compare(0, prefix_len,
                           librbd::ImageCtx::METADATA_CONF_PREFIX) == 0) {
    *config_key = metadata_key.substr(prefix_len,
                                      metadata_key.size() - prefix_len);
    return true;
  }
  return false;
}

int create_ioctx(librados::IoCtx& src_io_ctx, const std::string& pool_desc,
                 int64_t pool_id,
                 const std::optional<std::string>& pool_namespace,
                 librados::IoCtx* dst_io_ctx) {
  auto cct = (CephContext *)src_io_ctx.cct();

  librados::Rados rados(src_io_ctx);
  int r = rados.ioctx_create2(pool_id, *dst_io_ctx);
  if (r == -ENOENT) {
    ldout(cct, 1) << pool_desc << " pool " << pool_id << " no longer exists"
                  << dendl;
    return r;
  } else if (r < 0) {
    lderr(cct) << "error accessing " << pool_desc << " pool " << pool_id
               << dendl;
    return r;
  }

  dst_io_ctx->set_namespace(
    pool_namespace ? *pool_namespace : src_io_ctx.get_namespace());
  if (src_io_ctx.get_pool_full_try()) {
    dst_io_ctx->set_pool_full_try();
  }
  return 0;
}

int snap_create_flags_api_to_internal(CephContext *cct, uint32_t api_flags,
                                      uint64_t *internal_flags) {
  *internal_flags = 0;

  if (api_flags & RBD_SNAP_CREATE_SKIP_QUIESCE) {
    *internal_flags |= SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE;
    api_flags &= ~RBD_SNAP_CREATE_SKIP_QUIESCE;
  } else if (api_flags & RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR) {
    *internal_flags |= SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR;
    api_flags &= ~RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR;
  }

  if (api_flags != 0) {
    lderr(cct) << "invalid snap create flags: "
                     << std::bitset<32>(api_flags) << dendl;
    return -EINVAL;
  }

  return 0;
}

uint32_t get_default_snap_create_flags(ImageCtx *ictx) {
  auto mode = ictx->config.get_val<std::string>(
      "rbd_default_snapshot_quiesce_mode");

  if (mode == "required") {
    return 0;
  } else if (mode == "ignore-error") {
    return RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR;
  } else if (mode == "skip") {
    return RBD_SNAP_CREATE_SKIP_QUIESCE;
  } else {
    ceph_abort_msg("invalid rbd_default_snapshot_quiesce_mode");
  }
}

SnapContext get_snap_context(
    const std::optional<
      std::pair<std::uint64_t,
                std::vector<std::uint64_t>>>& write_snap_context) {
  SnapContext snapc;
  if (write_snap_context) {
    snapc = SnapContext{write_snap_context->first,
                        {write_snap_context->second.begin(),
                         write_snap_context->second.end()}};
  }
  return snapc;
}

uint64_t reserve_async_request_id() {
  static std::atomic<uint64_t> async_request_seq = 0;

  return ++async_request_seq;
}

bool is_config_key_uri(const std::string& uri) {
  return boost::starts_with(uri, CONFIG_KEY_URI_PREFIX);
}

int get_config_key(librados::Rados& rados, const std::string& uri,
                   std::string* value) {
  auto cct = reinterpret_cast<CephContext*>(rados.cct());

  if (!is_config_key_uri(uri)) {
    return -EINVAL;
  }

  std::string key = uri.substr(CONFIG_KEY_URI_PREFIX.size());
  std::string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";

  bufferlist in_bl;
  bufferlist out_bl;
  int r = rados.mon_command(cmd, in_bl, &out_bl, nullptr);
  if (r < 0) {
    lderr(cct) << "failed to retrieve MON config key " << key << ": "
               << cpp_strerror(r) << dendl;
    return r;
  }

  *value = std::string(out_bl.c_str(), out_bl.length());
  return 0;
}

} // namespace util
} // namespace librbd
