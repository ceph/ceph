// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "librbd/Utils.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include "include/rbd/features.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Features.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

namespace librbd {
namespace util {

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

std::string generate_image_id(librados::IoCtx &ioctx) {
  librados::Rados rados(ioctx);

  uint64_t bid = rados.get_instance_id();
  uint32_t extra = rand() % 0xFFFFFFFF;

  ostringstream bid_ss;
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
  auto value = cct->_conf->get_val<std::string>("rbd_default_features");
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

} // namespace util
} // namespace librbd
