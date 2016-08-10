// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Utils.h"
#include "include/rbd_types.h"
#include "include/stringify.h"

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

librados::AioCompletion *create_rados_ack_callback(Context *on_finish) {
  return create_rados_ack_callback<Context, &Context::complete>(on_finish);
}

std::string generate_image_id(librados::IoCtx &ioctx) {
  librados::Rados rados(ioctx);

  uint64_t bid = rados.get_instance_id();
  uint32_t extra = rand() % 0xFFFFFFFF;

  ostringstream bid_ss;
  bid_ss << std::hex << bid << std::hex << extra;
  std::string id = bid_ss.str();

  // ensure the image id won't overflow the fixed block name size
  const size_t max_id_length = RBD_MAX_BLOCK_NAME_SIZE - strlen(RBD_DATA_PREFIX) - 1;
  if (id.length() > max_id_length) {
    id = id.substr(id.length() - max_id_length);
  }

  return id;
}

} // namespace util
} // namespace librbd
