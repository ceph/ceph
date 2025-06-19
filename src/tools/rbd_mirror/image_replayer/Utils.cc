// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::util::" \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace util {

std::string compute_image_spec(librados::IoCtx& io_ctx,
                               const std::string& image_name) {
  std::string name = io_ctx.get_namespace();
  if (!name.empty()) {
    name += "/";
  }

  return io_ctx.get_pool_name() + "/" + name + image_name;
}

bool decode_client_meta(const cls::journal::Client& client,
                        librbd::journal::MirrorPeerClientMeta* client_meta) {
  dout(15) << dendl;

  librbd::journal::ClientData client_data;
  auto it = client.data.cbegin();
  try {
    decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << "failed to decode client meta data: " << err.what() << dendl;
    return false;
  }

  auto local_client_meta = std::get_if<librbd::journal::MirrorPeerClientMeta>(
    &client_data.client_meta);
  if (local_client_meta == nullptr) {
    derr << "unknown peer registration" << dendl;
    return false;
  }

  *client_meta = *local_client_meta;
  dout(15) << "client found: client_meta=" << *client_meta << dendl;
  return true;
}

} // namespace util
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

