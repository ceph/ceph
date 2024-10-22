// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NativeFormat.h"
#include "include/neorados/RADOS.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "json_spirit/json_spirit.h"
#include "boost/lexical_cast.hpp"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NativeFormat: " << __func__ \
                           << ": "

namespace librbd {
namespace migration {

namespace {

const std::string TYPE_KEY{"type"};
const std::string POOL_ID_KEY{"pool_id"};
const std::string POOL_NAME_KEY{"pool_name"};
const std::string POOL_NAMESPACE_KEY{"pool_namespace"};
const std::string IMAGE_NAME_KEY{"image_name"};
const std::string IMAGE_ID_KEY{"image_id"};
const std::string SNAP_NAME_KEY{"snap_name"};
const std::string SNAP_ID_KEY{"snap_id"};

} // anonymous namespace

template <typename I>
std::string NativeFormat<I>::build_source_spec(
    int64_t pool_id, const std::string& pool_namespace,
    const std::string& image_name, const std::string& image_id) {
  json_spirit::mObject source_spec;
  source_spec[TYPE_KEY] = "native";
  source_spec[POOL_ID_KEY] = pool_id;
  source_spec[POOL_NAMESPACE_KEY] = pool_namespace;
  source_spec[IMAGE_NAME_KEY] = image_name;
  if (!image_id.empty()) {
    source_spec[IMAGE_ID_KEY] = image_id;
  }
  return json_spirit::write(source_spec);
}

template <typename I>
bool NativeFormat<I>::is_source_spec(
    const json_spirit::mObject& source_spec_object) {
  auto it = source_spec_object.find(TYPE_KEY);
  return it != source_spec_object.end() &&
         it->second.type() == json_spirit::str_type &&
         it->second.get_str() == "native";
}

template <typename I>
int NativeFormat<I>::create_image_ctx(
    librados::IoCtx& dst_io_ctx,
    const json_spirit::mObject& source_spec_object,
    bool import_only, uint64_t src_snap_id, I** src_image_ctx) {
  auto cct = reinterpret_cast<CephContext*>(dst_io_ctx.cct());
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_name;
  std::string image_id;
  std::string snap_name;
  uint64_t snap_id = CEPH_NOSNAP;

  if (auto it = source_spec_object.find(POOL_NAME_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      librados::Rados rados(dst_io_ctx);
      pool_id = rados.pool_lookup(it->second.get_str().c_str());
      if (pool_id < 0) {
        lderr(cct) << "failed to lookup pool" << dendl;
        return static_cast<int>(pool_id);
      }
    } else {
      lderr(cct) << "invalid pool name" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(POOL_ID_KEY);
      it != source_spec_object.end()) {
    if (pool_id != -1) {
      lderr(cct) << "cannot specify both pool name and pool id" << dendl;
      return -EINVAL;
    }
    if (it->second.type() == json_spirit::int_type) {
      pool_id = it->second.get_int64();
    } else if (it->second.type() == json_spirit::str_type) {
      try {
        pool_id = boost::lexical_cast<int64_t>(it->second.get_str());
      } catch (boost::bad_lexical_cast&) {
      }
    }
    if (pool_id == -1) {
      lderr(cct) << "invalid pool id" << dendl;
      return -EINVAL;
    }
  }

  if (pool_id == -1) {
    lderr(cct) << "missing pool name or pool id" << dendl;
    return -EINVAL;
  }

  if (auto it = source_spec_object.find(POOL_NAMESPACE_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      pool_namespace = it->second.get_str();
    } else {
      lderr(cct) << "invalid pool namespace" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(IMAGE_NAME_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      image_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid image name" << dendl;
      return -EINVAL;
    }
  } else {
    lderr(cct) << "missing image name" << dendl;
    return -EINVAL;
  }

  if (auto it = source_spec_object.find(IMAGE_ID_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      image_id = it->second.get_str();
    } else {
      lderr(cct) << "invalid image id" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(SNAP_NAME_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      snap_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid snap name" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(SNAP_ID_KEY);
      it != source_spec_object.end()) {
    if (!snap_name.empty()) {
      lderr(cct) << "cannot specify both snap name and snap id" << dendl;
      return -EINVAL;
    }
    if (it->second.type() == json_spirit::int_type) {
      snap_id = it->second.get_uint64();
    } else if (it->second.type() == json_spirit::str_type) {
      try {
        snap_id = boost::lexical_cast<uint64_t>(it->second.get_str());
      } catch (boost::bad_lexical_cast&) {
      }
    }
    if (snap_id == CEPH_NOSNAP) {
      lderr(cct) << "invalid snap id" << dendl;
      return -EINVAL;
    }
  }

  // snapshot is required for import to keep source read-only
  if (import_only && snap_name.empty() && snap_id == CEPH_NOSNAP) {
    lderr(cct) << "snapshot required for import" << dendl;
    return -EINVAL;
  }

  // import snapshot is used only for destination image HEAD
  // otherwise, src_snap_id corresponds to destination image "opened at"
  // snap_id
  if (src_snap_id != CEPH_NOSNAP) {
    snap_id = src_snap_id;
  }

  // TODO add support for external clusters
  librados::IoCtx src_io_ctx;
  int r = util::create_ioctx(dst_io_ctx, "source image", pool_id,
                             pool_namespace, &src_io_ctx);
  if (r < 0) {
    return r;
  }

  if (!snap_name.empty() && snap_id == CEPH_NOSNAP) {
    *src_image_ctx = I::create(image_name, image_id, snap_name.c_str(),
                               src_io_ctx, true);
  } else {
    *src_image_ctx = I::create(image_name, image_id, snap_id, src_io_ctx,
                               true);
  }
  return 0;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NativeFormat<librbd::ImageCtx>;
