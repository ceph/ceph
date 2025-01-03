// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NativeFormat.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/scope_guard.h"
#include "librbd/ImageCtx.h"
#include "json_spirit/json_spirit.h"
#include "boost/lexical_cast.hpp"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NativeFormat: " << __func__ \
                           << ": "

namespace librbd {
namespace migration {

namespace {

const std::string TYPE_KEY{"type"};
const std::string CLUSTER_NAME_KEY{"cluster_name"};
const std::string CLIENT_NAME_KEY{"client_name"};
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
    bool import_only, uint64_t src_snap_id, I** src_image_ctx,
    librados::Rados** src_rados) {
  auto cct = reinterpret_cast<CephContext*>(dst_io_ctx.cct());
  std::string cluster_name;
  std::string client_name;
  std::string pool_name;
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_name;
  std::string image_id;
  std::string snap_name;
  uint64_t snap_id = CEPH_NOSNAP;
  int r;

  if (auto it = source_spec_object.find(CLUSTER_NAME_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      cluster_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid cluster name" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(CLIENT_NAME_KEY);
      it != source_spec_object.end()) {
    if (cluster_name.empty()) {
      lderr(cct) << "cannot specify client name without cluster name" << dendl;
      return -EINVAL;
    }
    if (it->second.type() == json_spirit::str_type) {
      client_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid client name" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(POOL_NAME_KEY);
      it != source_spec_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      pool_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid pool name" << dendl;
      return -EINVAL;
    }
  }

  if (auto it = source_spec_object.find(POOL_ID_KEY);
      it != source_spec_object.end()) {
    if (!pool_name.empty()) {
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

  if (pool_name.empty() && pool_id == -1) {
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
    lderr(cct) << "snap name or snap id required for import" << dendl;
    return -EINVAL;
  }

  // import snapshot is used only for destination image HEAD
  // otherwise, src_snap_id corresponds to destination image "opened at"
  // snap_id
  if (src_snap_id != CEPH_NOSNAP) {
    snap_id = src_snap_id;
  }

  std::unique_ptr<librados::Rados> rados_ptr;
  if (!cluster_name.empty()) {
    // manually bootstrap a CephContext, skipping reading environment
    // variables for now -- since we don't have access to command line
    // arguments here, the least confusing option is to limit initial
    // remote cluster config to a file in the default location
    // TODO: support specifying mon_host and key via source spec
    // TODO: support merging in effective local cluster config to get
    // overrides for log levels, etc
    CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
    if (!client_name.empty() && !iparams.name.from_str(client_name)) {
      lderr(cct) << "failed to set remote client name" << dendl;
      return -EINVAL;
    }

    auto remote_cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
    auto put_remote_cct = make_scope_guard([remote_cct] { remote_cct->put(); });

    remote_cct->_conf->cluster = cluster_name;

    // pass CEPH_CONF_FILE_DEFAULT instead of nullptr to prevent
    // CEPH_CONF environment variable from being picked up
    r = remote_cct->_conf.parse_config_files(CEPH_CONF_FILE_DEFAULT, nullptr,
                                             0);
    if (r < 0) {
      remote_cct->_conf.complain_about_parse_error(cct);
      lderr(cct) << "failed to read ceph conf for remote cluster: "
                 << cpp_strerror(r) << dendl;
      return r;
    }

    remote_cct->_conf.apply_changes(nullptr);

    rados_ptr.reset(new librados::Rados());
    r = rados_ptr->init_with_context(remote_cct);
    ceph_assert(r == 0);

    r = rados_ptr->connect();
    if (r < 0) {
      lderr(cct) << "failed to connect to remote cluster: " << cpp_strerror(r)
                 << dendl;
      return r;
    }
  } else {
    rados_ptr.reset(new librados::Rados(dst_io_ctx));
  }

  librados::IoCtx src_io_ctx;
  if (!pool_name.empty()) {
    r = rados_ptr->ioctx_create(pool_name.c_str(), src_io_ctx);
  } else {
    r = rados_ptr->ioctx_create2(pool_id, src_io_ctx);
  }
  if (r < 0) {
    lderr(cct) << "failed to open source image pool: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  src_io_ctx.set_namespace(pool_namespace);

  if (!snap_name.empty() && snap_id == CEPH_NOSNAP) {
    *src_image_ctx = I::create(image_name, image_id, snap_name.c_str(),
                               src_io_ctx, true);
  } else {
    *src_image_ctx = I::create(image_name, image_id, snap_id, src_io_ctx,
                               true);
  }

  if (!cluster_name.empty()) {
    *src_rados = rados_ptr.release();
  } else {
    *src_rados = nullptr;
  }

  return 0;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NativeFormat<librbd::ImageCtx>;
