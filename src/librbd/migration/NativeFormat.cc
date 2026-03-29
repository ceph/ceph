// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/migration/NativeFormat.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/scope_guard.h"
#include "librbd/ImageCtx.h"
#include "json_spirit/json_spirit.h"
#include "boost/lexical_cast.hpp"
#include "include/uuid.h"

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
const std::string SECRET_KEY{"key"};
const std::string KEYFILE_KEY{"keyfile"};
const std::string KEY_REF{"key_ref"};
const std::string MON_HOST_KEY{"mon_host"};
const std::string KEYRING_PATH_KEY{"keyring_path"};
const std::string CONIG_PATH_KEY{"config_path"};
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

static int get_config_key(librados::Rados& rados, const std::string& key,
                     std::string* value) {
  std::string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";
  bufferlist out_bl;
  int r = rados.mon_command(std::move(cmd), {}, &out_bl, nullptr);
  if (r == -EINVAL) {
    return -EOPNOTSUPP;
  } else if (r < 0 && r != -ENOENT) {
    return r;
  }
  *value = out_bl.to_str();
  return 0;
}

static int set_config_key(librados::Rados& rados, const std::string& key,
                   const std::string& value) {
  std::string cmd;
  if (value.empty()) {
    cmd = "{"
            "\"prefix\": \"config-key rm\", "
            "\"key\": \"" + key + "\""
          "}";
  } else {
    cmd = "{"
            "\"prefix\": \"config-key set\", "
            "\"key\": \"" + key + "\", "
            "\"val\": \"" + value + "\""
          "}";
  }
  bufferlist out_bl;
  int r = rados.mon_command(std::move(cmd), {}, &out_bl, nullptr);
  if (r == -EINVAL) {
    return -EOPNOTSUPP;
  } else if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int NativeFormat<I>::cleanup_secret_from_kv(
  const std::string& source_spec, librados::IoCtx &io_ctx) {
  json_spirit::mObject obj;
  json_spirit::mValue root;
  auto cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  if (!json_spirit::read(source_spec, root)) {
    ldout(cct, 5) << "source spec not found "  << dendl;
    return 0;
  }
  try {
    obj = root.get_obj();
  } catch (...) {
    return 0;
  }
  auto it = obj.find(KEY_REF);
  if (it == obj.end()) {
    ldout(cct, 5) << KEY_REF << " not found"  << dendl;
    return 0;
  }

  std::string key_ref = it->second.get_str();
  librados::Rados rados(io_ctx);
#ifdef DEBUG
  std::string value;
  int res = get_config_key(rados, key_ref, &value);
  if (res !=0 ) {
    lderr(cct) << "failed to find the key  " << key_ref
               << dendl;
  } else {
    ldout(cct, 5) << "found key " << key_ref << " value  "
                  << value << dendl;
  }
#endif
  int r = set_config_key(rados, key_ref, "");
  if (r !=0) {
    lderr(cct) << "failed to remove the key  " << key_ref
               << dendl;
  } else {
    ldout(cct, 5) << "successfully removed key " << key_ref << dendl;
  }
  return r;
}

template <typename I>
int NativeFormat<I>::validate_spec_set_secret_key(
             json_spirit::mObject& source_spec_object,
             librados::IoCtx& dest_io_ctx) {
  std::string secret_key;
  std::string mon_host;
  auto cct = reinterpret_cast<CephContext *>(dest_io_ctx.cct());
  auto it = source_spec_object.find(SECRET_KEY);
  if (it != source_spec_object.end()) {
    try {
      secret_key = it->second.get_str();
      ldout(cct, 5) << "found secret_key in source spec " << secret_key << dendl;
    } catch (std::runtime_error&) {
      lderr(cct) << "secret_key must be a string" << dendl;
      return -EINVAL;
    }
    it = source_spec_object.find(MON_HOST_KEY);// validate mon host
    if (it != source_spec_object.end()) {
      try {
        mon_host = it->second.get_str();
        ldout(cct, 5) << "found mon_host in source spec " << mon_host << dendl;
      } catch (std::runtime_error&) {
        lderr(cct) << "mon_host must be a string" << dendl;
        return -EINVAL;
      }
    } else {
      lderr(cct) << "mon_host must present in the spec" << dendl;
      return -EINVAL;
    }
    it = source_spec_object.find(CLUSTER_NAME_KEY);
    if (it != source_spec_object.end()) {
      lderr(cct) << CLUSTER_NAME_KEY << "presents in the spec" << dendl;
      return -EINVAL;
    }
    it = source_spec_object.find(CONIG_PATH_KEY);
    if (it != source_spec_object.end()) {
      lderr(cct) << CONIG_PATH_KEY << "presents in the spec" << dendl;
      return -EINVAL;
    }
    it = source_spec_object.find(KEYRING_PATH_KEY);
    if (it != source_spec_object.end()) {
      lderr(cct) << KEYRING_PATH_KEY << "presents in the spec" << dendl;
      return -EINVAL;
    }

    uuid_d uuid;
    uuid.generate_random();
    std::string migration_uuid = uuid.to_string();
    source_spec_object.erase(SECRET_KEY); //remove the key from the spec
    source_spec_object[KEY_REF] = migration_uuid; //add the uuid to the spec
    librados::Rados rados(dest_io_ctx);
    int r = set_config_key(rados,  migration_uuid, secret_key);
    if (r < 0) {
      lderr(reinterpret_cast<CephContext*>(rados.cct()))
           << "failed to store secret key in monitor KV: "
           << cpp_strerror(r) << dendl;
      return -EINVAL;
    }
    ldout(cct, 5) << "key set " <<  migration_uuid << dendl;
 #ifdef DEBUG
    std::string value;
    r = get_config_key(rados, migration_uuid, &value);
    if (r < 0) {
      lderr(reinterpret_cast<CephContext*>(rados.cct()))
           << "failed to fetch secret key from the monitor: "
           << cpp_strerror(r) << dendl;
      return -EINVAL;
    }
    ldout(cct, 5) << " get value by key " << migration_uuid
                  << " got "<< value << dendl;
 #endif
  } else {
    return -ENOENT; // old legacy schema so no need validations
  }
  return 0;
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
  auto it_migration_uuid = source_spec_object.find(KEY_REF);

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
    if (cluster_name.empty() &&
        it_migration_uuid == source_spec_object.end()) {
      lderr(cct) << "cannot specify client name without cluster name and secret key"
      << dendl;
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
  std::string migration_uuid = "";
  bool to_connect = true;
  std::unique_ptr<librados::Rados> rados_ptr;
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  auto remote_cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  auto put_remote_cct = make_scope_guard([remote_cct] { remote_cct->put(); });

  if (!cluster_name.empty()) {
    // manually bootstrap a CephContext, skipping reading environment
    // variables for now -- since we don't have access to command line
    // arguments here, the least confusing option is to limit initial
    // remote cluster config to a file in the default location
    // TODO: support specifying mon_host and key via source spec
    // TODO: support merging in effective local cluster config to get
    // overrides for log levels, etc
    if (!client_name.empty() && !iparams.name.from_str(client_name)) {
      lderr(cct) << "failed to set remote client name" << dendl;
      return -EINVAL;
    }

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
  }  else if (it_migration_uuid != source_spec_object.end()) {
     std::string mon_host;
     std::string mig_key;
     std::string migration_uuid = it_migration_uuid->second.get_str();
     mon_host = source_spec_object.at(MON_HOST_KEY).get_str();
     ldout(cct, 5) << "open image ctx: found mon-host in source spec "
                   << migration_uuid << dendl;
     librados::Rados dest_rados(dst_io_ctx);
     r = get_config_key(dest_rados, migration_uuid,  &mig_key);
     if (r < 0) {
       lderr(cct) << "failed to fetch the key from the monitor KV: "  << dendl;
       return r;
     } else {
       ldout(cct, 5) << " get value by key " <<  migration_uuid
                     << " got "<< mig_key << dendl;
       r = remote_cct->_conf.set_val(SECRET_KEY, mig_key);
       if(r != 0) {
         lderr(cct) << "failed to set_val " << SECRET_KEY << " res "
                    << r << dendl;
         return r;
       }
       r = remote_cct->_conf.set_val(KEYFILE_KEY, "");
       if(r != 0) {
         lderr(cct) << "failed to set_val "<< KEYFILE_KEY
                    << " res " << r << dendl;
         return r;
       }
       r = remote_cct->_conf.set_val(MON_HOST_KEY, mon_host);
       if(r != 0) {
         lderr(cct) << "failed to set_val " << MON_HOST_KEY
                    << " res " << r << dendl;
         return r;
       }
       remote_cct->_conf->cluster = "ceph";
     }
    } else {
       rados_ptr.reset(new librados::Rados(dst_io_ctx));
       to_connect = false;
    }
    if (to_connect) {
      remote_cct->_conf.apply_changes(nullptr);
      rados_ptr.reset(new librados::Rados());
      r = rados_ptr->init_with_context(remote_cct);
      ceph_assert(r == 0);
      ldout(cct, 5) << "going to connect to remote cluster" <<dendl;
      r = rados_ptr->connect();
      if (r < 0) {
        lderr(cct) << "failed to connect to remote cluster: " << cpp_strerror(r)
                   << dendl;
        return r;
      }
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
  if (to_connect) {
    *src_rados = rados_ptr.release();
  } else {
    *src_rados = nullptr;
  }
  return 0;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NativeFormat<librbd::ImageCtx>;
