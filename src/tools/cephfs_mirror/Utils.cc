// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"

#include "Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::Utils " << __func__

namespace cephfs {
namespace mirror {

int connect(std::string_view client_name, std::string_view cluster_name,
            RadosRef *cluster, std::string_view mon_host, std::string_view cephx_key,
            std::vector<const char *> args) {
  dout(20) << ": connecting to cluster=" << cluster_name << ", client=" << client_name
           << ", mon_host=" << mon_host << dendl;

  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << ": error initializing cluster handle for " << cluster_name << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  if (mon_host.empty()) {
    cct->_conf->cluster = cluster_name;
  }

  int r = cct->_conf.parse_config_files(nullptr, nullptr, 0);
  if (r < 0 && r != -ENOENT) {
    derr << ": could not read ceph conf: " << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  cct->_conf.parse_env(cct->get_module_type());

  if (!args.empty()) {
    r = cct->_conf.parse_argv(args);
    if (r < 0) {
      derr << ": could not parse command line args: " << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }
  cct->_conf.parse_env(cct->get_module_type());

  if (!mon_host.empty()) {
    r = cct->_conf.set_val("mon_host", std::string(mon_host));
    if (r < 0) {
      derr << "failed to set mon_host config: " << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }
  if (!cephx_key.empty()) {
    r = cct->_conf.set_val("key", std::string(cephx_key));
    if (r < 0) {
      derr << "failed to set key config: " << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  dout(10) << ": using mon addr=" << cct->_conf.get_val<std::string>("mon_host") << dendl;

  cluster->reset(new librados::Rados());

  r = (*cluster)->init_with_context(cct);
  ceph_assert(r == 0);
  cct->put();

  r = (*cluster)->connect();
  if (r < 0) {
    derr << ": error connecting to " << cluster_name << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  dout(10) << ": connected to cluster=" << cluster_name << " using client="
           << client_name << dendl;

  return 0;
}

int mount(RadosRef cluster, const Filesystem &filesystem, bool cross_check_fscid,
          MountRef *mount) {
  dout(20) << ": filesystem=" << filesystem << dendl;

  ceph_mount_info *cmi;
  int r = ceph_create_with_context(&cmi, reinterpret_cast<CephContext*>(cluster->cct()));
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_conf_set(cmi, "client_mount_uid", "0");
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_conf_set(cmi, "client_mount_gid", "0");
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  // mount timeout applies for local and remote mounts.
  auto mount_timeout = g_ceph_context->_conf.get_val<std::chrono::seconds>
    ("cephfs_mirror_mount_timeout").count();
  r = ceph_set_mount_timeout(cmi, mount_timeout);
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_init(cmi);
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_select_filesystem(cmi, filesystem.fs_name.c_str());
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_mount(cmi, NULL);
  if (r < 0) {
    derr << ": mount error: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto fs_id = ceph_get_fs_cid(cmi);
  if (cross_check_fscid && fs_id != filesystem.fscid) {
    // this can happen in the most remotest possibility when a
    // filesystem is deleted and recreated with the same name.
    // since all this is driven asynchronously, we were able to
    // mount the recreated filesystem. so bubble up the error.
    // cleanup will eventually happen since a mirror disable event
    // would have been queued.
    derr << ": filesystem-id mismatch " << fs_id << " vs " << filesystem.fscid
         << dendl;
    // ignore errors, we are shutting down anyway.
    ceph_unmount(cmi);
    return -EINVAL;
  }

  dout(10) << ": mounted filesystem=" << filesystem << dendl;

  *mount = cmi;
  return 0;
}

} // namespace mirror
} // namespace cephfs
