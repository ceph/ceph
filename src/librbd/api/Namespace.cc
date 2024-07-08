// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/api/Mirror.h"
#include "librbd/api/Namespace.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Namespace: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

const std::list<std::string> POOL_OBJECTS {
  RBD_CHILDREN,
  RBD_GROUP_DIRECTORY,
  RBD_INFO,
  RBD_MIRRORING,
  RBD_TASK,
  RBD_TRASH,
  RBD_DIRECTORY
};

} // anonymous namespace

template <typename I>
int Namespace<I>::create(librados::IoCtx& io_ctx, const std::string& name)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 5) << "name=" << name << dendl;

  if (name.empty()) {
    return -EINVAL;
  }

  librados::Rados rados(io_ctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  if (require_osd_release < CEPH_RELEASE_NAUTILUS) {
    ldout(cct, 1) << "namespace support requires nautilus or later OSD"
                  << dendl;
    return -ENOSYS;
  }


  librados::IoCtx default_ns_ctx;
  default_ns_ctx.dup(io_ctx);
  default_ns_ctx.set_namespace("");

  r = cls_client::namespace_add(&default_ns_ctx, name);
  if (r < 0) {
    lderr(cct) << "failed to add namespace: " << cpp_strerror(r) << dendl;
    return r;
  }

  int ret_val;
  librados::IoCtx ns_ctx;
  ns_ctx.dup(io_ctx);
  ns_ctx.set_namespace(name);

  r = ns_ctx.create(RBD_TRASH, false);
  if (r < 0) {
    lderr(cct) << "failed to create trash: " << cpp_strerror(r) << dendl;
    goto remove_namespace;
  }

  r = cls_client::dir_state_set(&ns_ctx, RBD_DIRECTORY,
                                cls::rbd::DIRECTORY_STATE_READY);
  if (r < 0) {
    lderr(cct) << "failed to initialize image directory: " << cpp_strerror(r)
               << dendl;
    goto remove_dir_and_trash;
  }

  return 0;

remove_dir_and_trash:
  // dir_state_set method may fail before or after implicitly creating
  // rbd_directory object
  ret_val = ns_ctx.remove(RBD_DIRECTORY);
  if (ret_val < 0 && ret_val != -ENOENT) {
    lderr(cct) << "failed to remove image directory: " << cpp_strerror(ret_val)
               << dendl;
  }

  ret_val = ns_ctx.remove(RBD_TRASH);
  if (ret_val < 0) {
    lderr(cct) << "failed to remove trash: " << cpp_strerror(ret_val)
               << dendl;
  }

remove_namespace:
  ret_val = cls_client::namespace_remove(&default_ns_ctx, name);
  if (ret_val < 0) {
    lderr(cct) << "failed to remove namespace: " << cpp_strerror(ret_val)
               << dendl;
  }

  return r;
}

template <typename I>
int Namespace<I>::remove(librados::IoCtx& io_ctx, const std::string& name)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 5) << "name=" << name << dendl;

  if (name.empty()) {
    return -EINVAL;
  }

  librados::IoCtx default_ns_ctx;
  default_ns_ctx.dup(io_ctx);
  default_ns_ctx.set_namespace("");

  librados::IoCtx ns_ctx;
  ns_ctx.dup(io_ctx);
  ns_ctx.set_namespace(name);

  std::map<std::string, cls::rbd::TrashImageSpec> trash_entries;

  librados::ObjectWriteOperation dir_op;
  librbd::cls_client::dir_state_set(
    &dir_op, cls::rbd::DIRECTORY_STATE_ADD_DISABLED);
  dir_op.remove();

  int r = ns_ctx.operate(RBD_DIRECTORY, &dir_op);
  if (r == -EBUSY) {
    ldout(cct, 5) << "image directory not empty" << dendl;
    goto rollback;
  } else if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to disable the namespace: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  r = cls_client::trash_list(&ns_ctx, "", 1, &trash_entries);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to list trash directory: " << cpp_strerror(r)
               << dendl;
    return r;
  } else if (!trash_entries.empty()) {
    ldout(cct, 5) << "image trash not empty" << dendl;
    goto rollback;
  }

  r = Mirror<I>::mode_set(ns_ctx, RBD_MIRROR_MODE_DISABLED);
  if (r < 0) {
    lderr(cct) << "failed to disable mirroring: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  for (auto& oid : POOL_OBJECTS) {
    r = ns_ctx.remove(oid);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "failed to remove object '" << oid << "': "
                 << cpp_strerror(r) << dendl;
      return r;
    }
  }

  r = cls_client::namespace_remove(&default_ns_ctx, name);
  if (r < 0) {
    lderr(cct) << "failed to remove namespace: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;

rollback:

  r = librbd::cls_client::dir_state_set(
    &ns_ctx, RBD_DIRECTORY, cls::rbd::DIRECTORY_STATE_READY);
  if (r < 0) {
    lderr(cct) << "failed to restore directory state: " << cpp_strerror(r)
               << dendl;
  }

  return -EBUSY;
}

template <typename I>
int Namespace<I>::list(IoCtx& io_ctx, std::vector<std::string> *names)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 5) << dendl;

  librados::IoCtx default_ns_ctx;
  default_ns_ctx.dup(io_ctx);
  default_ns_ctx.set_namespace("");

  int r;
  int max_read = 1024;
  std::string last_read = "";
  do {
    std::list<std::string> name_list;
    r = cls_client::namespace_list(&default_ns_ctx, last_read, max_read,
                                   &name_list);
    if (r == -ENOENT) {
      return 0;
    } else if (r < 0) {
      lderr(cct) << "error listing namespaces: " << cpp_strerror(r) << dendl;
      return r;
    }

    names->insert(names->end(), name_list.begin(), name_list.end());
    if (!name_list.empty()) {
      last_read = name_list.back();
    }
    r = name_list.size();
  } while (r == max_read);

  return 0;
}

template <typename I>
int Namespace<I>::exists(librados::IoCtx& io_ctx, const std::string& name, bool *exists)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 5) << "name=" << name << dendl;

  *exists = false;
  if (name.empty()) {
    return -EINVAL;
  }

  librados::IoCtx ns_ctx;
  ns_ctx.dup(io_ctx);
  ns_ctx.set_namespace(name);

  int r = librbd::cls_client::dir_state_assert(&ns_ctx, RBD_DIRECTORY,
                                               cls::rbd::DIRECTORY_STATE_READY);
  if (r == 0) {
    *exists = true;
  } else if (r != -ENOENT) {
    lderr(cct) << "error asserting namespace: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Namespace<librbd::ImageCtx>;
