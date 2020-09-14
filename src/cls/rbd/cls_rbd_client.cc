// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_client.h"
#include "cls/lock/cls_lock_client.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/rbd_types.h"
#include "include/rados/librados.hpp"
#include "include/neorados/RADOS.hpp"
#include "common/bit_vector.hpp"

#include <errno.h>

namespace librbd {
namespace cls_client {

using std::map;
using std::set;
using std::string;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

void create_image(librados::ObjectWriteOperation *op, uint64_t size,
                  uint8_t order, uint64_t features,
                  const std::string &object_prefix, int64_t data_pool_id)
{
  bufferlist bl;
  encode(size, bl);
  encode(order, bl);
  encode(features, bl);
  encode(object_prefix, bl);
  encode(data_pool_id, bl);

  op->exec("rbd", "create", bl);
}

int create_image(librados::IoCtx *ioctx, const std::string &oid,
                 uint64_t size, uint8_t order, uint64_t features,
                 const std::string &object_prefix, int64_t data_pool_id)
{
  librados::ObjectWriteOperation op;
  create_image(&op, size, order, features, object_prefix, data_pool_id);

  return ioctx->operate(oid, &op);
}

void get_features_start(librados::ObjectReadOperation *op, bool read_only)
{
  bufferlist bl;
  encode(static_cast<uint64_t>(CEPH_NOSNAP), bl);
  encode(read_only, bl);
  op->exec("rbd", "get_features", bl);
}

int get_features_finish(bufferlist::const_iterator *it, uint64_t *features,
                        uint64_t *incompatible_features)
{
  try {
    decode(*features, *it);
    decode(*incompatible_features, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int get_features(librados::IoCtx *ioctx, const std::string &oid,
                 bool read_only, uint64_t *features,
                 uint64_t *incompatible_features)
{
  librados::ObjectReadOperation op;
  get_features_start(&op, read_only);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_features_finish(&it, features, incompatible_features);
}

void set_features(librados::ObjectWriteOperation *op, uint64_t features,
                  uint64_t mask)
{
  bufferlist bl;
  encode(features, bl);
  encode(mask, bl);

  op->exec("rbd", "set_features", bl);
}

int set_features(librados::IoCtx *ioctx, const std::string &oid,
                  uint64_t features, uint64_t mask)
{
  librados::ObjectWriteOperation op;
  set_features(&op, features, mask);

  return ioctx->operate(oid, &op);
}

void get_object_prefix_start(librados::ObjectReadOperation *op)
{
  bufferlist bl;
  op->exec("rbd", "get_object_prefix", bl);
}

int get_object_prefix_finish(bufferlist::const_iterator *it,
                             std::string *object_prefix)
{
  try {
    decode(*object_prefix, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
                      std::string *object_prefix)
{
  librados::ObjectReadOperation op;
  get_object_prefix_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_object_prefix_finish(&it, object_prefix);
}

void get_data_pool_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("rbd", "get_data_pool", bl);
}

int get_data_pool_finish(bufferlist::const_iterator *it, int64_t *data_pool_id) {
  try {
    decode(*data_pool_id, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_data_pool(librados::IoCtx *ioctx, const std::string &oid,
                  int64_t *data_pool_id) {
  librados::ObjectReadOperation op;
  get_data_pool_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_data_pool_finish(&it, data_pool_id);
}

void get_size_start(librados::ObjectReadOperation *op, snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "get_size", bl);
}

int get_size_finish(bufferlist::const_iterator *it, uint64_t *size,
                    uint8_t *order)
{
  try {
    decode(*order, *it);
    decode(*size, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_size(librados::IoCtx *ioctx, const std::string &oid,
             snapid_t snap_id, uint64_t *size, uint8_t *order)
{
  librados::ObjectReadOperation op;
  get_size_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_size_finish(&it, size, order);
}

int set_size(librados::IoCtx *ioctx, const std::string &oid,
             uint64_t size)
{
  librados::ObjectWriteOperation op;
  set_size(&op, size);
  return ioctx->operate(oid, &op);
}

void set_size(librados::ObjectWriteOperation *op, uint64_t size)
{
  bufferlist bl;
  encode(size, bl);
  op->exec("rbd", "set_size", bl);
}

void get_flags_start(librados::ObjectReadOperation *op, snapid_t snap_id) {
  bufferlist in_bl;
  encode(static_cast<snapid_t>(snap_id), in_bl);
  op->exec("rbd", "get_flags", in_bl);
}

int get_flags_finish(bufferlist::const_iterator *it, uint64_t *flags) {
  try {
    decode(*flags, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_flags(librados::IoCtx *ioctx, const std::string &oid,
              snapid_t snap_id, uint64_t *flags)
{
  librados::ObjectReadOperation op;
  get_flags_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_flags_finish(&it, flags);
}

void set_flags(librados::ObjectWriteOperation *op, snapid_t snap_id,
               uint64_t flags, uint64_t mask)
{
  bufferlist inbl;
  encode(flags, inbl);
  encode(mask, inbl);
  encode(snap_id, inbl);
  op->exec("rbd", "set_flags", inbl);
}

void op_features_get_start(librados::ObjectReadOperation *op)
{
  bufferlist in_bl;
  op->exec("rbd", "op_features_get", in_bl);
}

int op_features_get_finish(bufferlist::const_iterator *it, uint64_t *op_features)
{
  try {
    decode(*op_features, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int op_features_get(librados::IoCtx *ioctx, const std::string &oid,
                    uint64_t *op_features)
{
  librados::ObjectReadOperation op;
  op_features_get_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return op_features_get_finish(&it, op_features);
}

void op_features_set(librados::ObjectWriteOperation *op,
                     uint64_t op_features, uint64_t mask)
{
  bufferlist inbl;
  encode(op_features, inbl);
  encode(mask, inbl);
  op->exec("rbd", "op_features_set", inbl);
}

int op_features_set(librados::IoCtx *ioctx, const std::string &oid,
                    uint64_t op_features, uint64_t mask)
{
  librados::ObjectWriteOperation op;
  op_features_set(&op, op_features, mask);

  return ioctx->operate(oid, &op);
}

void get_parent_start(librados::ObjectReadOperation *op, snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "get_parent", bl);
}

int get_parent_finish(bufferlist::const_iterator *it,
                      cls::rbd::ParentImageSpec *pspec,
                      uint64_t *parent_overlap)
{
  *pspec = {};
  try {
    decode(pspec->pool_id, *it);
    decode(pspec->image_id, *it);
    decode(pspec->snap_id, *it);
    decode(*parent_overlap, *it);
  } catch (const ceph::buffer::error &) {
    return -EBADMSG;
  }
  return 0;
}

int get_parent(librados::IoCtx *ioctx, const std::string &oid,
               snapid_t snap_id, cls::rbd::ParentImageSpec *pspec,
               uint64_t *parent_overlap)
{
  librados::ObjectReadOperation op;
  get_parent_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_parent_finish(&it, pspec, parent_overlap);
}

int set_parent(librados::IoCtx *ioctx, const std::string &oid,
               const cls::rbd::ParentImageSpec &pspec, uint64_t parent_overlap)
{
  librados::ObjectWriteOperation op;
  set_parent(&op, pspec, parent_overlap);
  return ioctx->operate(oid, &op);
}

void set_parent(librados::ObjectWriteOperation *op,
                const cls::rbd::ParentImageSpec &pspec,
                uint64_t parent_overlap) {
  assert(pspec.pool_namespace.empty());

  bufferlist in_bl;
  encode(pspec.pool_id, in_bl);
  encode(pspec.image_id, in_bl);
  encode(pspec.snap_id, in_bl);
  encode(parent_overlap, in_bl);

  op->exec("rbd", "set_parent", in_bl);
}

int remove_parent(librados::IoCtx *ioctx, const std::string &oid)
{
  librados::ObjectWriteOperation op;
  remove_parent(&op);
  return ioctx->operate(oid, &op);
}

void remove_parent(librados::ObjectWriteOperation *op)
{
  bufferlist inbl;
  op->exec("rbd", "remove_parent", inbl);
}

void parent_get_start(librados::ObjectReadOperation* op) {
  bufferlist in_bl;
  op->exec("rbd", "parent_get", in_bl);
}

int parent_get_finish(bufferlist::const_iterator* it,
                      cls::rbd::ParentImageSpec* parent_image_spec) {
  try {
    decode(*parent_image_spec, *it);
  } catch (const ceph::buffer::error &) {
    return -EBADMSG;
  }
  return 0;
}

int parent_get(librados::IoCtx* ioctx, const std::string &oid,
               cls::rbd::ParentImageSpec* parent_image_spec) {
  librados::ObjectReadOperation op;
  parent_get_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = parent_get_finish(&it, parent_image_spec);
  if (r < 0) {
    return r;
  }
  return 0;
}

void parent_overlap_get_start(librados::ObjectReadOperation* op,
                              snapid_t snap_id) {
  bufferlist in_bl;
  encode(snap_id, in_bl);
  op->exec("rbd", "parent_overlap_get", in_bl);
}

int parent_overlap_get_finish(bufferlist::const_iterator* it,
                              std::optional<uint64_t>* parent_overlap) {
  try {
    decode(*parent_overlap, *it);
  } catch (const ceph::buffer::error &) {
    return -EBADMSG;
  }
  return 0;
}

int parent_overlap_get(librados::IoCtx* ioctx, const std::string &oid,
                       snapid_t snap_id,
                       std::optional<uint64_t>* parent_overlap) {
  librados::ObjectReadOperation op;
  parent_overlap_get_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = parent_overlap_get_finish(&it, parent_overlap);
  if (r < 0) {
    return r;
  }
  return 0;
}

void parent_attach(librados::ObjectWriteOperation* op,
                   const cls::rbd::ParentImageSpec& parent_image_spec,
                   uint64_t parent_overlap, bool reattach) {
  bufferlist in_bl;
  encode(parent_image_spec, in_bl);
  encode(parent_overlap, in_bl);
  encode(reattach, in_bl);
  op->exec("rbd", "parent_attach", in_bl);
}

int parent_attach(librados::IoCtx *ioctx, const std::string &oid,
                  const cls::rbd::ParentImageSpec& parent_image_spec,
                  uint64_t parent_overlap, bool reattach) {
  librados::ObjectWriteOperation op;
  parent_attach(&op, parent_image_spec, parent_overlap, reattach);
  return ioctx->operate(oid, &op);
}

void parent_detach(librados::ObjectWriteOperation* op) {
  bufferlist in_bl;
  op->exec("rbd", "parent_detach", in_bl);
}

int parent_detach(librados::IoCtx *ioctx, const std::string &oid) {
  librados::ObjectWriteOperation op;
  parent_detach(&op);
  return ioctx->operate(oid, &op);
}

int add_child(librados::IoCtx *ioctx, const std::string &oid,
              const cls::rbd::ParentImageSpec &pspec,
              const std::string &c_imageid)
{
  librados::ObjectWriteOperation op;
  add_child(&op, pspec, c_imageid);
  return ioctx->operate(oid, &op);
}

void add_child(librados::ObjectWriteOperation *op,
               const cls::rbd::ParentImageSpec& pspec,
               const std::string &c_imageid)
{
  assert(pspec.pool_namespace.empty());

  bufferlist in;
  encode(pspec.pool_id, in);
  encode(pspec.image_id, in);
  encode(pspec.snap_id, in);
  encode(c_imageid, in);

  op->exec("rbd", "add_child", in);
}

void remove_child(librados::ObjectWriteOperation *op,
                  const cls::rbd::ParentImageSpec &pspec,
                  const std::string &c_imageid)
{
  assert(pspec.pool_namespace.empty());

  bufferlist in;
  encode(pspec.pool_id, in);
  encode(pspec.image_id, in);
  encode(pspec.snap_id, in);
  encode(c_imageid, in);
  op->exec("rbd", "remove_child", in);
}

int remove_child(librados::IoCtx *ioctx, const std::string &oid,
                 const cls::rbd::ParentImageSpec &pspec,
                 const std::string &c_imageid)
{
  librados::ObjectWriteOperation op;
  remove_child(&op, pspec, c_imageid);
  return ioctx->operate(oid, &op);
}

void get_children_start(librados::ObjectReadOperation *op,
                        const cls::rbd::ParentImageSpec &pspec) {
  bufferlist in_bl;
  encode(pspec.pool_id, in_bl);
  encode(pspec.image_id, in_bl);
  encode(pspec.snap_id, in_bl);
  op->exec("rbd", "get_children", in_bl);
}

int get_children_finish(bufferlist::const_iterator *it,
                        std::set<std::string>* children) {
  try {
    decode(*children, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_children(librados::IoCtx *ioctx, const std::string &oid,
                 const cls::rbd::ParentImageSpec &pspec, set<string>& children)
{
  librados::ObjectReadOperation op;
  get_children_start(&op, pspec);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_children_finish(&it, &children);
}

void snapshot_get_start(librados::ObjectReadOperation *op, snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "snapshot_get", bl);
}

int snapshot_get_finish(bufferlist::const_iterator* it,
                        cls::rbd::SnapshotInfo* snap_info)
{
  try {
    decode(*snap_info, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int snapshot_get(librados::IoCtx *ioctx, const std::string &oid,
                 snapid_t snap_id, cls::rbd::SnapshotInfo* snap_info)
{
  librados::ObjectReadOperation op;
  snapshot_get_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return snapshot_get_finish(&it, snap_info);
}

void snapshot_add(librados::ObjectWriteOperation *op, snapid_t snap_id,
                  const std::string &snap_name,
                  const cls::rbd::SnapshotNamespace &snap_namespace)
{
  bufferlist bl;
  encode(snap_name, bl);
  encode(snap_id, bl);
  encode(snap_namespace, bl);
  op->exec("rbd", "snapshot_add", bl);
}

void snapshot_remove(librados::ObjectWriteOperation *op, snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "snapshot_remove", bl);
}

void snapshot_rename(librados::ObjectWriteOperation *op,
                     snapid_t src_snap_id,
                     const std::string &dst_name)
{
  bufferlist bl;
  encode(src_snap_id, bl);
  encode(dst_name, bl);
  op->exec("rbd", "snapshot_rename", bl);
}

void snapshot_trash_add(librados::ObjectWriteOperation *op,
                        snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "snapshot_trash_add", bl);
}

void get_snapcontext_start(librados::ObjectReadOperation *op)
{
  bufferlist bl;
  op->exec("rbd", "get_snapcontext", bl);
}

int get_snapcontext_finish(bufferlist::const_iterator *it,
                           ::SnapContext *snapc)
{
  try {
    decode(*snapc, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  if (!snapc->is_valid()) {
    return -EBADMSG;
  }
  return 0;
}

int get_snapcontext(librados::IoCtx *ioctx, const std::string &oid,
                    ::SnapContext *snapc)
{
  librados::ObjectReadOperation op;
  get_snapcontext_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto bl_it = out_bl.cbegin();
  return get_snapcontext_finish(&bl_it, snapc);
}

void get_snapshot_name_start(librados::ObjectReadOperation *op,
                             snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "get_snapshot_name", bl);
}

int get_snapshot_name_finish(bufferlist::const_iterator *it,
                             std::string *name)
{
  try {
    decode(*name, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_snapshot_name(librados::IoCtx *ioctx, const std::string &oid,
                      snapid_t snap_id, std::string *name)
{
  librados::ObjectReadOperation op;
  get_snapshot_name_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_snapshot_name_finish(&it, name);
}

void get_snapshot_timestamp_start(librados::ObjectReadOperation *op,
                                  snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "get_snapshot_timestamp", bl);
}

int get_snapshot_timestamp_finish(bufferlist::const_iterator *it,
                                  utime_t *timestamp)
{
  try {
    decode(*timestamp, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_snapshot_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                           snapid_t snap_id, utime_t *timestamp)
{
  librados::ObjectReadOperation op;
  get_snapshot_timestamp_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_snapshot_timestamp_finish(&it, timestamp);
}

void old_snapshot_add(librados::ObjectWriteOperation *op,
                      snapid_t snap_id, const std::string &snap_name)
{
  bufferlist bl;
  encode(snap_name, bl);
  encode(snap_id, bl);
  op->exec("rbd", "snap_add", bl);
}

void old_snapshot_remove(librados::ObjectWriteOperation *op,
                         const std::string &snap_name)
{
  bufferlist bl;
  encode(snap_name, bl);
  op->exec("rbd", "snap_remove", bl);
}

void old_snapshot_rename(librados::ObjectWriteOperation *op,
                         snapid_t src_snap_id, const std::string &dst_name)
{
  bufferlist bl;
  encode(src_snap_id, bl);
  encode(dst_name, bl);
  op->exec("rbd", "snap_rename", bl);
}

void old_snapshot_list_start(librados::ObjectReadOperation *op) {
  bufferlist in_bl;
  op->exec("rbd", "snap_list", in_bl);
}

int old_snapshot_list_finish(bufferlist::const_iterator *it,
                             std::vector<string> *names,
                             std::vector<uint64_t> *sizes,
                             ::SnapContext *snapc) {
  try {
    uint32_t num_snaps;
    decode(snapc->seq, *it);
    decode(num_snaps, *it);

    names->resize(num_snaps);
    sizes->resize(num_snaps);
    snapc->snaps.resize(num_snaps);
    for (uint32_t i = 0; i < num_snaps; ++i) {
      decode(snapc->snaps[i], *it);
      decode((*sizes)[i], *it);
      decode((*names)[i], *it);
    }
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int old_snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
                      std::vector<string> *names,
                      std::vector<uint64_t> *sizes,
                      ::SnapContext *snapc)
{
  librados::ObjectReadOperation op;
  old_snapshot_list_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return old_snapshot_list_finish(&it, names, sizes, snapc);
}

void get_all_features_start(librados::ObjectReadOperation *op) {
  bufferlist in;
  op->exec("rbd", "get_all_features", in);
}

int get_all_features_finish(bufferlist::const_iterator *it,
                            uint64_t *all_features) {
  try {
    decode(*all_features, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_all_features(librados::IoCtx *ioctx, const std::string &oid,
                     uint64_t *all_features) {
  librados::ObjectReadOperation op;
  get_all_features_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_all_features_finish(&it, all_features);
}

template <typename O>
void copyup(O* op, ceph::buffer::list data) {
  op->exec("rbd", "copyup", data);
}

void copyup(neorados::WriteOp* op, ceph::buffer::list data) {
  copyup<neorados::WriteOp>(op, data);
}

void copyup(librados::ObjectWriteOperation *op, bufferlist data) {
  copyup<librados::ObjectWriteOperation>(op, data);
}

int copyup(librados::IoCtx *ioctx, const std::string &oid,
           bufferlist data) {
  librados::ObjectWriteOperation op;
  copyup(&op, data);

  return ioctx->operate(oid, &op);
}

template <typename O, typename E>
void sparse_copyup(O* op, const E& extent_map, ceph::buffer::list data) {
  bufferlist bl;
  encode(extent_map, bl);
  encode(data, bl);
  op->exec("rbd", "sparse_copyup", bl);
}

void sparse_copyup(neorados::WriteOp* op,
                   const std::map<uint64_t, uint64_t> &extent_map,
                   ceph::buffer::list data) {
  sparse_copyup<neorados::WriteOp>(op, extent_map, data);
}

void sparse_copyup(librados::ObjectWriteOperation *op,
                   const std::map<uint64_t, uint64_t> &extent_map,
                   bufferlist data) {
  sparse_copyup<librados::ObjectWriteOperation>(op, extent_map, data);
}

int sparse_copyup(librados::IoCtx *ioctx, const std::string &oid,
                  const std::map<uint64_t, uint64_t> &extent_map,
                  bufferlist data) {
  librados::ObjectWriteOperation op;
  sparse_copyup(&op, extent_map, data);

  return ioctx->operate(oid, &op);
}

void get_protection_status_start(librados::ObjectReadOperation *op,
                                 snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "get_protection_status", bl);
}

int get_protection_status_finish(bufferlist::const_iterator *it,
                                 uint8_t *protection_status)
{
  try {
    decode(*protection_status, *it);
  } catch (const ceph::buffer::error &) {
    return -EBADMSG;
  }
  return 0;
}

int get_protection_status(librados::IoCtx *ioctx, const std::string &oid,
                          snapid_t snap_id, uint8_t *protection_status)
{
  librados::ObjectReadOperation op;
  get_protection_status_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_protection_status_finish(&it, protection_status);
}

int set_protection_status(librados::IoCtx *ioctx, const std::string &oid,
                          snapid_t snap_id, uint8_t protection_status)
{
  // TODO remove
  librados::ObjectWriteOperation op;
  set_protection_status(&op, snap_id, protection_status);
  return ioctx->operate(oid, &op);
}

void set_protection_status(librados::ObjectWriteOperation *op,
                           snapid_t snap_id, uint8_t protection_status)
{
  bufferlist in;
  encode(snap_id, in);
  encode(protection_status, in);
  op->exec("rbd", "set_protection_status", in);
}

void snapshot_get_limit_start(librados::ObjectReadOperation *op)
{
  bufferlist bl;
  op->exec("rbd", "snapshot_get_limit", bl);
}

int snapshot_get_limit_finish(bufferlist::const_iterator *it, uint64_t *limit)
{
  try {
    decode(*limit, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int snapshot_get_limit(librados::IoCtx *ioctx, const std::string &oid,
                       uint64_t *limit)
{
  librados::ObjectReadOperation op;
  snapshot_get_limit_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return snapshot_get_limit_finish(&it, limit);
}

void snapshot_set_limit(librados::ObjectWriteOperation *op, uint64_t limit)
{
  bufferlist in;
  encode(limit, in);
  op->exec("rbd", "snapshot_set_limit", in);
}

void get_stripe_unit_count_start(librados::ObjectReadOperation *op) {
  bufferlist empty_bl;
  op->exec("rbd", "get_stripe_unit_count", empty_bl);
}

int get_stripe_unit_count_finish(bufferlist::const_iterator *it,
                                 uint64_t *stripe_unit,
                                 uint64_t *stripe_count) {
  ceph_assert(stripe_unit);
  ceph_assert(stripe_count);

  try {
    decode(*stripe_unit, *it);
    decode(*stripe_count, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
                          uint64_t *stripe_unit, uint64_t *stripe_count)
{
  librados::ObjectReadOperation op;
  get_stripe_unit_count_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_stripe_unit_count_finish(&it, stripe_unit, stripe_count);
}

void set_stripe_unit_count(librados::ObjectWriteOperation *op,
                           uint64_t stripe_unit, uint64_t stripe_count)
{
  bufferlist bl;
  encode(stripe_unit, bl);
  encode(stripe_count, bl);

  op->exec("rbd", "set_stripe_unit_count", bl);
}

int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
                          uint64_t stripe_unit, uint64_t stripe_count)
{
  librados::ObjectWriteOperation op;
  set_stripe_unit_count(&op, stripe_unit, stripe_count);

  return ioctx->operate(oid, &op);
}

void get_create_timestamp_start(librados::ObjectReadOperation *op) {
  bufferlist empty_bl;
  op->exec("rbd", "get_create_timestamp", empty_bl);
}

int get_create_timestamp_finish(bufferlist::const_iterator *it,
                                utime_t *timestamp) {
  ceph_assert(timestamp);

  try {
    decode(*timestamp, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_create_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                         utime_t *timestamp)
{
  librados::ObjectReadOperation op;
  get_create_timestamp_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_create_timestamp_finish(&it, timestamp);
}

void get_access_timestamp_start(librados::ObjectReadOperation *op) {
  bufferlist empty_bl;
  op->exec("rbd", "get_access_timestamp", empty_bl);
}

int get_access_timestamp_finish(bufferlist::const_iterator *it,
                                utime_t *timestamp) {
  ceph_assert(timestamp);

  try {
    decode(*timestamp, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_access_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                         utime_t *timestamp)
{
  librados::ObjectReadOperation op;
  get_access_timestamp_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_access_timestamp_finish(&it, timestamp);
}

void set_access_timestamp(librados::ObjectWriteOperation *op)
{
    bufferlist empty_bl;
    op->exec("rbd","set_access_timestamp",empty_bl);
}

int set_access_timestamp(librados::IoCtx *ioctx, const std::string &oid)
{
    librados::ObjectWriteOperation op;
    set_access_timestamp(&op);
    return ioctx->operate(oid, &op);
}

void get_modify_timestamp_start(librados::ObjectReadOperation *op) {
  bufferlist empty_bl;
  op->exec("rbd", "get_modify_timestamp", empty_bl);
}

int get_modify_timestamp_finish(bufferlist::const_iterator *it,
                                  utime_t *timestamp) {
  ceph_assert(timestamp);

  try {
    decode(*timestamp, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_modify_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                           utime_t *timestamp)
{
  librados::ObjectReadOperation op;
  get_modify_timestamp_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_modify_timestamp_finish(&it, timestamp);
}

void set_modify_timestamp(librados::ObjectWriteOperation *op)
{
    bufferlist empty_bl;
    op->exec("rbd","set_modify_timestamp",empty_bl);
}

int set_modify_timestamp(librados::IoCtx *ioctx, const std::string &oid)
{
    librados::ObjectWriteOperation op;
    set_modify_timestamp(&op);
    return ioctx->operate(oid, &op);
}


/************************ rbd_id object methods ************************/

void get_id_start(librados::ObjectReadOperation *op) {
  bufferlist empty_bl;
  op->exec("rbd", "get_id", empty_bl);
}

int get_id_finish(bufferlist::const_iterator *it, std::string *id) {
  try {
    decode(*id, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id)
{
  librados::ObjectReadOperation op;
  get_id_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return get_id_finish(&it, id);
}

void set_id(librados::ObjectWriteOperation *op, const std::string &id)
{
  bufferlist bl;
  encode(id, bl);
  op->exec("rbd", "set_id", bl);
}

int set_id(librados::IoCtx *ioctx, const std::string &oid, const std::string &id)
{
  librados::ObjectWriteOperation op;
  set_id(&op, id);

  return ioctx->operate(oid, &op);
}

/******************** rbd_directory object methods ********************/

void dir_get_id_start(librados::ObjectReadOperation *op,
                      const std::string &image_name) {
  bufferlist bl;
  encode(image_name, bl);

  op->exec("rbd", "dir_get_id", bl);
}

int dir_get_id_finish(bufferlist::const_iterator *iter, std::string *image_id) {
  try {
    decode(*image_id, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
               const std::string &name, std::string *id) {
  librados::ObjectReadOperation op;
  dir_get_id_start(&op, name);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  return dir_get_id_finish(&iter, id);
}

void dir_get_name_start(librados::ObjectReadOperation *op,
                        const std::string &id) {
  bufferlist in_bl;
  encode(id, in_bl);
  op->exec("rbd", "dir_get_name", in_bl);
}

int dir_get_name_finish(bufferlist::const_iterator *it, std::string *name) {
  try {
    decode(*name, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
                 const std::string &id, std::string *name) {
  librados::ObjectReadOperation op;
  dir_get_name_start(&op, id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return dir_get_name_finish(&it, name);
}

void dir_list_start(librados::ObjectReadOperation *op,
                    const std::string &start, uint64_t max_return)
{
  bufferlist in_bl;
  encode(start, in_bl);
  encode(max_return, in_bl);

  op->exec("rbd", "dir_list", in_bl);
}

int dir_list_finish(bufferlist::const_iterator *it, map<string, string> *images)
{
  try {
    decode(*images, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int dir_list(librados::IoCtx *ioctx, const std::string &oid,
             const std::string &start, uint64_t max_return,
             map<string, string> *images)
{
  librados::ObjectReadOperation op;
  dir_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  return dir_list_finish(&iter, images);
}

void dir_add_image(librados::ObjectWriteOperation *op,
                   const std::string &name, const std::string &id)
{
  bufferlist bl;
  encode(name, bl);
  encode(id, bl);
  op->exec("rbd", "dir_add_image", bl);
}

int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
                  const std::string &name, const std::string &id)
{
  librados::ObjectWriteOperation op;
  dir_add_image(&op, name, id);

  return ioctx->operate(oid, &op);
}

int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &name, const std::string &id)
{
  librados::ObjectWriteOperation op;
  dir_remove_image(&op, name, id);

  return ioctx->operate(oid, &op);
}

void dir_state_assert(librados::ObjectOperation *op,
                      cls::rbd::DirectoryState directory_state)
{
  bufferlist bl;
  encode(directory_state, bl);
  op->exec("rbd", "dir_state_assert", bl);
}

int dir_state_assert(librados::IoCtx *ioctx, const std::string &oid,
                     cls::rbd::DirectoryState directory_state)
{
  librados::ObjectWriteOperation op;
  dir_state_assert(&op, directory_state);

  return ioctx->operate(oid, &op);
}

void dir_state_set(librados::ObjectWriteOperation *op,
                   cls::rbd::DirectoryState directory_state)
{
  bufferlist bl;
  encode(directory_state, bl);
  op->exec("rbd", "dir_state_set", bl);
}

int dir_state_set(librados::IoCtx *ioctx, const std::string &oid,
                  cls::rbd::DirectoryState directory_state)
{
  librados::ObjectWriteOperation op;
  dir_state_set(&op, directory_state);

  return ioctx->operate(oid, &op);
}

void dir_remove_image(librados::ObjectWriteOperation *op,
                      const std::string &name, const std::string &id)
{
  bufferlist bl;
  encode(name, bl);
  encode(id, bl);

  op->exec("rbd", "dir_remove_image", bl);
}

void dir_rename_image(librados::ObjectWriteOperation *op,
                      const std::string &src, const std::string &dest,
                      const std::string &id)
{
  bufferlist in;
  encode(src, in);
  encode(dest, in);
  encode(id, in);
  op->exec("rbd", "dir_rename_image", in);
}

void object_map_load_start(librados::ObjectReadOperation *op) {
  bufferlist in_bl;
  op->exec("rbd", "object_map_load", in_bl);
}

int object_map_load_finish(bufferlist::const_iterator *it,
                           ceph::BitVector<2> *object_map) {
  try {
    decode(*object_map, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int object_map_load(librados::IoCtx *ioctx, const std::string &oid,
                    ceph::BitVector<2> *object_map)
{
  librados::ObjectReadOperation op;
  object_map_load_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return object_map_load_finish(&it, object_map);
}

void object_map_save(librados::ObjectWriteOperation *rados_op,
                     const ceph::BitVector<2> &object_map)
{
  ceph::BitVector<2> object_map_copy(object_map);
  object_map_copy.set_crc_enabled(false);

  bufferlist in;
  encode(object_map_copy, in);
  rados_op->exec("rbd", "object_map_save", in);
}

void object_map_resize(librados::ObjectWriteOperation *rados_op,
                       uint64_t object_count, uint8_t default_state)
{
  bufferlist in;
  encode(object_count, in);
  encode(default_state, in);
  rados_op->exec("rbd", "object_map_resize", in);
}

void object_map_update(librados::ObjectWriteOperation *rados_op,
                       uint64_t start_object_no, uint64_t end_object_no,
                       uint8_t new_object_state,
                       const boost::optional<uint8_t> &current_object_state)
{
  bufferlist in;
  encode(start_object_no, in);
  encode(end_object_no, in);
  encode(new_object_state, in);
  encode(current_object_state, in);
  rados_op->exec("rbd", "object_map_update", in);
}

void object_map_snap_add(librados::ObjectWriteOperation *rados_op)
{
  bufferlist in;
  rados_op->exec("rbd", "object_map_snap_add", in);
}

void object_map_snap_remove(librados::ObjectWriteOperation *rados_op,
                            const ceph::BitVector<2> &object_map)
{
  ceph::BitVector<2> object_map_copy(object_map);
  object_map_copy.set_crc_enabled(false);

  bufferlist in;
  encode(object_map_copy, in);
  rados_op->exec("rbd", "object_map_snap_remove", in);
}

void metadata_set(librados::ObjectWriteOperation *op,
                  const map<string, bufferlist> &data)
{
  bufferlist bl;
  encode(data, bl);

  op->exec("rbd", "metadata_set", bl);
}

int metadata_set(librados::IoCtx *ioctx, const std::string &oid,
                 const map<string, bufferlist> &data)
{
  librados::ObjectWriteOperation op;
  metadata_set(&op, data);

  return ioctx->operate(oid, &op);
}

void metadata_remove(librados::ObjectWriteOperation *op,
                     const std::string &key)
{
  bufferlist bl;
  encode(key, bl);

  op->exec("rbd", "metadata_remove", bl);
}

int metadata_remove(librados::IoCtx *ioctx, const std::string &oid,
                    const std::string &key)
{
  librados::ObjectWriteOperation op;
  metadata_remove(&op, key);

  return ioctx->operate(oid, &op);
}

int metadata_list(librados::IoCtx *ioctx, const std::string &oid,
                  const std::string &start, uint64_t max_return,
                  map<string, bufferlist> *pairs)
{
  librados::ObjectReadOperation op;
  metadata_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return metadata_list_finish(&it, pairs);
}

void metadata_list_start(librados::ObjectReadOperation *op,
                         const std::string &start, uint64_t max_return)
{
  bufferlist in_bl;
  encode(start, in_bl);
  encode(max_return, in_bl);
  op->exec("rbd", "metadata_list", in_bl);
}

int metadata_list_finish(bufferlist::const_iterator *it,
                         std::map<std::string, bufferlist> *pairs)
{
  ceph_assert(pairs);
  try {
    decode(*pairs, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

void metadata_get_start(librados::ObjectReadOperation* op,
                 const std::string &key) {
  bufferlist bl;
  encode(key, bl);

  op->exec("rbd", "metadata_get", bl);
}

int metadata_get_finish(bufferlist::const_iterator *it,
                         std::string* value) {
  try {
    decode(*value, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int metadata_get(librados::IoCtx *ioctx, const std::string &oid,
                 const std::string &key, string *s)
{
  ceph_assert(s);
  librados::ObjectReadOperation op;
  metadata_get_start(&op, key);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = metadata_get_finish(&it, s);
  if (r < 0) {
    return r;
  }
  return 0;
}

void child_attach(librados::ObjectWriteOperation *op, snapid_t snap_id,
                  const cls::rbd::ChildImageSpec& child_image)
{
  bufferlist bl;
  encode(snap_id, bl);
  encode(child_image, bl);
  op->exec("rbd", "child_attach", bl);
}

int child_attach(librados::IoCtx *ioctx, const std::string &oid,
                 snapid_t snap_id,
                 const cls::rbd::ChildImageSpec& child_image)
{
  librados::ObjectWriteOperation op;
  child_attach(&op, snap_id, child_image);

  int r = ioctx->operate(oid, &op);
  if (r < 0) {
    return r;
  }
  return 0;
}

void child_detach(librados::ObjectWriteOperation *op, snapid_t snap_id,
                  const cls::rbd::ChildImageSpec& child_image)
{
  bufferlist bl;
  encode(snap_id, bl);
  encode(child_image, bl);
  op->exec("rbd", "child_detach", bl);
}

int child_detach(librados::IoCtx *ioctx, const std::string &oid,
                 snapid_t snap_id,
                 const cls::rbd::ChildImageSpec& child_image)
{
  librados::ObjectWriteOperation op;
  child_detach(&op, snap_id, child_image);

  int r = ioctx->operate(oid, &op);
  if (r < 0) {
    return r;
  }
  return 0;
}

void children_list_start(librados::ObjectReadOperation *op,
                         snapid_t snap_id)
{
  bufferlist bl;
  encode(snap_id, bl);
  op->exec("rbd", "children_list", bl);
}

int children_list_finish(bufferlist::const_iterator *it,
                         cls::rbd::ChildImageSpecs *child_images)
{
  child_images->clear();
  try {
    decode(*child_images, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int children_list(librados::IoCtx *ioctx, const std::string &oid,
                  snapid_t snap_id,
                  cls::rbd::ChildImageSpecs *child_images)
{
  librados::ObjectReadOperation op;
  children_list_start(&op, snap_id);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = children_list_finish(&it, child_images);
  if (r < 0) {
    return r;
  }
  return 0;
}

int migration_set(librados::IoCtx *ioctx, const std::string &oid,
                  const cls::rbd::MigrationSpec &migration_spec) {
  librados::ObjectWriteOperation op;
  migration_set(&op, migration_spec);
  return ioctx->operate(oid, &op);
}

void migration_set(librados::ObjectWriteOperation *op,
                   const cls::rbd::MigrationSpec &migration_spec) {
  bufferlist bl;
  encode(migration_spec, bl);
  op->exec("rbd", "migration_set", bl);
}

int migration_set_state(librados::IoCtx *ioctx, const std::string &oid,
                        cls::rbd::MigrationState state,
                        const std::string &description) {
  librados::ObjectWriteOperation op;
  migration_set_state(&op, state, description);
  return ioctx->operate(oid, &op);
}

void migration_set_state(librados::ObjectWriteOperation *op,
                         cls::rbd::MigrationState state,
                         const std::string &description) {
  bufferlist bl;
  encode(state, bl);
  encode(description, bl);
  op->exec("rbd", "migration_set_state", bl);
}

void migration_get_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("rbd", "migration_get", bl);
}

int migration_get_finish(bufferlist::const_iterator *it,
                         cls::rbd::MigrationSpec *migration_spec) {
  try {
    decode(*migration_spec, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int migration_get(librados::IoCtx *ioctx, const std::string &oid,
                  cls::rbd::MigrationSpec *migration_spec) {
  librados::ObjectReadOperation op;
  migration_get_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = migration_get_finish(&iter, migration_spec);
  if (r < 0) {
    return r;
  }
  return 0;
}

int migration_remove(librados::IoCtx *ioctx, const std::string &oid) {
  librados::ObjectWriteOperation op;
  migration_remove(&op);
  return ioctx->operate(oid, &op);
}

void migration_remove(librados::ObjectWriteOperation *op) {
  bufferlist bl;
  op->exec("rbd", "migration_remove", bl);
}

template <typename O>
void assert_snapc_seq(O* op, uint64_t snapc_seq,
                      cls::rbd::AssertSnapcSeqState state) {
  bufferlist bl;
  encode(snapc_seq, bl);
  encode(state, bl);
  op->exec("rbd", "assert_snapc_seq", bl);
}

void assert_snapc_seq(neorados::WriteOp* op,
                      uint64_t snapc_seq,
                      cls::rbd::AssertSnapcSeqState state) {
  assert_snapc_seq<neorados::WriteOp>(op, snapc_seq, state);
}

void assert_snapc_seq(librados::ObjectWriteOperation *op,
                      uint64_t snapc_seq,
                      cls::rbd::AssertSnapcSeqState state) {
  assert_snapc_seq<librados::ObjectWriteOperation>(op, snapc_seq, state);
}

int assert_snapc_seq(librados::IoCtx *ioctx, const std::string &oid,
                     uint64_t snapc_seq,
                     cls::rbd::AssertSnapcSeqState state) {
  librados::ObjectWriteOperation op;
  assert_snapc_seq(&op, snapc_seq, state);
  return ioctx->operate(oid, &op);
}

void mirror_uuid_get_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("rbd", "mirror_uuid_get", bl);
}

int mirror_uuid_get_finish(bufferlist::const_iterator *it,
                           std::string *uuid) {
  try {
    decode(*uuid, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_uuid_get(librados::IoCtx *ioctx, std::string *uuid) {
  librados::ObjectReadOperation op;
  mirror_uuid_get_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = mirror_uuid_get_finish(&it, uuid);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_uuid_set(librados::IoCtx *ioctx, const std::string &uuid) {
  bufferlist in_bl;
  encode(uuid, in_bl);

  bufferlist out_bl;
  int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_uuid_set", in_bl,
                      out_bl);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_mode_get_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("rbd", "mirror_mode_get", bl);
}

int mirror_mode_get_finish(bufferlist::const_iterator *it,
                           cls::rbd::MirrorMode *mirror_mode) {
  try {
    uint32_t mirror_mode_decode;
    decode(mirror_mode_decode, *it);
    *mirror_mode = static_cast<cls::rbd::MirrorMode>(mirror_mode_decode);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int mirror_mode_get(librados::IoCtx *ioctx,
                    cls::rbd::MirrorMode *mirror_mode) {
  librados::ObjectReadOperation op;
  mirror_mode_get_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r == -ENOENT) {
    *mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
    return 0;
  } else if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = mirror_mode_get_finish(&it, mirror_mode);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_mode_set(librados::IoCtx *ioctx,
                    cls::rbd::MirrorMode mirror_mode) {
  bufferlist in_bl;
  encode(static_cast<uint32_t>(mirror_mode), in_bl);

  bufferlist out_bl;
  int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_mode_set", in_bl,
                      out_bl);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_peer_list_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("rbd", "mirror_peer_list", bl);
}

int mirror_peer_list_finish(bufferlist::const_iterator *it,
                            std::vector<cls::rbd::MirrorPeer> *peers) {
  peers->clear();
  try {
    decode(*peers, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_peer_list(librados::IoCtx *ioctx,
                     std::vector<cls::rbd::MirrorPeer> *peers) {
  librados::ObjectReadOperation op;
  mirror_peer_list_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  r = mirror_peer_list_finish(&it, peers);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_peer_ping(librados::IoCtx *ioctx,
                     const std::string& site_name,
                     const std::string& fsid) {
  librados::ObjectWriteOperation op;
  mirror_peer_ping(&op, site_name, fsid);

  int r = ioctx->operate(RBD_MIRRORING, &op);
  if (r < 0) {
    return r;
  }

  return 0;
}

void mirror_peer_ping(librados::ObjectWriteOperation *op,
                      const std::string& site_name,
                      const std::string& fsid) {
  bufferlist in_bl;
  encode(site_name, in_bl);
  encode(fsid, in_bl);
  encode(static_cast<uint8_t>(cls::rbd::MIRROR_PEER_DIRECTION_TX), in_bl);

  op->exec("rbd", "mirror_peer_ping", in_bl);
}

int mirror_peer_add(librados::IoCtx *ioctx,
                    const cls::rbd::MirrorPeer& mirror_peer) {
  librados::ObjectWriteOperation op;
  mirror_peer_add(&op, mirror_peer);

  int r = ioctx->operate(RBD_MIRRORING, &op);
  if (r < 0) {
    return r;
  }

  return 0;
}

void mirror_peer_add(librados::ObjectWriteOperation *op,
                     const cls::rbd::MirrorPeer& mirror_peer) {
  bufferlist in_bl;
  encode(mirror_peer, in_bl);

  op->exec("rbd", "mirror_peer_add", in_bl);
}

int mirror_peer_remove(librados::IoCtx *ioctx,
                       const std::string &uuid) {
  bufferlist in_bl;
  encode(uuid, in_bl);

  bufferlist out_bl;
  int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_remove", in_bl,
                      out_bl);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_peer_set_client(librados::IoCtx *ioctx,
                           const std::string &uuid,
                           const std::string &client_name) {
  bufferlist in_bl;
  encode(uuid, in_bl);
  encode(client_name, in_bl);

  bufferlist out_bl;
  int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_set_client",
                      in_bl, out_bl);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_peer_set_cluster(librados::IoCtx *ioctx,
                            const std::string &uuid,
                            const std::string &cluster_name) {
  bufferlist in_bl;
  encode(uuid, in_bl);
  encode(cluster_name, in_bl);

  bufferlist out_bl;
  int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_set_cluster",
                      in_bl, out_bl);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_peer_set_direction(
    librados::IoCtx *ioctx, const std::string &uuid,
    cls::rbd::MirrorPeerDirection mirror_peer_direction) {
  bufferlist in_bl;
  encode(uuid, in_bl);
  encode(static_cast<uint8_t>(mirror_peer_direction), in_bl);

  bufferlist out_bl;
  int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_set_direction",
                      in_bl, out_bl);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_list_start(librados::ObjectReadOperation *op,
                             const std::string &start, uint64_t max_return)
{
  bufferlist in_bl;
  encode(start, in_bl);
  encode(max_return, in_bl);
  op->exec("rbd", "mirror_image_list", in_bl);
}

int mirror_image_list_finish(bufferlist::const_iterator *it,
                             std::map<string, string> *mirror_image_ids)
{
  try {
    decode(*mirror_image_ids, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_list(librados::IoCtx *ioctx,
                      const std::string &start, uint64_t max_return,
                      std::map<std::string, std::string> *mirror_image_ids) {
  librados::ObjectReadOperation op;
  mirror_image_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto bl_it = out_bl.cbegin();
  return mirror_image_list_finish(&bl_it, mirror_image_ids);
}

void mirror_image_get_image_id_start(librados::ObjectReadOperation *op,
                                     const std::string &global_image_id) {
  bufferlist in_bl;
  encode(global_image_id, in_bl);
  op->exec( "rbd", "mirror_image_get_image_id", in_bl);
}

int mirror_image_get_image_id_finish(bufferlist::const_iterator *it,
                                     std::string *image_id) {
  try {
    decode(*image_id, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_get_image_id(librados::IoCtx *ioctx,
                              const std::string &global_image_id,
                              std::string *image_id) {
  librados::ObjectReadOperation op;
  mirror_image_get_image_id_start(&op, global_image_id);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return mirror_image_get_image_id_finish(&it, image_id);
}

int mirror_image_get(librados::IoCtx *ioctx, const std::string &image_id,
                     cls::rbd::MirrorImage *mirror_image) {
  librados::ObjectReadOperation op;
  mirror_image_get_start(&op, image_id);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_image_get_finish(&iter, mirror_image);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_get_start(librados::ObjectReadOperation *op,
                            const std::string &image_id) {
  bufferlist in_bl;
  encode(image_id, in_bl);

  op->exec("rbd", "mirror_image_get", in_bl);
}

int mirror_image_get_finish(bufferlist::const_iterator *iter,
                            cls::rbd::MirrorImage *mirror_image) {
  try {
    decode(*mirror_image, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

void mirror_image_set(librados::ObjectWriteOperation *op,
                      const std::string &image_id,
                      const cls::rbd::MirrorImage &mirror_image) {
  bufferlist bl;
  encode(image_id, bl);
  encode(mirror_image, bl);

  op->exec("rbd", "mirror_image_set", bl);
}

int mirror_image_set(librados::IoCtx *ioctx, const std::string &image_id,
                     const cls::rbd::MirrorImage &mirror_image) {
  librados::ObjectWriteOperation op;
  mirror_image_set(&op, image_id, mirror_image);

  int r = ioctx->operate(RBD_MIRRORING, &op);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_remove(librados::ObjectWriteOperation *op,
                         const std::string &image_id) {
  bufferlist bl;
  encode(image_id, bl);

  op->exec("rbd", "mirror_image_remove", bl);
}

int mirror_image_remove(librados::IoCtx *ioctx, const std::string &image_id) {
  librados::ObjectWriteOperation op;
  mirror_image_remove(&op, image_id);

  int r = ioctx->operate(RBD_MIRRORING, &op);
  if (r < 0) {
    return r;
  }
  return 0;
}

int mirror_image_status_set(librados::IoCtx *ioctx,
                            const std::string &global_image_id,
                            const cls::rbd::MirrorImageSiteStatus &status) {
  librados::ObjectWriteOperation op;
  mirror_image_status_set(&op, global_image_id, status);
  return ioctx->operate(RBD_MIRRORING, &op);
}

void mirror_image_status_set(librados::ObjectWriteOperation *op,
                             const std::string &global_image_id,
                             const cls::rbd::MirrorImageSiteStatus &status) {
  bufferlist bl;
  encode(global_image_id, bl);
  encode(status, bl);
  op->exec("rbd", "mirror_image_status_set", bl);
}

int mirror_image_status_get(librados::IoCtx *ioctx,
                            const std::string &global_image_id,
                            cls::rbd::MirrorImageStatus *status) {
  librados::ObjectReadOperation op;
  mirror_image_status_get_start(&op, global_image_id);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_image_status_get_finish(&iter, status);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_status_get_start(librados::ObjectReadOperation *op,
                                   const std::string &global_image_id) {
  bufferlist bl;
  encode(global_image_id, bl);
  op->exec("rbd", "mirror_image_status_get", bl);
}

int mirror_image_status_get_finish(bufferlist::const_iterator *iter,
                                   cls::rbd::MirrorImageStatus *status) {
  try {
    decode(*status, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_status_list(librados::IoCtx *ioctx,
                             const std::string &start, uint64_t max_return,
                             std::map<std::string, cls::rbd::MirrorImage> *images,
                             std::map<std::string, cls::rbd::MirrorImageStatus> *statuses) {
  librados::ObjectReadOperation op;
  mirror_image_status_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_image_status_list_finish(&iter, images, statuses);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_status_list_start(librados::ObjectReadOperation *op,
                                    const std::string &start,
                                    uint64_t max_return) {
  bufferlist bl;
  encode(start, bl);
  encode(max_return, bl);
  op->exec("rbd", "mirror_image_status_list", bl);
}

int mirror_image_status_list_finish(bufferlist::const_iterator *iter,
                                    std::map<std::string, cls::rbd::MirrorImage> *images,
                                    std::map<std::string, cls::rbd::MirrorImageStatus> *statuses) {
  images->clear();
  statuses->clear();
  try {
    decode(*images, *iter);
    decode(*statuses, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_status_get_summary(
    librados::IoCtx *ioctx,
    const std::vector<cls::rbd::MirrorPeer>& mirror_peer_sites,
    std::map<cls::rbd::MirrorImageStatusState, int32_t> *states) {
  librados::ObjectReadOperation op;
  mirror_image_status_get_summary_start(&op, mirror_peer_sites);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_image_status_get_summary_finish(&iter, states);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_status_get_summary_start(
    librados::ObjectReadOperation *op,
    const std::vector<cls::rbd::MirrorPeer>& mirror_peer_sites) {
  bufferlist bl;
  encode(mirror_peer_sites, bl);
  op->exec("rbd", "mirror_image_status_get_summary", bl);
}

int mirror_image_status_get_summary_finish(
    bufferlist::const_iterator *iter,
    std::map<cls::rbd::MirrorImageStatusState, int32_t> *states) {
  try {
    decode(*states, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_status_remove_down(librados::IoCtx *ioctx) {
  librados::ObjectWriteOperation op;
  mirror_image_status_remove_down(&op);
  return ioctx->operate(RBD_MIRRORING, &op);
}

void mirror_image_status_remove_down(librados::ObjectWriteOperation *op) {
  bufferlist bl;
  op->exec("rbd", "mirror_image_status_remove_down", bl);
}

int mirror_image_instance_get(librados::IoCtx *ioctx,
                              const std::string &global_image_id,
                              entity_inst_t *instance) {
  librados::ObjectReadOperation op;
  mirror_image_instance_get_start(&op, global_image_id);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_image_instance_get_finish(&iter, instance);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_instance_get_start(librados::ObjectReadOperation *op,
                                     const std::string &global_image_id) {
  bufferlist bl;
  encode(global_image_id, bl);
  op->exec("rbd", "mirror_image_instance_get", bl);
}

int mirror_image_instance_get_finish(bufferlist::const_iterator *iter,
                                     entity_inst_t *instance) {
  try {
    decode(*instance, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_instance_list(
    librados::IoCtx *ioctx, const std::string &start, uint64_t max_return,
    std::map<std::string, entity_inst_t> *instances) {
  librados::ObjectReadOperation op;
  mirror_image_instance_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_image_instance_list_finish(&iter, instances);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_image_instance_list_start(librados::ObjectReadOperation *op,
                                      const std::string &start,
                                      uint64_t max_return) {
  bufferlist bl;
  encode(start, bl);
  encode(max_return, bl);
  op->exec("rbd", "mirror_image_instance_list", bl);
}

int mirror_image_instance_list_finish(
    bufferlist::const_iterator *iter,
    std::map<std::string, entity_inst_t> *instances) {
  instances->clear();
  try {
    decode(*instances, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

void mirror_instances_list_start(librados::ObjectReadOperation *op) {
  bufferlist bl;
  op->exec("rbd", "mirror_instances_list", bl);
}

int mirror_instances_list_finish(bufferlist::const_iterator *iter,
                                 std::vector<std::string> *instance_ids) {
  instance_ids->clear();
  try {
    decode(*instance_ids, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_instances_list(librados::IoCtx *ioctx,
                          std::vector<std::string> *instance_ids) {
  librados::ObjectReadOperation op;
  mirror_instances_list_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRROR_LEADER, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  r = mirror_instances_list_finish(&iter, instance_ids);
  if (r < 0) {
    return r;
  }
  return 0;
}

void mirror_instances_add(librados::ObjectWriteOperation *op,
                          const std::string &instance_id) {
  bufferlist bl;
  encode(instance_id, bl);
  op->exec("rbd", "mirror_instances_add", bl);
}

int mirror_instances_add(librados::IoCtx *ioctx,
                         const std::string &instance_id) {
  librados::ObjectWriteOperation op;
  mirror_instances_add(&op, instance_id);
  return ioctx->operate(RBD_MIRROR_LEADER, &op);
}

void mirror_instances_remove(librados::ObjectWriteOperation *op,
                             const std::string &instance_id) {
  bufferlist bl;
  encode(instance_id, bl);
  op->exec("rbd", "mirror_instances_remove", bl);
}

int mirror_instances_remove(librados::IoCtx *ioctx,
                            const std::string &instance_id) {
  librados::ObjectWriteOperation op;
  mirror_instances_remove(&op, instance_id);
  return ioctx->operate(RBD_MIRROR_LEADER, &op);
}

void mirror_image_map_list_start(librados::ObjectReadOperation *op,
                                 const std::string &start_after,
                                 uint64_t max_read) {
  bufferlist bl;
  encode(start_after, bl);
  encode(max_read, bl);

  op->exec("rbd", "mirror_image_map_list", bl);
}

int mirror_image_map_list_finish(bufferlist::const_iterator *iter,
                                 std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping) {
  try {
    decode(*image_mapping, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int mirror_image_map_list(
    librados::IoCtx *ioctx, const std::string &start_after,
    uint64_t max_read,
    std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping) {
  librados::ObjectReadOperation op;
  mirror_image_map_list_start(&op, start_after, max_read);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  return mirror_image_map_list_finish(&iter, image_mapping);
}

void mirror_image_map_update(librados::ObjectWriteOperation *op,
                             const std::string &global_image_id,
                             const cls::rbd::MirrorImageMap &image_map) {
  bufferlist bl;
  encode(global_image_id, bl);
  encode(image_map, bl);

  op->exec("rbd", "mirror_image_map_update", bl);
}

void mirror_image_map_remove(librados::ObjectWriteOperation *op,
                             const std::string &global_image_id) {
  bufferlist bl;
  encode(global_image_id, bl);

  op->exec("rbd", "mirror_image_map_remove", bl);
}

void mirror_image_snapshot_unlink_peer(librados::ObjectWriteOperation *op,
                                       snapid_t snap_id,
                                       const std::string &mirror_peer_uuid) {
  bufferlist bl;
  encode(snap_id, bl);
  encode(mirror_peer_uuid, bl);

  op->exec("rbd", "mirror_image_snapshot_unlink_peer", bl);
}

int mirror_image_snapshot_unlink_peer(librados::IoCtx *ioctx,
                                      const std::string &oid,
                                      snapid_t snap_id,
                                      const std::string &mirror_peer_uuid) {
  librados::ObjectWriteOperation op;
  mirror_image_snapshot_unlink_peer(&op, snap_id, mirror_peer_uuid);
  return ioctx->operate(oid, &op);
}

void mirror_image_snapshot_set_copy_progress(librados::ObjectWriteOperation *op,
                                             snapid_t snap_id, bool complete,
                                             uint64_t copy_progress) {
  bufferlist bl;
  encode(snap_id, bl);
  encode(complete, bl);
  encode(copy_progress, bl);

  op->exec("rbd", "mirror_image_snapshot_set_copy_progress", bl);
}

int mirror_image_snapshot_set_copy_progress(librados::IoCtx *ioctx,
                                            const std::string &oid,
                                            snapid_t snap_id, bool complete,
                                            uint64_t copy_progress) {
  librados::ObjectWriteOperation op;
  mirror_image_snapshot_set_copy_progress(&op, snap_id, complete,
                                          copy_progress);
  return ioctx->operate(oid, &op);
}

// Groups functions
int group_dir_list(librados::IoCtx *ioctx, const std::string &oid,
                   const std::string &start, uint64_t max_return,
                   map<string, string> *cgs)
{
  bufferlist in, out;
  encode(start, in);
  encode(max_return, in);
  int r = ioctx->exec(oid, "rbd", "group_dir_list", in, out);
  if (r < 0)
    return r;

  auto iter = out.cbegin();
  try {
    decode(*cgs, iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int group_dir_add(librados::IoCtx *ioctx, const std::string &oid,
                  const std::string &name, const std::string &id)
{
  bufferlist in, out;
  encode(name, in);
  encode(id, in);
  return ioctx->exec(oid, "rbd", "group_dir_add", in, out);
}

int group_dir_rename(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &src, const std::string &dest,
                     const std::string &id)
{
  bufferlist in, out;
  encode(src, in);
  encode(dest, in);
  encode(id, in);
  return ioctx->exec(oid, "rbd", "group_dir_rename", in, out);
}

int group_dir_remove(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &name, const std::string &id)
{
  bufferlist in, out;
  encode(name, in);
  encode(id, in);
  return ioctx->exec(oid, "rbd", "group_dir_remove", in, out);
}

int group_image_remove(librados::IoCtx *ioctx, const std::string &oid,
                       const cls::rbd::GroupImageSpec &spec)
{
  bufferlist bl, bl2;
  encode(spec, bl);

  return ioctx->exec(oid, "rbd", "group_image_remove", bl, bl2);
}

int group_image_list(librados::IoCtx *ioctx,
                     const std::string &oid,
                     const cls::rbd::GroupImageSpec &start,
                     uint64_t max_return,
                     std::vector<cls::rbd::GroupImageStatus> *images)
{
  bufferlist bl, bl2;
  encode(start, bl);
  encode(max_return, bl);

  int r = ioctx->exec(oid, "rbd", "group_image_list", bl, bl2);
  if (r < 0)
    return r;

  auto iter = bl2.cbegin();
  try {
    decode(*images, iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int group_image_set(librados::IoCtx *ioctx, const std::string &oid,
                    const cls::rbd::GroupImageStatus &st)
{
  bufferlist bl, bl2;
  encode(st, bl);

  return ioctx->exec(oid, "rbd", "group_image_set", bl, bl2);
}

int image_group_add(librados::IoCtx *ioctx, const std::string &oid,
                    const cls::rbd::GroupSpec &group_spec)
{
  bufferlist bl, bl2;
  encode(group_spec, bl);

  return ioctx->exec(oid, "rbd", "image_group_add", bl, bl2);
}

int image_group_remove(librados::IoCtx *ioctx, const std::string &oid,
                       const cls::rbd::GroupSpec &group_spec)
{
  bufferlist bl, bl2;
  encode(group_spec, bl);

  return ioctx->exec(oid, "rbd", "image_group_remove", bl, bl2);
}

void image_group_get_start(librados::ObjectReadOperation *op)
{
  bufferlist in_bl;
  op->exec("rbd", "image_group_get", in_bl);
}

int image_group_get_finish(bufferlist::const_iterator *iter,
                           cls::rbd::GroupSpec *group_spec)
{
  try {
    decode(*group_spec, *iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int image_group_get(librados::IoCtx *ioctx, const std::string &oid,
                    cls::rbd::GroupSpec *group_spec)
{
  librados::ObjectReadOperation op;
  image_group_get_start(&op);

  bufferlist out_bl;
  int r = ioctx->operate(oid, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  return image_group_get_finish(&iter, group_spec);
}

int group_snap_set(librados::IoCtx *ioctx, const std::string &oid,
                   const cls::rbd::GroupSnapshot &snapshot)
{
  using ceph::encode;
  bufferlist inbl, outbl;
  encode(snapshot, inbl);
  int r = ioctx->exec(oid, "rbd", "group_snap_set", inbl, outbl);
  return r;
}

int group_snap_remove(librados::IoCtx *ioctx, const std::string &oid,
                      const std::string &snap_id)
{
  using ceph::encode;
  bufferlist inbl, outbl;
  encode(snap_id, inbl);
  return ioctx->exec(oid, "rbd", "group_snap_remove", inbl, outbl);
}

int group_snap_get_by_id(librados::IoCtx *ioctx, const std::string &oid,
                         const std::string &snap_id,
                         cls::rbd::GroupSnapshot *snapshot)
{
  using ceph::encode;
  using ceph::decode;
  bufferlist inbl, outbl;

  encode(snap_id, inbl);
  int r = ioctx->exec(oid, "rbd", "group_snap_get_by_id", inbl, outbl);
  if (r < 0) {
    return r;
  }

  auto iter = outbl.cbegin();
  try {
    decode(*snapshot, iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}
int group_snap_list(librados::IoCtx *ioctx, const std::string &oid,
                    const cls::rbd::GroupSnapshot &start,
                    uint64_t max_return,
                    std::vector<cls::rbd::GroupSnapshot> *snapshots)
{
  using ceph::encode;
  using ceph::decode;
  bufferlist inbl, outbl;
  encode(start, inbl);
  encode(max_return, inbl);

  int r = ioctx->exec(oid, "rbd", "group_snap_list", inbl, outbl);
  if (r < 0) {
    return r;
  }
  auto iter = outbl.cbegin();
  try {
    decode(*snapshots, iter);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

// rbd_trash functions
void trash_add(librados::ObjectWriteOperation *op,
               const std::string &id,
               const cls::rbd::TrashImageSpec &trash_spec)
{
  bufferlist bl;
  encode(id, bl);
  encode(trash_spec, bl);
  op->exec("rbd", "trash_add", bl);
}

int trash_add(librados::IoCtx *ioctx, const std::string &id,
              const cls::rbd::TrashImageSpec &trash_spec)
{
  librados::ObjectWriteOperation op;
  trash_add(&op, id, trash_spec);

  return ioctx->operate(RBD_TRASH, &op);
}

void trash_remove(librados::ObjectWriteOperation *op,
                  const std::string &id)
{
  bufferlist bl;
  encode(id, bl);
  op->exec("rbd", "trash_remove", bl);
}

int trash_remove(librados::IoCtx *ioctx, const std::string &id)
{
  librados::ObjectWriteOperation op;
  trash_remove(&op, id);

  return ioctx->operate(RBD_TRASH, &op);
}

void trash_list_start(librados::ObjectReadOperation *op,
                      const std::string &start, uint64_t max_return)
{
  bufferlist bl;
  encode(start, bl);
  encode(max_return, bl);
  op->exec("rbd", "trash_list", bl);
}

int trash_list_finish(bufferlist::const_iterator *it,
                      map<string, cls::rbd::TrashImageSpec> *entries)
{
  ceph_assert(entries);

  try {
    decode(*entries, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int trash_list(librados::IoCtx *ioctx,
               const std::string &start, uint64_t max_return,
               map<string, cls::rbd::TrashImageSpec> *entries)
{
  librados::ObjectReadOperation op;
  trash_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_TRASH, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  return trash_list_finish(&iter, entries);
}

void trash_get_start(librados::ObjectReadOperation *op,
                     const std::string &id)
{
  bufferlist bl;
  encode(id, bl);
  op->exec("rbd", "trash_get", bl);
}

int trash_get_finish(bufferlist::const_iterator *it,
                     cls::rbd::TrashImageSpec *trash_spec) {
  ceph_assert(trash_spec);
  try {
    decode(*trash_spec, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int trash_get(librados::IoCtx *ioctx, const std::string &id,
              cls::rbd::TrashImageSpec *trash_spec)
{
  librados::ObjectReadOperation op;
  trash_get_start(&op, id);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_TRASH, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto it = out_bl.cbegin();
  return trash_get_finish(&it, trash_spec);
}

void trash_state_set(librados::ObjectWriteOperation *op,
                     const std::string &id,
                     const cls::rbd::TrashImageState &trash_state,
                     const cls::rbd::TrashImageState &expect_state)
{
  bufferlist bl;
  encode(id, bl);
  encode(trash_state, bl);
  encode(expect_state, bl);
  op->exec("rbd", "trash_state_set", bl);
}

int trash_state_set(librados::IoCtx *ioctx, const std::string &id,
                    const cls::rbd::TrashImageState &trash_state,
                    const cls::rbd::TrashImageState &expect_state)
{
  librados::ObjectWriteOperation op;
  trash_state_set(&op, id, trash_state, expect_state);

  return ioctx->operate(RBD_TRASH, &op);
}

void namespace_add(librados::ObjectWriteOperation *op,
                   const std::string &name)
{
  bufferlist bl;
  encode(name, bl);
  op->exec("rbd", "namespace_add", bl);
}

int namespace_add(librados::IoCtx *ioctx, const std::string &name)
{
  librados::ObjectWriteOperation op;
  namespace_add(&op, name);

  return ioctx->operate(RBD_NAMESPACE, &op);
}

void namespace_remove(librados::ObjectWriteOperation *op,
                      const std::string &name)
{
  bufferlist bl;
  encode(name, bl);
  op->exec("rbd", "namespace_remove", bl);
}

int namespace_remove(librados::IoCtx *ioctx, const std::string &name)
{
  librados::ObjectWriteOperation op;
  namespace_remove(&op, name);

  return ioctx->operate(RBD_NAMESPACE, &op);
}

void namespace_list_start(librados::ObjectReadOperation *op,
                          const std::string &start, uint64_t max_return)
{
  bufferlist bl;
  encode(start, bl);
  encode(max_return, bl);
  op->exec("rbd", "namespace_list", bl);
}

int namespace_list_finish(bufferlist::const_iterator *it,
                          std::list<std::string> *entries)
{
  ceph_assert(entries);

  try {
    decode(*entries, *it);
  } catch (const ceph::buffer::error &err) {
    return -EBADMSG;
  }

  return 0;
}

int namespace_list(librados::IoCtx *ioctx,
                   const std::string &start, uint64_t max_return,
                   std::list<std::string> *entries)
{
  librados::ObjectReadOperation op;
  namespace_list_start(&op, start, max_return);

  bufferlist out_bl;
  int r = ioctx->operate(RBD_NAMESPACE, &op, &out_bl);
  if (r < 0) {
    return r;
  }

  auto iter = out_bl.cbegin();
  return namespace_list_finish(&iter, entries);
}

void sparsify(librados::ObjectWriteOperation *op, size_t sparse_size,
              bool remove_empty)
{
  bufferlist bl;
  encode(sparse_size, bl);
  encode(remove_empty, bl);
  op->exec("rbd", "sparsify", bl);
}

int sparsify(librados::IoCtx *ioctx, const std::string &oid, size_t sparse_size,
             bool remove_empty)
{
  librados::ObjectWriteOperation op;
  sparsify(&op, sparse_size, remove_empty);

  return ioctx->operate(oid, &op);
}

} // namespace cls_client
} // namespace librbd
