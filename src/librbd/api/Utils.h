// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_UTILS_H
#define CEPH_LIBRBD_API_UTILS_H

#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/EncryptionFormat.h"

namespace librbd {

struct ImageCtx;

namespace api {
namespace util {

template <typename ImageCtxT = librbd::ImageCtx>
int create_encryption_format(
        CephContext* cct, encryption_format_t format,
        encryption_options_t opts, size_t opts_size, bool c_api,
        crypto::EncryptionFormat<ImageCtxT>** result_format);

template <typename ImageCtxT = librbd::ImageCtx>
int notify_quiesce(std::vector<ImageCtxT *> &ictxs, ProgressContext &prog_ctx,
                   std::vector<uint64_t> *requests);
template <typename ImageCtxT = librbd::ImageCtx>
void notify_unquiesce(std::vector<ImageCtxT *> &ictxs,
                      const std::vector<uint64_t> &requests);

template <typename ImageCtxT = librbd::ImageCtx>
librados::snap_t get_group_snap_id(
    ImageCtxT *ictx, const cls::rbd::SnapshotNamespace& in_snap_namespace);
int group_snap_remove(librados::IoCtx& group_ioctx, const std::string& group_id,
                      const cls::rbd::GroupSnapshot& group_snap);

} // namespace util
} // namespace api
} // namespace librbd

#endif // CEPH_LIBRBD_API_UTILS_H
