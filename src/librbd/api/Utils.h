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

} // namespace util
} // namespace api
} // namespace librbd

#endif // CEPH_LIBRBD_API_UTILS_H
