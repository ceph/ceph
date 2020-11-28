// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Utils.h"
#include "common/Cond.h"
#include "common/dout.h"

#include "librbd/ImageWatcher.h"
#if defined(HAVE_LIBCRYPTSETUP)
#include "librbd/crypto/luks/EncryptionFormat.h"
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::util: " << __func__ << ": "

namespace librbd {
namespace api {
namespace util {

template <typename I>
int create_encryption_format(
        CephContext* cct, encryption_format_t format,
        encryption_options_t opts, size_t opts_size, bool c_api,
        crypto::EncryptionFormat<I>** result_format) {
  size_t expected_opts_size;
  switch (format) {
#if defined(HAVE_LIBCRYPTSETUP)
    case RBD_ENCRYPTION_FORMAT_LUKS1: {
      if (c_api) {
        expected_opts_size = sizeof(rbd_encryption_luks1_format_options_t);
        if (expected_opts_size == opts_size) {
          auto c_opts = (rbd_encryption_luks1_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS1EncryptionFormat<I>(
                  c_opts->alg, {c_opts->passphrase, c_opts->passphrase_size});
        }
      } else {
        expected_opts_size = sizeof(encryption_luks1_format_options_t);
        if (expected_opts_size == opts_size) {
          auto cpp_opts = (encryption_luks1_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS1EncryptionFormat<I>(
                  cpp_opts->alg, std::move(cpp_opts->passphrase));
        }
      }
      break;
    }
    case RBD_ENCRYPTION_FORMAT_LUKS2: {
      if (c_api) {
        expected_opts_size = sizeof(rbd_encryption_luks2_format_options_t);
        if (expected_opts_size == opts_size) {
          auto c_opts = (rbd_encryption_luks2_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS2EncryptionFormat<I>(
                  c_opts->alg, {c_opts->passphrase, c_opts->passphrase_size});
        }
      } else {
        expected_opts_size = sizeof(encryption_luks2_format_options_t);
        if (expected_opts_size == opts_size) {
          auto cpp_opts = (encryption_luks2_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS2EncryptionFormat<I>(
                  cpp_opts->alg, std::move(cpp_opts->passphrase));
        }
      }
      break;
    }
#endif
    default:
      lderr(cct) << "unsupported encryption format: " << format << dendl;
      return -ENOTSUP;
  }

  if (expected_opts_size != opts_size) {
    lderr(cct) << "expected opts_size: " << expected_opts_size << dendl;
    return -EINVAL;
  }

  return 0;
}

template <typename I>
int notify_quiesce(std::vector<I *> &ictxs, ProgressContext &prog_ctx,
                   std::vector<uint64_t> *requests) {
  int image_count = ictxs.size();
  std::vector<C_SaferCond> on_finishes(image_count);

  requests->resize(image_count);
  for (int i = 0; i < image_count; ++i) {
    auto ictx = ictxs[i];

    ictx->image_watcher->notify_quiesce(&(*requests)[i], prog_ctx,
                                        &on_finishes[i]);
  }

  int ret_code = 0;
  for (int i = 0; i < image_count; ++i) {
    int r = on_finishes[i].wait();
    if (r < 0) {
      ret_code = r;
    }
  }

  if (ret_code != 0) {
    notify_unquiesce(ictxs, *requests);
  }

  return ret_code;
}

template <typename I>
void notify_unquiesce(std::vector<I *> &ictxs,
                      const std::vector<uint64_t> &requests) {
  if (requests.empty()) {
    return;
  }

  ceph_assert(requests.size() == ictxs.size());
  int image_count = ictxs.size();
  std::vector<C_SaferCond> on_finishes(image_count);

  for (int i = 0; i < image_count; ++i) {
    ImageCtx *ictx = ictxs[i];

    ictx->image_watcher->notify_unquiesce(requests[i], &on_finishes[i]);
  }

  for (int i = 0; i < image_count; ++i) {
    on_finishes[i].wait();
  }
}

} // namespace util
} // namespace api
} // namespace librbd

template int librbd::api::util::create_encryption_format(
    CephContext* cct, encryption_format_t format, encryption_options_t opts,
    size_t opts_size, bool c_api,
    crypto::EncryptionFormat<librbd::ImageCtx>** result_format);

template int librbd::api::util::notify_quiesce(
    std::vector<librbd::ImageCtx *> &ictxs, librbd::ProgressContext &prog_ctx,
    std::vector<uint64_t> *requests);
template void librbd::api::util::notify_unquiesce(
    std::vector<librbd::ImageCtx *> &ictxs,
    const std::vector<uint64_t> &requests);
