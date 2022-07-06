// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Utils.h"
#include "common/dout.h"

#if defined(HAVE_LIBCRYPTSETUP)
#include "librbd/crypto/luks/LUKSEncryptionFormat.h"
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
    case RBD_ENCRYPTION_FORMAT_LUKS: {
      if (c_api) {
        expected_opts_size = sizeof(rbd_encryption_luks_format_options_t);
        if (expected_opts_size == opts_size) {
          auto c_opts = (rbd_encryption_luks_format_options_t*)opts;
          *result_format = new crypto::luks::LUKSEncryptionFormat<I>(
                  {c_opts->passphrase, c_opts->passphrase_size});
        }
      } else {
        expected_opts_size = sizeof(encryption_luks_format_options_t);
        if (expected_opts_size == opts_size) {
          auto cpp_opts = (encryption_luks_format_options_t*)opts;
          *result_format = new crypto::luks::LUKSEncryptionFormat<I>(
                  std::move(cpp_opts->passphrase));
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

} // namespace util
} // namespace api
} // namespace librbd

template int librbd::api::util::create_encryption_format(
    CephContext* cct, encryption_format_t format, encryption_options_t opts,
    size_t opts_size, bool c_api,
    crypto::EncryptionFormat<librbd::ImageCtx>** result_format);
