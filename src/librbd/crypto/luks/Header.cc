// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Header.h"

#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::luks::Header: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace crypto {
namespace luks {

Header::Header(CephContext* cct) : m_cct(cct), m_fd(-1), m_cd(nullptr) {
}

Header::~Header() {
  if (m_fd != -1) {
    close(m_fd);
    m_fd = -1;
  }
  if (m_cd != nullptr) {
    crypt_free(m_cd);
    m_cd = nullptr;
  }
}

void Header::libcryptsetup_log_wrapper(int level, const char* msg, void* header) {
  ((Header*)header)->libcryptsetup_log(level, msg);
}

void Header::libcryptsetup_log(int level, const char* msg) {
  switch (level) {
    case CRYPT_LOG_NORMAL:
      ldout(m_cct, 5) << "[libcryptsetup] " << msg << dendl;
      break;
    case CRYPT_LOG_ERROR:
      lderr(m_cct) << "[libcryptsetup] " << msg << dendl;
      break;
    case CRYPT_LOG_VERBOSE:
      ldout(m_cct, 10) << "[libcryptsetup] " << msg << dendl;
      break;
    case CRYPT_LOG_DEBUG:
      ldout(m_cct, 20) << "[libcryptsetup] " << msg << dendl;
      break;
  }
}

int Header::init() {
  // create anonymous file
  m_fd = syscall(SYS_memfd_create, "LibcryptsetupInterface", 0);
  if (m_fd == -1) {
    lderr(m_cct) << "error creating anonymous file: " << cpp_strerror(-errno)
                 << dendl;
    return -errno;
  }
  std::string path =
          "/proc/" + std::to_string(getpid()) + "/fd/" + std::to_string(m_fd);

  if (m_cct->_conf->subsys.should_gather<dout_subsys, 20>()) {
    crypt_set_debug_level(CRYPT_DEBUG_ALL);
  }

  // init libcryptsetup handle
  auto r = crypt_init(&m_cd, path.c_str());
  if (r != 0) {
    lderr(m_cct) << "crypt_init failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  // redirect logging
  crypt_set_log_callback(m_cd, &libcryptsetup_log_wrapper, this);

  return 0;
}

int Header::write(const ceph::bufferlist& bl) {
  ceph_assert(m_fd != -1);

  auto r = bl.write_fd(m_fd);
  if (r != 0) {
    lderr(m_cct) << "error writing header: " << cpp_strerror(r) << dendl;
  }
  return r;
}

ssize_t Header::read(ceph::bufferlist* bl) {
  ceph_assert(m_fd != -1);

  // get current header size
  struct stat st;
  ssize_t r = fstat(m_fd, &st);
  if (r < 0) {
    r = -errno;
    lderr(m_cct) << "failed to stat anonymous file: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  r = bl->read_fd(m_fd, st.st_size);
  if (r < 0) {
    lderr(m_cct) << "error reading header: " << cpp_strerror(r) << dendl;
  }

  ldout(m_cct, 20) << "read size = " << r << dendl;
  return r;
}

int Header::format(const char* type, const char* alg, const char* key,
                   size_t key_size, const char* cipher_mode,
                   uint32_t sector_size, uint32_t data_alignment,
                   bool insecure_fast_mode) {
  ceph_assert(m_cd != nullptr);

  ldout(m_cct, 20) << "sector size: " << sector_size << ", data alignment: "
                   << data_alignment << dendl;

  // required for passing libcryptsetup device size check
  if (ftruncate(m_fd, 4096) != 0) {
    lderr(m_cct) << "failed to truncate anonymous file: "
                 << cpp_strerror(-errno) << dendl;
    return -errno;
  }

  struct crypt_params_luks1 luks1params;
  struct crypt_params_luks2 luks2params;

#ifdef LIBCRYPTSETUP_LEGACY_DATA_ALIGNMENT
  size_t converted_data_alignment = data_alignment / sector_size;
#else
  size_t converted_data_alignment = data_alignment / 512;
#endif

  void* params = nullptr;
  if (strcmp(type, CRYPT_LUKS1) == 0) {
    memset(&luks1params, 0, sizeof(luks1params));
    luks1params.data_alignment = converted_data_alignment;
    params = &luks1params;
  } else if (strcmp(type, CRYPT_LUKS2) == 0) {
    memset(&luks2params, 0, sizeof(luks2params));
    luks2params.data_alignment = converted_data_alignment;
    luks2params.sector_size = sector_size;
    params = &luks2params;
  }

  // this mode should be used for testing only
  if (insecure_fast_mode) {
    struct crypt_pbkdf_type pbkdf;
    memset(&pbkdf, 0, sizeof(pbkdf));
    pbkdf.type = CRYPT_KDF_PBKDF2;
    pbkdf.flags = CRYPT_PBKDF_NO_BENCHMARK;
    pbkdf.hash = "sha256";
    pbkdf.iterations = 1000;
    pbkdf.time_ms = 1;
    auto r = crypt_set_pbkdf_type(m_cd, &pbkdf);
    if (r != 0) {
      lderr(m_cct) << "crypt_set_pbkdf_type failed: " << cpp_strerror(r)
                   << dendl;
      return r;
    }
  }

  auto r = crypt_format(
          m_cd, type, alg, cipher_mode, NULL, key, key_size, params);
  if (r != 0) {
    lderr(m_cct) << "crypt_format failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

int Header::add_keyslot(const char* passphrase, size_t passphrase_size) {
  ceph_assert(m_cd != nullptr);

  auto r = crypt_keyslot_add_by_volume_key(
          m_cd, CRYPT_ANY_SLOT, NULL, 0, passphrase, passphrase_size);
  if (r != 0) {
    lderr(m_cct) << "crypt_keyslot_add_by_volume_key failed: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

int Header::load(const char* type) {
  ceph_assert(m_cd != nullptr);

  // libcryptsetup checks if device size matches the header and keyslots size
  // in LUKS2, 2 X 4MB header + 128MB keyslots
  if (ftruncate(m_fd, 136 * 1024 * 1024) != 0) {
    lderr(m_cct) << "failed to truncate anonymous file: "
                 << cpp_strerror(-errno) << dendl;
    return -errno;
  }

  auto r = crypt_load(m_cd, type, NULL);
  if (r != 0) {
    lderr(m_cct) << "crypt_load failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  ldout(m_cct, 20) << "sector size: " << get_sector_size() << ", data offset: "
                   << get_data_offset() << dendl;

  return 0;
}

int Header::read_volume_key(const char* passphrase, size_t passphrase_size,
                            char* volume_key, size_t* volume_key_size) {
  ceph_assert(m_cd != nullptr);

  auto r = crypt_volume_key_get(
          m_cd, CRYPT_ANY_SLOT, volume_key, volume_key_size, passphrase,
          passphrase_size);
  if (r != 0) {
    lderr(m_cct) << "crypt_volume_key_get failed: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

int Header::get_sector_size() {
  ceph_assert(m_cd != nullptr);
  return crypt_get_sector_size(m_cd);
}

uint64_t Header::get_data_offset() {
  ceph_assert(m_cd != nullptr);
  return crypt_get_data_offset(m_cd) << 9;
}

const char* Header::get_cipher() {
  ceph_assert(m_cd != nullptr);
  return crypt_get_cipher(m_cd);
}

const char* Header::get_cipher_mode() {
  ceph_assert(m_cd != nullptr);
  return crypt_get_cipher_mode(m_cd);
}

} // namespace luks
} // namespace crypto
} // namespace librbd
