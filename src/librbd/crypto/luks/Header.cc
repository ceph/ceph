// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "Header.h"

#include <endian.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <openssl/evp.h>
#include "common/dout.h"
#include "common/errno.h"
#include "json_spirit/json_spirit.h"

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
  if (m_fd != -1) {
    return 0;
  }

  // create anonymous file
  m_fd = syscall(SYS_memfd_create, "LibcryptsetupInterface", 0);
  if (m_fd == -1) {
    lderr(m_cct) << "error creating anonymous file: " << cpp_strerror(-errno)
                 << dendl;
    return -errno;
  }
  std::string path =
          "/proc/" + std::to_string(getpid()) + "/fd/" + std::to_string(m_fd);

  if (m_cct->_conf->subsys.should_gather<dout_subsys, 30>()) {
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

  const size_t converted_data_alignment = data_alignment / 512;

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
  if (r < 0) {
    lderr(m_cct) << "crypt_keyslot_add_by_volume_key failed: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

// Post-process a LUKS2 header (already formatted via crypt_format() with a
// placeholder cipher) to produce output equivalent to crypt_format_inline().
// We cannot call crypt_format_inline() directly because it requires NOP DIF
// hardware, which is unavailable on a memfd.
//
// Assumes:
//   - Called after crypt_format() and add_keyslot() on this Header's memfd
//   - The header is LUKS2 (not LUKS1) — LUKS1 has no JSON area
//   - There is exactly one segment ("0") — crypt_format() always creates one
//   - The checksum algorithm is SHA-256 — the LUKS2 default
//   - The added fields (integrity section + requirements) fit within the
//     existing JSON area allocated by crypt_format() — the area is typically
//     ~12KB and we only modify the JSON string in-place without resizing it
//
// The memfd contains two LUKS2 headers (primary at offset 0, secondary at
// offset hdr_size). Each header is: a 4096-byte binary header followed by a
// JSON metadata area. For each copy we:
//   1. Parse the JSON and rewrite the segment with the real cipher/mode
//   2. Add the integrity section and "inline-hw-tags" requirement
//   3. Recalculate the SHA-256 checksum over (binary header + JSON area)
int Header::rewrite_segment_for_inline(const char* cipher,
                                       const char* cipher_mode,
                                       const char* integrity) {
  ceph_assert(m_fd != -1);

  // read the entire memfd
  struct stat st;
  if (fstat(m_fd, &st) < 0) {
    auto r = -errno;
    lderr(m_cct) << "failed to stat memfd: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (st.st_size <= 0) {
    lderr(m_cct) << "memfd is empty" << dendl;
    return -EINVAL;
  }

  std::vector<char> buf(st.st_size);
  if (lseek(m_fd, 0, SEEK_SET) < 0) {
    auto r = -errno;
    lderr(m_cct) << "failed to seek memfd: " << cpp_strerror(r) << dendl;
    return r;
  }

  ssize_t bytes_read = ::read(m_fd, buf.data(), buf.size());
  if (bytes_read < 0 || static_cast<size_t>(bytes_read) != buf.size()) {
    lderr(m_cct) << "failed to read memfd" << dendl;
    return -EIO;
  }

  // LUKS2 binary header constants — from struct luks2_hdr_disk in
  // cryptsetup lib/luks2/luks2.h (LUKS2_HDR_BIN_LEN, LUKS2_CHECKSUM_L)
  static constexpr size_t LUKS2_HDR_BIN_LEN = 4096;      // sizeof(luks2_hdr_disk) 
  // LUKS_HDR_BIN is a fixed binary header that precedes the JSON metadata area in a LUKS2 header.
  static constexpr size_t LUKS2_HDR_SIZE_OFFSET = 8;      // offset of hdr_size field
  static constexpr size_t LUKS2_CHECKSUM_OFFSET = 0x01c0; // offset of csum field
  static constexpr size_t LUKS2_CHECKSUM_LEN = 64;        // LUKS2_CHECKSUM_L

  // LUKS2 spec: device must be large enough to hold at least the binary header
  if (buf.size() < LUKS2_HDR_BIN_LEN + 1) {
    lderr(m_cct) << "memfd too small for LUKS2 header" << dendl;
    return -EINVAL;
  }

  // LUKS2 spec: hdr_size is a big-endian uint64 at offset 8 in the
  // 4096-byte fixed binary header (struct luks2_hdr_disk). It gives the
  // total size of one header copy: the binary header + the JSON metadata
  // area that follows it. See hdr_from_disk() in luks2_disk_metadata.c.
  uint64_t hdr_size;
  memcpy(&hdr_size, buf.data() + LUKS2_HDR_SIZE_OFFSET, sizeof(hdr_size));
  hdr_size = be64toh(hdr_size);

  // Note: cryptsetup validates the LUKS2 spec limits on hdr_size in
  // hdr_disk_sanity_check_pre() (>= 16KB, <= 4MB). Since crypt_format()
  // already enforced those limits when creating this header, we only need
  // to verify our buffer is self-consistent: hdr_size must cover at least
  // the binary header, and the memfd must be large enough to hold both
  // back-to-back copies (primary at offset 0, secondary at offset hdr_size)
  // that we're about to modify.
  if (hdr_size < LUKS2_HDR_BIN_LEN || hdr_size * 2 > buf.size()) {
    lderr(m_cct) << "invalid LUKS2 header size: " << hdr_size << dendl;
    return -EINVAL;
  }

  std::string encryption = std::string(cipher) + "-" + cipher_mode;

  // LUKS2 spec: both header copies must be kept in sync. Process primary
  // (offset 0) and secondary (offset hdr_size) identically.
  // See hdr_write_disk() in luks2_disk_metadata.c.
  for (int h = 0; h < 2; h++) {
    size_t hdr_offset = h * hdr_size;
    // LUKS2 spec: JSON area starts immediately after the 4096-byte binary
    // header and extends to hdr_size. See hdr_read_disk() in
    // luks2_disk_metadata.c.
    char* json_area = buf.data() + hdr_offset + LUKS2_HDR_BIN_LEN;
    size_t json_area_size = hdr_size - LUKS2_HDR_BIN_LEN;

    // parse JSON
    json_spirit::mValue root;
    std::string json_str(json_area, strnlen(json_area, json_area_size));
    if (!json_spirit::read(json_str, root)) {
      lderr(m_cct) << "failed to parse LUKS2 JSON in header " << h << dendl;
      return -EINVAL;
    }

    auto& root_obj = root.get_obj();

    // modify segments.0.encryption and add integrity
    auto seg_it = root_obj.find("segments");
    if (seg_it == root_obj.end()) {
      lderr(m_cct) << "missing segments in LUKS2 JSON" << dendl;
      return -EINVAL;
    }
    auto& segments = seg_it->second.get_obj();
    auto seg0_it = segments.find("0");
    if (seg0_it == segments.end()) {
      lderr(m_cct) << "missing segment 0 in LUKS2 JSON" << dendl;
      return -EINVAL;
    }
    auto& seg0 = seg0_it->second.get_obj();
    seg0["encryption"] = json_spirit::mValue(encryption);

    json_spirit::mObject integrity_obj;
    integrity_obj["type"] = json_spirit::mValue(std::string(integrity));
    integrity_obj["journal_encryption"] = json_spirit::mValue(std::string("none"));
    integrity_obj["journal_integrity"] = json_spirit::mValue(std::string("none"));
    seg0["integrity"] = json_spirit::mValue(integrity_obj);

    // add config.requirements.mandatory = ["inline-hw-tags"]
    auto config_it = root_obj.find("config");
    if (config_it == root_obj.end()) {
      lderr(m_cct) << "missing config in LUKS2 JSON" << dendl;
      return -EINVAL;
    }
    auto& config = config_it->second.get_obj();

    json_spirit::mObject requirements;
    json_spirit::mArray mandatory;
    mandatory.push_back(json_spirit::mValue(std::string("inline-hw-tags")));
    requirements["mandatory"] = json_spirit::mValue(mandatory);
    config["requirements"] = json_spirit::mValue(requirements);

    // serialize JSON
    std::string new_json = json_spirit::write(root);

    // LUKS2 spec: JSON must fit within the JSON area (hdr_size - 4096 bytes).
    // See LUKS2_disk_hdr_write() in luks2_disk_metadata.c.
    if (new_json.size() >= json_area_size) {
      lderr(m_cct) << "rewritten JSON exceeds area size" << dendl;
      return -ENOSPC;
    }

    // LUKS2 spec: JSON area is null-terminated and zero-padded.
    // See validate_json_area() in luks2_disk_metadata.c.
    memset(json_area, 0, json_area_size);
    memcpy(json_area, new_json.data(), new_json.size());

    // LUKS2 spec: checksum is computed over the binary header (with the
    // csum field zeroed) concatenated with the full JSON area (including
    // padding). See hdr_checksum_calculate() in luks2_disk_metadata.c.
    char* binary_hdr = buf.data() + hdr_offset;
    memset(binary_hdr + LUKS2_CHECKSUM_OFFSET, 0, LUKS2_CHECKSUM_LEN);

    unsigned char checksum[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (!mdctx) {
      lderr(m_cct) << "failed to create EVP_MD_CTX" << dendl;
      return -ENOMEM;
    }
    EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(mdctx, binary_hdr, LUKS2_HDR_BIN_LEN);
    EVP_DigestUpdate(mdctx, json_area, json_area_size);
    EVP_DigestFinal_ex(mdctx, checksum, &md_len);
    EVP_MD_CTX_free(mdctx);

    memcpy(binary_hdr + LUKS2_CHECKSUM_OFFSET, checksum, md_len);
  }

  // write back to memfd
  if (lseek(m_fd, 0, SEEK_SET) < 0) {
    auto r = -errno;
    lderr(m_cct) << "failed to seek memfd for write: " << cpp_strerror(r)
                 << dendl;
    return r;
  }
  ssize_t bytes_written = ::write(m_fd, buf.data(), buf.size());
  if (bytes_written < 0 || static_cast<size_t>(bytes_written) != buf.size()) {
    lderr(m_cct) << "failed to write memfd" << dendl;
    return -EIO;
  }

  // reset file position so subsequent Header::read() works correctly
  lseek(m_fd, 0, SEEK_SET);

  ldout(m_cct, 20) << "rewrote segment for inline integrity: "
                   << encryption << " + " << integrity << dendl;
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
    ldout(m_cct, 20) << "crypt_load failed: " << cpp_strerror(r) << dendl;
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
  if (r < 0) {
    ldout(m_cct, 20) << "crypt_volume_key_get failed: " << cpp_strerror(r)
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

const char* Header::get_format_name() {
  ceph_assert(m_cd != nullptr);
  return crypt_get_type(m_cd);
}

int Header::get_integrity_info(struct crypt_params_integrity* ip) {
  ceph_assert(m_cd != nullptr);
  return crypt_get_integrity_info(m_cd, ip);
}

} // namespace luks
} // namespace crypto
} // namespace librbd
