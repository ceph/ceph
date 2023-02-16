#include "rgw_common.h"
using namespace std;

bool RGWChecksum::is_supplied_checksum(const string &s) {
  if (s == RGW_ATTR_CHECKSUM_CRC32)
    return true;
  if (s == RGW_ATTR_CHECKSUM_SHA1)
    return true;
  return false;
}

int RGWChecksum::enable_supplied() {
  int enabled_num = 0;
  if (supplied_crc32_b64 != nullptr) {
    set_enable_crc32();
    enabled_num += 1;
  }
  if (supplied_sha1_b64 != nullptr) {
    set_enable_sha1();
    enabled_num += 1;
  }
  if (enabled_num > 1) {
    return -EINVAL;
  }
  return 0;
}

int RGWChecksum::check_specified_algorithm() {
  if (specified_algorithm == nullptr) {
    return 0;
  }
  bool valid_algorithm = false;
  if (strcmp(specified_algorithm, "CRC32") == 0) {
    if (!need_calc_crc32)
      return -EINVAL;
    valid_algorithm = true;
  }
  if (strcmp(specified_algorithm, "SHA1") == 0) {
    if (!need_calc_sha1)
      return -EINVAL;
    valid_algorithm = true;
  }
  if (!valid_algorithm)
    return -EINVAL;
  return 0;
}

void RGWChecksum::disable_all() {
  disable_crc32 = true;
  disable_sha1 = true;
  need_calc_crc32 = false;
  need_calc_sha1 = false;
}

int RGWChecksum::supplied_unarmor() {
  if (need_calc_crc32) {
    if (strlen(supplied_crc32_b64) > RGW_CHECKSUM_CRC32_ARMORED_SIZE) 
        return -ERR_INVALID_DIGEST;
    int ret = ceph_unarmor(
        supplied_crc32_bin,
        &supplied_crc32_bin[RGW_CHECKSUM_CRC32_DIGESTSIZE + 1],
        supplied_crc32_b64, supplied_crc32_b64 + strlen(supplied_crc32_b64));
    if (ret != RGW_CHECKSUM_CRC32_DIGESTSIZE) {
      return -ERR_INVALID_DIGEST;
    }
  }
  if (need_calc_sha1) {
    if (strlen(supplied_sha1_b64) > RGW_CHECKSUM_SHA1_ARMORED_SIZE) 
        return -ERR_INVALID_DIGEST;
    int ret = ceph_unarmor(
        supplied_sha1_bin, &supplied_sha1_bin[RGW_CHECKSUM_SHA1_DIGESTSIZE + 1],
        supplied_sha1_b64, supplied_sha1_b64 + strlen(supplied_sha1_b64));
    if (ret != RGW_CHECKSUM_SHA1_DIGESTSIZE) {
      return -ERR_INVALID_DIGEST;
    }
  }
  return 0;
}

void RGWChecksum::reset() {
  hash_crc32.reset();
  hash_sha1.Restart();
}

void RGWChecksum::update(bufferlist &data) {
  if (need_calc_crc32) {
    hash_crc32.process_bytes((const unsigned char *)data.c_str(),
                             data.length());
  }
  if (need_calc_sha1) {
    hash_sha1.Update((const unsigned char *)data.c_str(), data.length());
  }
}

void RGWChecksum::update(string &str) {
  char buf[400];
  if (need_calc_crc32) {
    hex_to_buf(str.c_str(), buf, RGW_CHECKSUM_CRC32_DIGESTSIZE);
    hash_crc32.process_bytes((const unsigned char *)&buf,
                             RGW_CHECKSUM_CRC32_DIGESTSIZE);
  }
  if (need_calc_sha1) {
    hex_to_buf(str.c_str(), buf, RGW_CHECKSUM_SHA1_DIGESTSIZE);
    hash_sha1.Update((const unsigned char *)&buf, RGW_CHECKSUM_SHA1_DIGESTSIZE);
  }
}

void RGWChecksum::final() {
  if (need_calc_crc32) {
    u_int32_t final_crc32 = hash_crc32();
    snprintf(final_crc32_str, RGW_CHECKSUM_CRC32_DIGESTSIZE * 2 + 1, "%08x",
             final_crc32);
    hex_to_buf(final_crc32_str, final_crc32_bin, RGW_CHECKSUM_CRC32_DIGESTSIZE);
  }
  if (need_calc_sha1) {
    hash_sha1.Final((unsigned char *)final_sha1_bin);
    buf_to_hex((unsigned char *)final_sha1_bin, RGW_CHECKSUM_SHA1_DIGESTSIZE,
               final_sha1_str);
  }
}

int RGWChecksum::check() {
  if (need_calc_crc32) {
    return strncmp(final_crc32_bin, supplied_crc32_bin,
                   RGW_CHECKSUM_CRC32_DIGESTSIZE);
  }
  if (need_calc_sha1) {
    return strncmp(final_sha1_bin, supplied_sha1_bin,
                   RGW_CHECKSUM_SHA1_DIGESTSIZE);
  }
  return 0;
}

void RGWChecksum::add_checksum_attr(map<string, bufferlist> &out_attrs) {
  if (need_calc_crc32) {
    bufferlist &bl = out_attrs[RGW_ATTR_PREFIX RGW_ATTR_CHECKSUM_CRC32];
    bl.clear();
    bl.append(final_crc32_str, RGW_CHECKSUM_CRC32_DIGESTSIZE * 2);
  }
  if (need_calc_sha1) {
    bufferlist &bl = out_attrs[RGW_ATTR_PREFIX RGW_ATTR_CHECKSUM_SHA1];
    bl.clear();
    bl.append(final_sha1_str, RGW_CHECKSUM_SHA1_DIGESTSIZE * 2);
  }
}

void RGWChecksum::add_checksum_attr(map<string, bufferlist> &out_attrs,
                                    long long partNumber) {
  char buf[20];
  snprintf(buf, sizeof(buf), "-%lld", partNumber);
  if (need_calc_crc32) {
    bufferlist &bl = out_attrs[RGW_ATTR_PREFIX RGW_ATTR_CHECKSUM_CRC32];
    bl.clear();
    bl.append(final_crc32_str, RGW_CHECKSUM_CRC32_DIGESTSIZE * 2);
    bl.append(buf, strlen(buf));
  }
  if (need_calc_sha1) {
    bufferlist &bl = out_attrs[RGW_ATTR_PREFIX RGW_ATTR_CHECKSUM_SHA1];
    bl.clear();
    bl.append(final_sha1_str, RGW_CHECKSUM_SHA1_DIGESTSIZE * 2);
    bl.append(buf, strlen(buf));
  }
}

void RGWChecksum::resp_armor(bufferlist &data, string &outstr) {
  if (need_calc_crc32) {
    hex_to_buf(data.c_str(), resp_crc32_bin, RGW_CHECKSUM_CRC32_DIGESTSIZE);
    const char *mult_sign = strchr(data.c_str(), '-');
    ceph_armor(resp_crc32_b64,
               resp_crc32_b64 + RGW_CHECKSUM_CRC32_ARMORED_SIZE + 1,
               resp_crc32_bin, resp_crc32_bin + RGW_CHECKSUM_CRC32_DIGESTSIZE);
    char *p_crc32_b64 = resp_crc32_b64 + RGW_CHECKSUM_CRC32_ARMORED_SIZE;
    if (mult_sign) {
      while (mult_sign - data.c_str() < data.length()) {
        *p_crc32_b64 = *mult_sign;
        p_crc32_b64++, mult_sign++;
      }
    }
    *p_crc32_b64 = '\0';
    outstr = string(resp_crc32_b64);
  }
  if (need_calc_sha1) {
    hex_to_buf(data.c_str(), resp_sha1_bin, RGW_CHECKSUM_SHA1_DIGESTSIZE);
    const char *mult_sign = strchr(data.c_str(), '-');
    ceph_armor(resp_sha1_b64,
               resp_sha1_b64 + RGW_CHECKSUM_SHA1_ARMORED_SIZE + 1,
               resp_sha1_bin, resp_sha1_bin + RGW_CHECKSUM_SHA1_DIGESTSIZE);
    char *p_sha1_b64 = resp_sha1_b64 + RGW_CHECKSUM_SHA1_ARMORED_SIZE;
    if (mult_sign) {
      while (mult_sign - data.c_str() < data.length()) {
        *p_sha1_b64 = *mult_sign;
        p_sha1_b64++, mult_sign++;
      }
    }
    *p_sha1_b64 = '\0';
    outstr = string(resp_sha1_b64);
  }
}

int RGWChecksum::check_upload_header(std::map<std::string, bufferlist> *attrs) {
  if (attrs && attrs->find(RGW_ATTR_CHECKSUM_ALGORITHM) != attrs->end()) {
    char* target_checksum_algorithm =
        (*attrs)[RGW_ATTR_CHECKSUM_ALGORITHM].c_str();
    if (std::strcmp(target_checksum_algorithm.c_str(), "CRC32") == 0 ||
       std::strcmp(target_checksum_algorithm.c_str(), "crc32") == 0) {
      if (!supplied_crc32_b64) {
        return -1;
      }
    }
    if (std::strcmp(target_checksum_algorithm.c_str(), "SHA1") == 0 ||
        std::strcmp(target_checksum_algorithm.c_str(), "sha1") == 0) {
      if (!supplied_sha1_b64) {
        return -1;
      }
    }
  }
  return 0;
}