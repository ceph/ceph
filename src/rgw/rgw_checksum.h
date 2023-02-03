#ifndef CEPH_RGW_CHECKSUM_H
#define CEPH_RGW_CHECKSUM_H
#include "common/armor.h"
#include "common/ceph_context.h"
#include "common/ceph_crypto.h"
#include <boost/crc.hpp>
#include <string>
#include <map>
using namespace std;

#define RGW_CHECKSUM_CRC32_DIGESTSIZE 4
#define RGW_CHECKSUM_CRC32_ARMORED_SIZE 8
#define RGW_CHECKSUM_SHA1_DIGESTSIZE 20
#define RGW_CHECKSUM_SHA1_ARMORED_SIZE 28

typedef boost::crc_optimal<32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true>
    crc32_type;

class RGWChecksum {
  bool need_calc_crc32;
  char supplied_crc32_bin[RGW_CHECKSUM_CRC32_DIGESTSIZE + 1];
  char final_crc32_bin[RGW_CHECKSUM_CRC32_DIGESTSIZE + 1];
  char final_crc32_str[RGW_CHECKSUM_CRC32_DIGESTSIZE * 2 + 1];
  char resp_crc32_bin[RGW_CHECKSUM_CRC32_DIGESTSIZE * 2 + 16];
  char resp_crc32_b64[RGW_CHECKSUM_CRC32_DIGESTSIZE * 2 + 16];
  crc32_type hash_crc32;
  bool need_calc_sha1;
  char supplied_sha1_bin[RGW_CHECKSUM_SHA1_DIGESTSIZE + 1];
  char final_sha1_bin[RGW_CHECKSUM_SHA1_DIGESTSIZE + 1];
  char final_sha1_str[RGW_CHECKSUM_SHA1_DIGESTSIZE * 2 + 1];
  char resp_sha1_bin[RGW_CHECKSUM_SHA1_DIGESTSIZE * 2 + 16];
  char resp_sha1_b64[RGW_CHECKSUM_SHA1_DIGESTSIZE * 2 + 16];
  ceph::crypto::SHA1 hash_sha1;

public:
  bool disable_crc32;
  bool disable_sha1;
  const char *supplied_crc32_b64;
  const char *supplied_sha1_b64;
  const char *specified_algorithm;
  RGWChecksum() {}

  static bool is_supplied_checksum(const string &name);

  void init(CephContext *cct) {
    supplied_crc32_b64 = nullptr;
    supplied_sha1_b64 = nullptr;
    need_calc_crc32 = false;
    need_calc_sha1 = false;
    disable_crc32 = false;
    disable_sha1 = false;
    reset();
    specified_algorithm = nullptr;
    /* assume x-amz-checksum are always enabled for now
    if (!cct->_conf.get_val<bool>("rgw_enable_checksum_crc32")) {
      disable_crc32 = true;
    }
    if (!cct->_conf.get_val<bool>("rgw_enable_checksum_sha1")) {
      disable_sha1 = true;
    }
    */
  }

  int enable_supplied();

  int check_specified_algorithm();

  void enable_by_name(const string& name) {
    if (std::strcmp(name.c_str(), "CRC32") == 0) {
      set_enable_crc32();
    }
    if (std::strcmp(name.c_str(), "SHA1") == 0) {
      set_enable_sha1();
    }
  }

  void set_enable_crc32() { need_calc_crc32 = true && (!disable_crc32); }

  void set_enable_sha1() { need_calc_sha1 = true && (!disable_sha1); }

  void disable_all();

  int supplied_unarmor();
  void reset();

  void update(bufferlist &data);

  void update(string &str);

  void final();
  int check();

  void add_checksum_attr(map<string, bufferlist> &out_attrs);

  void add_checksum_attr(map<string, bufferlist> &out_attrs,
                         long long partNumber);

  void resp_armor(bufferlist &data, string &outstr);

  int check_upload_header(std::map<string, bufferlist> *attrs);
};

#endif