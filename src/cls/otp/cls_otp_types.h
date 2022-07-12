// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_OTP_TYPES_H
#define CEPH_CLS_OTP_TYPES_H

#include "include/encoding.h"
#include "include/types.h"


#define CLS_OTP_MAX_REPO_SIZE 100

class JSONObj;

namespace rados {
  namespace cls {
    namespace otp {

      enum OTPType {
        OTP_UNKNOWN = 0,
        OTP_HOTP = 1,  /* unsupported */
        OTP_TOTP = 2,
      };

      enum SeedType {
        OTP_SEED_UNKNOWN = 0,
        OTP_SEED_HEX = 1,
        OTP_SEED_BASE32 = 2,
      };

      struct otp_info_t {
        OTPType type{OTP_TOTP};
        std::string id;
        std::string seed;
        SeedType seed_type{OTP_SEED_UNKNOWN};
        ceph::buffer::list seed_bin; /* parsed seed, built automatically by otp_set_op,
                              * not being json encoded/decoded on purpose
                              */
        int32_t time_ofs{0};
        uint32_t step_size{30}; /* num of seconds foreach otp to test */
        uint32_t window{2}; /* num of otp after/before start otp to test */

        otp_info_t() {}

        void encode(ceph::buffer::list &bl) const {
          ENCODE_START(1, 1, bl);
          encode((uint8_t)type, bl);
          /* if we ever implement anything other than TOTP
           * then we'll need to branch here */
          encode(id, bl);
          encode(seed, bl);
          encode((uint8_t)seed_type, bl);
          encode(seed_bin, bl);
          encode(time_ofs, bl);
          encode(step_size, bl);
          encode(window, bl);
          ENCODE_FINISH(bl);
        }
        void decode(ceph::buffer::list::const_iterator &bl) {
          DECODE_START(1, bl);
          uint8_t t;
          decode(t, bl);
          type = (OTPType)t;
          decode(id, bl);
          decode(seed, bl);
          uint8_t st;
          decode(st, bl);
          seed_type = (SeedType)st;
          decode(seed_bin, bl);
          decode(time_ofs, bl);
          decode(step_size, bl);
          decode(window, bl);
          DECODE_FINISH(bl);
        }
        void dump(ceph::Formatter *f) const;
        void decode_json(JSONObj *obj);
      };
      WRITE_CLASS_ENCODER(rados::cls::otp::otp_info_t)

      enum OTPCheckResult {
        OTP_CHECK_UNKNOWN = 0,
        OTP_CHECK_SUCCESS = 1,
        OTP_CHECK_FAIL = 2,
      };

      struct otp_check_t {
        std::string token;
        ceph::real_time timestamp;
        OTPCheckResult result{OTP_CHECK_UNKNOWN};

        void encode(ceph::buffer::list &bl) const {
          ENCODE_START(1, 1, bl);
          encode(token, bl);
          encode(timestamp, bl);
          encode((char)result, bl);
          ENCODE_FINISH(bl);
        }
        void decode(ceph::buffer::list::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(token, bl);
          decode(timestamp, bl);
          uint8_t t;
          decode(t, bl);
          result = (OTPCheckResult)t;
          DECODE_FINISH(bl);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::otp::otp_check_t)

      struct otp_repo_t {
        std::map<std::string, otp_info_t> entries;

        otp_repo_t() {}

        void encode(ceph::buffer::list &bl) const {
          ENCODE_START(1, 1, bl);
          encode(entries, bl);
          ENCODE_FINISH(bl);
        }
        void decode(ceph::buffer::list::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(entries, bl);
          DECODE_FINISH(bl);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::otp::otp_repo_t)
    }
  }
}

WRITE_CLASS_ENCODER(rados::cls::otp::otp_info_t)
WRITE_CLASS_ENCODER(rados::cls::otp::otp_check_t)
WRITE_CLASS_ENCODER(rados::cls::otp::otp_repo_t)

#endif
