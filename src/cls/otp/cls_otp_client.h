// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_OTP_CLIENT_H
#define CEPH_CLS_OTP_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls/otp/cls_otp_types.h"

namespace rados {
  namespace cls {
    namespace otp {

      class OTP {
      public:
        static void create(librados::ObjectWriteOperation *op, const otp_info_t& config);
        static void set(librados::ObjectWriteOperation *op, const list<otp_info_t>& entries);
        static void remove(librados::ObjectWriteOperation *op, const string& id);
        static int get(librados::ObjectReadOperation *op,
                       librados::IoCtx& ioctx, const string& oid,
                       const list<string> *ids, bool get_all, list<otp_info_t> *result);
        static int get(librados::ObjectReadOperation *op,
                       librados::IoCtx& ioctx, const string& oid,
                       const string& id, otp_info_t *result);
        static int get_all(librados::ObjectReadOperation *op,
                           librados::IoCtx& ioctx, const string& oid,
                           list<otp_info_t> *result);
        static int check(CephContext *cct, librados::IoCtx& ioctx, const string& oid,
                         const string& id, const string& val, otp_check_t *result);
        static int get_current_time(librados::IoCtx& ioctx, const string& oid,
                                    ceph::real_time *result);
      };

      class TOTPConfig {
        otp_info_t config;
        public:
          TOTPConfig(const string& id, const string& seed) {
            config.type = OTP_TOTP;
            config.id = id;
            config.seed = seed;
          }
          void set_step_size(int step_size) {
            config.step_size = step_size;
          }
          void set_window(int window) {
            config.window = window;
          }
          void get_config(otp_info_t *conf) {
            *conf = config;
          }
      };
    } // namespace otp
  }  // namespace cls
} // namespace rados

#endif
