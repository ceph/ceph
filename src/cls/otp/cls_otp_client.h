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
        static void set(librados::ObjectWriteOperation *op, const std::list<otp_info_t>& entries);
        static void remove(librados::ObjectWriteOperation *op, const std::string& id);
        static int get(librados::ObjectReadOperation *op,
                       librados::IoCtx& ioctx, const std::string& oid,
                       const std::string& id, otp_info_t *result);
        static int get_all(librados::ObjectReadOperation *op,
                           librados::IoCtx& ioctx, const std::string& oid,
                           std::list<otp_info_t> *result);
// these overloads which call io_ctx.operate() or io_ctx.exec() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()/exec()
#ifndef CLS_CLIENT_HIDE_IOCTX
        static int get(librados::ObjectReadOperation *op,
                       librados::IoCtx& ioctx, const std::string& oid,
                       const std::list<std::string> *ids, bool get_all, std::list<otp_info_t> *result);
        static int check(CephContext *cct, librados::IoCtx& ioctx, const std::string& oid,
                         const std::string& id, const std::string& val, otp_check_t *result);
        static int get_current_time(librados::IoCtx& ioctx, const std::string& oid,
                                    ceph::real_time *result);
#endif
      };

      class TOTPConfig {
        otp_info_t config;
        public:
          TOTPConfig(const std::string& id, const std::string& seed) {
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
