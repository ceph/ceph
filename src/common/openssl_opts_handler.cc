// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "openssl_opts_handler.h"

#include <openssl/bio.h>
#include <openssl/conf.h>

#include "common/debug.h"
#include "global/global_context.h"
#include "include/str_list.h"
#include "include/scope_guard.h"

using std::string;
using std::string_view;
using std::ostream;

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_common
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream &_prefix(std::ostream *_dout)
{
  return *_dout << "OpenSSLOptsHandler: ";
}

// -----------------------------------------------------------------------------

static string get_openssl_error()
{
  BIO *bio = BIO_new(BIO_s_mem());
  if (bio == nullptr) {
    return "failed to create BIO for more error printing";
  }
  char* buf;
  size_t len = BIO_get_mem_data(bio, &buf);
  string ret(buf, len);
  BIO_free(bio);
  return ret;
}

static void log_error(const string_view &err)
{
  derr << "Intended OpenSSL acceleration failed.\n"
       << "set by openssl_conf = "
       << g_ceph_context->_conf.get_val<std::string>("openssl_conf")
       << "\ndetail error information:\n" << err << dendl;
}

static void load_openssl_modules(const string &openssl_conf)
{
  BIO *bio = BIO_new_file(openssl_conf.c_str(), "r");
  if (bio == nullptr) {
    log_error("failed to open openssl conf");
    return;
  }

  auto sg_bio = make_scope_guard([bio] { BIO_free(bio); });

  CONF *conf = NCONF_new(nullptr);
  if (conf == nullptr) {
    log_error("failed to new OpenSSL CONF");
    return;
  }

  auto sg_conf = make_scope_guard([conf] { NCONF_free(conf); });

  if (NCONF_load_bio(conf, bio, nullptr) <= 0) {
    log_error("failed to load CONF from BIO:\n" + get_openssl_error());
    return;
  }

  OPENSSL_load_builtin_modules();

  if (CONF_modules_load(
          conf, nullptr,
          CONF_MFLAGS_DEFAULT_SECTION | CONF_MFLAGS_IGNORE_MISSING_FILE) <= 0) {
    log_error("failed to load modules from CONF:\n" + get_openssl_error());
  }
}

static void init_openssl()
{
  string openssl_conf = g_ceph_context->_conf.get_val<std::string>("openssl_conf");
  if (openssl_conf.empty()) {
    return;
  }

  load_openssl_modules(openssl_conf);
}

void ceph::crypto::init_openssl_once()
{
  static std::once_flag flag;
  std::call_once(flag, init_openssl);
}
