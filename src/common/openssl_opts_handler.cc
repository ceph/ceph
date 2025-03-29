// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2020 Huawei Technologies Co., Ltd.
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
#ifndef OPENSSL_NO_ENGINE
#include <openssl/engine.h>
#endif
#include <mutex>
#include <vector>
#include <algorithm>

#include "common/debug.h"
#include "global/global_context.h"
#include "include/str_list.h"
#include "include/scope_guard.h"

using std::string;
using std::ostream;
using std::vector;

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_common
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream &_prefix(std::ostream *_dout)
{
  return *_dout << "OpenSSLOptsHandler: ";
}

#ifndef OPENSSL_NO_ENGINE

// -----------------------------------------------------------------------------

string construct_engine_conf(const string &opts)
{
  const string conf_header = "openssl_conf=openssl_def\n[openssl_def]\n";
  const string engine_header = "engines=engine_section\n[engine_section]\n";

  string engine_id, engine_statement, engine_detail;
  const string id_prefix = "engine";
  const string suffix = "_section";
  const char delimiter = '\n';

  int index = 1;
  vector<string> confs = get_str_vec(opts, ":");
  for (auto conf : confs) {
    // Construct engine section statement like "engine1=engine1_section"
    engine_id = id_prefix + std::to_string(index++);
    engine_statement += engine_id + "=" + engine_id + suffix + delimiter;

    // Adapt to OpenSSL parser
    // Replace ',' with '\n' and add section in front
    std::replace(conf.begin(), conf.end(), ',', delimiter);
    engine_detail += "[" + engine_id + suffix + "]" + delimiter;
    engine_detail += conf + delimiter;
  }

  return conf_header + engine_header + engine_statement + engine_detail;
}

string get_openssl_error()
{
  BIO *bio = BIO_new(BIO_s_mem());
  if (bio == nullptr) {
    return "failed to create BIO for more error printing";
  }
  ERR_print_errors(bio);
  char* buf;
  size_t len = BIO_get_mem_data(bio, &buf);
  string ret(buf, len);
  BIO_free(bio);
  return ret;
}

void log_error(const string &err)
{
  derr << "Intended OpenSSL engine acceleration failed.\n"
       << "set by openssl_engine_opts = "
       << g_ceph_context->_conf->openssl_engine_opts
       << "\ndetail error information:\n" << err << dendl;
}

void load_module(const string &engine_conf)
{
  BIO *mem = BIO_new_mem_buf(engine_conf.c_str(), engine_conf.size());
  if (mem == nullptr) {
    log_error("failed to new BIO memory");
    return;
  }
  auto sg_mem = make_scope_guard([&mem] { BIO_free(mem); });

  CONF *conf = NCONF_new(nullptr);
  if (conf == nullptr) {
    log_error("failed to new OpenSSL CONF");
    return;
  }
  auto sg_conf = make_scope_guard([&conf] { NCONF_free(conf); });

  if (NCONF_load_bio(conf, mem, nullptr) <= 0) {
    log_error("failed to load CONF from BIO:\n" + get_openssl_error());
    return;
  }

  OPENSSL_load_builtin_modules();
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
  ENGINE_load_builtin_engines();
#pragma clang diagnostic pop
#pragma GCC diagnostic pop

  if (CONF_modules_load(
          conf, nullptr,
          CONF_MFLAGS_DEFAULT_SECTION | CONF_MFLAGS_IGNORE_MISSING_FILE) <= 0) {
    log_error("failed to load modules from CONF:\n" + get_openssl_error());
  }
}
#endif // !OPENSSL_NO_ENGINE

void init_engine()
{
  string opts = g_ceph_context->_conf->openssl_engine_opts;
  if (opts.empty()) {
    return;
  }
#ifdef OPENSSL_NO_ENGINE
  derr << "OpenSSL is compiled with no engine, but openssl_engine_opts is set" << dendl;
#else
  string engine_conf = construct_engine_conf(opts);
  load_module(engine_conf);
#endif
}

void ceph::crypto::init_openssl_engine_once()
{
  static std::once_flag flag;
  std::call_once(flag, init_engine);
}
