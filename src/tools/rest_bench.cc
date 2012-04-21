// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"

#include "common/obj_bencher.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

#include "libs3.h"

#include <errno.h>

#define DEFAULT_USER_AGENT "rest-bench"
#define DEFAULT_BUCKET "rest-bench-bucket"

void usage(ostream& out)
{
  out <<					\
"usage: rest_bench [options] [commands]\n"
"COMMANDS\n"
"\n";
}

static void usage_exit()
{
  usage(cerr);
  exit(1);
}

class RESTBencher : public ObjBencher {
  void **completions;
protected:
  int completions_init(int concurrentios) {
    return 0;
  }
  void completions_done() {
    delete[] completions;
    completions = NULL;
  }
  int create_completion(int slot, void (*cb)(void *, void*), void *arg) {
    if (!completions[slot])
      return -EINVAL;

    return 0;
  }
  void release_completion(int slot) {

    completions[slot] = 0;
  }

  int aio_read(const std::string& oid, int slot, bufferlist *pbl, size_t len) {
  }

  int aio_write(const std::string& oid, int slot, const bufferlist& bl, size_t len) {
  }

  int sync_read(const std::string& oid, bufferlist& bl, size_t len) {
  }
  int sync_write(const std::string& oid, bufferlist& bl, size_t len) {
  }

  bool completion_is_done(int slot) {
  }

  int completion_wait(int slot) {
  }
  int completion_ret(int slot) {
  }

public:
  RESTBencher() : completions(NULL) {}
  ~RESTBencher() { }
};

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::map < std::string, std::string > opts;
  std::vector<const char*>::iterator i;
  std::string val;
  std::string host;
  std::string user_agent;
  std::string access_key;
  std::string secret;
  std::string bucket;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage(cout);
      exit(0);
    } else if (ceph_argparse_witharg(args, i, &user_agent, "--agent", (char*)NULL)) {
      /* nothing */
    } else if (ceph_argparse_witharg(args, i, &user_agent, "--access-key", (char*)NULL)) {
      /* nothing */
    } else if (ceph_argparse_witharg(args, i, &user_agent, "--secret", (char*)NULL)) {
      /* nothing */
    } else if (ceph_argparse_witharg(args, i, &user_agent, "--bucket", (char*)NULL)) {
      /* nothing */
    } else {
      if (val[0] == '-')
        usage_exit();
      i++;
    }
  }

  host = g_conf->host;

  if (host.empty()) {
    cerr << "rest-bench: host not provided." << std::endl;
    usage_exit();
  }

  if (access_key.empty() || secret.empty()) {
    cerr << "rest-bench: access key or secret was not provided" << std::endl;
    usage_exit();
  }

  if (bucket.empty()) {
    bucket = DEFAULT_BUCKET;
  }

  if (user_agent.empty())
    user_agent = DEFAULT_USER_AGENT;

  S3Status status = S3_initialize(user_agent.c_str(), S3_INIT_ALL, host.c_str());
  if (status != S3StatusOK) {
    cerr << "failed to init: " << S3_get_status_name(status) << std::endl;
  }

  RESTBencher bencher();
}

