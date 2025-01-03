// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"
#include "include/str_list.h"

#include "rgw_token.h"
#include "rgw_b64.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using std::get;
  using std::string;

  RGWToken::token_type type{RGWToken::TOKEN_NONE};
  string access_key{""};
  string secret_key{""};

  Formatter* token_formatter{nullptr};

  bool verbose {false};
  bool do_encode {false};
  bool do_decode {false};

}

using namespace std;

void usage()
{
  cout << "usage: radosgw-token --encode --ttype=<token type> [options...]" << std::endl;
  cout << "\t(maybe exporting RGW_ACCESS_KEY_ID and RGW_SECRET_ACCESS_KEY)"    
       << std::endl;
  cout << "\t <token type> := ad | ldap" << std::endl;
  cout << "\n";
  generic_client_usage();
}

// This has an uncaught exception. Even if the exception is caught, the program
// would need to be terminated, so the warning is simply suppressed.
// coverity[root_function:SUPPRESS]
int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  std::string val;
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  char *v{nullptr};
  v = getenv("RGW_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }

  v = getenv("RGW_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--ttype",
				     (char*) nullptr)) {
      for (const auto& ttype : {"ad", "ldap"}) {
	if (boost::iequals(val, ttype)) {
	  type = RGWToken::to_type(val);
	  break;
	}
      }
    } else if (ceph_argparse_flag(args, arg_iter, "--encode",
					    (char*) nullptr)) {
      do_encode = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--decode",
					    (char*) nullptr)) {
      do_decode = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--verbose",
					    (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  if ((! do_encode) ||
      (type == RGWToken::TOKEN_NONE)) {
    return -EINVAL;
  }

  token_formatter = new JSONFormatter(true /* pretty */);

  RGWToken token(type, access_key, secret_key);
  if (do_encode) {
    token.encode_json(token_formatter);
    std::ostringstream os;
    token_formatter->flush(os);
    string token_str = os.str();
    if (verbose) {
      std::cout << "expanded token: " << token_str << std::endl;
      if (do_decode) {
	RGWToken token2(token_str);
	std::cout << "decoded expanded token: " << token2 << std::endl;
      }
    }
    std::cout << to_base64(token_str) << std::endl;
  }

  return 0;
}
