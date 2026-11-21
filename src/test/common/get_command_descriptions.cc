// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include <regex>
#include "mon/Monitor.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

using namespace std;

static void usage(ostream &out)
{
  out << "usage: get_command_descriptions [options ...]" << std::endl;
  out << "print on stdout the result of JSON formatted options\n";
  out << "found in mon/MonCommands.h as produced by the\n";
  out << "Monitor.cc::get_command_descriptions function.\n";
  out << "Designed as a helper for ceph_argparse.py unit tests.\n";
  out << "\n";
  out << "  --all               all of mon/MonCommands.h \n";
  out << "  --pull585           reproduce the bug fixed by #585\n";
  out << "  --req-fasle         a command of an earlier release with req=fasle\n";
  out << "\n";
  out << "Examples:\n";
  out << "  get_command_descriptions --all\n";
  out << "  get_command_descriptions --pull585\n";
}

static void json_print(const std::vector<MonCommand> &mon_commands)
{
  bufferlist rdata;
  auto f = Formatter::create_unique("json");
  Monitor::format_command_descriptions(mon_commands, f.get(),
                                       CEPH_FEATURES_ALL, &rdata);
  string data(rdata.c_str(), rdata.length());
  cout << data << std::endl;
}

// the monitor reads any value but "true" as false, as it must for the
// commands of a leader from an earlier release, so catch typos here
static void check_bool_args(const std::vector<MonCommand> &mon_commands)
{
  static const std::regex bool_arg{"[ ,](req|positional)=([^ ,]*)"};
  for (const auto& cmd : mon_commands) {
    for (std::sregex_iterator m{cmd.cmdstring.begin(), cmd.cmdstring.end(),
                                bool_arg}, end; m != end; ++m) {
      if ((*m)[2] != "true" && (*m)[2] != "false") {
        cerr << (*m)[1] << " must be true or false in \"" << cmd.cmdstring
             << "\"" << std::endl;
        exit(1);
      }
    }
  }
}

static void all()
{
#undef FLAG
#undef COMMAND
#undef COMMAND_WITH_FLAG
  std::vector<MonCommand> mon_commands = {
#define FLAG(f) (MonCommand::FLAG_##f)
#define COMMAND(parsesig, helptext, modulename, req_perms)	\
    {parsesig, helptext, modulename, req_perms, 0},
#define COMMAND_WITH_FLAG(parsesig, helptext, modulename, req_perms, flags) \
    {parsesig, helptext, modulename, req_perms, flags},
#include <mon/MonCommands.h>
#undef COMMAND
#undef COMMAND_WITH_FLAG

#define COMMAND(parsesig, helptext, modulename, req_perms)	\
  {parsesig, helptext, modulename, req_perms, FLAG(MGR)},
#define COMMAND_WITH_FLAG(parsesig, helptext, modulename, req_perms, flags) \
  {parsesig, helptext, modulename, req_perms, flags | FLAG(MGR)},
#include <mgr/MgrCommands.h>
 #undef COMMAND
#undef COMMAND_WITH_FLAG
 };

  check_bool_args(mon_commands);
  json_print(mon_commands);
}

// syntax error https://github.com/ceph/ceph/pull/585
static void pull585()
{
  std::vector<MonCommand> mon_commands = {
    { "osd pool create "		       
      "name=pool,type=CephPoolname " 
      "name=pg_num,type=CephInt,range=0,req=false "
      "name=pgp_num,type=CephInt,range=0,req=false" // !!! missing trailing space
      "name=properties,type=CephString,n=N,req=false,goodchars=[A-Za-z0-9-_.=]", 
      "create pool", "osd", "rw" }
  };

  json_print(mon_commands);
}

// a leader from a release before Vampire sends this typo
static void req_fasle()
{
  std::vector<MonCommand> mon_commands = {
    { "osd pool application get "
      "name=pool,type=CephPoolname,req=fasle "
      "name=app,type=CephString,req=false "
      "name=key,type=CephString,req=false",
      "get value of key <key> of application <app> on pool <poolname>",
      "osd", "r" }
  };

  json_print(mon_commands);
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  if (args.empty()) {
    usage(cerr);
    exit(1);
  }
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
    string err;

    if (*i == string("help") || *i == string("-h") || *i == string("--help")) {
      usage(cout);
      exit(0);
    } else if (*i == string("--all")) {
      all();
    } else if (*i == string("--pull585")) {
      pull585();
    } else if (*i == string("--req-fasle")) {
      req_fasle();
    }
  }  
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; 
 *   make get_command_descriptions && 
 *   ./get_command_descriptions --all --pull585"
 * End:
 */

