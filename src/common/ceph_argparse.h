// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2008-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_ARGPARSE_H
#define CEPH_ARGPARSE_H

/*
 * Ceph argument parsing library
 *
 * We probably should eventually replace this with something standard like popt.
 * Until we do that, though, this file is the place for argv parsing
 * stuff to live.
 */

#include <deque>
#include <stdint.h>
#include <string>
#include <vector>

#include "common/entity_name.h"
#include "msg/msg_types.h"

/////////////////////// Macros ///////////////////////
#define FOR_EACH_ARG(args) \
	__isarg = 1 < args.size(); \
	for (unsigned i=0; i<args.size(); i++, __isarg = i+1 < args.size())

#define ARGS_USAGE() args_usage();

#define DEFINE_CONF_VARS(usage_func) \
	unsigned int val_pos __attribute__((unused)); \
	void (*args_usage)() __attribute__((unused)) = usage_func; \
	bool __isarg __attribute__((unused))

/////////////////////// Types ///////////////////////
class CephInitParameters
{
public:
  CephInitParameters(uint32_t module_type);
  std::list<std::string> get_conf_files() const;

  std::string conf_file;
  EntityName name;
};

/////////////////////// Functions ///////////////////////
extern void env_override(char **ceph_var, const char * const env_var);
extern void env_to_vec(std::vector<const char*>& args);
extern void env_to_deq(std::deque<const char*>& args);
extern void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args);
extern void argv_to_deq(int argc, const char **argv,
                 std::deque<const char*>& args);
extern void vec_to_argv(std::vector<const char*>& args,
                 int& argc, const char **&argv);

extern bool parse_ip_port_vec(const char *s, std::vector<entity_addr_t>& vec);
extern void parse_config_option_string(std::string& s);
bool ceph_argparse_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, ...);
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...);
extern CephInitParameters ceph_argparse_early_args
		(std::vector<const char*>& args, uint32_t module_type);
extern void generic_server_usage();
extern void generic_client_usage();

#endif
