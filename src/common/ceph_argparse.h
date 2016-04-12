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

/////////////////////// Types ///////////////////////
class CephInitParameters
{
public:
  explicit CephInitParameters(uint32_t module_type_);
  std::list<std::string> get_conf_files() const;

  uint32_t module_type;
  EntityName name;
};

/////////////////////// Functions ///////////////////////
extern void string_to_vec(std::vector<std::string>& args, std::string argstr);
extern void env_to_vec(std::vector<const char*>& args, const char *name=NULL);
extern void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args);
extern void vec_to_argv(const char *argv0, std::vector<const char*>& args,
			int *argc, const char ***argv);

extern bool parse_ip_port_vec(const char *s, std::vector<entity_addr_t>& vec);
bool ceph_argparse_double_dash(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i);
bool ceph_argparse_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, ...);
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret,
	std::ostream &oss, ...);
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...);
template<class T>
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, T *ret,
	std::ostream &oss, ...);
bool ceph_argparse_binary_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream *oss, ...);
extern CephInitParameters ceph_argparse_early_args
	    (std::vector<const char*>& args, uint32_t module_type, int flags,
	     std::string *cluster, std::string *conf_file_list);
extern void generic_server_usage();
extern void generic_client_usage();

#endif
