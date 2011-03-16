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

#include "auth/Auth.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/ConfUtils.h"
#include "common/version.h"
#include "common/config.h"
#include "include/intarith.h"
#include "include/str_list.h"
#include "msg/msg_types.h"

#include <deque>
#include <stdarg.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <vector>

/*
 * Ceph argument parsing library
 *
 * We probably should eventually replace this with something standard like popt.
 * Until we do that, though, this file is the place for argv parsing
 * stuff to live.
 */

#undef dout
#undef pdout
#undef derr
#undef generic_dout
#undef dendl

void env_override(char **ceph_var, const char * const env_var)
{
  char *e = getenv(env_var);
  if (!e)
    return;
  if (*ceph_var)
    free(*ceph_var);
  *ceph_var = strdup(e);
}

void env_to_vec(std::vector<const char*>& args)
{
  char *p = getenv("CEPH_ARGS");
  if (!p) return;

  static char buf[1000];
  int len = MIN(strlen(p), sizeof(buf)-1);  // bleh.
  memcpy(buf, p, len);
  buf[len] = 0;
  //cout << "CEPH_ARGS='" << p << ";" << endl;

  p = buf;
  while (*p && p < buf + len) {
    char *e = p;
    while (*e && *e != ' ')
      e++;
    *e = 0;
    args.push_back(p);
    //cout << "arg " << p << std::endl;
    p = e+1;
  }
}

void env_to_deq(std::deque<const char*>& args)
{
  char *p = getenv("CEPH_ARGS");
  if (!p) return;

  static char buf[1000];
  int len = MIN(strlen(p), sizeof(buf)-1);  // bleh.
  memcpy(buf, p, len);
  buf[len] = 0;

  p = buf;
  while (*p && p < buf + len) {
    char *e = p;
    while (*e && *e != ' ')
      e++;
    *e = 0;
    args.push_back(p);
    p = e+1;
  }
}

void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args)
{
  for (int i=1; i<argc; i++)
    args.push_back(argv[i]);
}

void argv_to_deq(int argc, const char **argv,
                 std::deque<const char*>& args)
{
  for (int i=1; i<argc; i++)
    args.push_back(argv[i]);
}

void vec_to_argv(std::vector<const char*>& args,
                 int& argc, const char **&argv)
{
  const char *myname = "asdf";
  if (argc && argv)
    myname = argv[0];
  argv = (const char**)malloc(sizeof(char*) * argc);
  argc = 1;
  argv[0] = myname;

  for (unsigned i=0; i<args.size(); i++)
    argv[argc++] = args[i];
}

bool parse_ip_port_vec(const char *s, vector<entity_addr_t>& vec)
{
  const char *p = s;
  const char *end = p + strlen(p);
  while (p < end) {
    entity_addr_t a;
    //cout << " parse at '" << p << "'" << std::endl;
    if (!a.parse(p, &p)) {
      //dout(0) << " failed to parse address '" << p << "'" << dendl;
      return false;
    }
    //cout << " got " << a << ", rest is '" << p << "'" << std::endl;
    vec.push_back(a);
    while (*p == ',' || *p == ' ')
      p++;
  }
  return true;
}

void parse_config_option_string(std::string& s)
{
  char b[s.length()+1];
  strcpy(b, s.c_str());
  std::vector<const char*> nargs;
  char *p = b;
  while (*p) {
    nargs.push_back(p);
    while (*p && *p != ' ') p++;
    if (!*p)
      break;
    *p++ = 0;
    while (*p && *p == ' ') p++;
  }
  g_conf.parse_argv(nargs);
}

// The defaults for CephInitParameters
CephInitParameters::CephInitParameters(uint32_t module_type)
  : conf_file(CEPH_CONF_FILE_DEFAULT)
{
  const char *conf_file_ = getenv("CEPH_CONF");
  if (conf_file_)
    conf_file = conf_file_;
  name.set(module_type, "admin");
}

std::list<std::string> CephInitParameters::
get_conf_files() const
{
  std::list<std::string> ret;
  get_str_list(conf_file, ret);
  return ret;
}

bool ceph_argparse_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, ...)
{
  const char *first = *i;
  const char *a;
  va_list ap;

  va_start(ap, i);
  while (1) {
    a = va_arg(ap, char*);
    if (a == NULL)
      return false;
    if (strcmp(a, first) == 0) {
      i = args.erase(i);
      return true;
    }
  }
}

bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...)
{
  const char *first = *i;
  const char *a;
  va_list ap;
  int strlen_a;

  // does this argument match any of the possibilities?
  va_start(ap, ret);
  while (1) {
    a = va_arg(ap, char*);
    if (a == NULL)
      return false;
    strlen_a = strlen(a);
    if (strncmp(a, first, strlen(a)) == 0) {
      if (first[strlen_a] == '=') {
	*ret = first + strlen_a + 1;
	i = args.erase(i);
	return true;
      }
      else if (first[strlen_a] == '\0') {
	// find second part (or not)
	if (i+1 == args.end()) {
	  std::cerr << "Option " << *i << " requires an argument." << std::endl;
	  _exit(1);
	}
	i = args.erase(i);
	*ret = *i;
	i = args.erase(i);
	return true;
      }
    }
  }
}

CephInitParameters ceph_argparse_early_args
	  (std::vector<const char*>& args, uint32_t module_type)
{

  CephInitParameters iparams(module_type);
  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (strcmp(*i, "--") == 0)
      break;
    else if (ceph_argparse_flag(args, i, "--version", "-v", NULL)) {
      cout << pretty_version_to_str() << std::endl;
      _exit(0);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--conf", "-c", NULL)) {
      iparams.conf_file = val;
    }
    else if ((module_type != CEPH_ENTITY_TYPE_CLIENT) &&
	     (ceph_argparse_witharg(args, i, &val, "-i", NULL))) {
      iparams.name.set_id(val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--id", NULL)) {
      iparams.name.set_id(val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--name", "-n", NULL)) {
      if (!iparams.name.from_str(val)) {
	std::cerr << "You must pass a string of the form ID.TYPE to "
	  "the --name option." << std::endl;
	_exit(1);
      }
    }
    else {
      // ignore
      ++i;
    }
  }
  return iparams;
}

static void generic_usage(bool is_server)
{
  cout << "\
--conf/-c        Read configuration from the given configuration file\n\
-D               Run in the foreground.\n\
-f               Run in foreground. Show all log messages on stderr.\n\
--id             set ID\n\
--name           set ID.TYPE\n\
--version        show version and quit\n\
" << std::endl;

  if (is_server) {
    cout << "   --debug_ms N\n";
    cout << "        set message debug level (e.g. 1)\n";
  }
}

void generic_server_usage()
{
  generic_usage(true);
  exit(1);
}
void generic_client_usage()
{
  generic_usage(false);
  exit(1);
}
