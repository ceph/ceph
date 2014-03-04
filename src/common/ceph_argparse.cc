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
#include "common/ConfUtils.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/strtol.h"
#include "common/version.h"
#include "include/intarith.h"
#include "include/str_list.h"
#include "msg/msg_types.h"

#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <sstream>
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

void string_to_vec(std::vector<std::string>& args, std::string argstr)
{
  istringstream iss(argstr);
  while(iss) {
    string sub;
    iss >> sub;
    if (sub == "") break;
    args.push_back(sub);
  }
}

bool split_dashdash(const std::vector<const char*>& args,
		    std::vector<const char*>& options,
		    std::vector<const char*>& arguments) {
  bool dashdash = false;
  for (std::vector<const char*>::const_iterator i = args.begin();
       i != args.end();
       ++i) {
    if (dashdash) {
      arguments.push_back(*i);
    } else {
      if (strcmp(*i, "--") == 0)
	dashdash = true;
      else
	options.push_back(*i);
    }
  }
  return dashdash;
}

void env_to_vec(std::vector<const char*>& args, const char *name)
{
  if (!name)
    name = "CEPH_ARGS";
  char *p = getenv(name);
  if (!p)
    return;

  bool dashdash = false;
  std::vector<const char*> options;
  std::vector<const char*> arguments;
  if (split_dashdash(args, options, arguments))
    dashdash = true;

  std::vector<const char*> env_options;
  std::vector<const char*> env_arguments;
  static vector<string> str_vec;
  std::vector<const char*> env;
  str_vec.clear();
  get_str_vec(p, " ", str_vec);
  for (vector<string>::iterator i = str_vec.begin();
       i != str_vec.end();
       ++i)
    env.push_back(i->c_str());
  if (split_dashdash(env, env_options, env_arguments))
    dashdash = true;

  args.clear();
  args.insert(args.end(), options.begin(), options.end());
  args.insert(args.end(), env_options.begin(), env_options.end());
  if (dashdash)
    args.push_back("--");
  args.insert(args.end(), arguments.begin(), arguments.end());
  args.insert(args.end(), env_arguments.begin(), env_arguments.end());
}

void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args)
{
  for (int i=1; i<argc; i++)
    args.push_back(argv[i]);
}

void vec_to_argv(const char *argv0, std::vector<const char*>& args,
                 int *argc, const char ***argv)
{
  *argv = (const char**)malloc(sizeof(char*) * (args.size() + 1));
  if (!*argv)
    throw bad_alloc();
  *argc = 1;
  (*argv)[0] = argv0;

  for (unsigned i=0; i<args.size(); i++)
    (*argv)[(*argc)++] = args[i];
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
    while (*p == ',' || *p == ' ' || *p == ';')
      p++;
  }
  return true;
}

// The defaults for CephInitParameters
CephInitParameters::CephInitParameters(uint32_t module_type_)
  : module_type(module_type_)
{
  name.set(module_type, "admin");
}

static void dashes_to_underscores(const char *input, char *output)
{
  char c = 0;
  char *o = output;
  const char *i = input;
  // first two characters are copied as-is
  *o = *i++;
  if (*o++ == '\0')
    return;
  *o = *i++;
  if (*o++ == '\0')
    return;
  for (; ((c = *i)); ++i) {
    if (c == '=') {
      strcpy(o, i);
      return;
    }
    if (c == '-')
      *o++ = '_';
    else
      *o++ = c;
  }
  *o++ = '\0';
}

/** Once we see a standalone double dash, '--', we should remove it and stop
 * looking for any other options and flags. */
bool ceph_argparse_double_dash(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i)
{
  if (strcmp(*i, "--") == 0) {
    i = args.erase(i);
    return true;
  }
  return false;
}

bool ceph_argparse_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, ...)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;
  va_list ap;

  va_start(ap, i);
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL) {
      va_end(ap);
      return false;
    }
    char a2[strlen(a)+1];
    dashes_to_underscores(a, a2);
    if (strcmp(a2, first) == 0) {
      i = args.erase(i);
      va_end(ap);
      return true;
    }
  }
}

static bool va_ceph_argparse_binary_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream *oss, va_list ap)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;

  // does this argument match any of the possibilities?
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL)
      return false;
    int strlen_a = strlen(a);
    char a2[strlen_a+1];
    dashes_to_underscores(a, a2);
    if (strncmp(a2, first, strlen(a2)) == 0) {
      if (first[strlen_a] == '=') {
	i = args.erase(i);
	const char *val = first + strlen_a + 1;
	if ((strcmp(val, "true") == 0) || (strcmp(val, "1") == 0)) {
	  *ret = 1;
	  return true;
	}
	else if ((strcmp(val, "false") == 0) || (strcmp(val, "0") == 0)) {
	  *ret = 0;
	  return true;
	}
	if (oss) {
	  (*oss) << "Parse error parsing binary flag  " << a
	         << ". Expected true or false, but got '" << val << "'\n";
	}
	*ret = -EINVAL;
	return true;
      }
      else if (first[strlen_a] == '\0') {
	i = args.erase(i);
	*ret = 1;
	return true;
      }
    }
  }
}

bool ceph_argparse_binary_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream *oss, ...)
{
  bool r;
  va_list ap;
  va_start(ap, oss);
  r = va_ceph_argparse_binary_flag(args, i, ret, oss, ap);
  va_end(ap);
  return r;
}

static bool va_ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, va_list ap)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;

  // does this argument match any of the possibilities?
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL)
      return false;
    int strlen_a = strlen(a);
    char a2[strlen_a+1];
    dashes_to_underscores(a, a2);
    if (strncmp(a2, first, strlen(a2)) == 0) {
      if (first[strlen_a] == '=') {
	*ret = first + strlen_a + 1;
	i = args.erase(i);
	return true;
      }
      else if (first[strlen_a] == '\0') {
	// find second part (or not)
	if (i+1 == args.end()) {
	  cerr << "Option " << *i << " requires an argument." << std::endl;
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

bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...)
{
  bool r;
  va_list ap;
  va_start(ap, ret);
  r = va_ceph_argparse_witharg(args, i, ret, ap);
  va_end(ap);
  return r;
}

bool ceph_argparse_withint(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream *oss, ...)
{
  bool r;
  va_list ap;
  std::string str;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, &str, ap);
  va_end(ap);
  if (!r) {
    return false;
  }

  std::string err;
  int myret = strict_strtol(str.c_str(), 10, &err);
  *ret = myret;
  if (!err.empty()) {
    *oss << err;
  }
  return true;
}

bool ceph_argparse_withlonglong(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, long long *ret,
	std::ostream *oss, ...)
{
  bool r;
  va_list ap;
  std::string str;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, &str, ap);
  va_end(ap);
  if (!r) {
    return false;
  }

  std::string err;
  long long myret = strict_strtoll(str.c_str(), 10, &err);
  *ret = myret;
  if (!err.empty()) {
    *oss << err;
  }
  return true;
}

bool ceph_argparse_withfloat(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, float *ret,
	std::ostream *oss, ...)
{
  bool r;
  va_list ap;
  std::string str;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, &str, ap);
  va_end(ap);
  if (!r) {
    return false;
  }

  std::string err;
  float myret = strict_strtof(str.c_str(), &err);
  *ret = myret;
  if (!err.empty()) {
    *oss << err;
  }
  return true;
}

CephInitParameters ceph_argparse_early_args
	  (std::vector<const char*>& args, uint32_t module_type, int flags,
	   std::string *cluster, std::string *conf_file_list)
{
  CephInitParameters iparams(module_type);
  std::string val;

  vector<const char *> orig_args = args;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (strcmp(*i, "--") == 0) {
      /* Normally we would use ceph_argparse_double_dash. However, in this
       * function we *don't* want to remove the double dash, because later
       * argument parses will still need to see it. */
      break;
    }
    else if (ceph_argparse_flag(args, i, "--version", "-v", (char*)NULL)) {
      cout << pretty_version_to_str() << std::endl;
      _exit(0);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--conf", "-c", (char*)NULL)) {
      *conf_file_list = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--cluster", (char*)NULL)) {
      *cluster = val;
    }
    else if ((module_type != CEPH_ENTITY_TYPE_CLIENT) &&
	     (ceph_argparse_witharg(args, i, &val, "-i", (char*)NULL))) {
      iparams.name.set_id(val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--id", "--user", (char*)NULL)) {
      iparams.name.set_id(val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--name", "-n", (char*)NULL)) {
      if (!iparams.name.from_str(val)) {
	cerr << "error parsing '" << val << "': expected string of the form TYPE.ID, "
	     << "valid types are: " << EntityName::get_valid_types_as_str()
	     << std::endl;
	_exit(1);
      }
    }
    else if (ceph_argparse_flag(args, i, "--show_args", (char*)NULL)) {
      cout << "args: ";
      for (std::vector<const char *>::iterator ci = orig_args.begin(); ci != orig_args.end(); ++ci) {
        if (ci != orig_args.begin())
          cout << " ";
        cout << *ci;
      }
      cout << std::endl;
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
  --conf/-c FILE    read configuration from the given configuration file\n\
  --id/-i ID        set ID portion of my name\n\
  --name/-n TYPE.ID set name\n\
  --cluster NAME    set cluster name (default: ceph)\n\
  --version         show version and quit\n\
" << std::endl;

  if (is_server) {
    cout << "\
  -d                run in foreground, log to stderr.\n\
  -f                run in foreground, log to usual location.\n";
    cout << "\
  --debug_ms N      set message debug level (e.g. 1)\n";
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
