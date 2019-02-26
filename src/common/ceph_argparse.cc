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
#include <stdarg.h>

#include "auth/Auth.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/version.h"
#include "include/str_list.h"

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

struct strict_str_convert {
  const char *str;
  std::string *err;
  strict_str_convert(const char *str,  std::string *err)
    : str(str), err(err) {}

  inline operator float() const
  {
    return strict_strtof(str, err);
  }
  inline operator int() const
  {
    return strict_strtol(str, 10, err);
  }
  inline operator long long() const
  {
    return  strict_strtoll(str, 10, err);
  }
};

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

static std::mutex g_str_vec_lock;
static vector<string> g_str_vec;

void clear_g_str_vec()
{
  g_str_vec_lock.lock();
  g_str_vec.clear();
  g_str_vec_lock.unlock();
}

void env_to_vec(std::vector<const char*>& args, const char *name)
{
  if (!name)
    name = "CEPH_ARGS";

  bool dashdash = false;
  std::vector<const char*> options;
  std::vector<const char*> arguments;
  if (split_dashdash(args, options, arguments))
    dashdash = true;

  std::vector<const char*> env_options;
  std::vector<const char*> env_arguments;
  std::vector<const char*> env;

  /*
   * We can only populate str_vec once. Other threads could hold pointers into
   * it, so clearing it out and replacing it is not currently safe.
   */
  g_str_vec_lock.lock();
  if (g_str_vec.empty()) {
    char *p = getenv(name);
    if (!p) {
      g_str_vec_lock.unlock();
      return;
    }
    get_str_vec(p, " ", g_str_vec);
  }
  g_str_vec_lock.unlock();

  vector<string>::iterator i;
  for (i = g_str_vec.begin(); i != g_str_vec.end(); ++i)
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
  args.insert(args.end(), argv + 1, argv + argc);
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

void ceph_arg_value_type(const char * nextargstr, bool *bool_option, bool *bool_numeric)
{
  bool is_numeric = true;
  bool is_float = false;
  bool is_option;

  if (nextargstr == NULL) {
    return;
  }

  if (strlen(nextargstr) < 2) {
    is_option = false;
  } else {
    is_option = (nextargstr[0] == '-') && (nextargstr[1] == '-');
  }

  for (unsigned int i = 0; i < strlen(nextargstr); i++) {
    if (!(nextargstr[i] >= '0' && nextargstr[i] <= '9')) {
      // May be negative numeral value
      if ((i == 0) && (strlen(nextargstr) >= 2))  {
	if (nextargstr[0] == '-')
	  continue;
      }
      if ( (nextargstr[i] == '.') && (is_float == false) ) {
        is_float = true;
        continue;
      }
        
      is_numeric = false;
      break;
    }
  }

  // -<option>
  if (nextargstr[0] == '-' && is_numeric == false) {
    is_option = true;
  }

  *bool_option = is_option;
  *bool_numeric = is_numeric;

  return;
}


bool parse_ip_port_vec(const char *s, vector<entity_addrvec_t>& vec, int type)
{
  // first split by [ ;], which are not valid for an addrvec
  list<string> items;
  get_str_list(s, " ;", items);

  for (auto& i : items) {
    const char *s = i.c_str();
    while (*s) {
      const char *end;

      // try parsing as an addr
      entity_addr_t a;
      if (a.parse(s, &end, type)) {
	vec.push_back(entity_addrvec_t(a));
	s = end;
	if (*s == ',') {
	  ++s;
	}
	continue;
      }

      // ok, try parsing as an addrvec
      entity_addrvec_t av;
      if (!av.parse(s, &end)) {
	return false;
      }
      vec.push_back(av);
      s = end;
      if (*s == ',') {
	++s;
      }
    }
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

static int va_ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret,
	std::ostream &oss, va_list ap)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;

  // does this argument match any of the possibilities?
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL)
      return 0;
    int strlen_a = strlen(a);
    char a2[strlen_a+1];
    dashes_to_underscores(a, a2);
    if (strncmp(a2, first, strlen(a2)) == 0) {
      if (first[strlen_a] == '=') {
	*ret = first + strlen_a + 1;
	i = args.erase(i);
	return 1;
      }
      else if (first[strlen_a] == '\0') {
	// find second part (or not)
	if (i+1 == args.end()) {
	  oss << "Option " << *i << " requires an argument." << std::endl;
	  i = args.erase(i);
	  return -EINVAL;
	}
	i = args.erase(i);
	*ret = *i;
	i = args.erase(i);
	return 1;
      }
    }
  }
}

template<class T>
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, T *ret,
	std::ostream &oss, ...)
{
  int r;
  va_list ap;
  bool is_option = false;
  bool is_numeric = true;
  std::string str;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, &str, oss, ap);
  va_end(ap);
  if (r == 0) {
    return false;
  } else if (r < 0) {
    return true;
  }

  ceph_arg_value_type(str.c_str(), &is_option, &is_numeric);
  if ((is_option == true) || (is_numeric == false)) {
    *ret = EXIT_FAILURE;
    if (is_option == true) {
      oss << "Missing option value";
    } else {
      oss << "The option value '" << str << "' is invalid";
    }
    return true;
  }

  std::string err;
  T myret = strict_str_convert(str.c_str(), &err);
  *ret = myret;
  if (!err.empty()) {
    oss << err;
  }
  return true;
}

template bool ceph_argparse_witharg<int>(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream &oss, ...);

template bool ceph_argparse_witharg<long long>(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, long long *ret,
	std::ostream &oss, ...);

template bool ceph_argparse_witharg<float>(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, float *ret,
	std::ostream &oss, ...);

bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret,
	std::ostream &oss, ...)
{
  int r;
  va_list ap;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, ret, oss, ap);
  va_end(ap);
  return r != 0;
}

bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...)
{
  int r;
  va_list ap;
  va_start(ap, ret);
  r = va_ceph_argparse_witharg(args, i, ret, cerr, ap);
  va_end(ap);
  if (r < 0)
    _exit(1);
  return r != 0;
}

CephInitParameters ceph_argparse_early_args
	  (std::vector<const char*>& args, uint32_t module_type,
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
  cout <<
    "  --conf/-c FILE    read configuration from the given configuration file" << std::endl <<
    (is_server ?
    "  --id/-i ID        set ID portion of my name" :
    "  --id ID           set ID portion of my name") << std::endl <<
    "  --name/-n TYPE.ID set name" << std::endl <<
    "  --cluster NAME    set cluster name (default: ceph)" << std::endl <<
    "  --setuser USER    set uid to user or uid (and gid to user's gid)" << std::endl <<
    "  --setgroup GROUP  set gid to group or gid" << std::endl <<
    "  --version         show version and quit" << std::endl
    << std::endl;

  if (is_server) {
    cout <<
      "  -d                run in foreground, log to stderr" << std::endl <<
      "  -f                run in foreground, log to usual location" << std::endl <<
      std::endl <<
      "  --debug_ms N      set message debug level (e.g. 1)" << std::endl;
  }

  cout.flush();
}

bool ceph_argparse_need_usage(const std::vector<const char*>& args)
{
  if (args.empty()) {
    return true;
  }
  for (auto a : args) {
    if (strcmp(a, "-h") == 0 ||
	strcmp(a, "--help") == 0) {
      return true;
    }
  }
  return false;
}

void generic_server_usage()
{
  generic_usage(true);
}

void generic_client_usage()
{
  generic_usage(false);
}
