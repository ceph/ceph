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
#include "config.h"
#include "include/intarith.h"
#include "include/str_list.h"
#include "msg/msg_types.h"

#include <deque>
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

static void env_override(char **ceph_var, const char * const env_var)
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
  parse_config_options(nargs);
}

void parse_startup_config_options(std::vector<const char*>& args,
			  const char *module_type, int flags,
			  bool *force_fg_logging)
{
  bool show_config = false;
  DEFINE_CONF_VARS(NULL);
  std::vector<const char *> nargs;
  bool conf_specified = false;
  *force_fg_logging = ((flags & STARTUP_FLAG_FORCE_FG_LOGGING) != 0);

  if (!g_conf.type)
    g_conf.type = (char *)"";

  bool isdaemon = g_conf.daemonize;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("version", 'v')) {
      cout << ceph_version_to_string() << std::endl;
      _exit(0);
    } else if (CONF_ARG_EQ("conf", 'c')) {
	CONF_SAFE_SET_ARG_VAL(&g_conf.conf, OPT_STR);
	conf_specified = true;
    } else if (CONF_ARG_EQ("monmap", 'M')) {
	CONF_SAFE_SET_ARG_VAL(&g_conf.monmap, OPT_STR);
    } else if (CONF_ARG_EQ("show_conf", 'S')) {
      show_config = true;
    } else if (isdaemon && CONF_ARG_EQ("bind", 0)) {
      g_conf.public_addr.parse(args[++i]);
    } else if (CONF_ARG_EQ("nodaemon", 'D')) {
      g_conf.daemonize = false;
      *force_fg_logging = true;
    } else if (CONF_ARG_EQ("foreground", 'f')) {
      g_conf.daemonize = false;
      *force_fg_logging = false;
    } else if (isdaemon && (CONF_ARG_EQ("id", 'i') || CONF_ARG_EQ("name", 'n'))) {
      free(g_conf.id);
      CONF_SAFE_SET_ARG_VAL(&g_conf.id, OPT_STR);
    } else if (!isdaemon && (CONF_ARG_EQ("id", 'I') || CONF_ARG_EQ("name", 'n'))) {
      free(g_conf.id);
      CONF_SAFE_SET_ARG_VAL(&g_conf.id, OPT_STR);
    } else {
      nargs.push_back(args[i]);
    }
  }
  args.swap(nargs);
  nargs.clear();

  if (module_type) {
    g_conf.type = strdup(module_type);
    // is it "type.name"?
    const char *dot = strchr(g_conf.id, '.');
    if (dot) {
      int tlen = dot - g_conf.id;
      g_conf.type = (char *)malloc(tlen + 1);
      memcpy(g_conf.type, g_conf.id, tlen);
      g_conf.type[tlen] = 0;
      char *new_g_conf_id = strdup(dot + 1);
      free(g_conf.id);
      g_conf.id = new_g_conf_id;
    }

    int len = strlen(g_conf.type) + strlen(g_conf.id) + 2;
    g_conf.name = (char *)malloc(len);
    snprintf(g_conf.name, len, "%s.%s", g_conf.type, g_conf.id);
    g_conf.alt_name = (char *)malloc(len - 1);
    snprintf(g_conf.alt_name, len - 1, "%s%s", module_type, g_conf.id);
  }

  g_conf.entity_name = new EntityName;
  assert(g_conf.entity_name);

  g_conf.entity_name->from_type_id(g_conf.type, g_conf.id);

  if (g_conf.cf) {
    delete g_conf.cf;
    g_conf.cf = NULL;
  }

  // do post_process substitutions
  int len = num_config_options;
  for (int i = 0; i<len; i++) {
    config_option *opt = &config_optionsp[i];
    if (opt->type == OPT_STR && opt->val_ptr) {
      if (*(char**)opt->val_ptr) {
	*(char **)opt->val_ptr = conf_post_process_val(*(char **)opt->val_ptr);
      }
    }
  }

  if (!conf_specified)
    env_override(&g_conf.conf, "CEPH_CONF");

  // open new conf
  string fn = g_conf.conf;
  list<string> ls;
  get_str_list(fn, ls);
  bool read_conf = false;
  for (list<string>::iterator p = ls.begin(); p != ls.end(); p++) {
    g_conf.cf = new ConfFile(p->c_str());
    read_conf = parse_config_file(g_conf.cf, true);
    if (read_conf)
      break;
    delete g_conf.cf;
    g_conf.cf = NULL;
  }

  if (conf_specified && !read_conf) {
    cerr << "error reading config file(s) " << g_conf.conf << std::endl;
    exit(1);
  }

  if (show_config) {
    if (g_conf.cf)
      g_conf.cf->dump();
    exit(0);
  }
}

void parse_config_options(std::vector<const char*>& args)
{
  int opt_len = num_config_options;
  DEFINE_CONF_VARS(NULL);

  std::vector<const char*> nargs;
  FOR_EACH_ARG(args) {
    int optn;

    for (optn = 0; optn < opt_len; optn++) {
      if (CONF_ARG_EQ("lockdep", '\0')) {
	CONF_SAFE_SET_ARG_VAL(&g_lockdep, OPT_INT);
      } else if (CONF_ARG_EQ(config_optionsp[optn].name,
	    config_optionsp[optn].char_option)) {
        if (__isarg || val_pos || config_optionsp[optn].type == OPT_BOOL)
	    CONF_SAFE_SET_ARG_VAL(config_optionsp[optn].val_ptr, config_optionsp[optn].type);
        else
          continue;
      } else {
        continue;
      }
      break;
    }

    if (optn == opt_len)
        nargs.push_back(args[i]);
  }

  env_override(&g_conf.keyring, "CEPH_KEYRING");
  args = nargs;
}

static void generic_usage(bool is_server)
{
  cout << "   -c ceph.conf or --conf=ceph.conf\n";
  cout << "        get options from given conf file\n";
  cout << "   -D   run in foreground.\n";
  cout << "   -f   run in foreground. Show all log messages on stdout.\n";
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
