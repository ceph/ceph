// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include <unistd.h>
#include <string.h>
#include <stdarg.h>

#include <iostream>
#include <vector>
#include <list>

#include "config.h"
#include "str_list.h"


static void dashes_to_underscores(const char *input, char *output) {
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

static int va_ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret,
	std::ostream &oss, va_list ap) {
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

bool crimson::qos_simulation::ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...) {
  int r;
  va_list ap;
  va_start(ap, ret);
  r = va_ceph_argparse_witharg(args, i, ret, std::cerr, ap);
  va_end(ap);
  if (r < 0)
    _exit(1);
  return r != 0;
}

void crimson::qos_simulation::ceph_argparse_early_args(std::vector<const char*>& args, std::string *conf_file_list) {
  std::string val;

  std::vector<const char *> orig_args = args;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_witharg(args, i, &val, "--conf", "-c", (char*)NULL)) {
      *conf_file_list = val;
    }
    else {
      // ignore
      ++i;
    }
  }
  return;
}

static bool stobool(const std::string & v) {
    return !v.empty () &&
           (strcasecmp (v.c_str (), "true") == 0 ||
	   atoi (v.c_str ()) != 0);
}

int crimson::qos_simulation::parse_config_file(const std::string &fname, sim_config_t &g_conf) {
  ConfFile cf;
  std::deque<std::string> err;
  std::ostringstream warn;
  int ret = cf.parse_file(fname.c_str(), &err, &warn);
  if (ret) {
    // error
    return ret;
  }

  std::string val;
  if (!cf.read("global", "server_groups", val))
    g_conf.server_groups = std::stoul(val);
  if (!cf.read("global", "client_groups", val))
    g_conf.client_groups = std::stoul(val);
  if (!cf.read("global", "server_random_selection", val))
    g_conf.server_random_selection = stobool(val);
  if (!cf.read("global", "server_soft_limit", val))
    g_conf.server_soft_limit = stobool(val);

  for (uint i = 0; i < g_conf.server_groups; i++) {
    srv_group_t st;
    std::string section = "server." + std::to_string(i);
    if (!cf.read(section, "server_count", val))
      st.server_count = std::stoul(val);
    if (!cf.read(section, "server_iops", val))
      st.server_iops = std::stoul(val);
    if (!cf.read(section, "server_threads", val))
      st.server_threads = std::stoul(val);
    g_conf.srv_group.push_back(st);
  }

  for (uint i = 0; i < g_conf.client_groups; i++) {
    cli_group_t ct;
    std::string section = "client." + std::to_string(i);
    if (!cf.read(section, "client_count", val))
      ct.client_count = std::stoul(val);
    if (!cf.read(section, "client_wait", val))
      ct.client_wait = std::chrono::seconds(std::stoul(val));
    if (!cf.read(section, "client_total_ops", val))
      ct.client_total_ops = std::stoul(val);
    if (!cf.read(section, "client_server_select_range", val))
      ct.client_server_select_range = std::stoul(val);
    if (!cf.read(section, "client_iops_goal", val))
      ct.client_iops_goal = std::stoul(val);
    if (!cf.read(section, "client_outstanding_ops", val))
      ct.client_outstanding_ops = std::stoul(val);
    if (!cf.read(section, "client_reservation", val))
      ct.client_reservation = std::stod(val);
    if (!cf.read(section, "client_limit", val))
      ct.client_limit = std::stod(val);
    if (!cf.read(section, "client_weight", val))
      ct.client_weight = std::stod(val);
    g_conf.cli_group.push_back(ct);
  }

  return 0;
}
