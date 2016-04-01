// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <errno.h>
#include <sys/utsname.h>
#include <boost/lexical_cast.hpp>

#include "include/util.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/strtol.h"

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif

int64_t unit_to_bytesize(string val, ostream *pss)
{
  if (val.empty()) {
    if (pss)
      *pss << "value is empty!";
    return -EINVAL;
  }

  char c = val[val.length()-1];
  int modifier = 0;
  if (!::isdigit(c)) {
    if (val.length() < 2) {
      if (pss)
        *pss << "invalid value: " << val;
      return -EINVAL;
    }
    val = val.substr(0,val.length()-1);
    switch (c) {
    case 'B':
      break;
    case 'k':
    case 'K':
      modifier = 10;
      break;
    case 'M':
      modifier = 20;
      break;
    case 'G':
      modifier = 30;
      break;
    case 'T':
      modifier = 40;
      break;
    case 'P':
      modifier = 50;
      break;
    case 'E':
      modifier = 60;
      break;
    default:
      if (pss)
        *pss << "unrecognized modifier '" << c << "'" << std::endl;
      return -EINVAL;
    }
  }

  if (val[0] == '+' || val[0] == '-') {
    if (pss)
      *pss << "expected numerical value, got: " << val;
    return -EINVAL;
  }

  string err;
  int64_t r = strict_strtoll(val.c_str(), 10, &err);
  if ((r == 0) && !err.empty()) {
    if (pss)
      *pss << err;
    return -1;
  }
  if (r < 0) {
    if (pss)
      *pss << "unable to parse positive integer '" << val << "'";
    return -1;
  }
  return (r * (1LL << modifier));
}

int get_fs_stats(ceph_data_stats_t &stats, const char *path)
{
  if (!path)
    return -EINVAL;

  struct statfs stbuf;
  int err = ::statfs(path, &stbuf);
  if (err < 0) {
    return -errno;
  }

  stats.byte_total = stbuf.f_blocks * stbuf.f_bsize;
  stats.byte_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize;
  stats.byte_avail = stbuf.f_bavail * stbuf.f_bsize;
  stats.avail_percent = (((float)stats.byte_avail/stats.byte_total)*100);
  return 0;
}

static char* value_sanitize(char *value)
{
  while (isspace(*value) || *value == '"')
    value++;

  char* end = value + strlen(value) - 1;
  while (end > value && (isspace(*end) || *end == '"'))
    end--;

  *(end + 1) = '\0';

  return value;
}

static bool value_set(char *buf, const char *prefix,
			    map<string, string> *pm, const char *key)
{
  if (strncmp(buf, prefix, strlen(prefix))) {
    return false;
  }

  (*pm)[key] = value_sanitize(buf + strlen(prefix));
  return true;
}

static void file_values_parse(const map<string, string>& kvm, FILE *fp, map<string, string> *m, CephContext *cct) {
  char buf[512];
  while (fgets(buf, sizeof(buf) - 1, fp) != NULL) {
    for (auto& kv : kvm) {
      if (value_set(buf, kv.second.c_str(), m, kv.first.c_str()))
        continue;
    }
  }
}

static bool lsb_release_parse(map<string, string> *m, CephContext *cct)
{
  static const map<string, string> kvm = {
      { "distro", "Distributor ID:" },
      { "distro_description", "Description:" },
      { "distro_codename", "Codename:", },
      { "distro_version", "Release:" }
  };

  FILE *fp = popen("lsb_release -idrc", "r");
  if (!fp) {
    int ret = -errno;
    lderr(cct) << "lsb_release_parse - failed to call lsb_release binary with error: " << cpp_strerror(ret) << dendl;
    return false;
  }

  file_values_parse(kvm, fp, m, cct);

  if (pclose(fp)) {
    int ret = -errno;
    lderr(cct) << "lsb_release_parse - pclose failed: " << cpp_strerror(ret) << dendl;
    return false;
  }

  return true;
}

static bool os_release_parse(map<string, string> *m, CephContext *cct)
{
  static const map<string, string> kvm = {
    { "distro", "ID=" },
    { "distro_description", "PRETTY_NAME=" },
    { "distro_version", "VERSION_ID=" }
  };

  FILE *fp = fopen("/etc/os-release", "r");
  if (!fp) {
    int ret = -errno;
    lderr(cct) << "os_release_parse - failed to open /etc/os-release: " << cpp_strerror(ret) << dendl;
    return false;
  }

  file_values_parse(kvm, fp, m, cct);

  fclose(fp);

  return true;
}

static void distro_detect(map<string, string> *m, CephContext *cct)
{
  if (!lsb_release_parse(m, cct) && !os_release_parse(m, cct)) {
    lderr(cct) << "distro_detect - lsb_release or /etc/os-release is required" << dendl;
  }

  for (const char* rk: {"distro", "distro_version"}) {
    if (m->find(rk) == m->end())
      lderr(cct) << "distro_detect - can't detect " << rk << dendl;
  }
}

void collect_sys_info(map<string, string> *m, CephContext *cct)
{
  // kernel info
  struct utsname u;
  int r = uname(&u);
  if (r >= 0) {
    (*m)["os"] = u.sysname;
    (*m)["kernel_version"] = u.release;
    (*m)["kernel_description"] = u.version;
    (*m)["hostname"] = u.nodename;
    (*m)["arch"] = u.machine;
  }

  // memory
  FILE *f = fopen("/proc/meminfo", "r");
  if (f) {
    char buf[100];
    while (!feof(f)) {
      char *line = fgets(buf, sizeof(buf), f);
      if (!line)
	break;
      char key[40];
      long long value;
      int r = sscanf(line, "%s %lld", key, &value);
      if (r == 2) {
	if (strcmp(key, "MemTotal:") == 0)
	  (*m)["mem_total_kb"] = boost::lexical_cast<string>(value);
	else if (strcmp(key, "SwapTotal:") == 0)
	  (*m)["mem_swap_kb"] = boost::lexical_cast<string>(value);
      }
    }
    fclose(f);
  }

  // processor
  f = fopen("/proc/cpuinfo", "r");
  if (f) {
    char buf[100];
    while (!feof(f)) {
      char *line = fgets(buf, sizeof(buf), f);
      if (!line)
	break;
      if (strncmp(line, "model name", 10) == 0) {
	char *c = strchr(buf, ':');
	c++;
	while (*c == ' ')
	  ++c;
	char *nl = c;
	while (*nl != '\n')
	  ++nl;
	*nl = '\0';
	(*m)["cpu"] = c;
	break;
      }
    }
    fclose(f);
  }

  // distro info
  distro_detect(m, cct);
}

void dump_services(Formatter* f, const map<string, list<int> >& services, const char* type)
{
  assert(f);

  f->open_object_section(type);
  for (map<string, list<int> >::const_iterator host = services.begin();
       host != services.end(); ++host) {
    f->open_array_section(host->first.c_str());
    const list<int>& hosted = host->second;
    for (list<int>::const_iterator s = hosted.begin();
	 s != hosted.end(); ++s) {
      f->dump_int(type, *s);
    }
    f->close_section();
  }
  f->close_section();
}
