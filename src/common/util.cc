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

#include <sys/utsname.h>
#include <boost/lexical_cast.hpp>

#include "include/compat.h"
#include "include/util.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/version.h"

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#if defined(__APPLE__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif
#endif

#include <string>

#include <stdio.h>

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

static bool os_release_parse(map<string, string> *m, CephContext *cct)
{
#if defined(__linux__)
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
#elif defined(__FreeBSD__)
  struct utsname u;
  int r = uname(&u);
  if (!r) {
     m->insert(std::make_pair("distro", u.sysname));
     m->insert(std::make_pair("distro_description", u.version));
     m->insert(std::make_pair("distro_version", u.release));
  }
#endif

  return true;
}

static void distro_detect(map<string, string> *m, CephContext *cct)
{
  if (!os_release_parse(m, cct)) {
    lderr(cct) << "distro_detect - /etc/os-release is required" << dendl;
  }

  for (const char* rk: {"distro", "distro_description"}) {
    if (m->find(rk) == m->end())
      lderr(cct) << "distro_detect - can't detect " << rk << dendl;
  }
}

void collect_sys_info(map<string, string> *m, CephContext *cct)
{
  // version
  (*m)["ceph_version"] = pretty_version_to_str();
  (*m)["ceph_version_short"] = ceph_version_to_str();
  (*m)["ceph_release"] = ceph_release_to_str();

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

  // but wait, am i in a container?
  bool in_container = false;

  if (const char *pod_name = getenv("POD_NAME")) {
    (*m)["pod_name"] = pod_name;
    in_container = true;
  }
  if (const char *container_name = getenv("CONTAINER_NAME")) {
    (*m)["container_name"] = container_name;
    in_container = true;
  }
  if (in_container) {
    if (const char *node_name = getenv("NODE_NAME")) {
      (*m)["container_hostname"] = u.nodename;
      (*m)["hostname"] = node_name;
    }
    if (const char *ns = getenv("POD_NAMESPACE")) {
      (*m)["pod_namespace"] = ns;
    }
  }

#ifdef __APPLE__
  // memory
  {
    uint64_t size;
    size_t len = sizeof(size);
    r = sysctlbyname("hw.memsize", &size, &len, NULL, 0);
    if (r == 0) {
      (*m)["mem_total_kb"] = std::to_string(size);
    }
  }
  {
    xsw_usage vmusage;
    size_t len = sizeof(vmusage);
    r = sysctlbyname("vm.swapusage", &vmusage, &len, NULL, 0);
    if (r == 0) {
      (*m)["mem_swap_kb"] = std::to_string(vmusage.xsu_total);
    }
  }
  // processor
  {
    char buf[100];
    size_t len = sizeof(buf);
    r = sysctlbyname("machdep.cpu.brand_string", buf, &len, NULL, 0);
    if (r == 0) {
      buf[len - 1] = '\0';
      (*m)["cpu"] = buf;
    }
  }
#else
  // memory
  FILE *f = fopen(PROCPREFIX "/proc/meminfo", "r");
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
  f = fopen(PROCPREFIX "/proc/cpuinfo", "r");
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
#endif
  // distro info
  distro_detect(m, cct);
}

void dump_services(Formatter* f, const map<string, list<int> >& services, const char* type)
{
  ceph_assert(f);

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

void dump_services(Formatter* f, const map<string, list<string> >& services, const char* type)
{
  ceph_assert(f);

  f->open_object_section(type);
  for (const auto& host : services) {
    f->open_array_section(host.first.c_str());
    const auto& hosted = host.second;
    for (const auto& s : hosted) {
      f->dump_string(type, s);
    }
    f->close_section();
  }
  f->close_section();
}

// If non-printable characters found then convert bufferlist to
// base64 encoded string indicating whether it did.
string cleanbin(bufferlist &bl, bool &base64)
{
  bufferlist::iterator it;
  for (it = bl.begin(); it != bl.end(); ++it) {
    if (iscntrl(*it))
      break;
  }
  if (it == bl.end()) {
    base64 = false;
    string result(bl.c_str(), bl.length());
    return result;
  }

  bufferlist b64;
  bl.encode_base64(b64);
  string encoded(b64.c_str(), b64.length());
  base64 = true;
  return encoded;
}

// If non-printable characters found then convert to "Base64:" followed by
// base64 encoding
string cleanbin(string &str)
{
  bool base64;
  bufferlist bl;
  bl.append(str);
  string result = cleanbin(bl, base64);
  if (base64)
    result = "Base64:" + result;
  return result;
}

std::string bytes2str(uint64_t count) {
  static char s[][2] = {"\0", "k", "M", "G", "T", "P", "E", "\0"};
  int i = 0;
  while (count >= 1024 && *s[i+1]) {
    count >>= 10;
    i++;
  }
  char str[128];
  snprintf(str, sizeof str, "%" PRIu64 "%sB", count, s[i]);
  return std::string(str);
}
