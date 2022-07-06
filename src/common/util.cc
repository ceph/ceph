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

#ifndef _WIN32
#include <sys/utsname.h>
#endif

#include <fstream>
#include <boost/algorithm/string.hpp>

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

using std::list;
using std::map;
using std::string;

using ceph::bufferlist;
using ceph::Formatter;

#ifndef _WIN32
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
#else
int get_fs_stats(ceph_data_stats_t &stats, const char *path)
{
  ULARGE_INTEGER avail_bytes, total_bytes, total_free_bytes;

  if (!GetDiskFreeSpaceExA(path, &avail_bytes,
                           &total_bytes, &total_free_bytes)) {
    return -EINVAL;
  }

  stats.byte_total = total_bytes.QuadPart;
  stats.byte_used = total_bytes.QuadPart - total_free_bytes.QuadPart;
  // may not be equal to total_free_bytes due to quotas
  stats.byte_avail = avail_bytes.QuadPart;
  stats.avail_percent = ((float)stats.byte_avail / stats.byte_total) * 100;
  return 0;
}
#endif

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

int get_cgroup_memory_limit(uint64_t *limit)
{
#if defined(__linux__)
  // /sys/fs/cgroup/memory/memory.limit_in_bytes

  // the magic value 9223372036854771712 or 0x7ffffffffffff000
  // appears to mean no limit.
  FILE *f = fopen(PROCPREFIX "/sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
  if (!f) {
    return -errno;
  }
  char buf[100];
  int ret = 0;
  long long value;
  char *line = fgets(buf, sizeof(buf), f);
  if (!line) {
    ret = -EINVAL;
    goto out;
  }
  if (sscanf(line, "%lld", &value) != 1) {
    ret = -EINVAL;
  }
  if (value == 0x7ffffffffffff000) {
    *limit = 0;  // no limit
  } else {
    *limit = value;
  }
out:
  fclose(f);
  return ret;
#else
  return 0;
#endif
}

#ifdef _WIN32
int get_windows_version(POSVERSIONINFOEXW ver) {
  using  get_version_func_t = DWORD (WINAPI *)(OSVERSIONINFOEXW*);

  // We'll load the library directly to avoid depending on the NTDDK.
  HMODULE ntdll_lib = LoadLibraryW(L"Ntdll.dll");
  if (!ntdll_lib) {
    return -EINVAL;
  }

  // The standard "GetVersion" returned values depend on the application
  // manifest. We'll get the "real" version by using the Rtl* version.
  auto get_version_func = (
    get_version_func_t)GetProcAddress(ntdll_lib, "RtlGetVersion");
  int ret = 0;
  if (!get_version_func || get_version_func(ver)) {
    // RtlGetVersion returns non-zero values in case of errors.
    ret = -EINVAL;
  }

  FreeLibrary(ntdll_lib);
  return ret;
}
#endif

void collect_sys_info(map<string, string> *m, CephContext *cct)
{
  // version
  (*m)["ceph_version"] = pretty_version_to_str();
  (*m)["ceph_version_short"] = ceph_version_to_str();
  (*m)["ceph_release"] = ceph_release_to_str();

  #ifndef _WIN32
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
  #else
  OSVERSIONINFOEXW ver = {0};
  ver.dwOSVersionInfoSize = sizeof(ver);
  get_windows_version(&ver);

  char version_str[64];
  snprintf(version_str, 64, "%lu.%lu (%lu)",
           ver.dwMajorVersion, ver.dwMinorVersion, ver.dwBuildNumber);

  char hostname[64];
  DWORD hostname_sz = sizeof(hostname);
  GetComputerNameA(hostname, &hostname_sz);

  SYSTEM_INFO sys_info;
  const char* arch_str;
  GetNativeSystemInfo(&sys_info);

  switch (sys_info.wProcessorArchitecture) {
    case PROCESSOR_ARCHITECTURE_AMD64:
      arch_str = "x86_64";
      break;
    case PROCESSOR_ARCHITECTURE_INTEL:
      arch_str = "x86";
      break;
    case PROCESSOR_ARCHITECTURE_ARM:
      arch_str = "arm";
      break;
    default:
      arch_str = "unknown";
      break;
  }

  (*m)["os"] = "Windows";
  (*m)["kernel_version"] = version_str;
  (*m)["kernel_description"] = version_str;
  (*m)["hostname"] = hostname;
  (*m)["arch"] = arch_str;
  #endif

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
  if (const char *container_image = getenv("CONTAINER_IMAGE")) {
    (*m)["container_image"] = container_image;
    in_container = true;
  }
  if (in_container) {
    if (const char *node_name = getenv("NODE_NAME")) {
      (*m)["container_hostname"] = (*m)["hostname"];
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
#elif !defined(_WIN32)
  // memory
  if (std::ifstream f{PROCPREFIX "/proc/meminfo"}; !f.fail()) {
    for (std::string line; std::getline(f, line); ) {
      std::vector<string> parts;
      boost::split(parts, line, boost::is_any_of(":\t "), boost::token_compress_on);
      if (parts.size() != 3) {
	continue;
      }
      if (parts[0] == "MemTotal") {
	(*m)["mem_total_kb"] = parts[1];
      } else if (parts[0] == "SwapTotal") {
	(*m)["mem_swap_kb"] = parts[1];
      }
    }
  }
  uint64_t cgroup_limit;
  if (get_cgroup_memory_limit(&cgroup_limit) == 0 &&
      cgroup_limit > 0) {
    (*m)["mem_cgroup_limit"] = std::to_string(cgroup_limit);
  }

  // processor
  if (std::ifstream f{PROCPREFIX "/proc/cpuinfo"}; !f.fail()) {
    for (std::string line; std::getline(f, line); ) {
      std::vector<string> parts;
      boost::split(parts, line, boost::is_any_of(":"));
      if (parts.size() != 2) {
	continue;
      }
      boost::trim(parts[0]);
      boost::trim(parts[1]);
      if (parts[0] == "model name") {
	(*m)["cpu"] = parts[1];
	break;
      }
    }
  }
#endif
  // distro info
  distro_detect(m, cct);
}

void dump_services(Formatter* f, const map<string, list<int> >& services, const char* type)
{
  ceph_assert(f);

  f->open_object_section(type);
  for (auto host = services.begin();
       host != services.end(); ++host) {
    f->open_array_section(host->first.c_str());
    const list<int>& hosted = host->second;
    for (auto s = hosted.cbegin();
	 s != hosted.cend(); ++s) {
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
string cleanbin(bufferlist &bl, bool &base64, bool show)
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
  if (show)
    encoded = "Base64:" + encoded;
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
  string result = cleanbin(bl, base64, true);
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
